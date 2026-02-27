# app/pipeline/orchestrator.py
from __future__ import annotations

import asyncio
import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.services.gemini_client import GeminiClient
from app.services.openai_client import OpenAIClient
from app.services.rag.local_rag import LocalRAG, split_metadata_and_body
from app.services.rendering.docx_renderer import render_docx
from app.services.export.pdf_exporter import docx_to_pdf_libreoffice

from app.pipeline.debug_dump import DebugDumper
from app.pipeline.json_repair import parse_json_with_repair
from app.pipeline.act_engine import build_act_context, dedupe_personas

from app.pipeline.prompts import (
    RADICACION_PROMPT,
    DOCS_PROMPT,
    CEDULA_PROMPT,
    DATABINDER_SYSTEM,
    DATABINDER_USER,
)

from app.pipeline.boilerplate import (
    EP_CARATULA_TEMPLATE,
    EP_INSERTOS_TEMPLATE,
    EP_OTORGAMIENTO_TEMPLATE,
    EP_DERECHOS_TEMPLATE,
    EP_UIAF_TEMPLATE,
    EP_FIRMAS_TEMPLATE,
)


# ----------------------------
# Utils
# ----------------------------

def _pick_radicacion_file(paths: List[str]) -> Optional[str]:
    if not paths:
        return None
    for p in paths:
        name = Path(p).name.lower()
        if any(k in name for k in ["radic", "turno", "hoja", "radicación", "radicacion"]):
            return p
    for p in paths:
        if Path(p).suffix.lower() == ".pdf":
            return p
    return None


def _guess_mime(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    return "application/octet-stream"


def _safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _extract_radicado_from_radicacion_json(radicacion: Dict[str, Any]) -> str:
    r = _safe_get(radicacion, ["negocio_actual", "numero_radicado"])
    if r:
        return str(r)
    r2 = _safe_get(radicacion, ["radicacion", "numero"])
    if r2:
        return str(r2)
    return str(uuid.uuid4().int)[:8]


def _is_garbage(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip().upper()
    if s in {"", "NULL", "UNDEFINED"}:
        return True
    if "NO_DETECTADO" in s or "NO_APLICA" in s or "PENDIENTE" in s:
        return True
    return False


def _is_current_linderos(v: str) -> bool:
    """True si los linderos están en formato actual POR EL NORTE/SUR/ORIENTE/OCCIDENTE."""
    s = v.strip().upper()
    return s.startswith("POR EL") or s.startswith("AL NORTE") or s.startswith("NORTE:")


def _merge_smart(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    for k, v in (source or {}).items():
        if _is_garbage(v):
            continue
        old = target.get(k)
        if old and not _is_garbage(old):
            if k == "linderos":
                # Preferir formato actual "POR EL NORTE/SUR..." sobre linderos históricos
                if _is_current_linderos(str(old)) or not _is_current_linderos(str(v)):
                    continue  # conservar el actual si ya es correcto, o no pisar con histórico
            elif k == "cabida_area":
                # Preferir valores con "hectáreas" sobre "Terreno: X Ha. / Construida: Y"
                if "HECTÁREA" in str(old).upper() or "HECTAREA" in str(old).upper():
                    continue  # el valor existente ya está en formato correcto
            elif k == "direccion":
                # Preferir descripción con "CORREGIMIENTO" (más precisa) sobre "REGION"
                has_corr_old = "CORREGIMIENTO" in str(old).upper()
                has_corr_new = "CORREGIMIENTO" in str(v).upper()
                if has_corr_old and not has_corr_new:
                    continue  # conservar la más precisa
                if not has_corr_new and len(str(v)) <= len(str(old)):
                    continue
            else:
                # Por defecto: conservar el más largo
                if len(str(v)) < len(str(old)):
                    continue
        target[k] = v


def _is_actual_party(p: Dict[str, Any]) -> bool:
    """
    True solo si la persona es parte del negocio actual.
    La fuente de verdad es la hoja de radicacion: solo esas personas comparecen
    ante el notario. Los soportes enriquecen datos de esas personas via dedup,
    pero no agregan partes nuevas.
    """
    return p.get("_source") == "radicacion"


def _normalize_person_roles(p: Dict[str, Any], source: str = "soporte") -> Dict[str, Any]:
    # Preferir rol_en_hoja (radicación) sobre rol genérico
    rol_en_hoja = p.get("rol_en_hoja") or ""
    rol_raw = rol_en_hoja or p.get("rol") or "INTERVINIENTE"
    rol = str(rol_raw).upper()

    # Heurística DE/A SOLO cuando viene de radicación y es texto corto tipo "DE (Vendedor)"
    if rol_en_hoja and len(rol_en_hoja) < 30:
        if re.search(r"\bDE\b", rol):
            rol = "VENDEDOR/OTORGANTE"
        elif re.search(r"^\s*A\b", rol) or re.search(r"\bA\s*\(", rol):
            rol = "COMPRADOR/BENEFICIARIO"
        elif "RL" in rol:
            rol = "REPRESENTANTE LEGAL"

    return {
        "nombre": (p.get("nombre") or p.get("nombre_completo") or "").upper().strip(),
        "identificacion": (p.get("identificacion") or p.get("cedula") or "").strip(),
        "rol_detectado": rol,
        "datos_contacto": p.get("datos") or {},
        "estado_civil": (p.get("estado_civil") or _safe_get(p, ["datos", "estado_civil"]) or "").strip(),
        "direccion": (p.get("direccion") or _safe_get(p, ["datos", "domicilio"]) or "").strip(),
        "telefono": (p.get("telefono") or _safe_get(p, ["datos", "telefono"]) or "").strip(),
        "email": (p.get("email") or _safe_get(p, ["datos", "email"]) or "").strip(),
        "ocupacion": (p.get("ocupacion") or _safe_get(p, ["datos", "ocupacion"]) or "").strip(),
        "lugar_expedicion": (p.get("lugar_expedicion") or "").strip(),
        "representa_a": (p.get("representa_a") or "").strip(),
        "cargo": (p.get("cargo") or "").strip(),
        "_source": source,
    }


def _normalize_id(x: str) -> str:
    if not x:
        return ""
    return re.sub(r"[^0-9]", "", str(x))


def _normalize_name(x: str) -> str:
    if not x:
        return ""
    s = str(x).upper()
    s = re.sub(r"[^A-ZÁÉÍÓÚÜÑ ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _format_cc(cc: str) -> str:
    """Formatea número de cédula/NIT con puntos cada 3 dígitos desde la derecha. Ej: 63501152 → 63.501.152"""
    digits = re.sub(r"[^0-9]", "", str(cc))
    if len(digits) >= 5:
        parts = []
        d = digits
        while d:
            parts.append(d[-3:])
            d = d[:-3]
        return ".".join(reversed(parts))
    return cc


def _build_camaras_text(empresa_rl_map: Dict[str, Any]) -> str:
    """Genera líneas de cámara de comercio para cada empresa jurídica presente."""
    lines = []
    for emp_nombre in (empresa_rl_map or {}):
        # Reconstituir nombre con mayúsculas título si es todo caps
        lines.append(f"Copia de cámara de comercio de {emp_nombre}.")
    return "\n".join(lines) if lines else ""


def _merge_cedula_ocr_into_people(personas: List[Dict[str, Any]], cedulas_json_list: List[Dict[str, Any]]) -> None:
    if not personas or not cedulas_json_list:
        return

    idx_by_id: Dict[str, Dict[str, Any]] = {}
    for p in personas:
        pid = _normalize_id(p.get("identificacion") or "")
        if pid:
            idx_by_id[pid] = p

    for c in cedulas_json_list:
        if not isinstance(c, dict):
            continue
        cid = _normalize_id(c.get("cedula") or c.get("identificacion") or "")
        cname = _normalize_name(c.get("nombre") or "")
        lugar_exp = (c.get("lugar_expedicion") or "").strip()
        ocupacion = (c.get("ocupacion") or "").strip()

        target = None
        if cid and cid in idx_by_id:
            target = idx_by_id[cid]
        else:
            for p in personas:
                pn = _normalize_name(p.get("nombre") or "")
                if cname and pn and (cname in pn or pn in cname):
                    target = p
                    break

        if target is None:
            continue

        if lugar_exp and not target.get("lugar_expedicion"):
            target["lugar_expedicion"] = lugar_exp
        if ocupacion and not target.get("ocupacion"):
            target["ocupacion"] = ocupacion
        if cid and not _normalize_id(target.get("identificacion") or ""):
            target["identificacion"] = cid


def _build_empresa_rl_map(
    radicacion_json: Dict[str, Any],
    personas_deduped: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Construye mapeo empresa_nombre → persona RL a partir del orden de la radicación."""
    mapping: Dict[str, Dict[str, Any]] = {}
    rad_personas = (radicacion_json or {}).get("personas_detalle") or []

    last_empresa_nombre = None
    for p in rad_personas:
        pid = (p.get("identificacion") or "").upper()
        rol = (p.get("rol_en_hoja") or "").upper()
        nombre = (p.get("nombre") or "").upper().strip()

        if "NI " in pid or pid.startswith("NIT"):
            last_empresa_nombre = nombre
        elif "RL" in rol and last_empresa_nombre:
            # Buscar la persona en la lista deduplicada (por ID o nombre)
            cc_norm = _normalize_id(pid)
            found = None
            for fp in personas_deduped:
                if cc_norm and _normalize_id(fp.get("identificacion") or "") == cc_norm:
                    found = fp
                    break
                if _normalize_name(fp.get("nombre") or "") == _normalize_name(nombre):
                    found = fp
                    break
            if found:
                mapping[last_empresa_nombre] = found
            last_empresa_nombre = None

    return mapping


def _build_universal_context(
    radicacion_json: Dict[str, Any],
    soportes_json_list: List[Dict[str, Any]],
    cedulas_json_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    contexto: Dict[str, Any] = {
        "RADICACION": "PENDIENTE",
        "INMUEBLE": {},
        "NEGOCIO": {},
        "PERSONAS": [],
        "DATOS_EXTRA": {},
        "HISTORIA": {},
        "FUENTES": {
            "radicacion": radicacion_json,
            "soportes": soportes_json_list,
            "cedulas": cedulas_json_list,
        }
    }

    if radicacion_json:
        contexto["RADICACION"] = _extract_radicado_from_radicacion_json(radicacion_json)
        _merge_smart(contexto["INMUEBLE"], radicacion_json.get("datos_inmueble") or {})
        _merge_smart(contexto["NEGOCIO"], radicacion_json.get("negocio_actual") or {})

        persons = radicacion_json.get("personas_detalle") or []
        if isinstance(persons, list):
            for p in persons:
                if isinstance(p, dict):
                    contexto["PERSONAS"].append(_normalize_person_roles(p, source="radicacion"))

        extra = radicacion_json.get("hallazgos_variables") or {}
        if isinstance(extra, dict):
            _merge_smart(contexto["DATOS_EXTRA"], extra)

        rad = radicacion_json.get("radicacion") or {}
        if isinstance(rad, dict):
            contexto["DATOS_EXTRA"]["NOTARIA_NOMBRE"] = rad.get("notaria") or contexto["DATOS_EXTRA"].get("NOTARIA_NOMBRE")
            contexto["DATOS_EXTRA"]["CIUDAD"] = rad.get("ciudad") or contexto["DATOS_EXTRA"].get("CIUDAD")
            if rad.get("notario_encargado"):
                contexto["DATOS_EXTRA"]["NOTARIA_ENCARGADO"] = rad["notario_encargado"]

    for s in soportes_json_list or []:
        if not isinstance(s, dict):
            continue

        inm = s.get("datos_inmueble") or {}
        if isinstance(inm, dict):
            _merge_smart(contexto["INMUEBLE"], inm)

        hist = s.get("historia_y_antecedentes") or {}
        if isinstance(hist, dict):
            _merge_smart(contexto["HISTORIA"], hist)

        persons = s.get("personas_detalle") or s.get("personas") or []
        if isinstance(persons, list):
            for p in persons:
                if isinstance(p, dict):
                    contexto["PERSONAS"].append(_normalize_person_roles(p, source="soporte"))

        extra = s.get("hallazgos_variables") or {}
        if isinstance(extra, dict):
            _merge_smart(contexto["DATOS_EXTRA"], extra)

        # Extraer documento_ep_info del antecedente (EP que se referencia/cancela)
        ep_info = s.get("documento_ep_info") or {}
        if isinstance(ep_info, dict) and ep_info.get("numero_ep") and not _is_garbage(ep_info["numero_ep"]):
            if not contexto["INMUEBLE"].get("ep_antecedente_pacto"):
                contexto["INMUEBLE"]["ep_antecedente_pacto"] = ep_info

        # Extraer nombre_nuevo_predio si está en hallazgos_variables
        nombre_nuevo = (extra or {}).get("nombre_nuevo_predio") or ""
        if nombre_nuevo and not _is_garbage(nombre_nuevo):
            if not contexto["INMUEBLE"].get("nombre_nuevo"):
                contexto["INMUEBLE"]["nombre_nuevo"] = nombre_nuevo

    _merge_cedula_ocr_into_people(contexto["PERSONAS"], cedulas_json_list)

    # Restaurar campos ID canónicos de radicación (evitar que OCR erróneo de soportes los sobreescriba)
    rad_inmueble = (radicacion_json or {}).get("datos_inmueble") or {}
    for field in ("matricula", "predial_nacional"):
        val = rad_inmueble.get(field)
        if val and not _is_garbage(val):
            contexto["INMUEBLE"][field] = val

    # Neutralizar fecha_otorgamiento proveniente de soportes: pertenece al antecedente, no a esta EP
    if "fecha_otorgamiento" in contexto["DATOS_EXTRA"]:
        contexto["DATOS_EXTRA"]["fecha_escritura_antecedente"] = contexto["DATOS_EXTRA"].pop("fecha_otorgamiento")

    # PERSONAS_ACTIVOS: solo partes del negocio actual (sin colindantes ni históricos)
    contexto["PERSONAS_ACTIVOS"] = [p for p in contexto["PERSONAS"] if _is_actual_party(p)]

    # EMPRESA_RL_MAP: mapeo empresa → persona RL
    contexto["EMPRESA_RL_MAP"] = _build_empresa_rl_map(
        radicacion_json, contexto["PERSONAS"]
    )

    return contexto


def _build_comparecientes_text(personas_activos: List[Dict[str, Any]]) -> str:
    """Genera el bloque COMPARECEN solo con partes del negocio actual."""
    lines = []
    for p in personas_activos or []:
        nom = (p.get("nombre") or "").strip()
        cc = (p.get("identificacion") or "").strip()
        rol = (p.get("rol_detectado") or "INTERVINIENTE").strip()
        if not nom:
            continue
        cc_limpia = cc if cc and "NO_DETECTADO" not in cc.upper() else ""
        if cc_limpia:
            lines.append(f"- {nom}, identificado(a) con {cc_limpia}, en calidad de {rol}.")
        else:
            lines.append(f"- {nom}, en calidad de {rol}.")
    return "\n".join(lines) if lines else "[[PENDIENTE: COMPARECIENTES]]"


def _build_ui_af_blocks(personas_activos: List[Dict[str, Any]]) -> str:
    """Genera bloques UIAF solo para personas naturales partes del negocio actual."""
    out = []
    # Solo personas naturales (sin NIT) que sean partes activas
    naturales = [
        p for p in (personas_activos or [])
        if not re.search(r"\bNIT\b|\bNI\b", (p.get("identificacion") or "").upper())
    ]
    for p in naturales:
        cedula_raw = (p.get("identificacion") or "").strip()
        # extraer solo dígitos para formatear
        cedula_fmt = _format_cc(re.sub(r"[^0-9]", "", cedula_raw)) if re.sub(r"[^0-9]", "", cedula_raw) else "[[PENDIENTE: CEDULA]]"
        telefono_raw = (p.get("telefono") or "").strip()
        # separar fijo/celular: si hay dos números, usar segundo como celular
        tel_parts = re.split(r"[/,;]", telefono_raw)
        tel_fijo = tel_parts[0].strip() if len(tel_parts) > 1 else ""
        tel_celular = tel_parts[-1].strip() if telefono_raw else "[[PENDIENTE: CELULAR]]"
        datos = p.get("datos_contacto") or {}
        ciudad = (datos.get("domicilio") or p.get("direccion") or "[[PENDIENTE: CIUDAD]]").split(",")[0].strip()
        out.append(
            EP_UIAF_TEMPLATE
            .replace("[[NOMBRE]]", (p.get("nombre") or "[[PENDIENTE: NOMBRE]]"))
            .replace("[[CEDULA]]", cedula_fmt)
            .replace("[[TELEFONO_FIJO]]", tel_fijo or "")
            .replace("[[CELULAR]]", tel_celular)
            .replace("[[DIRECCION]]", (p.get("direccion") or "[[PENDIENTE: DIRECCION]]"))
            .replace("[[CIUDAD]]", ciudad)
            .replace("[[EMAIL]]", (p.get("email") or "[[PENDIENTE: EMAIL]]"))
            .replace("[[OCUPACION]]", (p.get("ocupacion") or "[[PENDIENTE: OCUPACION]]"))
            .replace("[[ACTIVIDAD_ECONOMICA]]", (p.get("ocupacion") or "[[PENDIENTE: ACTIVIDAD_ECONOMICA]]"))
            .replace("[[ESTADO_CIVIL]]", (p.get("estado_civil") or "[[PENDIENTE: ESTADO_CIVIL]]"))
        )
    return "\n\n".join(out).strip()


def _build_firmas_block(personas_activos: List[Dict[str, Any]], empresa_rl_map: Dict[str, Any]) -> str:
    """Genera bloque de firmas: personas naturales con su cargo/empresa si aplica."""
    lines = []
    for p in personas_activos or []:
        nom = p.get("nombre") or "[[PENDIENTE: NOMBRE]]"
        cc_raw = re.sub(r"[^0-9]", "", p.get("identificacion") or "")
        cc_fmt = _format_cc(cc_raw) if cc_raw else "[[PENDIENTE: CEDULA]]"
        # Saltar empresas (NIT) — sus RLs ya aparecen como personas naturales
        raw_id = (p.get("identificacion") or "").upper()
        if re.search(r"\bNIT\b|\bNI\b", raw_id):
            continue
        # ¿Esta persona es RL de alguna empresa?
        empresa_repr = None
        for emp_nombre, rl in (empresa_rl_map or {}).items():
            if _normalize_name(rl.get("nombre") or "") == _normalize_name(nom):
                empresa_repr = emp_nombre
                break
        if empresa_repr:
            lines.append(
                f"(Firma) {nom}\n"
                f"C.C. No. {cc_fmt}\n"
                f"Obrando en calidad de representante legal de la sociedad {empresa_repr}.\n"
                f"Huella: _________"
            )
        else:
            # Persona natural sin rol de RL: solo nombre y CC (sin descripción de rol)
            lines.append(f"(Firma) {nom}\nC.C. No. {cc_fmt}\nHuella: _________")
    return "\n\n".join(lines) if lines else "[[PENDIENTE: BLOQUE_FIRMAS]]"


def _build_resumen_actos(
    contexto: Dict[str, Any],
) -> str:
    """Construye el resumen por acto para la carátula (dinámico)."""
    from app.pipeline.act_engine import infer_roles_por_acto  # import local para evitar circular

    actos_list = _safe_get(contexto, ["NEGOCIO", "actos_a_firmar"], default=[])
    if not isinstance(actos_list, list):
        actos_list = []

    personas_activos = contexto.get("PERSONAS_ACTIVOS") or contexto.get("PERSONAS") or []
    empresa_rl_map = contexto.get("EMPRESA_RL_MAP") or {}

    lines = []
    for idx, acto in enumerate(actos_list):
        nombre = (acto.get("nombre") if isinstance(acto, dict) else str(acto)).upper()
        cuantia = acto.get("cuantia", 0) if isinstance(acto, dict) else 0

        lines.append(f"ACTO {idx + 1}: {nombre}")

        roles = infer_roles_por_acto(nombre, personas_activos, None)
        vendedores = roles.get("VENDEDORES", []) or roles.get("SOLICITANTES", [])
        compradores = roles.get("COMPRADORES", [])
        deudores = roles.get("DEUDORES", [])
        acreedores = roles.get("ACREEDORES", [])

        def _fmt_persona(p: Dict) -> str:
            n = (p.get("nombre") or "").strip()
            cc = (p.get("identificacion") or "").strip()
            if cc and "NO_DETECTADO" not in cc.upper():
                return f"{n} - {cc}"
            return n

        if vendedores:
            partes = []
            for v in vendedores:
                s = _fmt_persona(v)
                rl = empresa_rl_map.get(_normalize_name(v.get("nombre") or ""))
                if rl:
                    s += f" (RL: {rl.get('nombre','')} - {rl.get('identificacion','')})"
                partes.append(s)
            lines.append(f"OTORGANTE(S): {', '.join(partes)}")

        if compradores:
            for c in compradores:
                s = _fmt_persona(c)
                rl = empresa_rl_map.get(_normalize_name(c.get("nombre") or ""))
                lines.append(f"COMPRADOR(A): {s}")
                if rl:
                    lines.append(f"REPRESENTANTE LEGAL: {_fmt_persona(rl)}")

        if cuantia:
            valor_fmt = f"$ {cuantia:,.0f}".replace(",", ".")
            lines.append(f"VALOR: {valor_fmt}")

        if deudores:
            for d in deudores:
                lines.append(f"DEUDOR: {_fmt_persona(d)}")
        if acreedores:
            for a in acreedores:
                lines.append(f"ACREEDOR: {_fmt_persona(a)}")

        lines.append("")  # línea en blanco entre actos

    return "\n".join(lines).strip()


def _prepare_ep_sections(contexto: Dict[str, Any], actos_docs: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    misiones: List[Dict[str, Any]] = []

    personas_activos = contexto.get("PERSONAS_ACTIVOS") or []
    empresa_rl_map = contexto.get("EMPRESA_RL_MAP") or {}
    inmueble = contexto.get("INMUEBLE") or {}
    datos_extra = contexto.get("DATOS_EXTRA") or {}

    radicado = str(contexto.get("RADICACION") or "PENDIENTE")
    ciudad = datos_extra.get("CIUDAD") or inmueble.get("ciudad_registro") or "[[PENDIENTE: CIUDAD]]"
    notaria = datos_extra.get("NOTARIA_NOMBRE") or "[[PENDIENTE: NOTARIA_NOMBRE]]"
    matricula = inmueble.get("matricula") or "[[PENDIENTE: MATRICULA_INMOBILIARIA]]"

    # Resumen de actos para carátula (construido en Python, dinámico)
    resumen_actos = _build_resumen_actos(contexto)

    # CARÁTULA
    misiones.append({
        "orden": 1,
        "descripcion": "EP_CARATULA",
        "plantilla_con_huecos": EP_CARATULA_TEMPLATE,
        "contexto_datos": {
            **contexto,
            "NUMERO_RADICADO": radicado,
            "NOTARIA_NOMBRE": notaria,
            "CIUDAD": ciudad,
            "MATRICULA_INMOBILIARIA": matricula,
            "RESUMEN_ACTOS": resumen_actos,
            "DESCRIPCION_INMUEBLE": inmueble.get("direccion") or "[[PENDIENTE: DESCRIPCION_INMUEBLE]]",
        },
        "instrucciones": (
            "Completa la caratula transcribiendo RESUMEN_ACTOS exactamente como viene. "
            "Sin markdown. No inventes datos. "
            "CRÍTICO: El campo FECHA debe quedar EXACTAMENTE como [[PENDIENTE: FECHA_OTORGAMIENTO]] "
            "— NO lo llenes aunque encuentres una fecha en el contexto. "
            "La fecha del antecedente (fecha_escritura_antecedente) NO es la fecha de esta escritura."
        ),
    })

    # ACTOS — respeta el orden de radicacion
    actos_list = _safe_get(contexto, ["NEGOCIO", "actos_a_firmar"], default=[])
    if isinstance(actos_list, str):
        try:
            actos_list = json.loads(actos_list)
        except Exception:
            actos_list = []
    if not isinstance(actos_list, list):
        actos_list = [actos_list]

    ordinales = ["PRIMER", "SEGUNDO", "TERCER", "CUARTO", "QUINTO", "SEXTO"]

    for idx, acto in enumerate(actos_list):
        if isinstance(acto, dict):
            nombre_acto = str(acto.get("nombre") or "ACTO")
        else:
            nombre_acto = str(acto)

        doc = actos_docs[idx] if idx < len(actos_docs) else {"contenido_legal": "TEXTO NO ENCONTRADO"}
        texto_crudo = doc.get("contenido_legal") or "TEXTO NO ENCONTRADO"

        # CONTEXTO POR ACTO (motor de roles por acto)
        ctx_acto = build_act_context(contexto, acto, idx)

        # Propagar PERSONAS_ACTIVOS y EMPRESA_RL_MAP al contexto de acto
        ctx_acto["PERSONAS_ACTIVOS"] = personas_activos
        ctx_acto["EMPRESA_RL_MAP"] = empresa_rl_map
        ctx_acto["ORDINAL_ACTO"] = ordinales[idx] if idx < len(ordinales) else "SIGUIENTE"
        ctx_acto["NOMBRE_ACTO_ACTUAL"] = nombre_acto
        ctx_acto["CIUDAD"] = ciudad
        ctx_acto["NOTARIA_NOMBRE"] = notaria

        from app.pipeline.act_engine import _acto_kind as _ak
        instruccion_acto = (
            f"Encabeza con: {ctx_acto['ORDINAL_ACTO']} ACTO: {nombre_acto.upper()}. "
            "Sin markdown. No inventes. "
            "Usa EMPRESA_RL_MAP para completar representantes legales de empresas. "
            "La fecha de comparecencia de ESTA escritura es [[PENDIENTE: FECHA_OTORGAMIENTO]] — "
            "NO uses fechas de escrituras referenciadas en el texto (ej. la fecha del antecedente). "
        )
        if _ak(nombre_acto) == "CANCELACION":
            instruccion_acto += (
                "Para CANCELACION PACTO RETROVENTA: en la cláusula PRIMERO, referencia "
                "el campo INMUEBLE.ep_antecedente_pacto (numero_ep, fecha, notaria) como la escritura "
                "que se cancela — NO uses INMUEBLE.tradicion para obtener el número de EP. "
            )
        elif _ak(nombre_acto) == "CAMBIO_NOMBRE":
            instruccion_acto += (
                "IMPORTANTE: Este acto tiene UN SOLO compareciente: el nuevo propietario "
                "(COMPRADOR) o su representante legal. NO incluir al vendedor/otorgante anterior. "
                "Si el comprador es empresa, usar su RL del EMPRESA_RL_MAP. "
            )
            if inmueble.get("nombre_nuevo") and not _is_garbage(inmueble["nombre_nuevo"]):
                ctx_acto["NUEVO_NOMBRE_PREDIO"] = inmueble["nombre_nuevo"]

        misiones.append({
            "orden": 20 + idx,
            "descripcion": f"EP_ACTO_{idx+1}",
            "plantilla_con_huecos": texto_crudo,
            "contexto_datos": ctx_acto,
            "instrucciones": instruccion_acto,
        })

    # INSERTOS
    misiones.append({
        "orden": 80,
        "descripcion": "EP_INSERTOS",
        "plantilla_con_huecos": EP_INSERTOS_TEMPLATE,
        "contexto_datos": {
            **contexto,
            "MATRICULA_INMOBILIARIA": matricula,
            "PAZ_SALVO_PREDIAL": datos_extra.get("PAZ_SALVO_PREDIAL") or "[[PENDIENTE: PAZ_SALVO_PREDIAL]]",
            "PAZ_SALVO_VALORIZACION": datos_extra.get("PAZ_SALVO_VALORIZACION") or "[[PENDIENTE: PAZ_SALVO_VALORIZACION]]",
            "PAZ_SALVO_AREA_METRO": datos_extra.get("PAZ_SALVO_AREA_METRO") or "[[PENDIENTE: PAZ_SALVO_AREA_METRO]]",
            "CERTIFICADOS_PAZ_Y_SALVO_DETALLE": datos_extra.get("CERTIFICADOS_PAZ_Y_SALVO_DETALLE") or "[[PENDIENTE: CERTIFICADOS_PAZ_Y_SALVO_DETALLE]]",
            "CAMARAS_COMERCIO": _build_camaras_text(empresa_rl_map),
        },
        "instrucciones": "Completa insertos. Sin markdown. No inventes.",
    })

    # OTORGAMIENTO
    misiones.append({
        "orden": 90,
        "descripcion": "EP_OTORGAMIENTO",
        "plantilla_con_huecos": EP_OTORGAMIENTO_TEMPLATE,
        "contexto_datos": {
            **contexto,
            "EMAIL_NOTIFICACIONES": datos_extra.get("EMAIL_NOTIFICACIONES") or "tramites@notaria3bga.com",
        },
        "instrucciones": "Manten el texto; solo reemplaza placeholders si hay datos. Sin markdown.",
    })

    # DERECHOS
    misiones.append({
        "orden": 95,
        "descripcion": "EP_DERECHOS",
        "plantilla_con_huecos": EP_DERECHOS_TEMPLATE,
        "contexto_datos": {
            **contexto,
            "RESOLUCION_DERECHOS": datos_extra.get("RESOLUCION_DERECHOS") or "00585 del 24 de enero de 2025",
            "VALOR_DERECHOS": datos_extra.get("VALOR_DERECHOS") or "[[PENDIENTE: VALOR_DERECHOS]]",
            "VALOR_IVA": datos_extra.get("VALOR_IVA") or "[[PENDIENTE: VALOR_IVA]]",
            "VALOR_RETEFUENTE": datos_extra.get("VALOR_RETEFUENTE") or "[[PENDIENTE: VALOR_RETEFUENTE]]",
            "VALOR_SUPER": datos_extra.get("VALOR_SUPER") or "[[PENDIENTE: VALOR_SUPER]]",
            "VALOR_FONDO": datos_extra.get("VALOR_FONDO") or "[[PENDIENTE: VALOR_FONDO]]",
            "NUMEROS_PAPEL_NOTARIAL": datos_extra.get("NUMEROS_PAPEL_NOTARIAL") or "[[PENDIENTE: NUMEROS_PAPEL_NOTARIAL]]",
        },
        "instrucciones": "No inventes valores. Sin markdown.",
    })

    # UIAF — solo personas naturales partes del negocio actual
    # Solo agregar si hay personas naturales; template vacío hace que el binder alucine contenido
    uiaf_template = _build_ui_af_blocks(personas_activos)
    if uiaf_template:
        misiones.append({
            "orden": 96,
            "descripcion": "EP_UIAF",
            "plantilla_con_huecos": uiaf_template,
            "contexto_datos": contexto,
            "instrucciones": "Bloque UIAF ya construido. Transcribe sin modificar.",
        })

    # FIRMAS
    misiones.append({
        "orden": 99,
        "descripcion": "EP_FIRMAS",
        "plantilla_con_huecos": EP_FIRMAS_TEMPLATE.replace(
            "[[BLOQUE_FIRMAS]]", _build_firmas_block(personas_activos, empresa_rl_map)
        ),
        "contexto_datos": {
            **contexto,
            "NOTARIA_FIRMA_NOMBRE": datos_extra.get("NOTARIA_ENCARGADO") or datos_extra.get("notario_encargado") or "[[PENDIENTE: NOTARIA_FIRMA_NOMBRE]]",
            "NOTARIA_CARGO": notaria if notaria and "PENDIENTE" not in notaria else "[[PENDIENTE: NOTARIA_CARGO]]",
        },
        "instrucciones": "Bloque de firmas. Transcribe sin modificar.",
    })

    return misiones


# ----------------------------
# Main pipeline
# ----------------------------

async def run_pipeline(
    scanner_paths: List[str],
    documentos_paths: List[str],
    comentario: str = "(Sin comentarios)",
    template_id: Optional[str] = None,
) -> Dict[str, Any]:
    tpl_path = Path(settings.TEMPLATE_DOCX_PATH)
    if not tpl_path.exists():
        raise RuntimeError(f"No existe plantilla DOCX: {tpl_path}")

    gemini = GeminiClient()
    openai = OpenAIClient()
    rag = LocalRAG()

    # 1) RADICACIÓN
    rad_path = _pick_radicacion_file(documentos_paths)
    rad_json: Dict[str, Any] = {}
    rad_text = ""

    if rad_path:
        rad_bytes = Path(rad_path).read_bytes()
        rad_text = await asyncio.to_thread(
            gemini.analyze_binary,
            rad_bytes,
            _guess_mime(rad_path),
            RADICACION_PROMPT,
            settings.GEMINI_MODEL_VISION,
            0.1,
            8192,
        )
        # Usar repair para manejar JSON truncado/malformado de Gemini
        rad_json = parse_json_with_repair(
            rad_text,
            kind="radicacion",
            openai_client=openai,
            temperature=0.0,
            max_tokens=4000,
        )

    radicado = _extract_radicado_from_radicacion_json(rad_json)

    # CASE DIR + DEBUG
    case_dir = Path(settings.OUTPUT_DIR) / f"CASE-{radicado}"
    case_dir.mkdir(parents=True, exist_ok=True)

    debug = DebugDumper(case_dir=case_dir, enabled=True)
    debug.write_manifest(scanner_paths=scanner_paths, documentos_paths=documentos_paths)

    if rad_path:
        debug.dump_gemini_output("01_radicacion", rad_path, rad_text, rad_json)

    # 2) SOPORTES
    soporte_paths = [p for p in documentos_paths if p != rad_path]
    soportes_json_list: List[Dict[str, Any]] = []

    async def _analyze_soporte(p: str) -> Dict[str, Any]:
        b = Path(p).read_bytes()
        txt = await asyncio.to_thread(
            gemini.analyze_binary,
            b,
            _guess_mime(p),
            DOCS_PROMPT,
            settings.GEMINI_MODEL_VISION,
            0.1,
            16384,
        )
        out = parse_json_with_repair(
            txt,
            kind="docs",
            openai_client=openai,
            temperature=0.0,
            max_tokens=4000,
        )
        out["_fileName"] = Path(p).name
        debug.dump_gemini_output("02_soportes", p, txt, out)
        return out

    if soporte_paths:
        soportes_json_list = await asyncio.gather(*[_analyze_soporte(p) for p in soporte_paths])

    debug.dump_stage_json("02_soportes.json", soportes_json_list)

    # 3) CÉDULAS
    cedulas_json_list: List[Dict[str, Any]] = []

    async def _analyze_cedula(p: str) -> Dict[str, Any]:
        b = Path(p).read_bytes()
        txt = await asyncio.to_thread(
            gemini.analyze_binary,
            b,
            _guess_mime(p),
            CEDULA_PROMPT,
            settings.GEMINI_MODEL_VISION,
            0.1,
            2048,
        )
        out = parse_json_with_repair(
            txt,
            kind="cedula",
            openai_client=openai,
            temperature=0.0,
            max_tokens=2000,
        )
        out["_fileName"] = Path(p).name
        debug.dump_gemini_output("03_cedulas", p, txt, out)
        return out

    if scanner_paths:
        cedulas_json_list = await asyncio.gather(*[_analyze_cedula(p) for p in scanner_paths])

    debug.dump_stage_json("03_cedulas.json", cedulas_json_list)

    # 4) CONTEXTO UNIVERSAL + DEDUPE
    contexto = _build_universal_context(rad_json, soportes_json_list, cedulas_json_list)
    contexto["PERSONAS"] = dedupe_personas(contexto.get("PERSONAS") or [])
    debug.dump_stage_json("04_contexto_universal.json", contexto)

    # 5) RAG POR ACTOS (en el orden de radicación)
    actos = _safe_get(contexto, ["NEGOCIO", "actos_a_firmar"], default=[])
    if isinstance(actos, str):
        try:
            actos = json.loads(actos)
        except Exception:
            actos = []
    if not isinstance(actos, list):
        actos = [actos]

    actos_docs: List[Dict[str, str]] = []
    for acto in actos:
        nombre_acto = acto.get("nombre") if isinstance(acto, dict) else str(acto)
        raw = rag.retrieve_acto_text(nombre_acto)
        split = split_metadata_and_body(raw)
        actos_docs.append(split)
        debug.dump_rag_hit(acto_nombre=nombre_acto, raw_text=raw, meta_body=split)

    debug.dump_stage_json("05_actos_docs.json", actos_docs)

    # 6) MISIONES EP
    misiones = _prepare_ep_sections(contexto, actos_docs)
    debug.dump_misiones(misiones)

    # 7) BINDER POR SECCIÓN
    async def _run_mision(m: Dict[str, Any]) -> Dict[str, Any]:
        user = DATABINDER_USER.format(
            contexto_json=json.dumps(m["contexto_datos"], ensure_ascii=False),
            plantilla=m["plantilla_con_huecos"],
        )
        out = await asyncio.to_thread(openai.chat, DATABINDER_SYSTEM, user, 0.1, 8000)
        debug.dump_binder_output(m["orden"], m["descripcion"], out)
        return {"orden": m["orden"], "descripcion": m["descripcion"], "texto": out}

    results = await asyncio.gather(*[_run_mision(m) for m in misiones])
    results.sort(key=lambda x: x["orden"])

    cuerpo_escritura = "\n\n".join([r["texto"].strip() for r in results if r["texto"].strip()])
    debug.dump_stage_text("08_cuerpo_escritura.txt", cuerpo_escritura)
    debug.write_checklist()

    # 8) RENDER
    docx_path = str(case_dir / f"Minuta_Caso_{radicado}.docx")
    pdf_path = str(case_dir / f"Escritura_Caso_{radicado}.pdf")

    context_docx = {"RADICADO": radicado, "COMENTARIO": comentario or "", "CONTENIDO_IA": cuerpo_escritura}
    render_docx(str(tpl_path), context_docx, docx_path)

    generated_pdf = docx_to_pdf_libreoffice(docx_path, str(case_dir))
    gen_p = Path(generated_pdf)
    final_p = Path(pdf_path)
    if gen_p.resolve() != final_p.resolve():
        if final_p.exists():
            final_p.unlink()
        gen_p.replace(final_p)

    return {
        "radicado": radicado,
        "docx_path": docx_path,
        "pdf_path": pdf_path,
        "case_dir": str(case_dir),
        "contexto": contexto,
        "debug": {
            "radicacion_json": rad_json,
            "soportes_json": soportes_json_list,
            "cedulas_json": cedulas_json_list,
            "actos_docs": actos_docs,
            "secciones": [r["descripcion"] for r in results],
        },
    }