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
from app.pipeline.act_engine import build_act_context, dedupe_personas, _extract_city_from_address

from app.pipeline.prompts import (
    RADICACION_PROMPT,
    DOCS_PROMPT,
    CEDULA_PROMPT,
    DATABINDER_SYSTEM,
    DATABINDER_USER,
)

from app.pipeline.boilerplate import (
    EP_CARATULA_TEMPLATE,
    EP_APERTURA_TEMPLATE,
    EP_INSERTOS_TEMPLATE,
    EP_OTORGAMIENTO_TEMPLATE,
    EP_DERECHOS_TEMPLATE,
    EP_UIAF_TEMPLATE,
    EP_FIRMAS_TEMPLATE,
)


# ----------------------------
# Constantes de cláusulas legales
# ----------------------------

_BALDIOS_CLAUSE_FULL = (
    "PROHIBICIÓN ACUMULACIÓN BALDÍOS LEY 160 DE 1994: La Notaria indagó sobre la "
    "naturaleza jurídica del predio objeto del negocio y como quiera que fue adjudicado "
    "inicialmente como baldío por el INCORA/Agencia Nacional de Tierras (ANT), se les "
    "advierte a los comparecientes que está prohibida por la Ley 160 de 1994 la acumulación "
    "de terrenos proveniente de baldíos en extensión superior a la Unidad Agrícola Familiar, "
    "en el respectivo Municipio o Región, establecida por la Resolución 041 de 1996 del "
    "INCORA, quedando viciados de nulidad absoluta los actos o contratos que contraríen estas "
    "disposiciones, a partir de la entrada en vigencia de la Ley 160 de 1.994. ----------\n\n"
    "PARÁGRAFO: Se advierte a los compradores que conforme al artículo 72 de la ley 160 de "
    "1994, que el terreno que aquí adquieren, por provenir de adjudicación de baldíos, "
    "cualquier fraccionamiento o división, deberá ser previamente autorizado por el INCODER "
    "Hoy Agencia Nacional de Tierras, y no podrá realizarse en extensiones inferiores "
    "señaladas por el INCODER, Hoy Agencia Nacional de Tierras, como Unidad Agrícola Familiar, "
    "para la respectiva Zona o Municipio, salvo las excepciones previstas en la ley. ----------\n\n"
    "NOTA: El(los) Comprador(es) bajo juramento declaró(aron) que no es(son) titular(es) "
    "del derecho de dominio de otros predios provenientes de adjudicación de baldíos que "
    "superen el área determinada para la unidad agrícola familiar en el respectivo Municipio "
    "o región, y que se encuentren bajo el Régimen de la Ley 160 de 1994. "
    "(I.A. No 8 de 2013); como consta en la(s) declaración(es) juramentada(s), que se "
    "adjunta(n) a la presente para su protocolización. ----------"
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
    if s in {"", "NULL", "UNDEFINED", "N/A", "NA", "EXTRAER", "S/I",
             "NO APLICA", "ILEGIBLE", "NO DISPONIBLE", "DESCONOCIDO"}:
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
    # G1: Colapsar sufijos legales fragmentados (ej: "S A S" → "SAS", generado por eliminación de puntos)
    s = re.sub(r"\bS A S\b", "SAS", s)
    return s


_SOCIEDAD_KW = ("S.A.S", " SAS", "LTDA", "S.A.", " S.A,", "E.U.", "S.C.A", "S.E.M.", "ASOCIADOS")


_COMENTARIO_SYSTEM = """Eres un extractor de instrucciones de un comentario de usuario para un pipeline notarial colombiano.
Extrae SOLO los campos que están explícitamente mencionados. Devuelve JSON con estas claves (todas opcionales, null si no se menciona):
{
  "NOMBRE_NUEVO_PREDIO": "nombre nuevo o definitivo del predio/finca/hacienda",
  "FORMA_DE_PAGO": "descripción completa del plan de pagos (cuotas, montos, fechas, crédito hipotecario)",
  "CODIGO_CATASTRAL_ANTERIOR": "código catastral anterior del predio si el usuario lo especifica para corregir el extraído automáticamente (ej: '00 03 00 0020 0001 000')",
  "NOMBRE_ANTERIOR_PREDIO": "nombre histórico o anterior del predio/finca antes del cambio",
  "REDAM_CONTINGENCIA": true si el usuario menciona falla técnica REDAM, error de conexión, sistema caído, no fue posible consultar REDAM — de lo contrario null,
  "ESTADO_CIVIL": {"<nombre_persona>": "<estado civil>"}
}
Responde ÚNICAMENTE con JSON válido. Sin markdown."""


def _parse_comentario_overrides(comentario: str, openai_client=None) -> dict:
    """Extrae overrides del comentario libre usando lenguaje natural vía LLM.
    Fallback a regex básico si no hay cliente OpenAI disponible."""
    if not comentario or comentario.strip() in ("", "(Sin comentarios)"):
        return {}

    # Intentar extracción LLM si hay cliente disponible
    if openai_client is not None:
        try:
            raw = openai_client.chat(
                _COMENTARIO_SYSTEM,
                f"Comentario del usuario:\n{comentario}",
                temperature=0.0,
                max_tokens=400,
            )
            parsed = parse_json_with_repair(raw, kind="comentario", openai_client=openai_client)
            overrides: dict = {}
            if isinstance(parsed, dict):
                for key in ("NOMBRE_NUEVO_PREDIO", "FORMA_DE_PAGO", "CODIGO_CATASTRAL_ANTERIOR", "NOMBRE_ANTERIOR_PREDIO"):
                    val = parsed.get(key)
                    if val and str(val).strip() not in ("", "null", "None"):
                        overrides[key] = str(val).strip()
                        if key == "NOMBRE_NUEVO_PREDIO":
                            overrides[key] = overrides[key].upper()
                # Estado civil por persona
                ec = parsed.get("ESTADO_CIVIL") or {}
                if isinstance(ec, dict):
                    for nombre, estado in ec.items():
                        if nombre and estado:
                            norm_key = "ESTADO_CIVIL_" + re.sub(r'\s+', '_', nombre.strip().upper())
                            overrides[norm_key] = str(estado).strip()
            return overrides
        except Exception:
            pass  # fallback a regex

    # Fallback regex básico: KEY: valor en líneas estructuradas
    overrides = {}
    for line in comentario.splitlines():
        m = re.match(r'^([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ0-9_ ]{1,60}):\s*(.+)', line.strip(), re.IGNORECASE)
        if m:
            key = re.sub(r'\s+', '_', m.group(1).strip().upper())
            overrides[key] = m.group(2).strip()
    return overrides


def _is_sociedad_party(party_name: str, personas_activos: list) -> bool:
    """
    Detecta si un nombre de parte corresponde a una sociedad.
    Estrategia doble:
    1) Palabras clave típicas de personas jurídicas en el nombre (SAS, LTDA, etc.)
    2) Búsqueda fuzzy en PERSONAS_ACTIVOS por overlap de palabras + NIT en identificacion.
    """
    if not party_name:
        return False
    pup = party_name.upper()
    # 1) Keywords directas en el nombre
    if any(kw in pup for kw in _SOCIEDAD_KW):
        return True
    # 2) Búsqueda por overlap de palabras en personas_activos
    party_words = set(re.sub(r"[^A-ZÁÉÍÓÚÜÑ ]", " ", pup).split())
    party_words -= {"DE", "Y", "LA", "EL", "LOS", "LAS", "DEL", "EN"}
    if len(party_words) < 2:
        return False
    for p in personas_activos:
        pid = (p.get("identificacion") or "").upper()
        has_nit = "NIT" in pid or pid.startswith("NI ")
        if not has_nit:
            continue
        p_words = set(re.sub(r"[^A-ZÁÉÍÓÚÜÑ ]", " ", (p.get("nombre") or "").upper()).split())
        p_words -= {"DE", "Y", "LA", "EL", "LOS", "LAS", "DEL", "EN"}
        if len(party_words & p_words) >= 2:
            return True
    return False


def _build_rag_query(acto_dict, contexto: Dict[str, Any]) -> str:
    """
    Para COMPRAVENTA: elige el sub-tipo de template según tipo de partes y predio.
    Para otros actos devuelve el nombre directamente.
    """
    nombre = (acto_dict.get("nombre") or str(acto_dict)).upper() if isinstance(acto_dict, dict) else str(acto_dict).upper()

    if "COMPRAVENTA" not in nombre:
        return acto_dict.get("nombre") if isinstance(acto_dict, dict) else str(acto_dict)

    personas_activos = contexto.get("PERSONAS_ACTIVOS") or []
    otorgantes = (acto_dict.get("otorgantes") or []) if isinstance(acto_dict, dict) else []
    benefs = (acto_dict.get("beneficiarios") or []) if isinstance(acto_dict, dict) else []

    # — Detección de sociedad vendedora/compradora ————————————————————
    # Primario: per-acto listas de Gemini (otorgantes/beneficiarios)
    hay_soc_v = any(_is_sociedad_party(o, personas_activos) for o in otorgantes) if otorgantes else None
    hay_soc_c = any(_is_sociedad_party(b, personas_activos) for b in benefs) if benefs else None

    # Fallback: rol_detectado de PERSONAS_ACTIVOS cuando Gemini no extrajo listas
    if hay_soc_c is None:
        hay_soc_c = any(
            ("NIT" in (p.get("identificacion") or "").upper()
             or (p.get("identificacion") or "").upper().startswith("NI "))
            and ("COMPRADOR" in (p.get("rol_detectado") or "").upper()
                 or " A " in (p.get("rol_detectado") or "").upper())
            for p in personas_activos
        )
    if hay_soc_v is None:
        # No podemos detectar confiablemente el vendor-sociedad desde roles globales
        # cuando hay múltiples actos (el mismo vendedor puede aparecer en otros actos).
        # Por seguridad, dejamos False para evitar false-positives.
        hay_soc_v = False

    hist_text = json.dumps(contexto.get("HISTORIA") or {}).upper()
    es_baldio = any(kw in hist_text for kw in ("BALDIO", "INCODER", "INCORA"))

    if hay_soc_v and hay_soc_c:
        return "compraventa bien inmueble vende sociedad compra sociedad"
    if hay_soc_v and not hay_soc_c:
        return "compraventa bien inmueble vende sociedad compra persona natural"
    if not hay_soc_v and hay_soc_c:
        return "compraventa bien inmueble vende persona natural compra sociedad"
    if es_baldio:
        return "compraventa bien inmueble lote baldio"
    return "compraventa bien inmueble lote"


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


def _build_camaras_text(
    empresa_rl_map: Dict[str, Any],
    empresa_display_map: Optional[Dict[str, str]] = None,
) -> str:
    """Genera líneas de cámara de comercio para cada empresa jurídica presente (deduplicadas).

    Usa token-overlap: si dos nombres comparten ≥ 4 de sus primeros 6 tokens, se consideran
    la misma empresa con variaciones ortográficas (ej: 'INGENIERIA DE SERVICIO' vs 'SERVICIOS').
    empresa_display_map: normalized_key → razón social completa para display.
    """
    seen_tokens: list = []
    lines = []
    for emp_nombre in (empresa_rl_map or {}):
        display = (empresa_display_map or {}).get(emp_nombre) or emp_nombre
        tokens = frozenset(_normalize_name(display).split()[:6])
        is_dup = any(len(tokens & prev) >= 4 for prev in seen_tokens)
        if not is_dup:
            seen_tokens.append(tokens)
            lines.append(f"Copia de cámara de comercio de {display}.")
    return "\n".join(lines) if lines else ""


def _merge_cedula_ocr_into_people(personas: List[Dict[str, Any]], cedulas_json_list: List[Dict[str, Any]]) -> None:
    if not personas or not cedulas_json_list:
        return

    idx_by_id: Dict[str, Dict[str, Any]] = {}
    # Radicacion persons take priority: they are in PERSONAS_ACTIVOS
    # so cedula data must update their dicts, not soporte copies
    for p in personas:
        pid = _normalize_id(p.get("identificacion") or "")
        if pid:
            if pid not in idx_by_id or p.get("_source") == "radicacion":
                idx_by_id[pid] = p

    for c in cedulas_json_list:
        if not isinstance(c, dict):
            continue
        cid = _normalize_id(c.get("cedula") or c.get("identificacion") or "")
        cname = _normalize_name(c.get("nombre") or "")
        lugar_exp = (c.get("lugar_expedicion") or "").strip()
        ocupacion = (c.get("ocupacion") or "").strip()
        estado_civil_ced = (c.get("estado_civil") or "").strip()

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
        if estado_civil_ced and estado_civil_ced.upper() not in ("ILEGIBLE", "") and not target.get("estado_civil"):
            target["estado_civil"] = estado_civil_ced


def _build_empresa_rl_map(
    _radicacion_json: Dict[str, Any],
    personas_deduped: List[Dict[str, Any]],
) -> tuple:
    """
    Construye mapeo nombre_empresa_normalizado → persona RL.
    Retorna (empresa_rl_map, empresa_display_map):
    - empresa_rl_map:     normalized_key → RL person dict (LIMPIO, sin _empresa_display)
    - empresa_display_map: normalized_key → razón social original (para firmas/insertos)
    Usa orden secuencial de personas_deduped: la persona RL sigue
    inmediatamente a la empresa que representa en la lista de radicación.
    """
    mapping: Dict[str, Dict[str, Any]] = {}
    display_map: Dict[str, str] = {}
    last_empresa: Optional[Dict[str, Any]] = None

    for p in personas_deduped:
        # Prioridad 1: mapeo explícito via campo representa_a (extraído del prompt de radicación)
        rep_a = _normalize_name(p.get("representa_a") or "")
        if rep_a:
            # K-1a: Guardar como RL SOLO si la persona es persona natural (no empresa con NIT)
            # Gemini puede extraer self-reference: empresa con represents_a apuntando a sí misma
            _pid_rl = (p.get("identificacion") or "").upper()
            _es_empresa_rl = "NIT" in _pid_rl or _pid_rl.startswith("NI ")
            if not _es_empresa_rl:
                # Buscar nombre original de la empresa para display (preservar razón social completa)
                empresa_original = next(
                    (pe.get("nombre") or rep_a for pe in personas_deduped
                     if _normalize_name(pe.get("nombre") or "") == rep_a),
                    rep_a
                )
                mapping[rep_a] = dict(p)       # ← limpio, sin _empresa_display
                display_map[rep_a] = empresa_original.upper()

        pid_upper = (p.get("identificacion") or "").upper()
        rol = (p.get("rol_detectado") or p.get("rol") or p.get("rol_en_hoja") or "").upper()

        is_emp = "NIT" in pid_upper or pid_upper.startswith("NI ")
        is_rl  = "REPRESENTANTE" in rol or rol.startswith("RL")

        if is_emp:
            last_empresa = p
        elif is_rl and last_empresa is not None:
            key = _normalize_name(last_empresa.get("nombre") or "")
            # Prioridad 2: mapeo secuencial — solo si no hay ya un mapeo explícito
            if key and key not in mapping:
                mapping[key] = dict(p)         # ← limpio, sin _empresa_display
                display_map[key] = (last_empresa.get("nombre") or "").upper()
            last_empresa = None

    return mapping, display_map


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
            if rad.get("departamento"):
                contexto["DATOS_EXTRA"]["DEPARTAMENTO"] = f"Departamento de {rad['departamento'].strip()}"
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
            # G3C: Guardar hallazgos individuales de cada soporte para acceso per-fuente
            contexto.setdefault("_SOPORTES_HALLAZGOS", []).append(extra)

        # Extraer documento_ep_info del antecedente (EP que se referencia/cancela)
        ep_info = s.get("documento_ep_info") or {}
        if isinstance(ep_info, dict) and ep_info.get("numero_ep") and not _is_garbage(ep_info["numero_ep"]):
            if not contexto["INMUEBLE"].get("ep_antecedente_pacto"):
                contexto["INMUEBLE"]["ep_antecedente_pacto"] = ep_info

        # Extraer nombre_nuevo_predio: last-wins (doc más reciente tiene prioridad)
        nombre_nuevo = (extra or {}).get("nombre_nuevo_predio") or ""
        if nombre_nuevo and not _is_garbage(nombre_nuevo):
            contexto["INMUEBLE"]["nombre_nuevo"] = nombre_nuevo

        # Extraer nombre_anterior_predio: first-wins (el primero que lo mencione es el más fiable)
        nombre_anterior = (extra or {}).get("nombre_anterior_predio") or ""
        if nombre_anterior and not _is_garbage(nombre_anterior):
            if not contexto["INMUEBLE"].get("nombre_anterior"):
                contexto["INMUEBLE"]["nombre_anterior"] = nombre_anterior

        # Extraer plazo_retroventa para actos de CANCELACION
        plazo = (extra or {}).get("plazo_retroventa") or ""
        if plazo and not _is_garbage(plazo):
            if not contexto["DATOS_EXTRA"].get("plazo_retroventa"):
                contexto["DATOS_EXTRA"]["plazo_retroventa"] = plazo

        # Extraer números de paz y salvo y forma_de_pago de documentos soporte
        for _campo in ["paz_salvo_predial", "paz_salvo_valorizacion", "paz_salvo_area_metro"]:
            _val = str((extra or {}).get(_campo) or "").strip()
            if _val and not _is_garbage(_val):
                _key = _campo.upper()
                if not contexto["DATOS_EXTRA"].get(_key):
                    contexto["DATOS_EXTRA"][_key] = _val

        # Bug 8: forma_de_pago — first-wins (primer doc que lo mencione)
        _fdp = str((extra or {}).get("forma_de_pago") or "").strip()
        if _fdp and not _is_garbage(_fdp):
            if not contexto["DATOS_EXTRA"].get("FORMA_DE_PAGO"):
                contexto["DATOS_EXTRA"]["FORMA_DE_PAGO"] = _fdp

    _merge_cedula_ocr_into_people(contexto["PERSONAS"], cedulas_json_list)

    # B6: forma_de_pago desde radicación (prioridad sobre DOCS si está explícita)
    _fdp_rad = str((radicacion_json or {}).get("negocio_actual", {}).get("forma_de_pago") or "").strip()
    if _fdp_rad and not _is_garbage(_fdp_rad):
        contexto["DATOS_EXTRA"]["FORMA_DE_PAGO"] = _fdp_rad

    # B6: nombre_nuevo_predio desde radicación
    _nnp_rad = str((radicacion_json or {}).get("negocio_actual", {}).get("nombre_nuevo_predio") or "").strip()
    if _nnp_rad and not _is_garbage(_nnp_rad):
        contexto["DATOS_EXTRA"]["NOMBRE_NUEVO_PREDIO"] = _nnp_rad.upper()

    # Robustez actos_a_firmar: si rad_json tiene más actos que los que quedaron en NEGOCIO, usar los de rad_json
    rad_actos = (radicacion_json or {}).get("negocio_actual", {}).get("actos_a_firmar") or []
    existing_actos = contexto["NEGOCIO"].get("actos_a_firmar") or []
    if isinstance(rad_actos, list) and len(rad_actos) > len(existing_actos):
        contexto["NEGOCIO"]["actos_a_firmar"] = rad_actos

    # CIUDAD = ciudad de la notaría (otorgamiento), NO la ciudad del registro del inmueble
    if not contexto["DATOS_EXTRA"].get("CIUDAD"):
        contexto["DATOS_EXTRA"]["CIUDAD"] = (
            (radicacion_json or {}).get("radicacion", {}).get("ciudad")
            or (radicacion_json or {}).get("datos_inmueble", {}).get("ciudad_registro")
            or "[[PENDIENTE: CIUDAD]]"
        )

    # Restaurar campos ID canónicos de radicación (evitar que OCR erróneo de soportes los sobreescriba)
    rad_inmueble = (radicacion_json or {}).get("datos_inmueble") or {}
    for field in ("matricula", "predial_nacional"):
        val = rad_inmueble.get(field)
        if val and not _is_garbage(val):
            contexto["INMUEBLE"][field] = val

    # N-5A: Si predial_nacional contiene " - CÓDIGO NPN: " o similar, separar los dos códigos
    _pn_raw = contexto["INMUEBLE"].get("predial_nacional") or ""
    _npn_match = re.search(r"[-–]\s*(?:CÓDIGO\s+)?NPN\s*:\s*(.+)", _pn_raw, re.IGNORECASE)
    if _npn_match:
        _npn_only = _npn_match.group(1).strip()
        _cad_part = _pn_raw[:_npn_match.start()].strip().rstrip("-–").strip()
        contexto["INMUEBLE"]["predial_nacional"] = _npn_only
        if not contexto["INMUEBLE"].get("codigo_catastral_anterior") or _is_garbage(
            contexto["INMUEBLE"].get("codigo_catastral_anterior") or ""
        ):
            contexto["INMUEBLE"]["codigo_catastral_anterior"] = _cad_part
            contexto["INMUEBLE"]["CODIGO_CATASTRAL_ANTERIOR"] = _cad_part
            contexto["INMUEBLE"]["CEDULA_CATASTRAL"] = _cad_part

    # Surfacear codigo_catastral_anterior desde el inmueble fusionado
    cat = contexto["INMUEBLE"].get("codigo_catastral_anterior") or ""
    if cat and not _is_garbage(cat):
        contexto["INMUEBLE"]["CODIGO_CATASTRAL_ANTERIOR"] = cat
        contexto["INMUEBLE"]["CEDULA_CATASTRAL"] = cat

    # P-1: Sanitizar nombre_anterior si contiene nombre de empresa (hallucination de Gemini desde cámara)
    _EMPRESA_KW_PROP = ("LTDA", "S.A.S", " SAS", "S.A.", "E.U.", "LIMITADA", "ASESORES", "CONSULTORES", "ASOCIADOS")
    _nom_ant_raw = contexto["INMUEBLE"].get("nombre_anterior") or ""
    if _nom_ant_raw and any(kw in _nom_ant_raw.upper() for kw in _EMPRESA_KW_PROP):
        contexto["INMUEBLE"]["nombre_anterior"] = ""
    # P-1b: Si nombre_anterior vacío tras limpiar, derivar del CTL: nombre_nuevo del INMUEBLE
    # (= resultado del último CAMBIO_NOMBRE histórico = nombre actual del predio antes de este EP)
    if not contexto["INMUEBLE"].get("nombre_anterior"):
        _ctx_nombre_actual = contexto["INMUEBLE"].get("nombre_nuevo") or ""
        if _ctx_nombre_actual and not _is_garbage(_ctx_nombre_actual) and not any(
            kw in _ctx_nombre_actual.upper() for kw in _EMPRESA_KW_PROP
        ):
            contexto["INMUEBLE"]["nombre_anterior"] = _ctx_nombre_actual.upper()
        else:
            # Fallback: extraer del CTL linderos "SE DENOMINA: X"
            for _s_ctl in ((contexto.get("FUENTES") or {}).get("soportes") or []):
                _linderos_ctl = ((_s_ctl.get("datos_inmueble") or {}).get("linderos") or "")
                _denom_matches = re.findall(r"SE DENOMINA[:\s]+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]+)", _linderos_ctl, re.IGNORECASE)
                if _denom_matches:
                    _last_denom = _denom_matches[-1].strip().upper()
                    if 2 < len(_last_denom) < 40 and not any(kw in _last_denom for kw in _EMPRESA_KW_PROP):
                        contexto["INMUEBLE"]["nombre_anterior"] = _last_denom
                        break

    # Neutralizar fecha_otorgamiento proveniente de soportes: pertenece al antecedente, no a esta EP
    if "fecha_otorgamiento" in contexto["DATOS_EXTRA"]:
        contexto["DATOS_EXTRA"]["fecha_escritura_antecedente"] = contexto["DATOS_EXTRA"].pop("fecha_otorgamiento")

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
        _addr_raw = datos.get("domicilio") or p.get("direccion") or ""
        ciudad = _extract_city_from_address(_addr_raw) if _addr_raw else "[[PENDIENTE: CIUDAD]]"
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


def _build_firmas_block(
    personas_activos: List[Dict[str, Any]],
    empresa_rl_map: Dict[str, Any],
    empresa_display_map: Optional[Dict[str, str]] = None,
) -> str:
    """Genera bloque de firmas: personas naturales con su cargo/empresa si aplica.
    empresa_display_map: normalized_key → razón social completa (para 'representante legal de X').
    """
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
        for emp_norm, rl_data in (empresa_rl_map or {}).items():
            if _normalize_name(rl_data.get("nombre") or "") == _normalize_name(nom):
                # Preferir display_map (razón social completa) > clave normalizada
                # Si hay múltiples entradas (representa_a + secuencial), tomar la más larga (más completa)
                candidate = (empresa_display_map or {}).get(emp_norm) or emp_norm
                if empresa_repr is None or len(candidate) > len(empresa_repr):
                    empresa_repr = candidate
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

        # Filtrar personas al conjunto de ESTE acto y marcar rol_detectado per-acto
        personas_act = personas_activos
        if isinstance(acto, dict):
            _rotor_raw = [n for n in (acto.get("otorgantes") or []) if n]
            _rbenef_raw = [n for n in (acto.get("beneficiarios") or []) if n]
            _rparts = _rotor_raw + _rbenef_raw
            if _rparts:
                _rotor_norms = {_normalize_name(n) for n in _rotor_raw}
                _rbenef_norms = {_normalize_name(n) for n in _rbenef_raw}
                _rall_norms = _rotor_norms | _rbenef_norms
                _rsw = {"DE", "LA", "EL", "LOS", "LAS", "Y", "E", "S", "A", "SAS", "LTDA", "SA"}

                def _rname_words(s: str) -> set:
                    return {w for w in s.split() if w not in _rsw and len(w) > 2}

                def _rname_in_set(pname: str, nset: set) -> bool:
                    if pname in nset:
                        return True
                    pw = _rname_words(pname)
                    for an in nset:
                        if pw and len(pw & _rname_words(an)) >= 2:
                            return True
                    return False

                def _racto_role_for(pname: str) -> Optional[str]:
                    in_o = _rname_in_set(pname, _rotor_norms)
                    in_b = _rname_in_set(pname, _rbenef_norms)
                    if in_o and not in_b:
                        return "VENDEDOR/OTORGANTE"
                    if in_b and not in_o:
                        return "COMPRADOR/BENEFICIARIO"
                    return None

                _rfilt_raw = [p for p in personas_activos if _rname_in_set(
                    _normalize_name(p.get("nombre") or ""), _rall_norms
                ) or not _normalize_name(p.get("nombre") or "")]

                if _rfilt_raw:
                    _rfilt = []
                    for p in _rfilt_raw:
                        pname = _normalize_name(p.get("nombre") or "")
                        acto_role = _racto_role_for(pname)
                        if acto_role:
                            p_copy = dict(p)
                            p_copy["rol_detectado"] = acto_role
                            _rfilt.append(p_copy)
                        else:
                            _rfilt.append(p)
                    personas_act = _rfilt

        roles = infer_roles_por_acto(nombre, personas_act, None)
        raw_vendedores   = roles.get("VENDEDORES", [])
        raw_solicitantes = roles.get("SOLICITANTES", [])
        compradores = roles.get("COMPRADORES", [])
        deudores = roles.get("DEUDORES", [])
        acreedores = roles.get("ACREEDORES", [])

        def _fmt_persona(p: Dict) -> str:
            n = (p.get("nombre") or "").strip()
            cc = (p.get("identificacion") or "").strip()
            if cc and "NO_DETECTADO" not in cc.upper():
                return f"{n} - {cc}"
            return n

        # Nombres normalizados de todas las personas que son RL en el mapa
        rl_names_norm = {_normalize_name(v.get("nombre", "")) for v in empresa_rl_map.values()}

        # Mapa inverso: nombre_rl_normalizado → nombre_empresa_normalizado
        rl_to_empresa: Dict[str, str] = {
            _normalize_name(rl.get("nombre", "")): emp_norm
            for emp_norm, rl in empresa_rl_map.items()
        }

        def _is_rl(p: Dict) -> bool:
            return _normalize_name(p.get("nombre", "")) in rl_names_norm

        def _smart_filter(lst: List[Dict]) -> List[Dict]:
            """
            Filtra RLs de una lista de personas:
            - RLs cuya empresa YA está en la lista → excluir (solo son RL, no actúan individualmente)
            - RLs cuya empresa NO está en la lista → mantener (actúan a título personal, p.ej.
              RL actúa a título personal como comprador independiente)
            """
            non_rl_names = {_normalize_name(p.get("nombre", "")) for p in lst if not _is_rl(p)}
            result = []
            for p in lst:
                if not _is_rl(p):
                    result.append(p)
                else:
                    emp_norm = rl_to_empresa.get(_normalize_name(p.get("nombre", "")))
                    if emp_norm not in non_rl_names:
                        result.append(p)  # empresa ausente → actúa personalmente
            return result

        # OTORGANTES puros (COMPRAVENTA, CANCELACION): los RLs solo aparecen via "(RL: X)" de su empresa
        if raw_vendedores:
            vendedores = [v for v in raw_vendedores if not _is_rl(v)]
        # SOLICITANTES (CAMBIO_NOMBRE, CANCELACION como nuevos dueños): aplica smart filter
        # para mantener RLs que actúan a título personal (ej: RL actúa como comprador personal en otro acto)
        elif raw_solicitantes:
            vendedores = _smart_filter(raw_solicitantes)
        else:
            vendedores = []

        # COMPRADORES: smart filter (RL de empresa vendedora puede ser comprador personal en otro acto)
        compradores = _smart_filter(compradores)

        # DEUDORES: mismo smart filter que compradores (RL de empresa vendedora puede ser deudor personal)
        deudores = _smart_filter(deudores)

        # ACREEDORES: excluir RLs (las empresas acreedoras se representan con su RL vía "(RL: X)")
        acreedores = [a for a in acreedores if not _is_rl(a)]

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
                s = _fmt_persona(d)
                rl = empresa_rl_map.get(_normalize_name(d.get("nombre") or ""))
                if rl:
                    s += f" (RL: {rl.get('nombre','')} - {rl.get('identificacion','')})"
                lines.append(f"DEUDOR: {s}")
        if acreedores:
            for a in acreedores:
                s = _fmt_persona(a)
                rl = empresa_rl_map.get(_normalize_name(a.get("nombre") or ""))
                if rl:
                    s += f" (RL: {rl.get('nombre','')} - {rl.get('identificacion','')})"
                lines.append(f"ACREEDOR: {s}")

        lines.append("")  # línea en blanco entre actos

    return "\n".join(lines).strip()


def _enrich_camara_data(
    ctx_acto: Dict[str, Any],
    empresa_norm: str,
    contexto: Dict[str, Any],
) -> None:
    """Extrae y setdefault datos de cámara de comercio desde soportes para una empresa.
    Reutilizable en CAMBIO_NOMBRE, HIPOTECA y cualquier acto con empresa compareciente.
    """
    soportes_fuentes = (contexto.get("FUENTES") or {}).get("soportes") or []
    for s in soportes_fuentes:
        if not isinstance(s, dict):
            continue
        hvars = s.get("hallazgos_variables") or {}
        razon = _normalize_name(hvars.get("razon_social") or "")
        if not razon:
            continue
        if empresa_norm and empresa_norm not in razon and razon not in empresa_norm:
            continue
        hist = (s.get("historia_y_antecedentes") or {}).get("constitucion_sociedad") or ""
        dom = (hvars.get("municipio_direccion_principal") or hvars.get("municipio_domicilio") or "")
        ctx_acto.setdefault("DOMICILIO_SOCIEDAD", dom)
        ctx_acto.setdefault("CIUDAD_CAMARA", hvars.get("municipio_domicilio") or "")
        m_fecha = re.search(r'[Cc]ámara[^,]*\s+el\s+(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})', hist)
        if m_fecha:
            ctx_acto.setdefault("FECHA_INSCRIPCION_CAMARA", m_fecha.group(1))
        m_num = re.search(r'\bNo[.°\s]*(\d+)', hist)
        if m_num:
            ctx_acto.setdefault("NUMERO_INSCRIPCION_CAMARA", m_num.group(1))
        m_libro = re.search(r'\blibro\s+(\w+)', hist, re.I)
        if m_libro:
            ctx_acto.setdefault("LIBRO_INSCRIPCION_CAMARA", m_libro.group(1))
        m_tipo = re.search(r'constituida\s+(?:por\s+)?((?:Documento|Escritura)\s+\w+)', hist, re.I)
        if m_tipo:
            ctx_acto.setdefault("ACTO_CONSTITUCION", m_tipo.group(1).strip())
        for pd in (s.get("personas_detalle") or []):
            if "REPRESENTANTE" in (pd.get("rol") or "").upper():
                ctx_acto.setdefault("CALIDAD_RL", pd.get("rol") or "Representante Legal")
                break
        break


def _prepare_ep_sections(contexto: Dict[str, Any], actos_docs: List[Dict[str, str]], knowledge_rag=None) -> List[Dict[str, Any]]:
    misiones: List[Dict[str, Any]] = []

    personas_activos = contexto.get("PERSONAS_ACTIVOS") or []
    empresa_rl_map = contexto.get("EMPRESA_RL_MAP") or {}
    inmueble = contexto.get("INMUEBLE") or {}
    datos_extra = contexto.get("DATOS_EXTRA") or {}
    comentario_overrides = contexto.get("COMENTARIO_OVERRIDES") or {}

    radicado = str(contexto.get("RADICACION") or "PENDIENTE")
    ciudad = datos_extra.get("CIUDAD") or inmueble.get("ciudad_registro") or "[[PENDIENTE: CIUDAD]]"
    _notaria_raw = datos_extra.get("NOTARIA_NOMBRE") or "[[PENDIENTE: NOTARIA_NOMBRE]]"
    # Normalizar "Notaría X de Ciudad" → "Notaría X del Círculo de Ciudad"
    _notaria_raw = re.sub(
        r"(?i)(notar[íi]a\s+\w+)\s+de\s+(?!el\b|la\b|los\b|las\b|un\b|una\b)([A-ZÁÉÍÓÚÑ])",
        r"\1 del Círculo de \2",
        _notaria_raw,
    )
    notaria = _notaria_raw
    matricula = inmueble.get("matricula") or "[[PENDIENTE: MATRICULA_INMOBILIARIA]]"

    # Resumen de actos para carátula (construido en Python, dinámico)
    resumen_actos = _build_resumen_actos(contexto)

    # APERTURA (fórmula notarial de apertura + datos EP — va entre carátula y primer acto)
    # Resolver ACREEDOR_HIPOTECA para la apertura (nombre del acreedor del acto de hipoteca)
    acreedor_hipoteca_nombre = ""
    actos_a_firmar_list = (contexto.get("NEGOCIO") or {}).get("actos_a_firmar") or []
    for _acto_hip in actos_a_firmar_list:
        if "HIPOTECA" in (_acto_hip.get("nombre") or "").upper():
            _ctx_hip = build_act_context(contexto, _acto_hip, actos_a_firmar_list.index(_acto_hip))
            acreedor_hipoteca_nombre = (
                _ctx_hip.get("EMPRESA_ACREEDOR_1")
                or _ctx_hip.get("EMPRESA_ACREEDOR")
                or _ctx_hip.get("NOMBRE_ACREEDOR_1")
                or _ctx_hip.get("NOMBRE_ACREEDOR")
                or ""
            )
            break
    misiones.append({
        "orden": 10,
        "descripcion": "EP_APERTURA",
        "plantilla_con_huecos": EP_APERTURA_TEMPLATE,
        "contexto_datos": {
            # Solo los tres placeholders que tiene la plantilla.
            # NO pasar PERSONAS ni NEGOCIO para que el LLM no genere contenido extra.
            "CIUDAD": ciudad,
            "DEPARTAMENTO": datos_extra.get("DEPARTAMENTO") or "[[PENDIENTE: DEPARTAMENTO]]",
            "NOTARIA_ENCARGADO": datos_extra.get("NOTARIA_ENCARGADO") or "[[PENDIENTE: NOTARIA_ENCARGADO]]",
            "NOTARIA_NOMBRE": notaria,
        },
        "instrucciones": (
            "CRÍTICO: La plantilla de apertura está COMPLETA — solo la fórmula de apertura. "
            "Rellena ÚNICAMENTE los placeholders [[CIUDAD]], [[NOTARIA_ENCARGADO]] y [[NOTARIA_NOMBRE]]. "
            "NO agregues EP número, NO agregues radicado, NO agregues COMPARECEN, "
            "NO agregues resumen de actos, NO agregues ningún otro texto. Sin markdown."
        ),
    })

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

    ordinales = ["PRIMER", "SEGUNDO", "TERCER", "CUARTO", "QUINTO", "SEXTO", "SÉPTIMO", "OCTAVO", "NOVENO", "DÉCIMO"]

    for idx, acto in enumerate(actos_list):
        if isinstance(acto, dict):
            nombre_acto = str(acto.get("nombre") or "ACTO")
        else:
            nombre_acto = str(acto)

        doc = actos_docs[idx] if idx < len(actos_docs) else {"contenido_legal": "TEXTO NO ENCONTRADO"}

        # Construir texto del acto combinando metadatos limpios (OTORGANTES/VALOR/MATRICULA)
        # con el cuerpo legal (Compareció..., PRIMERO:, etc.)
        # El split_metadata_and_body ya separó en {Cuerpo del Acto}, así que:
        # - metadata_especifica = ==...==\nACTO:...\n==...==\n{Crear meta...}\nOTORGANTES:...
        # - contenido_legal = cuerpo legal sin el bloque de apertura (ciudad/notario)
        body_legal = doc.get("contenido_legal") or "TEXTO NO ENCONTRADO"
        metadata_esp = doc.get("metadata_especifica") or ""

        # Limpiar identificador RAG ===...===\nACTO LINE\n===...=== del principio
        metadata_clean = re.sub(r"^={3,}[^\n]*\n[^\n]*\n={3,}[^\n]*\n?", "", metadata_esp, count=1)
        # Limpiar línea separadora suelta al inicio (solo guiones)
        metadata_clean = re.sub(r"^-{3,}\s*\n?", "", metadata_clean)
        # Limpiar comentarios {Crear metadata/metalada para usar en encabezado}
        metadata_clean = re.sub(r"\{Crear met[^\}]+\}\s*\n?", "", metadata_clean, flags=re.IGNORECASE)
        metadata_clean = metadata_clean.strip()

        # Combinar: encabezado del acto (OTORGANTES/VALOR/MATRICULA) + cuerpo legal
        texto_crudo = (metadata_clean + "\n" + body_legal).strip() if metadata_clean else body_legal

        # CONTEXTO POR ACTO (motor de roles por acto)
        ctx_acto = build_act_context(contexto, acto, idx)

        # Eliminar NEGOCIO del contexto del acto: evita que DataBinder construya
        # RESUMEN_ACTOS dentro de la sección del acto (solo va en la carátula).
        ctx_acto.pop("NEGOCIO", None)

        # Propagar PERSONAS_ACTIVOS y EMPRESA_RL_MAP al contexto de acto
        ctx_acto["PERSONAS_ACTIVOS"] = personas_activos
        ctx_acto["EMPRESA_RL_MAP"] = empresa_rl_map
        ctx_acto["ORDINAL_ACTO"] = ordinales[idx] if idx < len(ordinales) else "SIGUIENTE"
        ctx_acto["NOMBRE_ACTO_ACTUAL"] = nombre_acto
        ctx_acto["CIUDAD"] = ciudad
        ctx_acto["NOTARIA_NOMBRE"] = notaria

        # Propagar ep_antecedente_pacto como variables explícitas para todos los actos
        ep_ant_global = inmueble.get("ep_antecedente_pacto") or {}
        ctx_acto["EP_ANTECEDENTE_NUMERO"] = ep_ant_global.get("numero_ep") or "[[PENDIENTE: EP_ANTECEDENTE_NUMERO]]"
        ctx_acto["EP_ANTECEDENTE_FECHA"] = ep_ant_global.get("fecha") or "[[PENDIENTE: EP_ANTECEDENTE_FECHA]]"
        ctx_acto["EP_ANTECEDENTE_NOTARIA"] = ep_ant_global.get("notaria") or "[[PENDIENTE: EP_ANTECEDENTE_NOTARIA]]"

        # Variables granulares del antecedente (para templates CUARTO/SEGUNDO TITULO)
        _ep_fecha  = ep_ant_global.get("fecha") or ""
        _ep_notaria = ep_ant_global.get("notaria") or ""
        _fm = re.match(r"(\d{1,2})\s+de\s+(\w+)\s+del?\s+(\d{4})", _ep_fecha, re.IGNORECASE)
        ctx_acto["DIA_ESCRITURA_ANTERIOR"]    = _fm.group(1) if _fm else "[[PENDIENTE: DIA_ESCRITURA_ANTERIOR]]"
        ctx_acto["MES_ESCRITURA_ANTERIOR"]    = _fm.group(2) if _fm else "[[PENDIENTE: MES_ESCRITURA_ANTERIOR]]"
        ctx_acto["ANO_ESCRITURA_ANTERIOR"]    = _fm.group(3) if _fm else "[[PENDIENTE: ANO_ESCRITURA_ANTERIOR]]"
        ctx_acto["FECHA_ESCRITURA_ANTERIOR"]  = _ep_fecha or "[[PENDIENTE: FECHA_ESCRITURA_ANTERIOR]]"
        _nm = re.search(r"Notar[íi]a\s+(\w+)(?:\s+del?\s+[Cc][íi]rculo\s+de\s+(.+))?", _ep_notaria, re.IGNORECASE)
        ctx_acto["NUMERO_NOTARIA_ANTERIOR"]  = (_nm.group(1).strip() if _nm else "[[PENDIENTE: NUMERO_NOTARIA_ANTERIOR]]")
        ctx_acto["CIRCULO_NOTARIA_ANTERIOR"] = (_nm.group(2).strip() if _nm and _nm.group(2) else (_ep_notaria or "[[PENDIENTE: CIRCULO_NOTARIA_ANTERIOR]]"))
        ctx_acto["NUMERO_ESCRITURA_ANTERIOR"] = ep_ant_global.get("numero_ep") or "[[PENDIENTE: NUMERO_ESCRITURA_ANTERIOR]]"
        ctx_acto["VENDEDOR_ANTERIOR"]         = ep_ant_global.get("vendedor") or "[[PENDIENTE: VENDEDOR_ANTERIOR]]"
        ctx_acto["TIPO_ADQUISICION_ANTERIOR"] = "COMPRAVENTA"
        def _norm_orip(raw: str) -> str:
            """Normaliza OFICINA_REGISTRO al formato oficial completo."""
            r = (raw or "").strip()
            if not r:
                return ""
            # Ya correcto
            if "REGISTRO DE INSTRUMENTOS" in r.upper():
                return r
            # "Oficina de Instrumentos Públicos" → insertar "Registro de"
            _fixed = re.sub(r"(?i)oficina\s+de\s+instrumentos",
                            "Oficina de Registro de Instrumentos", r)
            if _fixed != r:
                return _fixed
            # Solo ciudad o código → construir nombre completo
            if not r.upper().startswith("OFICINA"):
                return f"Oficina de Registro de Instrumentos Públicos de {r}"
            return r

        _orip_raw = inmueble.get("oficina_registro") or inmueble.get("ciudad_registro") or ""
        ctx_acto["OFICINA_REGISTRO"] = _norm_orip(_orip_raw) if _orip_raw else "[[PENDIENTE: OFICINA_REGISTRO]]"

        from app.pipeline.act_engine import _acto_kind as _ak
        instruccion_acto = (
            f"Encabeza con el separador exacto:\n---{ctx_acto['ORDINAL_ACTO']} ACTO---\n---{nombre_acto.upper()}---\n"
            "Sin markdown. No inventes. "
            "Usa EMPRESA_RL_MAP para completar representantes legales de empresas. "
            "La fecha de comparecencia de ESTA escritura es [[PENDIENTE: FECHA_OTORGAMIENTO]] — "
            "NO uses fechas de escrituras referenciadas en el texto (ej. la fecha del antecedente). "
            "NO incluir RESUMEN DE ACTOS ni listado de los demás actos en esta sección. "
            "Conserva los guiones de relleno '---' que aparecen en la plantilla. "
        )
        # G4B: Inyectar números de matrícula mercantil disponibles de soportes (cámaras de comercio)
        _registros_mercantiles = [
            _sp_hv.get("registro_mercantil")
            for _sp_hv in (contexto.get("_SOPORTES_HALLAZGOS") or [])
            if (_sp_hv or {}).get("registro_mercantil")
        ]
        if _registros_mercantiles:
            instruccion_acto += (
                "MATRÍCULAS MERCANTILES DE EMPRESAS PARTICIPANTES disponibles en el contexto: "
                + "; ".join(_registros_mercantiles) + ". "
                "Usar el número correspondiente para completar el campo 'matrícula N°' o "
                "'bajo el número' al describir cada empresa. NO inventar números. "
            )

        if _ak(nombre_acto) in ("COMPRAVENTA", "COMPRAVENTA_CUOTA"):
            # M-A: COMPRAVENTA también es anterior al cambio de nombre → eliminar nombre_nuevo
            for _k_ma in ("nombre_nuevo", "NOMBRE_NUEVO_PREDIO", "NUEVO_NOMBRE_PREDIO"):
                ctx_acto.pop(_k_ma, None)
            # N-2: Extender M-A pop a DATOS_EXTRA (DataBinder ve la subdictionary completa)
            (ctx_acto.get("DATOS_EXTRA") or {}).pop("NOMBRE_NUEVO_PREDIO", None)
            # N-2b COMPRAVENTA: usar nombre_nuevo (nombre registrado actual, ej: VILLA LUZ),
            # NO nombre_anterior (nombre histórico pre-EP-anterior, ej: LAS DELICIAS).
            _nom_actual_cv = inmueble.get("nombre_nuevo") or inmueble.get("nombre_anterior") or ""
            if _nom_actual_cv and not _is_garbage(_nom_actual_cv):
                ctx_acto["NOMBRE_PREDIO_ACTO"] = _nom_actual_cv
            # Bug 4: Inject buyer empresa keys so DataBinder can fill compareciente section
            compradores_act = ctx_acto.get("COMPRADORES") or []
            for comp in compradores_act:
                nombre_comp = (comp.get("nombre") or "").upper().strip()
                if not nombre_comp:
                    continue
                id_raw = (comp.get("identificacion") or "").upper()
                if "NIT" in id_raw:
                    comp_norm = _normalize_name(nombre_comp)
                    rl_data = empresa_rl_map.get(comp_norm) or {}
                    if rl_data:
                        ctx_acto.setdefault("NOMBRE_COMPRADOR", nombre_comp)
                        ctx_acto.setdefault("NIT_COMPRADOR", comp.get("identificacion") or "")
                        ctx_acto.setdefault("DOMICILIO_COMPRADOR", rl_data.get("domicilio") or rl_data.get("direccion") or "")
                        ctx_acto.setdefault("RL_COMPRADOR", rl_data.get("nombre") or "")
                        ctx_acto.setdefault("CC_RL_COMPRADOR", rl_data.get("identificacion") or "")
                        ctx_acto.setdefault("ESTADO_CIVIL_RL_COMPRADOR", rl_data.get("estado_civil") or "")
                        ctx_acto.setdefault("CONSTITUCION_COMPRADOR", rl_data.get("empresa_constitucion") or "")
                    break
            # N-5B: Si no hay EP constitución, buscar datos de matrícula en cámara soportes
            # Nota: en formato DOCS_PROMPT el NIT está en personas_detalle[].identificacion, no en hallazgos_variables.nit
            if not ctx_acto.get("CONSTITUCION_COMPRADOR") and ctx_acto.get("NIT_COMPRADOR"):
                _comp_nit_digits = re.sub(r"[^0-9]", "", ctx_acto.get("NIT_COMPRADOR") or "")
                for _s_cam in ((contexto.get("FUENTES") or {}).get("soportes") or []):
                    # Buscar NIT en personas_detalle (formato DOCS_PROMPT)
                    _found_nit = False
                    for _pd_cam in (_s_cam.get("personas_detalle") or []):
                        _pd_id_digits = re.sub(r"[^0-9]", "", str(_pd_cam.get("identificacion") or ""))
                        if _pd_id_digits and _pd_id_digits == _comp_nit_digits:
                            _found_nit = True
                            break
                    if not _found_nit:
                        continue
                    _hv_cam = (_s_cam.get("hallazgos_variables") or {})
                    _mat_num = (_hv_cam.get("registro_mercantil") or "").strip()
                    _mat_fecha = (_hv_cam.get("fecha_matricula_mercantil") or "").strip()
                    _mun_dom = (_hv_cam.get("municipio_domicilio") or ciudad or "[[PENDIENTE: MUNICIPIO_DOMICILIO]]").strip()
                    if _mat_num:
                        ctx_acto["MATRICULA_COMPRADOR"] = _mat_num
                        # P-5: Corregir registro_mercantil en DATOS_EXTRA para que DataBinder use el correcto
                        (ctx_acto.get("DATOS_EXTRA") or {})["registro_mercantil_comprador"] = _mat_num
                    if _mat_fecha:
                        ctx_acto["FECHA_MATRICULA_COMPRADOR"] = _mat_fecha
                    if _mat_num or _mat_fecha:
                        # P-5: Limpiar HISTORIA.constitucion (pertenece a INSELEM, no al comprador)
                        (ctx_acto.get("HISTORIA") or {}).pop("constitucion", None)
                        (ctx_acto.get("HISTORIA") or {}).pop("reformas_especiales", None)
                        instruccion_acto += (
                            "CONSTITUCIÓN COMPRADOR (empresa SAS): NO tiene EP de constitución disponible. "
                            "Usar: 'constituida conforme consta en los documentos inscritos en la Cámara de "
                            f"Comercio de {_mun_dom}, bajo el número {_mat_num} del libro correspondiente"
                            + (f", inscrita el {_mat_fecha}" if _mat_fecha else "")
                            + "'. "
                            f"La matrícula mercantil del comprador en esta escritura es {_mat_num}. "
                            "NO usar ningún otro número de matrícula que aparezca en HISTORIA, DATOS_EXTRA "
                            "o ANTECEDENTE — esos pertenecen a la empresa vendedora, no al comprador. "
                            "ELIMINAR todos los [[PENDIENTE: CONSTITUCION_COMPRADOR]], "
                            "[[PENDIENTE: NOTARIA_CONSTITUCION_COMPRADOR]], "
                            "[[PENDIENTE: CIUDAD_CONSTITUCION_COMPRADOR]], "
                            "[[PENDIENTE: FECHA_INSCRIPCION_COMPRADOR]], "
                            "[[PENDIENTE: ULTIMA_REFORMA_COMPRADOR]]. "
                        )
                    break
            instruccion_acto += (
                "Para el COMPRADOR empresa: usar NOMBRE_COMPRADOR, NIT_COMPRADOR, RL_COMPRADOR, "
                "CC_RL_COMPRADOR, DOMICILIO_COMPRADOR, CONSTITUCION_COMPRADOR, ESTADO_CIVIL_RL_COMPRADOR "
                "del contexto para llenar la ficha de compareciente del comprador. "
            )
            # Consistencia de nombre: en COMPRAVENTA, la propiedad se vende con su nombre ANTERIOR
            # (antes del cambio de nombre que se formaliza en el acto CAMBIO DE NOMBRE de esta misma EP).
            _nom_instruc_cv = inmueble.get("nombre_nuevo") or inmueble.get("nombre_anterior") or ""
            if _nom_instruc_cv and not _is_garbage(_nom_instruc_cv):
                instruccion_acto += (
                    f"NOMBRE DEL INMUEBLE EN ESTA VENTA: el predio se describe con su nombre ACTUAL "
                    f"al momento de la venta = '{_nom_instruc_cv}' — usar NOMBRE_PREDIO_ACTO del contexto. "
                    "NO usar ningún nombre histórico anterior ni el nombre que se asignará en el "
                    "acto CAMBIO DE NOMBRE de esta misma escritura. "
                )
            # P-2: Redacción gramaticalmente correcta del TÍTULO DE ADQUISICIÓN (cláusula SEGUNDO)
            instruccion_acto += (
                "CLÁUSULA SEGUNDO - TÍTULO DE ADQUISICIÓN: la redacción correcta es: "
                "'el inmueble fue adquirido por LA PARTE VENDEDORA, mediante escritura pública número [N] "
                "de fecha [fecha] de la Notaría [X], en la cual [VENDEDOR_ANTERIOR] le transfirió el dominio "
                "[con pacto de retroventa, pacto cancelado en el [ordinal] Acto del presente instrumento]. "
                "Debidamente registrada en [ORIP].' "
                "NUNCA usar 'por [EMPRESA] le transfirió' como si el sujeto de 'fue adquirido' fuera la empresa. "
                "El sujeto de 'fue adquirido' es siempre LA PARTE VENDEDORA. "
            )
            # Bug 8 / H-C: Inject forma_de_pago into COMPRAVENTA context
            fdp = datos_extra.get("FORMA_DE_PAGO") or ""
            if fdp and not _is_garbage(fdp):
                ctx_acto["FORMA_DE_PAGO"] = fdp
                instruccion_acto += (
                    f"FORMA_DE_PAGO OBLIGATORIA: en la cláusula CUARTO usar exactamente: '{fdp}'. "
                    "Si la forma de pago menciona crédito hipotecario, incluir referencia al "
                    "Acto de hipoteca del mismo instrumento. No resumir ni simplificar. "
                )
                # M-C: Forma de pago con cuotas/fechas → cambiar wording "recibido a su entera satisfacción"
                _fdp_upper = fdp.upper()
                _CUOTAS_KW = (
                    "CUOTA", "MENSUAL", "BIMESTRAL", "TRIMESTRAL",
                    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
                    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
                )
                if any(kw in _fdp_upper for kw in _CUOTAS_KW):
                    instruccion_acto += (
                        "PRECIO CON PAGOS PENDIENTES: la forma de pago incluye cuotas a futuro. "
                        "En cláusula CUARTO-PRECIO: "
                        "1) NO usar 'haber recibido... a su entera satisfacción' "
                        "(esa frase implica recibo total). "
                        "2) La vendedora NO ha recibido suma alguna de contado al momento de la firma. "
                        "3) Usar: 'precio que la parte compradora pagará de la siguiente manera: [FORMA_DE_PAGO]'. "
                        "4) Si la forma de pago menciona hipoteca/crédito, indicar: "
                        "'...y la suma de [monto hipoteca] garantizada mediante hipoteca constituida "
                        "en el [ordinal] Acto del presente instrumento.' "
                        "5) Escribir los montos en pesos completos (ej: $595.000.000) y también en letras "
                        "(QUINIENTOS NOVENTA Y CINCO MILLONES). "
                        "DECLARACIÓN DEL COMPRADOR (párrafo de aceptación): "
                        "6) NO usar 'que ha pagado el precio... a entera satisfacción' "
                        "(el precio no ha sido pagado en su totalidad al momento de la firma). "
                        "7) Usar en cambio: 'que acepta la presente escritura con el contrato de venta "
                        "en ella contenido a favor de la sociedad que representa; que se obliga a pagar "
                        "el precio en la forma pactada en esta escritura y que recibe el inmueble comprado "
                        "para la sociedad que representa.' "
                        "8) Expandir TODAS las abreviaturas monetarias: si la forma de pago dice "
                        "'N cuotas de XM' o 'X cuotas de $X.000.000', escribir CADA cuota con su "
                        "monto individual en número y letras. "
                        "Ej: '5 cuotas de $119.000.000 (CIENTO DIECINUEVE MILLONES DE PESOS)' — "
                        "una por cada mes/período indicado. NUNCA colapsar en monto total solamente. "
                        "Nunca usar abreviaturas 'M' para millones en escritura pública. "
                        "9) Añadir en la cláusula CUARTO, antes de detallar la forma de pago, la frase: "
                        "'Suma que LA PARTE VENDEDORA declara NO haber recibido de contado "
                        "al momento del otorgamiento de esta escritura, quedando pendiente de pago "
                        "en la forma a continuación indicada:' "
                    )
            # H-D: Servidumbres pasivas desde CTL
            _serv = datos_extra.get("SERVIDUMBRES_PASIVAS") or datos_extra.get("servidumbres_pasivas") or ""
            if _serv and not _is_garbage(_serv):
                ctx_acto["SERVIDUMBRES_PASIVAS"] = _serv
                instruccion_acto += (
                    f"SERVIDUMBRES GENÉRICAS: el inmueble tiene las siguientes servidumbres pasivas: {_serv}. "
                    "Incluirlas en la cláusula TERCERO (limitaciones y gravámenes). "
                )
            # L-2: Servidumbre específica constituida (anotada en CTL por EP)
            _serv_const = (contexto.get("HISTORIA") or {}).get("servidumbres_constituidas") or ""
            # L-2b: Si no hay campo explícito, escanear tradicion de soportes
            if not _serv_const or _is_garbage(_serv_const):
                for _s_sv in ((contexto.get("FUENTES") or {}).get("soportes") or []):
                    for _trad_entry in ((_s_sv.get("datos_inmueble") or {}).get("tradicion") or []):
                        if not isinstance(_trad_entry, dict):
                            continue
                        _acto_trad = (_trad_entry.get("acto") or "").upper()
                        if "SERVIDUMBRE" in _acto_trad:
                            _ep_sv = _trad_entry.get("escritura_publica") or ""
                            _not_sv = _trad_entry.get("notaria") or ""
                            _fec_sv = _trad_entry.get("fecha") or ""
                            _tipo_sv = _trad_entry.get("acto") or "Servidumbre"
                            if _ep_sv:
                                _serv_const = (
                                    f"{_tipo_sv} — EP N° {_ep_sv}"
                                    + (f" de fecha {_fec_sv}" if _fec_sv else "")
                                    + (f", otorgada en {_not_sv}" if _not_sv else "")
                                )
                            break
                    if _serv_const:
                        break
            if _serv_const and not _is_garbage(_serv_const):
                ctx_acto["SERVIDUMBRE_CONSTITUIDA"] = _serv_const
                # Q-2: Instrucción verbatim para que DataBinder cite EP y notaría exactos
                instruccion_acto += (
                    "SERVIDUMBRE ESPECÍFICA CONSTITUIDA POR EP: en la cláusula TERCERO-LIBERTAD, "
                    f"AÑADIR TEXTUALMENTE después de las servidumbres genéricas: "
                    f"'Adicionalmente, el inmueble tiene constituida {_serv_const}.' "
                    "NO omitir número de escritura ni notaría. Usar este texto tal cual, sin parafrasear. "
                )
            # B1: Precio autoritativo desde cuantía de radicación (ignora precios de antecedentes)
            _cuantia_acto = int(acto.get("cuantia") or 0) if isinstance(acto, dict) else 0
            if _cuantia_acto:
                ctx_acto["PRECIO_COMPRAVENTA"] = f"${_cuantia_acto:,.0f}".replace(",", ".")
                instruccion_acto += (
                    f"El precio EXACTO de esta compraventa es {ctx_acto['PRECIO_COMPRAVENTA']} "
                    "(cuantía de la hoja de radicación). Ignorar cualquier precio extraído de "
                    "documentos de antecedente histórico. Usar este valor en la cláusula CUARTO-PRECIO. "
                )
            # Bug 9: Detect baldío and add Ley 160/1994 clause
            historia_text = (
                inmueble.get("historia") or inmueble.get("tradicion") or
                (contexto.get("HISTORIA") or {}).get("tradicion") or
                (contexto.get("HISTORIA") or {}).get("historia") or ""
            ).upper()
            es_baldio = any(kw in historia_text for kw in ("BALDIO", "INCODER", "INCORA", "REFORMA AGRARIA"))
            if es_baldio:
                ctx_acto["BALDIOS_APLICA"] = True
                ctx_acto["BALDIOS_CLAUSE_FULL"] = _BALDIOS_CLAUSE_FULL
                instruccion_acto += (
                    "CLÁUSULA BALDÍOS — OBLIGATORIO VERBATIM: añadir como cláusula numerada SEPARADA "
                    "(SÉPTIMO u OCTAVO según numeración del acto) el siguiente texto EXACTO, sin resumir:\n\n"
                    f"{_BALDIOS_CLAUSE_FULL}\n\n"
                    "NO convertir en una sola línea. NO abreviar. Incluir el PARÁGRAFO y la NOTA completos. "
                )
            # Detección de predio rural (vereda/corregimiento o unidad hectáreas)
            _area_raw = str(inmueble.get("area") or "").upper()
            es_rural = bool(
                inmueble.get("vereda") or
                inmueble.get("corregimiento") or
                "HECTAR" in _area_raw or
                (str(inmueble.get("unidad_area") or "").upper() in ("HA", "HECTAREA", "HECTÁREA", "HECTAREAS", "HECTÁREAS"))
            )
            if es_rural:
                ctx_acto["RURAL_APLICA"] = True
                instruccion_acto += (
                    "PREDIO RURAL — Ley 160 de 1994 (TEXTO OBLIGATORIO, NO RESUMIR): "
                    "En la cláusula correspondiente (REDAM o cláusula separada de advertencia notarial) "
                    "incluir ÍNTEGRAMENTE: "
                    "a) La Notaría advierte que conforme a la Sentencia SC-877/2022 de la Corte Suprema "
                    "de Justicia y la Ley 160 de 1994, los predios rurales no pueden acumularse en "
                    "extensión superior a la Unidad Agrícola Familiar (UAF). "
                    "b) El(la) vendedor(a) declara bajo la gravedad del juramento que enajena "
                    "VOLUNTARIAMENTE, libre de toda presión, intimidación o desplazamiento forzado. "
                    "c) El(la) comprador(a) declara bajo juramento que no es titular de otros predios "
                    "que superen la UAF del municipio o región y que cumple los requisitos del Art. 72 "
                    "de la Ley 160 de 1994. "
                    "d) Se verificó en el portal VUR (Ventanilla Única de Registro) que el comprador "
                    "no acumula predios en exceso de la UAF. "
                    "e) Las declaraciones juramentadas se adjuntan para su protocolización. "
                )
            # J-1/I-4: Si hay ep_antecedente_pacto, aclarar contexto de retroventa con phrasing explícito
            ep_ant_trad = inmueble.get("ep_antecedente_pacto") or {}
            if ep_ant_trad.get("numero_ep") and not _is_garbage(ep_ant_trad.get("numero_ep") or ""):
                _ref_canc = contexto.get("_ACTO_CANCELACION_REFERENCIA") or "el Primer Acto del presente instrumento"
                # Obtener nombre completo del vendedor en EP antecedente (= empresa con la retroventa)
                _disp_map = contexto.get("EMPRESA_DISPLAY_MAP") or {}
                _empresa_ant_names = [_disp_map.get(k) or k for k in (empresa_rl_map or {})]
                _vendedor_ep_ant = max(_empresa_ant_names, key=len) if _empresa_ant_names else ""
                if _vendedor_ep_ant:
                    ctx_acto["VENDEDOR_EP_ANTECEDENTE"] = _vendedor_ep_ant
                instruccion_acto += (
                    f"TRADICIÓN VENDEDOR: La parte vendedora adquirió el inmueble mediante Escritura "
                    f"Pública número {ep_ant_trad.get('numero_ep')} de {ep_ant_trad.get('fecha', '')} "
                    f"de {ep_ant_trad.get('notaria', '')}, en la cual "
                    f"{_vendedor_ep_ant or 'EL ANTERIOR PROPIETARIO'} le transfirió el dominio "
                    f"con pacto de retroventa — pacto cancelado mediante {_ref_canc}. "
                    "REDACTAR EXACTAMENTE ASÍ: '[VENDEDORA] adquirió el inmueble mediante escritura pública "
                    "número [num] de fecha [fecha] de [notaría], en la cual VENDEDOR_EP_ANTECEDENTE le "
                    "transfirió el dominio con pacto de retroventa, pacto cancelado en [ref]'. "
                    "NUNCA escribir 'compraventa efectuada a X' ni 'fue adquirido ... de X' en voz pasiva. "
                    "Usar voz activa: 'X le transfirió' o '[vendedora] adquirió DE X'. "
                )
            # J-2: Registrar vendedores de esta compraventa para uso en HIPOTECA (excluye empresa NIT)
            _vendedores_cv = [
                v.get("nombre") for v in (ctx_acto.get("VENDEDORES") or [])
                if v.get("nombre") and "NIT" not in (v.get("identificacion") or "").upper()
            ]
            if _vendedores_cv:
                contexto["_VENDEDOR_COMPRAVENTA_NOMBRES"] = " y ".join(_vendedores_cv)
            # B3: Registrar referencia cruzada del acto de compraventa para uso en Actos 3 y 4
            contexto["_ACTO_COMPRAVENTA_REFERENCIA"] = (
                f"el {ctx_acto.get('ORDINAL_ACTO', ordinales[idx])} Acto del presente instrumento"
            )
        elif _ak(nombre_acto) == "CANCELACION":
            # M-A: CANCELACION ocurre ANTES del cambio de nombre → eliminar nombre_nuevo del ctx_acto
            for _k_ma in ("nombre_nuevo", "NOMBRE_NUEVO_PREDIO", "NUEVO_NOMBRE_PREDIO"):
                ctx_acto.pop(_k_ma, None)
            # N-2: Extender M-A pop a DATOS_EXTRA (DataBinder ve la subdictionary completa)
            (ctx_acto.get("DATOS_EXTRA") or {}).pop("NOMBRE_NUEVO_PREDIO", None)
            # N-2b CANCELACION: usar nombre_nuevo (nombre registrado actual, ej: VILLA LUZ),
            # NO nombre_anterior (nombre histórico pre-EP-anterior, ej: LAS DELICIAS).
            _nom_actual_canc = inmueble.get("nombre_nuevo") or inmueble.get("nombre_anterior") or ""
            if _nom_actual_canc and not _is_garbage(_nom_actual_canc):
                ctx_acto["NOMBRE_PREDIO_ACTO"] = _nom_actual_canc
                instruccion_acto += (
                    f"NOMBRE DEL PREDIO EN ESTE ACTO: '{_nom_actual_canc}' (nombre registrado actual "
                    "antes del cambio que ocurre en un Acto POSTERIOR). Usar NOMBRE_PREDIO_ACTO "
                    "y NO usar NOMBRE_NUEVO_PREDIO. "
                )
            instruccion_acto += (
                "NOMBRE DEL PREDIO: Usar SOLO el nombre histórico del inmueble tal como aparece "
                "en INMUEBLE.nombre o en ep_antecedente_pacto. NO añadir 'hoy denominado X' ni "
                "ninguna variante — el cambio de nombre ocurre en un Acto POSTERIOR de este "
                "instrumento, no en este acto de Cancelación. "
            )
            # Registrar referencia cruzada del acto de cancelación (para COMPRAVENTA cláusula de tradición)
            contexto["_ACTO_CANCELACION_REFERENCIA"] = (
                f"el {ctx_acto.get('ORDINAL_ACTO', ordinales[idx])} Acto del presente instrumento"
            )
            instruccion_acto += (
                "Para CANCELACION PACTO RETROVENTA: en la cláusula PRIMERO, referencia "
                "el campo INMUEBLE.ep_antecedente_pacto (numero_ep, fecha, notaria) como la escritura "
                "que se cancela — NO uses INMUEBLE.tradicion para obtener el número de EP. "
            )
            ctx_acto["PLAZO_RETROVENTA"] = (
                datos_extra.get("plazo_retroventa")
                or "[[PENDIENTE: PLAZO_RETROVENTA]]"
            )
            ep_ant = inmueble.get("ep_antecedente_pacto") or {}
            # Cuantía autoritativa: acto.cuantia radicación > ep_antecedente valor
            # NOTA: PRECIO_CANCELACION fue eliminado de comentario_overrides para evitar que
            # el LLM confunda el precio de compraventa (595M+10M) con el valor del pacto (600M).
            _cuantia_canc = int(acto.get("cuantia") or 0) if isinstance(acto, dict) else 0
            _precio_canc_fmt = f"${_cuantia_canc:,.0f}".replace(",", ".") if _cuantia_canc else ""
            _ep_ant_valor = (ep_ant.get("valor") or "").strip()
            _valor_canc_autoritativo = (
                (_precio_canc_fmt if _cuantia_canc else None)
                or (_ep_ant_valor if _ep_ant_valor and not _is_garbage(_ep_ant_valor) else None)
                or "[[PENDIENTE: VALOR_ANTECEDENTE]]"
            )
            ctx_acto["VALOR_ANTECEDENTE"] = _valor_canc_autoritativo
            ctx_acto["PRECIO_CANCELACION"] = _valor_canc_autoritativo
            # Suprimir total_venta_hoy para que no contamine el valor del pacto
            for _k_tvh in ("total_venta_hoy", "TOTAL_VENTA_HOY"):
                ctx_acto.pop(_k_tvh, None)
                (ctx_acto.get("DATOS_EXTRA") or {}).pop(_k_tvh, None)
            instruccion_acto += (
                f"VALOR DEL PACTO CANCELADO (AUTORITATIVO): EXACTAMENTE {_valor_canc_autoritativo} "
                "— usar PRECIO_CANCELACION del contexto en TODOS los párrafos donde aparezca el monto "
                "(SEGUNDO y CUARTO). "
                "Este valor viene del ANTECEDENTE EP (escritura que se cancela), NO del precio de compraventa. "
                "NO usar total_venta_hoy ni ninguna suma de cuotas de compraventa. "
            )
            # Fix dirección del pacto: identificar quien vendió y quien compró en el antecedente EP.
            # ep_antecedente_pacto.vendedor puede estar mal extraído por Gemini (muestra empresa en lugar
            # de la persona natural). La regla: persona CC (natural) fue la VENDEDORA original,
            # empresa NIT fue la COMPRADORA con pacto de retroventa.
            _canc_cc_persons = [p for p in personas_activos if "NIT" not in (p.get("identificacion") or "").upper()]
            _canc_nit_persons = [p for p in personas_activos if "NIT" in (p.get("identificacion") or "").upper()]
            if _canc_cc_persons and _canc_nit_persons:
                _vendedor_original_canc = _canc_cc_persons[0].get("nombre") or ""
                _empresa_receptora_canc = _canc_nit_persons[0].get("nombre") or ""
                if _vendedor_original_canc and _empresa_receptora_canc:
                    # Override VENDEDOR_ANTERIOR (puede estar mal extraído en ep_antecedente_pacto)
                    ctx_acto["VENDEDOR_ANTERIOR"] = _vendedor_original_canc
                    instruccion_acto += (
                        f"DIRECCIÓN DEL PACTO DE RETROVENTA (CRÍTICO): "
                        f"En la escritura antecedente EP N° {ep_ant.get('numero_ep', '')}, fue "
                        f"'{_vendedor_original_canc}' (persona natural, CC) quien TRANSFIRIÓ "
                        f"el inmueble A FAVOR DE '{_empresa_receptora_canc}' (empresa, NIT) "
                        "con pacto de retroventa. "
                        f"La dirección correcta es: '{_vendedor_original_canc}' → '{_empresa_receptora_canc}'. "
                        "NO invertir esta dirección. "
                        "Si ep_antecedente_pacto.vendedor muestra el nombre de la empresa, IGNORAR — "
                        "está mal extraído. VENDEDOR_ANTERIOR en el contexto es la persona natural. "
                    )
            # I-1/I-2: Inyectar RL explícito para todos los comparecientes empresa en CANCELACION
            for role_key in ("OTORGANTES", "SOLICITANTES", "COMPRADORES", "INTERVINIENTES"):
                for part in (ctx_acto.get(role_key) or []):
                    id_raw = (part.get("identificacion") or "").upper()
                    if "NIT" in id_raw:
                        part_norm = _normalize_name(part.get("nombre") or "")
                        rl_data = empresa_rl_map.get(part_norm) or {}
                        if rl_data:
                            ctx_acto.setdefault("NIT_EMPRESA_CANCELACION", part.get("identificacion") or "")
                            ctx_acto.setdefault("NOMBRE_EMPRESA_CANCELACION", part.get("nombre") or "")
                            ctx_acto.setdefault("RL_CANCELACION", rl_data.get("nombre") or "")
                            ctx_acto.setdefault("CC_RL_CANCELACION", rl_data.get("identificacion") or "")
                            ctx_acto.setdefault("ESTADO_CIVIL_RL_CANCELACION", rl_data.get("estado_civil") or "")
                            # N-5C: usar datos_contacto.domicilio (más confiable que .direccion OCR)
                            _rl_dom_c = (rl_data.get("datos_contacto") or {}).get("domicilio") or rl_data.get("domicilio") or ""
                            _rl_ciudad_c = _extract_city_from_address(_rl_dom_c) if _rl_dom_c else ""
                            if _rl_ciudad_c:
                                ctx_acto.setdefault("CIUDAD_RL_CANCELACION", _rl_ciudad_c)
                                instruccion_acto += (
                                    f"RL empresa en CANCELACION: domiciliado en {_rl_ciudad_c}. "
                                )
                        break
            # Q-1: Fallback domicilio RL cuando name-match falla (normalization mismatch entre
            # versión radicación y versión soporte del nombre de la empresa)
            if not ctx_acto.get("CIUDAD_RL_CANCELACION"):
                for _rl_cand in empresa_rl_map.values():
                    _rl_repr = _normalize_name(_rl_cand.get("representa_a") or "")
                    if not _rl_repr:
                        continue
                    _found_match_q1 = False
                    for role_key3 in ("OTORGANTES", "SOLICITANTES", "COMPRADORES", "INTERVINIENTES"):
                        for _part3 in (ctx_acto.get(role_key3) or []):
                            if "NIT" not in ((_part3.get("identificacion") or "").upper()):
                                continue
                            _comp_norm3 = _normalize_name(_part3.get("nombre") or "")
                            # Match parcial: primeros 8 chars normalizados coinciden
                            if _rl_repr and _comp_norm3 and (
                                _rl_repr[:8] in _comp_norm3 or _comp_norm3[:8] in _rl_repr
                            ):
                                _found_match_q1 = True
                                break
                        if _found_match_q1:
                            break
                    if not _found_match_q1:
                        continue
                    _rl_dom_q1 = (
                        (_rl_cand.get("datos_contacto") or {}).get("domicilio")
                        or _rl_cand.get("domicilio") or ""
                    )
                    _rl_city_q1 = _rl_dom_q1.strip().split(",")[0].strip() if _rl_dom_q1 else ""
                    if _rl_city_q1 and not _is_garbage(_rl_city_q1):
                        ctx_acto.setdefault("CIUDAD_RL_CANCELACION", _rl_city_q1)
                        if not ctx_acto.get("RL_CANCELACION"):
                            ctx_acto.setdefault("RL_CANCELACION", _rl_cand.get("nombre") or "")
                            ctx_acto.setdefault("CC_RL_CANCELACION", _rl_cand.get("identificacion") or "")
                            ctx_acto.setdefault("ESTADO_CIVIL_RL_CANCELACION", _rl_cand.get("estado_civil") or "")
                        instruccion_acto += (
                            f"DOMICILIO RL EMPRESA EN CANCELACION: {_rl_cand.get('nombre', '')} "
                            f"está domiciliado en {_rl_city_q1} — usar esta ciudad, "
                            f"NO la dirección OCR de la cédula que pueda aparecer en EMPRESA_RL_MAP. "
                        )
                    break
            instruccion_acto += (
                "Si un compareciente es empresa (NIT), usar RL_CANCELACION y CC_RL_CANCELACION "
                "del contexto como nombre y cédula del representante legal — NO la razón social. "
            )
        elif _ak(nombre_acto) == "CAMBIO_NOMBRE":
            instruccion_acto += (
                "CRÍTICO: El único compareció ES EL COMPRADOR/NUEVO PROPIETARIO, NO la vendedora anterior. "
                "Si el nuevo propietario es empresa, usar OBLIGATORIAMENTE su representante legal de "
                "EMPRESA_RL_MAP como el compareciente (solo el RL persona natural, actuando en nombre "
                "de la empresa). El vendedor/otorgante anterior NO aparece en este acto bajo ningún concepto. "
                "Si hay NOMBRE_ANTERIOR_PREDIO en el contexto, úsalo para indicar cómo se llamaba "
                "el predio antes del cambio. "
            )
            # Bug 5: Inject RL keys for the solicitante empresa so DataBinder uses the correct person
            solicitantes_act = ctx_acto.get("SOLICITANTES") or []
            sol_norm = ""
            for sol in solicitantes_act:
                nombre_sol = (sol.get("nombre") or "").upper().strip()
                id_raw = (sol.get("identificacion") or "").upper()
                if "NIT" in id_raw:
                    sol_norm = _normalize_name(nombre_sol)
                    rl_data = empresa_rl_map.get(sol_norm) or {}
                    if rl_data:
                        ctx_acto.setdefault("NOMBRE_SOLICITANTE", nombre_sol)
                        ctx_acto.setdefault("NIT_SOLICITANTE", sol.get("identificacion") or "")
                        ctx_acto.setdefault("RL_SOLICITANTE", rl_data.get("nombre") or "")
                        ctx_acto.setdefault("CC_RL_SOLICITANTE", rl_data.get("identificacion") or "")
                        ctx_acto.setdefault("ESTADO_CIVIL_RL_SOLICITANTE", rl_data.get("estado_civil") or "")
                    break
            # CAMBIO_NOMBRE: NOMBRE_ANTERIOR_PREDIO = nombre registrado actual (nombre_nuevo, ej: VILLA LUZ)
            # — el nombre que TENÍA el predio ANTES de este cambio, no el histórico pre-EP-anterior.
            # NUEVO_NOMBRE_PREDIO = el nombre que quiere asignar el comprador (ej: HACIENDA LEJANIAS)
            _nom_antes_cambio = inmueble.get("nombre_nuevo") or inmueble.get("nombre_anterior") or ""
            if _nom_antes_cambio and not _is_garbage(_nom_antes_cambio):
                ctx_acto["NOMBRE_ANTERIOR_PREDIO"] = _nom_antes_cambio
            # G3B: Buscar nombre_nuevo_asignado desde soportes individuales (declaración comprador)
            # Prioridad: lo que el COMPRADOR llama el predio (nombre que quiere asignar, ej. HACIENDA LEJANÍAS)
            _nombre_asignado_comprador = None
            for _sp_hv in (contexto.get("_SOPORTES_HALLAZGOS") or []):
                _nv = (_sp_hv or {}).get("nombre_nuevo_asignado")
                if _nv and not _is_garbage(_nv):
                    _nombre_asignado_comprador = _nv.upper()
                    break
            # B2: Nombre nuevo del predio — prioridad: comentario > soportes > radicación > inmueble
            _nombre_nuevo_b2 = (
                comentario_overrides.get("NOMBRE_NUEVO_PREDIO")  # H-A: prioridad máxima (usuario)
                or _nombre_asignado_comprador                    # G3B: nombre asignado por comprador
                or datos_extra.get("NOMBRE_NUEVO_PREDIO")        # radicación
                or inmueble.get("nombre_nuevo") or ""
            )
            if _nombre_nuevo_b2 and not _is_garbage(_nombre_nuevo_b2):
                ctx_acto["NOMBRE_NUEVO_PREDIO"] = _nombre_nuevo_b2.upper()
                ctx_acto["NUEVO_NOMBRE_PREDIO"] = _nombre_nuevo_b2.upper()  # alias compatibilidad
                contexto["_NOMBRE_NUEVO_PREDIO_CALCULADO"] = _nombre_nuevo_b2.upper()
                instruccion_acto += (
                    f"El nuevo nombre del predio es '{_nombre_nuevo_b2.upper()}'. "
                    "Usar este nombre exactamente en la cláusula CUARTO. "
                )
            # B3: Tradición inmediata — referenciar acto compraventa del mismo instrumento
            _ref_comp_b3 = contexto.get("_ACTO_COMPRAVENTA_REFERENCIA") or ""
            if _ref_comp_b3:
                ctx_acto["ACTO_COMPRAVENTA_REFERENCIA"] = _ref_comp_b3
                instruccion_acto += (
                    f"La adquisición del inmueble se efectuó mediante {_ref_comp_b3}. "
                    "En la cláusula SEGUNDO/TRADICIÓN, referenciar SIEMPRE ese acto del presente instrumento, "
                    "NO ninguna escritura pública de antecedente histórico previo. "
                )
            # Inyectar datos de Cámara de Comercio para la empresa solicitante
            _enrich_camara_data(ctx_acto, sol_norm, contexto)
            instruccion_acto += (
                "CRÍTICO: NOMBRE_ANTERIOR_PREDIO es el nombre de la FINCA/PREDIO antes del cambio, "
                "NO el nombre anterior de la empresa solicitante. Son cosas distintas. "
                "NO generes párrafo de 'transformación de razón social' ni 'cambio de denominación "
                "social' a menos que ACTO_TRANSFORMACION esté explícitamente en el contexto. "
                "Datos de constitución disponibles: CALIDAD_RL, DOMICILIO_SOCIEDAD, "
                "ACTO_CONSTITUCION, FECHA_INSCRIPCION_CAMARA, NUMERO_INSCRIPCION_CAMARA, "
                "LIBRO_INSCRIPCION_CAMARA, CIUDAD_CAMARA — úsalos directamente. "
            )
        elif _ak(nombre_acto) == "HIPOTECA":
            # Cuantía autoritativa desde radicación para este acto
            _cuantia_hip = int(acto.get("cuantia") or 0) if isinstance(acto, dict) else 0
            # Fallback: extraer monto del crédito desde FORMA_DE_PAGO cuando cuantia=0
            # Patrón: "10M por crédito garantizado con hipoteca" → $10.000.000
            if not _cuantia_hip:
                _fdp_hip = datos_extra.get("FORMA_DE_PAGO") or ""
                _hip_m = re.search(
                    r'(\d+(?:[.,]\d+)?)\s*[Mm]\s*(?:pesos\s*)?(?:por|de)\s*(?:cr[eé]dito|pr[eé]stamo)[^.]*hipoteca'
                    r'|hipoteca[^.]*?(\d+(?:[.,]\d+)?)\s*[Mm]'
                    r'|(\d+(?:[.,]\d+)?)\s*[Mm].*?garantizad[ao].*?hipoteca',
                    _fdp_hip, re.IGNORECASE
                )
                if _hip_m:
                    _hip_val_str = (_hip_m.group(1) or _hip_m.group(2) or _hip_m.group(3) or "").replace(",", "").replace(".", "")
                    if _hip_val_str:
                        _cuantia_hip = int(float(_hip_val_str)) * 1_000_000
            _precio_hip_fmt = f"${_cuantia_hip:,.0f}".replace(",", ".") if _cuantia_hip else ""
            if _cuantia_hip:
                ctx_acto["MONTO_CREDITO_HIPOTECA"] = _precio_hip_fmt
                instruccion_acto += (
                    f"MONTO DEL CRÉDITO GARANTIZADO CON HIPOTECA: el valor del crédito "
                    f"comunicado en este acto es EXACTAMENTE {_precio_hip_fmt} "
                    "(extraído de radicación o FORMA_DE_PAGO). "
                    "Usar este valor en la cláusula NOTA de comunicación del crédito (Decreto 1681). "
                    "NO confundir con el valor de la cancelación del pacto de retroventa ni con el "
                    "precio total de la compraventa — son actos distintos con montos distintos. "
                )
            instruccion_acto += (
                "ESTE ACTO CONSTITUYE UNA NUEVA HIPOTECA. NO es cancelación de ninguna hipoteca anterior. "
                "DEUDOR = COMPRADORES[0] del contexto (quien hipoteca el bien que acaba de adquirir). "
                "ACREEDOR = ACREEDORES[0] del contexto (el vendedor que financia). "
                "Ignorar cualquier referencia a hipotecas de escrituras anteriores — son irrelevantes. "
            )
            # I-1/I-2: Inyectar RL explícito para deudor/acreedor empresa en HIPOTECA
            for role_key, prefix in (("COMPRADORES", "DEUDOR"), ("VENDEDORES", "ACREEDOR"), ("ACREEDORES", "ACREEDOR")):
                for part in (ctx_acto.get(role_key) or []):
                    id_raw = (part.get("identificacion") or "").upper()
                    if "NIT" in id_raw:
                        part_norm = _normalize_name(part.get("nombre") or "")
                        rl_data = empresa_rl_map.get(part_norm) or {}
                        if rl_data:
                            ctx_acto.setdefault(f"NIT_{prefix}", part.get("identificacion") or "")
                            ctx_acto.setdefault(f"NOMBRE_{prefix}", part.get("nombre") or "")
                            ctx_acto.setdefault(f"RL_{prefix}", rl_data.get("nombre") or "")
                            ctx_acto.setdefault(f"CC_RL_{prefix}", rl_data.get("identificacion") or "")
                            ctx_acto.setdefault(f"ESTADO_CIVIL_RL_{prefix}", rl_data.get("estado_civil") or "")
                        break
            instruccion_acto += (
                "BENEFICIARIO/ACREEDOR DE LA HIPOTECA: si el acreedor es una EMPRESA (tiene NIT), "
                "el BENEFICIARIO FORMAL de la hipoteca es LA EMPRESA (persona jurídica), NO el RL. "
                "La hipoteca se constituye 'a favor de [NOMBRE_EMPRESA], NIT [NIT_EMPRESA]' y "
                "el RL actúa REPRESENTANDO a la empresa, no como acreedor personal. "
                "Formato correcto: 'hipoteca a favor de [EMPRESA] NIT [NIT], "
                "representada por [RL_NOMBRE] CC [CC_RL]'. "
                "INCORRECTO: 'hipoteca a favor de [RL_NOMBRE]' — el RL no es el acreedor. "
            )
            # K-1b: Instrucción explícita empresa→RL (blindaje contra self-reference en empresa_rl_map)
            _hip_display_map = contexto.get("EMPRESA_DISPLAY_MAP") or {}
            for _emp_key_k1, _rl_d_k1 in (empresa_rl_map or {}).items():
                _emp_disp_k1 = _hip_display_map.get(_emp_key_k1) or _emp_key_k1.upper()
                _rl_nom_k1 = _rl_d_k1.get("nombre") or ""
                _rl_id_raw_k1 = re.sub(r"[^0-9]", "", _rl_d_k1.get("identificacion") or "")
                # Solo cédulas (≤10 dígitos) — filtrar NITs (≥9 sin dv, pero como string pueden ser >10)
                _rl_cc_k1 = _format_cc(_rl_id_raw_k1) if _rl_id_raw_k1 and len(_rl_id_raw_k1) <= 10 else ""
                if _rl_nom_k1 and _rl_cc_k1:
                    instruccion_acto += (
                        f"CRÍTICO: '{_emp_disp_k1}' representada por RL '{_rl_nom_k1}' "
                        f"CC {_rl_cc_k1}. USAR EXACTAMENTE este nombre y CC para el compareciente "
                        f"de la empresa '{_emp_disp_k1}'. NUNCA usar la razón social como nombre del RL "
                        f"ni el NIT como cédula. "
                    )
            # J-2: Inyectar vendedor exacto del Acto de Compraventa — texto verbatim para CUARTO
            _vendedor_cv = contexto.get("_VENDEDOR_COMPRAVENTA_NOMBRES") or ""
            if _vendedor_cv:
                _ref_cv = contexto.get("_ACTO_COMPRAVENTA_REFERENCIA") or "el acto de compraventa"
                ctx_acto["VENDEDOR_COMPRAVENTA"] = _vendedor_cv
                _adq_text = (
                    f"lo adquirió EL HIPOTECANTE, por compraventa efectuada a {_vendedor_cv}, "
                    f"como consta en {_ref_cv}, y debidamente registrada en la "
                    f"Oficina de Registro de Instrumentos Públicos de "
                    f"{_norm_orip(inmueble.get('oficina_registro') or inmueble.get('ciudad_registro') or '')} "
                    f"bajo el folio de matrícula inmobiliaria número {matricula}"
                )
                ctx_acto["ADQUISICION_HIPOTECANTE_TEXTO"] = _adq_text
                instruccion_acto += (
                    f"CUARTO ADQUISICIÓN (OBLIGATORIO): La cláusula CUARTO debe contener LITERALMENTE: "
                    f"'{_adq_text}'. "
                    "PROHIBIDO mencionar a INSELEM ni ninguna otra entidad como vendedora — "
                    "INSELEM es el ACREEDOR hipotecario, NO participó en la compraventa. "
                    "Transcribir ADQUISICION_HIPOTECANTE_TEXTO textual sin añadir nombres. "
                )
            # J-4: Extraer datos de cámara de comercio para el deudor empresa
            _deudor_emp_norm = ""
            for _part_hip in (ctx_acto.get("COMPRADORES") or []):
                if "NIT" in (_part_hip.get("identificacion") or "").upper():
                    _deudor_emp_norm = _normalize_name(_part_hip.get("nombre") or "")
                    break
            if _deudor_emp_norm:
                _enrich_camara_data(ctx_acto, _deudor_emp_norm, contexto)
                instruccion_acto += (
                    "Datos de constitución del DEUDOR empresa disponibles en contexto: "
                    "CALIDAD_RL, DOMICILIO_SOCIEDAD, ACTO_CONSTITUCION, FECHA_INSCRIPCION_CAMARA, "
                    "NUMERO_INSCRIPCION_CAMARA, LIBRO_INSCRIPCION_CAMARA, CIUDAD_CAMARA — úsalos. "
                )
            # B2: Nombre actual del predio (post cambio de nombre si ocurrió antes en este instrumento)
            _nombre_actual_hip = contexto.get("_NOMBRE_NUEVO_PREDIO_CALCULADO") or ""
            if _nombre_actual_hip:
                ctx_acto["NOMBRE_ACTUAL_PREDIO"] = _nombre_actual_hip
                instruccion_acto += (
                    f"El predio se denomina actualmente '{_nombre_actual_hip}' "
                    "(renombrado en el acto de Cambio de Nombre de este mismo instrumento). "
                    "Usar ese nombre en la descripción del inmueble. "
                )
            # B3: Tradición inmediata — referenciar acto compraventa del mismo instrumento
            _ref_comp_hip = contexto.get("_ACTO_COMPRAVENTA_REFERENCIA") or ""
            if _ref_comp_hip:
                ctx_acto["ACTO_COMPRAVENTA_REFERENCIA"] = _ref_comp_hip
                instruccion_acto += (
                    f"La adquisición del inmueble hipotecado se efectuó mediante {_ref_comp_hip}. "
                    "En la cláusula de ADQUISICIÓN/CUARTO, referenciar SIEMPRE ese acto del presente instrumento, "
                    "NO ninguna escritura pública de antecedente histórico previo. "
                )
        elif _ak(nombre_acto) in (
            "CANCELACION_HIPOTECA", "CANCELACION_GENERICA", "CANCELACION_USUFRUCTO",
            "CANCELACION_FIDEICOMISO", "CANCELACION_ARRENDAMIENTO",
            "CANCELACION_PATRIMONIO", "CANCELACION_PH", "CANCELACION_CONDICION",
        ):
            instruccion_acto += (
                "Para CANCELACIONES: referenciar la escritura original con "
                "EP_ANTECEDENTE_NUMERO, EP_ANTECEDENTE_FECHA y EP_ANTECEDENTE_NOTARIA. "
            )
        elif _ak(nombre_acto) == "DACION_PAGO":
            instruccion_acto += (
                "Para DACION EN PAGO: el deudor (VENDEDOR) transfiere el inmueble al acreedor "
                "(ACREEDOR) como pago de deuda. Identificar claramente cada rol y la obligación extinguida. "
            )
        elif _ak(nombre_acto) == "DONACION":
            instruccion_acto += (
                "Para DONACION: el donante (VENDEDOR) transfiere a título gratuito al donatario "
                "(COMPRADOR). Si hay insinuación, referenciar la resolución con EP_ANTECEDENTE_NUMERO. "
            )
        elif _ak(nombre_acto) == "AFECTACION_VF":
            instruccion_acto += (
                "Para AFECTACION A VIVIENDA FAMILIAR: comparecen los cónyuges/compañeros. "
                "Marco legal: Ley 258 de 1996 modificada por Ley 854 de 2003. "
            )
        elif _ak(nombre_acto) in ("ACLARACION", "ACTUALIZACION_NOMENCLATURA"):
            instruccion_acto += (
                "Para ACLARACION/ACTUALIZACION: referenciar la escritura que se aclara con "
                "EP_ANTECEDENTE_NUMERO, EP_ANTECEDENTE_FECHA y EP_ANTECEDENTE_NOTARIA. "
                "Indicar explícitamente qué dato se corrige. "
            )

        # Enriquecer con contexto legal del Knowledge RAG
        if knowledge_rag:
            legal_context = knowledge_rag.retrieve(nombre_acto, top_k=2)
            if legal_context:
                instruccion_acto += (
                    "\n\nMARCO LEGAL Y REQUISITOS (referencia, no copiar literalmente):\n"
                    + legal_context
                )

        misiones.append({
            "orden": 20 + idx,
            "descripcion": f"EP_ACTO_{idx+1}",
            "plantilla_con_huecos": texto_crudo,
            "contexto_datos": ctx_acto,
            "instrucciones": instruccion_acto,
        })

    # INSERTOS — construir campos dinámicamente desde datos extraídos
    _pz_predial = datos_extra.get("PAZ_SALVO_PREDIAL") or ""
    _pz_valoriz = datos_extra.get("PAZ_SALVO_VALORIZACION") or ""
    _pz_metro = datos_extra.get("PAZ_SALVO_AREA_METRO") or "no aplica (municipio fuera de área metropolitana)"
    # CERTIFICADOS_PAZ_Y_SALVO_DETALLE: construir lista de todos los documentos protocolizados
    _cert_lines = []
    if _pz_predial and not _is_garbage(_pz_predial):
        _cert_lines.append(f"Paz y Salvo Predial N° {_pz_predial}.")
    if _pz_valoriz and not _is_garbage(_pz_valoriz) and "NO COBRA" not in _pz_valoriz.upper():
        _cert_lines.append(f"Paz y Salvo de Valorización N° {_pz_valoriz}.")
    elif _pz_valoriz and "NO COBRA" in _pz_valoriz.upper():
        _cert_lines.append(f"Constancia de no cobro de valorización: {_pz_valoriz}.")
    # Agregar cámaras de comercio de empresas participantes (dedup por nombre normalizado)
    _disp_map_ins = contexto.get("EMPRESA_DISPLAY_MAP") or {}
    _seen_cert_emp: set = set()
    for _emp_k_ins, _rl_ins in (empresa_rl_map or {}).items():
        _emp_name_ins = _disp_map_ins.get(_emp_k_ins) or _emp_k_ins.upper()
        _emp_norm_key = _normalize_name(_emp_name_ins)[:30]
        if _emp_norm_key in _seen_cert_emp:
            continue
        _seen_cert_emp.add(_emp_norm_key)
        _cert_lines.append(f"Certificado de Existencia y Representación Legal de {_emp_name_ins}, expedido por la Cámara de Comercio.")
    _certificados_detalle = "\n".join(_cert_lines) if _cert_lines else datos_extra.get("CERTIFICADOS_PAZ_Y_SALVO_DETALLE") or "[[PENDIENTE: CERTIFICADOS_PAZ_Y_SALVO_DETALLE]]"
    misiones.append({
        "orden": 80,
        "descripcion": "EP_INSERTOS",
        "plantilla_con_huecos": EP_INSERTOS_TEMPLATE,
        "contexto_datos": {
            **contexto,
            "MATRICULA_INMOBILIARIA": matricula,
            "PAZ_SALVO_PREDIAL": _pz_predial or "[[PENDIENTE: PAZ_SALVO_PREDIAL]]",
            "PAZ_SALVO_VALORIZACION": _pz_valoriz or "no aplica",
            "PAZ_SALVO_AREA_METRO": _pz_metro,
            "CERTIFICADOS_PAZ_Y_SALVO_DETALLE": _certificados_detalle,
            "CAMARAS_COMERCIO": _build_camaras_text(empresa_rl_map, contexto.get("EMPRESA_DISPLAY_MAP")),
        },
        "instrucciones": (
            "Completa insertos usando EXACTAMENTE los valores del contexto. "
            "PAZ_SALVO_PREDIAL, PAZ_SALVO_VALORIZACION, PAZ_SALVO_AREA_METRO y "
            "CERTIFICADOS_PAZ_Y_SALVO_DETALLE ya están resueltos — transcríbelos sin modificar. "
            "Si PAZ_SALVO_VALORIZACION es 'no aplica', OMITIR ESA LÍNEA por completo. "
            "Sin markdown. No inventes números ni documentos no listados."
        ),
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
    uiaf_blocks = _build_ui_af_blocks(personas_activos)
    if uiaf_blocks:
        # Envolver cada bloque persona en marcadores de tabla para que el renderer cree tablas DOCX
        uiaf_blocks_con_marcadores = "\n\n".join(
            f"###TABLE_START###\n{bloque}\n###TABLE_END###"
            for bloque in uiaf_blocks.split("\n\n")
            if bloque.strip()
        )
        misiones.append({
            "orden": 96,
            "descripcion": "EP_UIAF",
            "plantilla_con_huecos": uiaf_blocks_con_marcadores,
            "contexto_datos": contexto,
            "instrucciones": "Bloque UIAF ya construido. Transcribe sin modificar incluyendo marcadores ###TABLE_START### y ###TABLE_END###.",
        })

    # FIRMAS
    misiones.append({
        "orden": 99,
        "descripcion": "EP_FIRMAS",
        "plantilla_con_huecos": EP_FIRMAS_TEMPLATE.replace(
            "[[BLOQUE_FIRMAS]]", _build_firmas_block(personas_activos, empresa_rl_map, contexto.get("EMPRESA_DISPLAY_MAP"))
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

    async def _analyze_cedula(p: str) -> List[Dict[str, Any]]:
        b = Path(p).read_bytes()
        txt = await asyncio.to_thread(
            gemini.analyze_binary,
            b,
            _guess_mime(p),
            CEDULA_PROMPT,
            settings.GEMINI_MODEL_VISION,
            0.1,
            4096,
        )
        parsed = parse_json_with_repair(
            txt,
            kind="cedula",
            openai_client=openai,
            temperature=0.0,
            max_tokens=3000,
        )
        # Unwrap {"cedulas": [...]} wrapper; also accept bare list or dict
        if isinstance(parsed, dict) and "cedulas" in parsed:
            cedula_list = parsed["cedulas"] if isinstance(parsed["cedulas"], list) else [parsed["cedulas"]]
        elif isinstance(parsed, list):
            cedula_list = parsed
        elif isinstance(parsed, dict) and parsed:
            cedula_list = [parsed]
        else:
            cedula_list = []
        # Tag with source filename
        for c in cedula_list:
            if isinstance(c, dict):
                c.setdefault("_fileName", Path(p).name)
        debug.dump_gemini_output("03_cedulas", p, txt, cedula_list)
        return cedula_list

    if scanner_paths:
        cedula_results = await asyncio.gather(*[_analyze_cedula(p) for p in scanner_paths])
        # Flatten list of lists into a single list
        cedulas_json_list = [c for sublist in cedula_results for c in sublist]

    debug.dump_stage_json("03_cedulas.json", cedulas_json_list)

    # 4) CONTEXTO UNIVERSAL + DEDUPE
    contexto = _build_universal_context(rad_json, soportes_json_list, cedulas_json_list)
    contexto["PERSONAS"] = dedupe_personas(contexto.get("PERSONAS") or [])

    # Reconstruir PERSONAS_ACTIVOS y EMPRESA_RL_MAP DESPUÉS del dedup para que los
    # dicts de radicacion ya tengan estado_civil/email/ocupacion enriquecidos desde soportes
    contexto["PERSONAS_ACTIVOS"] = [p for p in contexto["PERSONAS"] if _is_actual_party(p)]
    contexto["EMPRESA_RL_MAP"], contexto["EMPRESA_DISPLAY_MAP"] = _build_empresa_rl_map(rad_json, contexto["PERSONAS"])

    # J-5/K-2: Enriquecer EMPRESA_DISPLAY_MAP con nombre completo de soportes
    # Fuente 1: hallazgos_variables.razon_social; Fuente 2: personas_detalle con NIT
    _soportes_src = (contexto.get("FUENTES") or {}).get("soportes") or []
    for _s in _soportes_src:
        if not isinstance(_s, dict):
            continue
        _candidates: list = []
        # Fuente 1: hallazgos_variables.razon_social
        _rs = (_s.get("hallazgos_variables") or {}).get("razon_social") or ""
        if _rs and not _is_garbage(_rs):
            _candidates.append(_rs.upper().strip())
        # Fuente 2: personas_detalle con NIT (nombre completo de empresa en el documento)
        for _pd in (_s.get("personas_detalle") or []):
            _pd_id = (_pd.get("identificacion") or "").upper()
            _pd_nom = (_pd.get("nombre") or "").strip()
            if "NIT" in _pd_id and _pd_nom and not _is_garbage(_pd_nom):
                _candidates.append(_pd_nom.upper())
        for _cand in _candidates:
            _cand_norm = _normalize_name(_cand)
            for _key in list(contexto["EMPRESA_DISPLAY_MAP"]):
                _existing = contexto["EMPRESA_DISPLAY_MAP"][_key]
                _tok_c = frozenset(_cand_norm.split()[:8])
                _tok_e = frozenset(_normalize_name(_existing).split()[:8])
                # Token overlap ≥4/8: misma empresa → usar nombre más largo (más completo)
                if len(_tok_c & _tok_e) >= 4 and len(_cand) > len(_existing):
                    contexto["EMPRESA_DISPLAY_MAP"][_key] = _cand

    # H-series: Aplicar overrides del campo comentario (prioridad máxima)
    # Usa LLM para entender lenguaje natural (ej: "FORMA DE PAGO de 595M...", 'nombre a "HACIENDA LEJANIAS"')
    comentario_overrides = _parse_comentario_overrides(comentario or "", openai_client=openai)
    contexto["COMENTARIO_OVERRIDES"] = comentario_overrides

    # H-A: nombre nuevo predio desde comentario
    if comentario_overrides.get("NOMBRE_NUEVO_PREDIO"):
        contexto["DATOS_EXTRA"]["NOMBRE_NUEVO_PREDIO"] = comentario_overrides["NOMBRE_NUEVO_PREDIO"].upper()

    # H-C: forma de pago desde comentario
    if comentario_overrides.get("FORMA_DE_PAGO"):
        contexto["DATOS_EXTRA"]["FORMA_DE_PAGO"] = comentario_overrides["FORMA_DE_PAGO"]

    # M-B: código catastral anterior desde comentario (mayor prioridad que OCR)
    _cat_ov = comentario_overrides.get("CODIGO_CATASTRAL_ANTERIOR") or ""
    if _cat_ov and not _is_garbage(_cat_ov):
        contexto["INMUEBLE"]["codigo_catastral_anterior"] = _cat_ov
        contexto["INMUEBLE"]["CODIGO_CATASTRAL_ANTERIOR"] = _cat_ov
        contexto["INMUEBLE"]["CEDULA_CATASTRAL"] = _cat_ov

    # N-2: nombre anterior del predio desde comentario → se usa en CANCELACION y COMPRAVENTA
    if comentario_overrides.get("NOMBRE_ANTERIOR_PREDIO"):
        contexto["INMUEBLE"]["nombre_anterior"] = comentario_overrides["NOMBRE_ANTERIOR_PREDIO"].upper()

    # REDAM_CONTINGENCIA: detectar falla técnica REDAM desde comentario o desde soportes
    _redam_contingencia = bool(comentario_overrides.get("REDAM_CONTINGENCIA"))
    if not _redam_contingencia:
        _REDAM_KW = ("REDAM", "ERROR DE CONEXIÓN", "FALLA TÉCNICA", "NO FUE POSIBLE CONSULTAR",
                     "SISTEMA REDAM", "CONTINGENCIA REDAM")
        for _s_redam in ((contexto.get("FUENTES") or {}).get("soportes") or []):
            _obs_redam = str((_s_redam.get("observaciones") or _s_redam.get("notas") or "")).upper()
            _hv_redam = str((_s_redam.get("hallazgos_variables") or {}).get("redam_resultado") or "").upper()
            if any(kw in _obs_redam or kw in _hv_redam for kw in _REDAM_KW):
                _redam_contingencia = True
                break
    if _redam_contingencia:
        contexto["DATOS_EXTRA"]["REDAM_CONTINGENCIA"] = True

    # H-E: estado civil por persona (KEY: ESTADO_CIVIL_<NOMBRE_PARCIAL>)
    for _p in contexto["PERSONAS"]:
        _nom_norm = _normalize_name(_p.get("nombre") or "")
        for _key, _val in comentario_overrides.items():
            if _key.startswith("ESTADO_CIVIL_"):
                _name_part = _normalize_name(_key[len("ESTADO_CIVIL_"):])
                if _name_part and _name_part in _nom_norm:
                    _p["estado_civil"] = _val
                    break

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
        rag_query = _build_rag_query(acto, contexto)  # sub-tipo inteligente para COMPRAVENTA
        raw = rag.retrieve_acto_text(rag_query)
        split = split_metadata_and_body(raw)
        actos_docs.append(split)
        debug.dump_rag_hit(acto_nombre=nombre_acto, raw_text=raw, meta_body=split)

    debug.dump_stage_json("05_actos_docs.json", actos_docs)

    # 6) MISIONES EP
    misiones = _prepare_ep_sections(contexto, actos_docs, knowledge_rag=rag.knowledge)
    debug.dump_misiones(misiones)

    # 7) BINDER POR SECCIÓN
    async def _run_mision(m: Dict[str, Any]) -> Dict[str, Any]:
        user = DATABINDER_USER.format(
            contexto_json=json.dumps(m["contexto_datos"], ensure_ascii=False),
            plantilla=m["plantilla_con_huecos"],
            instrucciones=m.get("instrucciones") or "Rellena los placeholders del texto.",
        )
        # Actos individuales pueden tener templates muy largos (ej. hipoteca con 13 cláusulas)
        max_tok = 16000 if m["descripcion"].startswith("EP_ACTO") else 8000
        out = await asyncio.to_thread(openai.chat, DATABINDER_SYSTEM, user, 0.1, max_tok)
        # Comprimir cadenas de guiones excesivas que OpenAI a veces genera
        out = re.sub(r'-{11,}', '----------', out)
        debug.dump_binder_output(m["orden"], m["descripcion"], out)
        return {"orden": m["orden"], "descripcion": m["descripcion"], "texto": out}

    results = await asyncio.gather(*[_run_mision(m) for m in misiones])
    results.sort(key=lambda x: x["orden"])

    # Los actos individuales (EP_ACTO_*) reciben un salto de línea extra antes
    # para que haya separación visual clara entre el inicio de cada acto y la sección anterior.
    partes_escritura = []
    for r in results:
        texto = r["texto"].strip()
        if not texto:
            continue
        if r["descripcion"].startswith("EP_ACTO"):
            partes_escritura.append("\n\n" + texto)
        else:
            partes_escritura.append(texto)
    cuerpo_escritura = "\n\n".join(partes_escritura)
    debug.dump_stage_text("08_cuerpo_escritura.txt", cuerpo_escritura)
    debug.write_checklist()

    # 8) RENDER
    docx_path = str(case_dir / f"Minuta_Caso_{radicado}.docx")
    pdf_path = str(case_dir / f"Escritura_Caso_{radicado}.pdf")

    _de = contexto.get("DATOS_EXTRA") or {}
    context_docx = {
        "RADICADO": radicado,
        "COMENTARIO": comentario or "",
        "CONTENIDO_IA": cuerpo_escritura,
        "NOTARIA_FIRMA_NOMBRE": _de.get("NOTARIA_ENCARGADO") or _de.get("notario_encargado") or "",
        "NOTARIA_CARGO": _de.get("NOTARIA_NOMBRE") or "",
    }
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