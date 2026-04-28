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
    REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT,
    build_certificados_paz_y_salvo_detalle,
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
    ranked: List[tuple[int, str]] = []
    for p in paths:
        name = Path(p).name.lower()
        stem = Path(p).stem.lower()
        score = 0
        if any(k in name for k in ["radicacion", "radicación", "hoja de radicacion", "hoja de radicación"]):
            score = 100
        elif "turno" in name:
            score = 90
        elif re.search(r"(^|[\s._-])rad($|[\s._-])", stem):
            score = 80
        elif "radic" in name:
            score = 70
        elif "hoja" in name:
            score = 60
        if score:
            ranked.append((score, p))
    if ranked:
        ranked.sort(key=lambda item: item[0], reverse=True)
        return ranked[0][1]
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


def _is_antecedente_support_filename(filename: str | None) -> bool:
    name = Path(str(filename or "")).name.lower()
    stem = Path(name).stem.lower()
    return (
        any(kw in name for kw in ("antecedente", "ep_anterior"))
        or bool(re.match(r"^ant(?:$|[_\-.\s\d])", stem))
    )


def _safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _resolve_static_placeholders(template: str, replacements: Dict[str, Any]) -> str:
    resolved = template
    for key, value in (replacements or {}).items():
        if value is None:
            continue
        resolved = resolved.replace(f"[[{key}]]", str(value))
    return resolved


def _should_keep_condicion_resolutoria_paragraph(forma_de_pago: Any) -> bool:
    """Return True only when the payment terms show deferred or financed payment.

    Word feedback indicates the estándar parágrafo segundo de condición resolutoria
    should stay only when the price is not paid de contado.
    """
    text = (forma_de_pago or "").strip()
    if not text:
        return False

    normalized = text.upper()
    contado_markers = (
        "CONTADO",
        "DE CONTADO",
        "PAGO TOTAL",
        "PAGADO EN SU TOTALIDAD",
        "CANCELADO EN SU TOTALIDAD",
    )
    if any(marker in normalized for marker in contado_markers):
        return False

    deferred_markers = (
        "CUOTA",
        "CUOTAS",
        "SALDO",
        "PENDIENTE",
        "PLAZO",
        "CRÉDITO",
        "CREDITO",
        "HIPOTECA",
        "LEASING",
        "FINANCIA",
        "MENSUAL",
        "ABONO",
        "SE OBLIGA A PAGAR",
        "PAGARÁ",
        "PAGARA",
    )
    return any(marker in normalized for marker in deferred_markers)


_RADICADO_TEXT_PATTERNS = (
    re.compile(r'"numero_radicado"\s*:\s*"?(?P<rad>\d{4,12})', re.IGNORECASE),
    re.compile(r'"radicacion"\s*:\s*\{.*?"numero"\s*:\s*"?(?P<rad>\d{4,12})', re.IGNORECASE | re.DOTALL),
    re.compile(r'\bradicaci[oó]n\b[^\d]{0,32}(?P<rad>\d{4,12})', re.IGNORECASE),
    re.compile(r'\bradicad[oa]\b[^\d]{0,24}(?P<rad>\d{4,12})', re.IGNORECASE),
)


def _normalize_radicado_candidate(value: Any) -> Optional[str]:
    digits = re.sub(r"\D+", "", str(value or ""))
    if 4 <= len(digits) <= 12:
        return digits
    return None


def _extract_radicado_from_structure(node: Any, *, parent_key: str = "") -> Optional[str]:
    if isinstance(node, dict):
        for key, value in node.items():
            key_norm = str(key or "").strip().lower()
            if "radicado" in key_norm:
                candidate = _normalize_radicado_candidate(value)
                if candidate:
                    return candidate
            if key_norm == "numero" and parent_key in {"radicacion", "negocio_actual"}:
                candidate = _normalize_radicado_candidate(value)
                if candidate:
                    return candidate
            candidate = _extract_radicado_from_structure(value, parent_key=key_norm)
            if candidate:
                return candidate
    elif isinstance(node, list):
        for item in node:
            candidate = _extract_radicado_from_structure(item, parent_key=parent_key)
            if candidate:
                return candidate
    return None


def _extract_radicado_from_raw_text(raw_text: str) -> Optional[str]:
    text = (raw_text or "").strip()
    if not text:
        return None
    for pattern in _RADICADO_TEXT_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        candidate = _normalize_radicado_candidate(match.group("rad"))
        if candidate:
            return candidate
    return None


def _extract_radicado_from_radicacion_json(
    radicacion: Dict[str, Any],
    raw_text: Optional[str] = None,
) -> str:
    r = _safe_get(radicacion, ["negocio_actual", "numero_radicado"])
    candidate = _normalize_radicado_candidate(r)
    if candidate:
        return candidate
    r2 = _safe_get(radicacion, ["radicacion", "numero"])
    candidate = _normalize_radicado_candidate(r2)
    if candidate:
        return candidate
    candidate = _extract_radicado_from_structure(radicacion)
    if candidate:
        return candidate
    candidate = _extract_radicado_from_raw_text(raw_text or "")
    if candidate:
        return candidate
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
        "rol_en_hoja": (p.get("rol_en_hoja") or "").strip(),
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
    # RAG-1: usar tipo_inmueble de Gemini para seleccionar sub-template correcto (PN→PN)
    _tipo_inmueble = ((contexto.get("INMUEBLE") or {}).get("tipo_inmueble") or "").upper().strip()
    # Fix A: si Gemini no extrajo tipo_inmueble (o lo extrajo mal), inferir de inmueble.direccion.
    # Gemini a veces clasifica "CASA en URBANIZACION" como PROPIEDAD_HORIZONTAL por error.
    _inm_dir_ra = ((contexto.get("INMUEBLE") or {}).get("direccion") or "").upper().strip()
    if not _tipo_inmueble or _tipo_inmueble == "PROPIEDAD_HORIZONTAL":
        # Si la dirección comienza con "CASA", es una casa, no PH
        if re.search(r'^CASA\b', _inm_dir_ra):
            _tipo_inmueble = "CASA"
        elif not _tipo_inmueble and re.search(r'\bAPTO\b|\bAPARTAMENTO\b', _inm_dir_ra):
            _tipo_inmueble = "APARTAMENTO"
        if _tipo_inmueble and isinstance(contexto.get("INMUEBLE"), dict):
            contexto["INMUEBLE"]["tipo_inmueble"] = _tipo_inmueble
    if _tipo_inmueble == "LOTE_BALDIO" or es_baldio:
        return "compraventa bien inmueble lote baldio"
    if _tipo_inmueble == "CASA":
        return "compraventa bien inmueble casa"
    if _tipo_inmueble == "APARTAMENTO":
        return "compraventa bien inmueble apartamento parqueadero"
    if _tipo_inmueble == "PROPIEDAD_HORIZONTAL":
        return "compraventa bien inmueble propiedad horizontal"
    if _tipo_inmueble == "AERONAVE":
        return "compraventa aeronave"
    # LOTE, RURAL o desconocido → default lote
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
    resolved_radicado: Optional[str] = None,
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
        contexto["RADICACION"] = resolved_radicado or _extract_radicado_from_radicacion_json(radicacion_json)
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

        # Extraer números de paz y salvo — PAZ-1: excluir antecedentes (tienen números históricos).
        # PAZ-2: usar el nombre del archivo para saber qué campo es confiable en cada documento.
        _fname_lower = (s.get("_fileName") or "").lower()
        _is_antecedente_doc = _is_antecedente_support_filename(s.get("_fileName"))
        if not _is_antecedente_doc:
            for _campo in ["paz_salvo_predial", "paz_salvo_valorizacion", "paz_salvo_area_metro"]:
                _val = str((extra or {}).get(_campo) or "").strip()
                if not _val or _is_garbage(_val):
                    continue
                # PAZ-2: si el filename indica explícitamente el tipo de paz y salvo,
                # solo actualizar el campo correspondiente para evitar confusión entre tipos.
                _fname_hints = {
                    "paz_salvo_predial":     ("predial",),
                    "paz_salvo_valorizacion": ("valoriz",),
                    "paz_salvo_area_metro":  ("area", "metro", "metropolitana"),
                }
                _hints = _fname_hints[_campo]
                _fname_matches_tipo = any(h in _fname_lower for h in _hints)
                _other_tipos_match = any(
                    h in _fname_lower
                    for other_campo, other_hints in _fname_hints.items()
                    if other_campo != _campo
                    for h in other_hints
                )
                # Si el archivo es de OTRO tipo de paz y salvo (ej: "area_metropolitana.pdf"
                # leyendo paz_salvo_valorizacion), omitir ese campo.
                if _other_tipos_match and not _fname_matches_tipo:
                    continue
                _key = _campo.upper()
                if not contexto["DATOS_EXTRA"].get(_key):
                    contexto["DATOS_EXTRA"][_key] = _val

        # Bug 8: forma_de_pago — first-wins (primer doc que lo mencione)
        _fdp = str((extra or {}).get("forma_de_pago") or "").strip()
        if _fdp and not _is_garbage(_fdp):
            if not contexto["DATOS_EXTRA"].get("FORMA_DE_PAGO"):
                contexto["DATOS_EXTRA"]["FORMA_DE_PAGO"] = _fdp

    _merge_cedula_ocr_into_people(contexto["PERSONAS"], cedulas_json_list)

    # FIX D v38 — PAZ-3: Área Metro fallback por patrón de ceros líderes.
    # Gemini a veces graba el número de Área Metropolitana en paz_salvo_valorizacion (lowercase)
    # porque el soporte no tiene "area/metro" en el filename → PAZ-2 no lo discrimina.
    # Patrón: números Área Metro empiezan con 0 (ej: "000191005"); valorización no (ej: "2619752").
    if not contexto["DATOS_EXTRA"].get("PAZ_SALVO_AREA_METRO"):
        for _paz3_field in ("paz_salvo_valorizacion", "paz_salvo_predial"):
            _paz3_v = str(contexto["DATOS_EXTRA"].get(_paz3_field) or "").strip()
            if re.match(r'^0\d{5,8}$', _paz3_v):
                contexto["DATOS_EXTRA"]["PAZ_SALVO_AREA_METRO"] = _paz3_v
                break

    # CTL STATE ENGINE — Point 1: detectar soporte CTL y ejecutar engine determinista.
    # El soporte CTL se identifica por _fileName. Se ejecuta aquí para que DATOS_EXTRA
    # (con PAZ_SALVO_VALORIZACION ya populado) esté disponible como entrada al engine.
    _ctl_soporte_json = next(
        (s for s in (soportes_json_list or [])
         if any(kw in (s.get("_fileName") or "").lower()
                for kw in ("ctl", "certificado_tradicion", "certificado de tradicion"))),
        None,
    )
    if _ctl_soporte_json:
        try:
            from app.services.ctl import resolve_ctl as _ctl_resolve
            from app.services.ctl import to_deed_context as _ctl_deed_ctx_fn
            _ctl_paz_data = {
                k: v for k, v in (contexto.get("DATOS_EXTRA") or {}).items()
                if "PAZ_SALVO" in k.upper() and v
            }
            import dataclasses as _dc
            _ctl_state = _ctl_resolve(_ctl_soporte_json, _ctl_paz_data)
            contexto["ctl_state"] = _dc.asdict(_ctl_state)   # JSON-serializable
            contexto["_ctl_deed_ctx"] = _ctl_deed_ctx_fn(_ctl_state)
        except Exception as _ctl_exc:
            contexto["ctl_state"] = None
            contexto["_ctl_deed_ctx"] = {}
            contexto.setdefault("_warnings", []).append(
                f"CTL State Engine falló: {_ctl_exc}"
            )
    else:
        contexto["ctl_state"] = None
        contexto["_ctl_deed_ctx"] = {}

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

    # Fix D v32: Derivar código catastral IGAC (≥20 dígitos) desde soportes
    # Ampliado para buscar en 3 campos de datos_inmueble (no solo predial_nacional)
    _cc_act = (contexto.get("INMUEBLE") or {}).get("codigo_catastral_anterior", "")
    if not _cc_act or _is_garbage(_cc_act) or _cc_act.strip().upper() in ("SIN INFORMACION", "SIN INFORMACIÓN"):
        for _src_d in ((contexto.get("FUENTES") or {}).get("soportes") or []):
            _di_src_d = _src_d.get("datos_inmueble") or {}
            for _campo_d in ("codigo_catastral_anterior", "cedula_catastral", "predial_nacional"):
                _raw_d = _di_src_d.get(_campo_d) or ""
                _digits_d = re.sub(r'[^0-9]', '', _raw_d)
                if len(_digits_d) >= 20:  # Código catastral IGAC: 30 dígitos
                    contexto.setdefault("INMUEBLE", {})["codigo_catastral_anterior"] = _digits_d
                    contexto["INMUEBLE"]["CODIGO_CATASTRAL_ANTERIOR"] = _digits_d
                    break
            # Fix 1 v35: Fix D outer-break premature exit cuando valor sigue siendo "SIN INFORMACION"
            # _is_garbage("SIN INFORMACION") = False → el break se disparaba antes de llegar
            # a paz_y_salvo files que sí tienen predial_nacional con 30 dígitos.
            _cc_post_d = (contexto.get("INMUEBLE") or {}).get("codigo_catastral_anterior", "")
            if (not _is_garbage(_cc_post_d)
                    and _cc_post_d.strip().upper() not in ("SIN INFORMACION", "SIN INFORMACIÓN")):
                break

    # A-02 (v12): Normalizar cabida_area — si empieza con "0 hectáreas", omitir esa parte.
    # Ejemplo: "0 hectáreas 160 metros cuadrados" → "160 metros cuadrados"
    _ca_raw = contexto["INMUEBLE"].get("cabida_area") or ""
    _ca_norm = re.sub(r'^\s*0\s*hect[áa]reas?\s*', '', _ca_raw, flags=re.IGNORECASE).strip()
    if _ca_norm and _ca_norm != _ca_raw.strip():
        contexto["INMUEBLE"]["cabida_area"] = _ca_norm

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
    """Genera bloques UIAF vacíos para personas naturales partes del negocio actual.
    Per MMFL29-39: los campos deben quedar EN BLANCO — el cliente los llena en la notaría."""
    naturales = [
        p for p in (personas_activos or [])
        if not re.search(r"\bNIT\b|\bNI\b", (p.get("identificacion") or "").upper())
    ]
    # Un bloque en blanco por persona — sin datos pre-llenados
    out = [EP_UIAF_TEMPLATE for _ in naturales]
    return "\n\n".join(out).strip()


def _build_firmas_block(
    personas_activos: List[Dict[str, Any]],
    empresa_rl_map: Dict[str, Any],
    empresa_display_map: Optional[Dict[str, str]] = None,
) -> str:
    """Genera bloque de firmas: personas naturales con su cargo/empresa si aplica.
    empresa_display_map: normalized_key → razón social completa (para 'representante legal de X').
    """
    # NI-01/NI-06: AP map — apoderado normalizado → nombre del representado
    # Usar frozenset de palabras para tolerar orden diferente en nombres (ej: "JAIME URIBE EDITH" vs "EDITH JAIME URIBE")
    _ap_represents: dict = {}
    for _p in personas_activos or []:
        if re.search(r"\bAP\b", (_p.get("rol_en_hoja") or "").upper()):
            _rep_a = (_p.get("representa_a") or "").strip()
            if _rep_a:
                _ap_represents[_normalize_name(_p.get("nombre") or "")] = _rep_a
    # word-sets de representados para exclusión (orden-invariante)
    _excluidos_word_sets = [frozenset(_normalize_name(rep).split()) for rep in _ap_represents.values()]

    lines = []
    for p in personas_activos or []:
        nom = p.get("nombre") or "[[PENDIENTE: NOMBRE]]"
        cc_raw = re.sub(r"[^0-9]", "", p.get("identificacion") or "")
        cc_fmt = _format_cc(cc_raw) if cc_raw else "[[PENDIENTE: CEDULA]]"
        # Saltar empresas (NIT) — sus RLs ya aparecen como personas naturales
        raw_id = (p.get("identificacion") or "").upper()
        if re.search(r"\bNIT\b|\bNI\b", raw_id):
            continue
        nom_norm = _normalize_name(nom)
        nom_words = frozenset(nom_norm.split())
        # Saltar representados (persona ausente representada por AP) — orden-invariante
        if nom_words and any(nom_words == exc_ws for exc_ws in _excluidos_word_sets):
            continue
        # ¿Esta persona es AP de alguien? (buscar por word-set para tolerar orden de nombre)
        _ap_rep_match = next(
            (rep_name for ap_key, rep_name in _ap_represents.items()
             if nom_words and frozenset(ap_key.split()) == nom_words),
            None
        )
        if _ap_rep_match:
            lines.append(
                f"(Firma) {nom}\n"
                f"C.C. No. {cc_fmt}\n"
                f"Obrando como apoderado de {_ap_rep_match}.\n"
                f"Huella: _________"
            )
            continue
        # ¿Esta persona es RL de alguna empresa?
        empresa_repr = None
        for emp_norm, rl_data in (empresa_rl_map or {}).items():
            if _normalize_name(rl_data.get("nombre") or "") == nom_norm:
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

        def _fmt_cc_id(id_str: str) -> str:
            """Añade puntos de miles colombianos a CC/NIT. '27952821' → 'CC 27.952.821'"""
            m_cc = re.match(r'^(CC|NIT|NUIP|CE|PEP|T\.?I\.?)\s*(.+)$',
                            (id_str or "").strip(), re.IGNORECASE)
            if not m_cc:
                return id_str or ""
            prefix_cc, num_part_cc = m_cc.group(1).upper(), m_cc.group(2).strip()
            # Fix 3 v35: limpiar texto adicional tras el número ("de Bucaramanga", "expedida en X", etc.)
            # Gemini a veces incluye la ciudad de expedición dentro del campo identificacion
            _num_clean = re.match(r'^([\d.]+(?:-\d+)?)', num_part_cc)
            if _num_clean:
                num_part_cc = _num_clean.group(1)
            dash_m_cc = re.match(r'^([\d.]+)(-\d+)?$', num_part_cc)
            if not dash_m_cc:
                return f"{prefix_cc} {num_part_cc}"
            digits_cc = re.sub(r'\.', '', dash_m_cc.group(1))
            suffix_cc = dash_m_cc.group(2) or ""
            formatted_cc, i_cc = "", 0
            for c_cc in reversed(digits_cc):
                if i_cc > 0 and i_cc % 3 == 0:
                    formatted_cc = "." + formatted_cc
                formatted_cc = c_cc + formatted_cc
                i_cc += 1
            return f"{prefix_cc} {formatted_cc}{suffix_cc}"

        def _fmt_persona(p: Dict) -> str:
            n = (p.get("nombre") or "").strip()
            cc = (p.get("identificacion") or "").strip()
            if cc and "NO_DETECTADO" not in cc.upper():
                return f"{n} - {_fmt_cc_id(cc)}"
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
            _rsw_car = {"DE", "LA", "EL", "LOS", "LAS", "Y", "E"}
            for c in compradores:
                c_nom_words = {w for w in _normalize_name(c.get("nombre") or "").split()
                               if w not in _rsw_car and len(w) > 2}
                # CAR-1: buscar en personas_activos si hay un AP que representa a este comprador
                # (el AP puede no estar en compradores si Gemini lo clasificó como representante)
                _ap_for_c = next(
                    (p for p in (personas_activos or [])
                     if re.search(r"\bAP\b", (p.get("rol_en_hoja") or "").upper())
                     and len(c_nom_words & {w for w in _normalize_name(p.get("representa_a") or "").split()
                                            if w not in _rsw_car and len(w) > 2}) >= 2),
                    None
                )
                if _ap_for_c:
                    s_ap = _fmt_persona(_ap_for_c)
                    _rep_name = (c.get("nombre") or "").strip()
                    lines.append(f"APODERADO(A) de {_rep_name}: {s_ap}")
                else:
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
            # Fix D v14: añadir prefijo "Departamento de " en fallback (consistente con extracción Gemini)
            "DEPARTAMENTO": datos_extra.get("DEPARTAMENTO") or (
                lambda _dv: f"Departamento de {_dv}" if _dv else "[[PENDIENTE: DEPARTAMENTO]]"
            )({
                # E-03: fallback ciudad → departamento cuando Gemini no extrae el departamento
                "BUCARAMANGA": "Santander", "BOGOTÁ": "Cundinamarca", "BOGOTA": "Cundinamarca",
                "MEDELLÍN": "Antioquia", "MEDELLIN": "Antioquia",
                "CALI": "Valle del Cauca", "BARRANQUILLA": "Atlántico",
                "CARTAGENA": "Bolívar", "CÚCUTA": "Norte de Santander",
                "MANIZALES": "Caldas", "PEREIRA": "Risaralda", "ARMENIA": "Quindío",
                "IBAGUÉ": "Tolima", "NEIVA": "Huila", "VILLAVICENCIO": "Meta",
                "TUNJA": "Boyacá", "POPAYÁN": "Cauca", "MONTERÍA": "Córdoba",
                "VALLEDUPAR": "Cesar", "SINCELEJO": "Sucre", "RIOHACHA": "La Guajira",
            }.get(ciudad.upper().strip())),
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
    _predial_nacional = inmueble.get("predial_nacional") or "[[PENDIENTE: NUMERO_PREDIAL_NACIONAL]]"
    _codigo_catastral = (
        inmueble.get("CODIGO_CATASTRAL_ANTERIOR")
        or inmueble.get("codigo_catastral_anterior")
        or "[[PENDIENTE: CODIGO_CATASTRAL_ANTERIOR]]"
    )
    _afectacion_vivienda = (inmueble.get("afectacion_vivienda") or "").strip().upper()
    if _afectacion_vivienda in {"SI", "SÍ", "NO"}:
        _afectacion_vivienda = "SÍ" if _afectacion_vivienda in {"SI", "SÍ"} else "NO"
    else:
        _afectacion_vivienda = "[[PENDIENTE: AFECTACION_VIVIENDA_FAMILIAR]]"
    _patrimonio_familia = (inmueble.get("patrimonio_familia") or "").strip().upper()
    if _patrimonio_familia in {"SI", "SÍ", "NO"}:
        _patrimonio_familia = "SÍ" if _patrimonio_familia in {"SI", "SÍ"} else "NO"
    else:
        _patrimonio_familia = "[[PENDIENTE: PATRIMONIO_FAMILIA_INEMBARGABLE]]"
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
            "NUMERO_PREDIAL_NACIONAL": _predial_nacional,
            "CODIGO_CATASTRAL_ANTERIOR": _codigo_catastral,
            "AFECTACION_VIVIENDA_FAMILIAR": _afectacion_vivienda,
            "PATRIMONIO_FAMILIA_INEMBARGABLE": _patrimonio_familia,
            "RESUMEN_ACTOS": resumen_actos,
            "DESCRIPCION_INMUEBLE": re.sub(r'\bDELA\b', 'DE LA', inmueble.get("direccion") or "[[PENDIENTE: DESCRIPCION_INMUEBLE]]"),
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
        ctx_acto["TIPO_ADQUISICION_ANTERIOR"] = ep_ant_global.get("tipo_adquisicion") or "COMPRAVENTA"
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

        # AP-1: detectar apoderados en personas_activos (pueden estar en 'representantes',
        # no necesariamente en COMPRADORES si Gemini los clasificó correctamente)
        _acto_benef_raw = (acto.get("beneficiarios") or []) if isinstance(acto, dict) else []
        _acto_benef_norms = {_normalize_name(b) for b in _acto_benef_raw}
        _compradores_acto_norms = {_normalize_name(c.get("nombre") or "")
                                   for c in ((ctx_acto.get("ROLES_ACTO") or {}).get("COMPRADORES") or [])}
        _todos_compradores_norms = _acto_benef_norms | _compradores_acto_norms
        _ap_list = [p for p in (personas_activos or [])
                    if re.search(r"\bAP\b", (p.get("rol_en_hoja") or "").upper())]
        if _ap_list:
            _ap_instr_parts = []
            for _ap_p in _ap_list:
                _ap_nombre = (_ap_p.get("nombre") or "").strip()
                _ap_rep_a = (_ap_p.get("representa_a") or "").strip()
                if not _ap_rep_a and _todos_compradores_norms:
                    # Inferir: AP representa al primer comprador del acto
                    _first_comp = next(
                        (p for p in (personas_activos or [])
                         if _normalize_name(p.get("nombre") or "") in _todos_compradores_norms
                         and not re.search(r"\bAP\b", (p.get("rol_en_hoja") or "").upper())),
                        None
                    )
                    _ap_rep_a = (_first_comp.get("nombre") or "").strip() if _first_comp else ""
                # Solo inyectar si el representado está en este acto (o no hay filtro por acto)
                _rep_en_acto = (not _todos_compradores_norms
                                or (_ap_rep_a and _normalize_name(_ap_rep_a) in _todos_compradores_norms))
                if not _rep_en_acto:
                    continue
                if _ap_rep_a:
                    _ap_instr_parts.append(
                        f"'{_ap_nombre}' actúa como APODERADO de '{_ap_rep_a}'. "
                        f"Para LA PARTE COMPRADORA: mencionar ÚNICAMENTE a '{_ap_nombre}' con sus "
                        f"datos personales (cédula en PERSONAS_ACTIVOS) y la frase: 'obrando como "
                        f"apoderado de {_ap_rep_a}, según poder que se adjunta y protocoliza con la "
                        f"presente escritura'. "
                        f"NO mencionar a '{_ap_rep_a}' como compareciente que actúa 'en nombre y "
                        f"representación propia' — {_ap_rep_a} es el representado AUSENTE, no comparece "
                        f"directamente. En PRESENTES: usar '{_ap_nombre}, apoderado de {_ap_rep_a}'."
                    )
                else:
                    _ap_instr_parts.append(
                        f"'{_ap_nombre}' actúa como APODERADO — incluirlo en la comparecencia "
                        "indicando que actúa como apoderado según poder que se adjunta y protocoliza. "
                        "El representado NO comparece directamente; NO usar 'en nombre y representación propia'."
                    )
            if _ap_instr_parts:
                instruccion_acto += (
                    "APODERADO EN ESTE ACTO: " + "; ".join(_ap_instr_parts) + ". "
                    "PROHIBIDO usar 'obrando en nombre y representación propia' para estas personas. "
                )

        if _ak(nombre_acto) in ("COMPRAVENTA", "COMPRAVENTA_CUOTA"):
            # MMFL15: cláusula obligatoria en toda compraventa, entre REDAM y PRESENTE(S)
            instruccion_acto += (
                "CLÁUSULA OBLIGATORIA (MMFL15): Entre REDAM y PRESENTE(S) incluir OBLIGATORIAMENTE: "
                "'CLAUSULA ESPECIAL SOBRE SERVICIOS PÚBLICOS: la parte vendedora manifiesta(n) que las "
                "facturas que han llegado hasta la fecha, correspondientes a los servicios públicos del "
                "inmueble objeto de esta venta, ya se encuentran canceladas. ----------' "
                "NO omitir esta cláusula bajo ninguna circunstancia. "
            )
            # MMFL / feedback corpus: texto REDAM de protocolización MINTIC para compraventa
            instruccion_acto += (
                "CLÁUSULA REDAM (MMFL / corpus): Cuando la parte vendedora sea persona natural, usar "
                f"EXACTAMENTE este texto: '{REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT}' "
                "No sustituirlo por la fórmula antigua de 'la suscrita notaria consultó... artículo 2'. "
                "Si aplica una contingencia REDAM, conservar la constancia de contingencia conforme al contexto. "
            )
            # Fix 2 v33: instrucción explícita contra DECONOCIMIENTO (sin espacio)
            instruccion_acto += (
                "CLÁUSULA DE CONOCIMIENTO (Fix 2 v33): El nombre de la cláusula es 'CLÁUSULA DE CONOCIMIENTO' "
                "(tres palabras separadas: CLÁUSULA + DE + CONOCIMIENTO). "
                "ABSOLUTAMENTE PROHIBIDO escribir 'CLAUSULA DECONOCIMIENTO' o 'CLÁUSULA DECONOCIMIENTO' "
                "(sin espacio entre DE y CONOCIMIENTO). Siempre separar: 'DE CONOCIMIENTO'. "
            )
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
            compradores_act = (ctx_acto.get("ROLES_ACTO") or {}).get("COMPRADORES") or []
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
            # A-04 (v8): Corrección ortográfica de ocupaciones (tildes)
            instruccion_acto += (
                "CORRECCIÓN ORTOGRÁFICA DE OCUPACIONES: aplicar tildes correctas. "
                "'Policia' → 'Policía', 'Medico' → 'Médico', 'Tecnico' → 'Técnico', "
                "'Economista' sin cambio, 'Ingeniero' sin cambio. "
            )
            # A-02/A-03 (v12): Área del inmueble ya normalizada en Python antes de llegar aquí.
            # Instrucción reforzada: SIEMPRE incluir el área, NUNCA omitirla.
            instruccion_acto += (
                "DESCRIPCIÓN DEL INMUEBLE — ÁREA: SIEMPRE incluir el área del inmueble "
                "tal como aparece en el campo cabida_area/area del INMUEBLE. "
                "NUNCA omitir el área ni dejarla en blanco. "
                "Si el valor ya viene limpio (ej: '160 metros cuadrados'), transcribirlo exactamente. "
            )
            # A-04v2 (v10): Título de la sección descripción del inmueble en MAYÚSCULAS
            instruccion_acto += (
                "TÍTULO DE SECCIÓN: usar SIEMPRE 'DESCRIPCIÓN DEL INMUEBLE (CASA):' en MAYÚSCULAS. "
                "NUNCA 'Descripción del inmueble:' con minúsculas. "
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
                "CLÁUSULA SEGUNDO - TÍTULO DE ADQUISICIÓN: "
                "usar TIPO_ADQUISICION_ANTERIOR del contexto. "
                "Si TIPO_ADQUISICION_ANTERIOR = 'COMPRAVENTA', redactar: "
                "'[vendedor] adquirió mediante escritura pública número [N] de fecha [fecha] de la Notaría [X], "
                "en la cual [VENDEDOR_ANTERIOR] le transfirió el dominio. Debidamente registrada en [ORIP].' "
                "Si TIPO_ADQUISICION_ANTERIOR indica adjudicación/sucesión, redactar: "
                "'[vendedor] adquirió el inmueble por adjudicación en sucesión del causante [causante], "
                "mediante escritura pública número [N] de fecha [fecha] de la Notaría [X], "
                "debidamente registrada en [ORIP].' "
                "PROHIBIDO agregar 'pacto de retroventa' o 'pacto cancelado en el Acto X' "
                "a menos que el presente instrumento contenga explícitamente un acto de cancelación. "
                "NUNCA usar 'por [EMPRESA] le transfirió' — el sujeto es siempre LA PARTE VENDEDORA. "
            )
            # CTL-1: si la cadena_de_tradicion del CTL tiene la entrada de adquisición del vendedor,
            # inyectarla como instrucción autoritativa para el SEGUNDO (evita hallucination del tipo
            # de adquisición — ej: "compraventa" cuando en realidad es "adjudicación en sucesión").
            # PRECAUCIÓN: ep_antecedente_pacto.vendedor suele ser la apoderada, no el causante real.
            # Limpiar SIEMPRE para evitar que DataBinder construya "X le transfirió" incorrecto.
            _ep_ant_pre = inmueble.get("ep_antecedente_pacto")
            if isinstance(_ep_ant_pre, dict) and _ep_ant_pre.get("vendedor"):
                _ep_ant_pre["vendedor"] = ""
            _cadena_raw = (contexto.get("HISTORIA") or {}).get("cadena_de_tradicion") or []
            # Gemini puede devolver la cadena como string o como lista — normalizar a lista
            if isinstance(_cadena_raw, str):
                _cadena_historia = [_cadena_raw]  # tratar el string completo como un item
            elif isinstance(_cadena_raw, list):
                _cadena_historia = _cadena_raw
            else:
                _cadena_historia = []
            _roles_acto_ctl = ctx_acto.get("ROLES_ACTO") or {}
            _vendedores_ctl_list = _roles_acto_ctl.get("VENDEDORES") or []
            _vendedor_norms_ctl = {_normalize_name(v.get("nombre") or "")
                                   for v in _vendedores_ctl_list}
            # CTL-1 PRIMARY: anotaciones_registrales (structured list from Gemini CTL extraction).
            # Prioridad sobre todos los fallbacks: contiene EP, fecha, notaría y tipo de adquisición.
            # Gemini usa dos esquemas: "anotaciones_registrales" (keys de/a) o "anotaciones"
            # (key intervinientes como string "DE: X, A: Y").  Normalizar ambos.
            _segundo_ctl = ""
            _ctl_causante_primary = ""

            # CTL STATE ENGINE — Point 2 (SEGUNDO): usar CTLState como fuente autoritativa.
            # Solo si el mode es específico Y hay from_party conocido (alta confianza).
            # ACQUISITION_UNKNOWN sin from_party = resultado débil → dejar fallbacks correr.
            _ctl_deed_ctx_s2 = contexto.get("_ctl_deed_ctx") or {}
            _ctl_mode_s2 = _ctl_deed_ctx_s2.get("title_acquisition_mode") or ""
            _ctl_from_s2 = _ctl_deed_ctx_s2.get("title_from_party") or ""
            _ctl_text_s2 = _ctl_deed_ctx_s2.get("titulo_adquisicion_text") or ""
            _ctl_high_confidence = (
                _ctl_text_s2
                and _ctl_mode_s2 not in ("adquisicion", "")
                and _ctl_from_s2
                and _ctl_from_s2 != "el causante"
            )
            if _ctl_high_confidence:
                _segundo_ctl = _ctl_text_s2
                _ctl_causante_primary = _ctl_from_s2

            # Gemini usa varios nombres para la lista de anotaciones del CTL.
            # Intentar todos los nombres conocidos con estructura de/a directa.
            _hist_ctx = contexto.get("HISTORIA") or {}
            _anotas_ctl = (
                _hist_ctx.get("anotaciones_registrales") or
                _hist_ctx.get("anotaciones_historicas") or
                []
            )
            if not _anotas_ctl:
                _anotas_raw_alt = (contexto.get("HISTORIA") or {}).get("anotaciones") or []
                _anotas_ctl = []
                for _ar in (_anotas_raw_alt if isinstance(_anotas_raw_alt, list) else []):
                    if not isinstance(_ar, dict):
                        continue
                    _ar2 = dict(_ar)
                    if not _ar2.get("a") and _ar2.get("intervinientes"):
                        _intv_up = (_ar2["intervinientes"] or "").upper()
                        _de_m2 = re.search(r'\bDE:\s*(.+?)(?:,\s*A:|$)', _intv_up)
                        _a_m2  = re.search(r'\bA:\s*(.+?)(?:\s*\(|\s*,|$)', _intv_up)
                        if _de_m2:
                            _ar2["de"] = _de_m2.group(1).strip()
                        if _a_m2:
                            _ar2["a"] = _a_m2.group(1).strip()
                    _anotas_ctl.append(_ar2)
            if not _anotas_ctl:
                # Fix D: HISTORIA.resumen_eventos (lista de strings narrativos)
                # Gemini a veces serializa el CTL completo como strings tipo:
                # "Anotación 007 (Adjudicación en Sucesión): Fecha 24-06-2003, Escritura Pública 1579
                #  del 16-06-2003 de Notaría 1 de Bucaramanga. Adjudicación en sucesión de
                #  CORNEJO GONZALEZ GABRIEL a MARTINEZ DE CORNEJO HILDE."
                _rev_evts = (contexto.get("HISTORIA") or {}).get("resumen_eventos") or []
                for _rev in (_rev_evts if isinstance(_rev_evts, list) else []):
                    if not isinstance(_rev, str):
                        continue
                    _rev_up = _rev.upper()
                    _espec_rv = ""
                    if "ADJUDICACI" in _rev_up and "SUCES" in _rev_up:
                        _espec_rv = "ADJUDICACION EN SUCESION"
                    elif "COMPRAVENTA" in _rev_up:
                        _espec_rv = "COMPRAVENTA"
                    else:
                        continue
                    # Extraer "de X a Y" desde "Adjudicación en sucesión de X a Y"
                    _de_a_rv = re.search(
                        r'[Aa]djudicaci[oó]n\s+en\s+sucesi[oó]n\s+de\s+'
                        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]+?)\s+[Aa]\s+'
                        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]+?)(?:\.|,|\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ])',
                        _rev)
                    _de_rv = _de_a_rv.group(1).strip() if _de_a_rv else ""
                    _a_rv  = _de_a_rv.group(2).strip() if _de_a_rv else ""
                    # Extraer EP: "Escritura Pública 1579 del 16-06-2003"
                    _ep_rv = re.search(
                        r'[Ee]scritura\s+[Pp][úu]blica\s+(\d[\d.]*)\s+del\s+'
                        r'(\d{2}[-/]\d{2}[-/]\d{4})',
                        _rev, re.IGNORECASE)
                    _ep_rv_num   = _ep_rv.group(1) if _ep_rv else ""
                    _ep_rv_fecha = _ep_rv.group(2) if _ep_rv else ""
                    # Extraer notaría: "de Notaría 1 de Bucaramanga"
                    _nota_rv = re.search(
                        r'de\s+([Nn]otar[íi]a\s+\d+\s+de\s+[A-Za-záéíóúñ]+)',
                        _rev)
                    _ep_rv_nota = (_nota_rv.group(1).upper() if _nota_rv else "")
                    # Extraer fecha del evento
                    _fecha_rv = re.search(r'[Ff]echa\s+(\d{2}[-/]\d{2}[-/]\d{4})', _rev)
                    _ev_fecha = _fecha_rv.group(1) if _fecha_rv else _ep_rv_fecha
                    _doc_rv = ""
                    if _ep_rv_num:
                        _doc_rv = f"ESCRITURA {_ep_rv_num} DEL {_ep_rv_fecha}"
                        if _ep_rv_nota:
                            _doc_rv += f" {_ep_rv_nota}"
                    _anotas_ctl.append({
                        "de": _de_rv,
                        "a": _a_rv,
                        "especificacion": _espec_rv,
                        "documento": _doc_rv,
                        "fecha": _ev_fecha,
                    })
            # Fix 1B v34: HISTORIA con keys anotacion_NNN (strings estructurados del CTL)
            # Formato: "... Modo de Adquisición: TIPO. DE: CAUSANTE. A: ADQUIRENTE (CC# X). Valor Acto: $X."
            # Gemini a veces serializa el CTL como dict {anotacion_001: "string", anotacion_007: "string"...}
            # en lugar de {anotaciones_registrales: [lista de dicts]}. Ninguno de los parsers anteriores
            # maneja este formato → _anotas_ctl queda vacío → fallback usa inmueble.tradicion (hallucinated).
            if not _anotas_ctl:
                _anot_nn_items = sorted(
                    [(k, v) for k, v in _hist_ctx.items()
                     if re.match(r'anotacion[_\s]*\d+', k, re.IGNORECASE) and isinstance(v, str)],
                    key=lambda x: x[0]
                )
                for _hk_nn, _hv_nn in _anot_nn_items:
                    _modo_nn = re.search(
                        r'Modo de Adquisi[cç]i[oó]n[:\s]+(.+?)(?:\.\s*DE:|$)', _hv_nn, re.IGNORECASE)
                    _de_nn = re.search(
                        r'\bDE:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?)(?:\.\s*A:|$)', _hv_nn)
                    _a_nn = re.search(
                        r'\bA:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?)(?:\s*\(CC|\s*\.\s*Valor|$)', _hv_nn)
                    if not (_modo_nn and _de_nn and _a_nn):
                        continue
                    # documento="" → Fix B usará ep_antecedente_pacto para EP número+fecha+notaría completos
                    _anotas_ctl.append({
                        "de": _de_nn.group(1).strip(),
                        "a": _a_nn.group(1).strip(),
                        "especificacion": _modo_nn.group(1).strip(),
                        "documento": "",
                        "fecha": "",
                    })
            if isinstance(_anotas_ctl, list):
                for _anota in reversed(_anotas_ctl):       # reversed = anotación más reciente primero
                    _a_a_norm = _normalize_name(_anota.get("a") or "")
                    _a_de     = (_anota.get("de") or "").strip()
                    _a_doc    = (_anota.get("documento") or "").strip()
                    _a_espec  = (_anota.get("especificacion") or "").strip()
                    _a_fecha  = (_anota.get("fecha") or "").strip()
                    if not _a_a_norm:
                        continue
                    _matched_vend = ""
                    for _vn_ctl in _vendedor_norms_ctl:
                        if not _vn_ctl or len(_vn_ctl) <= 3:
                            continue
                        _vn_parts_a = [w for w in _vn_ctl.split() if len(w) > 3]
                        if _vn_parts_a and sum(1 for p in _vn_parts_a if p in _a_a_norm) >= 2:
                            _matched_vend = next(
                                (v.get("nombre") or "" for v in _vendedores_ctl_list
                                 if _normalize_name(v.get("nombre") or "") == _vn_ctl), "")
                            break
                    if not _matched_vend:
                        continue
                    # Parsear EP desde "ESCRITURA 1579 DEL 16-06-2003 NOTARIA 1 DE BUCARAMANGA"
                    _ep_doc_up = _a_doc.upper()
                    _ep_m = re.search(
                        r'ESCRITURA\s+N?[°º]?\s*(\d[\d.]*)\s+DEL?\s+'
                        r'(\d{2}[-/ ]\d{2}[-/ ]\d{4}|\d+\s+DE\s+\w+\s+DE\s+\d{4})'
                        r'(?:\s+NOT[AÁ]RI[AO]\s+(\d+\s+DE\s+[A-ZÁÉÍÓÚÑ ]+))?',
                        _ep_doc_up
                    )
                    _ep_num_p   = _ep_m.group(1) if _ep_m else ""
                    _ep_fecha_p = _ep_m.group(2) if _ep_m else _a_fecha
                    _ep_nota_p  = (_ep_m.group(3) or "").strip().title() if _ep_m else ""
                    _espec_up = _a_espec.upper()
                    if "ADJUDICAC" in _espec_up and "SUCESI" in _espec_up:
                        _seg_txt = (f"{_matched_vend} adquirió el inmueble mediante adjudicación "
                                    f"en sucesión de {_a_de}")
                    elif "COMPRAVENTA" in _espec_up:
                        _seg_txt = f"{_matched_vend} adquirió el inmueble por compraventa de {_a_de}"
                    else:
                        _seg_txt = f"{_matched_vend} adquirió el inmueble ({_a_espec}) de {_a_de}"
                    if _ep_num_p:
                        _seg_txt += f", mediante escritura pública número {_ep_num_p}"
                    if _ep_fecha_p:
                        _seg_txt += f" de fecha {_ep_fecha_p}"
                    if _ep_nota_p:
                        _seg_txt += f" de la Notaría {_ep_nota_p}"
                    _segundo_ctl = _seg_txt
                    # Fix B: si la anotación no tenía campo 'documento', complementar EP
                    # desde ep_antecedente_pacto (número + notaría).
                    # Además limpiar ep_antecedente_pacto.vendedor (es la apoderada, no el causante).
                    _ep_ant_b = inmueble.get("ep_antecedente_pacto") or {}
                    if not _ep_num_p:
                        _ep_ant_b_num = (_ep_ant_b.get("numero_ep") or "").strip()
                        _ep_ant_b_fecha = (_ep_ant_b.get("fecha") or "").strip()
                        _ep_ant_b_nota = (_ep_ant_b.get("notaria") or "").strip()
                        if _ep_ant_b_num:
                            _segundo_ctl += f", mediante escritura pública número {_ep_ant_b_num}"
                        if _ep_ant_b_fecha and not _ep_fecha_p:
                            _segundo_ctl += f" de fecha {_ep_ant_b_fecha}"
                        if _ep_ant_b_nota:
                            _segundo_ctl += f" de la {_ep_ant_b_nota}"
                    # Limpiar vendedor erróneo (apoderada ≠ causante)
                    if isinstance(_ep_ant_b, dict) and _ep_ant_b.get("vendedor"):
                        _ep_ant_b["vendedor"] = ""
                    _ctl_causante_primary = _a_de  # para instrucción anti-Ley160
                    break
            # CTL-1 legacy: cadena_de_tradicion (lista de strings)
            if not _segundo_ctl:
                for _item_ct in reversed(_cadena_historia):
                    _item_ct_norm = _normalize_name(_item_ct)
                    for _vn_ctl in _vendedor_norms_ctl:
                        if _vn_ctl and len(_vn_ctl) > 3 and _vn_ctl in _item_ct_norm:
                            _segundo_ctl = _item_ct
                            break
                    if _segundo_ctl:
                        break
            # CTL-1 fallback 1: Gemini a veces pone la tradición en datos_inmueble.tradicion
            # en lugar de historia_y_antecedentes.cadena_de_tradicion — buscar por token-overlap
            if not _segundo_ctl:
                _inm_tradicion = (inmueble.get("tradicion") or "").strip()
                if _inm_tradicion:
                    _inm_trad_words = set(_normalize_name(_inm_tradicion).split())
                    for _vn_ctl in _vendedor_norms_ctl:
                        if not _vn_ctl or len(_vn_ctl) <= 3:
                            continue
                        _vn_words = set(_vn_ctl.split())
                        if len(_vn_words & _inm_trad_words) >= 3:
                            _segundo_ctl = _inm_tradicion
                            break
            # CTL-1 fallback 2: HISTORIA.texto / descripcion contiene la historia del CTL.
            # Dividir en oraciones y buscar la que menciona al vendedor actual.
            if not _segundo_ctl:
                _hist_texto = ((contexto.get("HISTORIA") or {}).get("texto") or
                               (contexto.get("HISTORIA") or {}).get("descripcion") or "")
                if _hist_texto:
                    _hist_sents = re.split(r'\.\s+', _hist_texto)
                    for _hs in _hist_sents:
                        _hs_norm = _normalize_name(_hs)
                        for _vn_ctl in _vendedor_norms_ctl:
                            if not _vn_ctl or len(_vn_ctl) <= 3:
                                continue
                            _vn_parts_h = [w for w in _vn_ctl.split() if len(w) > 3]
                            if _vn_parts_h and sum(1 for p in _vn_parts_h if p in _hs_norm) >= 2:
                                _segundo_ctl = _hs.strip()
                                break
                        if _segundo_ctl:
                            break
            # CTL-1 fallback 3 (MEJORADO): Gemini usa muchos nombres de clave para la historia de
            # sucesión (antecedentes_sucesion, sucesion_causante, descripcion, fallecimiento_causante…).
            # Agregar TODOS los valores string de HISTORIA y buscar señales de sucesión + vendedor.
            if not _segundo_ctl:
                _ep_ant3 = inmueble.get("ep_antecedente_pacto") or {}
                _ep_ant3_num = (_ep_ant3.get("numero_ep") or "").strip()
                _ep_ant3_fecha = (_ep_ant3.get("fecha") or "").strip()
                _ep_ant3_notaria = (_ep_ant3.get("notaria") or "").strip()
                if _ep_ant3_num:
                    # Concatenar todos los campos string/lista-de-strings de HISTORIA
                    _hist_parts3 = []
                    for _hv in (contexto.get("HISTORIA") or {}).values():
                        if isinstance(_hv, str):
                            _hist_parts3.append(_hv)
                        elif isinstance(_hv, list):
                            _hist_parts3.extend(s for s in _hv if isinstance(s, str))
                    _hist_suc_ctl3 = " ".join(_hist_parts3)
                    _hist_suc_norm3 = _normalize_name(_hist_suc_ctl3)
                    # Verificar señales de sucesión en el texto
                    _suc_kws3 = ("causante", "sucesion", "herencia", "adjudicacion",
                                 "conyuge sobreviviente", "fallecio", "fallecimiento")
                    _has_suc3 = any(kw in _hist_suc_norm3 for kw in _suc_kws3)
                    if _has_suc3:
                        for _vn_ctl3 in _vendedor_norms_ctl:
                            if not _vn_ctl3 or len(_vn_ctl3) <= 3:
                                continue
                            _vn_parts_ctl3 = [w for w in _vn_ctl3.split() if len(w) > 3]
                            if _vn_parts_ctl3 and sum(1 for p in _vn_parts_ctl3 if p in _hist_suc_norm3) >= 2:
                                _causante_m = (
                                    re.search(
                                        r'\b([A-ZÁÉÍÓÚÑ]{2,}(?:\s+[A-ZÁÉÍÓÚÑ]{2,}){2,})\s+falleci[oó]',
                                        _hist_suc_ctl3) or
                                    re.search(
                                        r'causante\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{8,}?)(?:\s*,|\s*\(|\s*tram|\s*$)',
                                        _hist_suc_ctl3, re.IGNORECASE)
                                )
                                if _causante_m:
                                    _causante_n = _causante_m.group(1).strip()
                                else:
                                    # Intentar extraer causante desde inmueble.tradicion
                                    # "fue adquirido por (el/la doctor/a )? NOMBRE_CAUSANTE"
                                    _trad_caus = (inmueble.get("tradicion") or "").strip()
                                    _caus_from_trad = re.search(
                                        r'adquirido por (?:el doctor |la doctora |el |la )?'
                                        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{6,}?)(?:,|\s+por\s|\s+mediante)',
                                        _trad_caus, re.IGNORECASE
                                    )
                                    _causante_n = _caus_from_trad.group(1).strip() if _caus_from_trad else "el causante"
                                _vend_n_ctl3 = next(
                                    (v.get("nombre") or "" for v in _vendedores_ctl_list), "")
                                _segundo_ctl = (
                                    f"{_vend_n_ctl3} adquirió el inmueble mediante adjudicación "
                                    f"en sucesión de {_causante_n}"
                                )
                                if _ep_ant3_num:
                                    _segundo_ctl += f", mediante escritura pública número {_ep_ant3_num}"
                                if _ep_ant3_fecha:
                                    _segundo_ctl += f" de fecha {_ep_ant3_fecha}"
                                if _ep_ant3_notaria:
                                    _segundo_ctl += f" de la {_ep_ant3_notaria}"
                                _ctl_causante_primary = _causante_n
                                break
            # FIX v42-SEGUNDO: Fallback 4 — adjudicataria detectada en PERSONAS_ACTIVOS
            # Cubre cuando HISTORIA vacío Y _vendedores_ctl_list vacío en el mismo run.
            # No requiere datos de HISTORIA: usa rol_en_hoja + inmueble.tradicion + ep_antecedente_pacto.
            if not _segundo_ctl:
                for _pa_s2 in (contexto.get("PERSONAS_ACTIVOS") or []):
                    _rol_s2 = _normalize_name(_pa_s2.get("rol_en_hoja") or "")
                    if not any(kw in _rol_s2 for kw in ("adjudicatari", "conyuge", "viuda")):
                        continue
                    _nom_s2 = (_pa_s2.get("nombre") or "").strip()
                    if not _nom_s2:
                        continue
                    # Causante: buscar en inmueble.tradicion con regex del Fallback 3
                    _trad_s2 = (inmueble.get("tradicion") or "").strip()
                    _causante_s2 = "el causante"
                    _caus_m_s2 = re.search(
                        r'adquirido por (?:el doctor |la doctora |el |la )?'
                        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{6,}?)(?:,|\s+por\s|\s+mediante)',
                        _trad_s2, re.IGNORECASE
                    )
                    if _caus_m_s2:
                        _causante_s2 = _caus_m_s2.group(1).strip()
                    # EP data desde ep_antecedente_pacto
                    _ep_ant_s2 = inmueble.get("ep_antecedente_pacto") or {}
                    _ep_num_s2 = (_ep_ant_s2.get("numero_ep") or "").strip()
                    _ep_fec_s2 = (_ep_ant_s2.get("fecha") or "").strip()
                    _ep_nota_s2 = (_ep_ant_s2.get("notaria") or "").strip()
                    _segundo_ctl = (f"{_nom_s2} adquirió el inmueble mediante adjudicación "
                                    f"en sucesión de {_causante_s2}")
                    if _ep_num_s2:
                        _segundo_ctl += f", mediante escritura pública número {_ep_num_s2}"
                    if _ep_fec_s2:
                        _segundo_ctl += f" de fecha {_ep_fec_s2}"
                    if _ep_nota_s2:
                        _segundo_ctl += f" de la {_ep_nota_s2}"
                    _ctl_causante_primary = _causante_s2
                    break
            if _segundo_ctl:
                # E-02: detectar adjudicación desde rol_en_hoja DEL VENDEDOR — más fiable que keywords en _segundo_ctl
                # El CTL puede clasificar una adjudicación como "compraventa" → confiar en rol_en_hoja primero
                # E-02: usar _normalize_name + substring — evita word-boundary + accent bug
                # "\bADJUDICATARI\b" no matchea "Adjudicataria"; "CONYUGE" no matchea "CÓNYUGE"
                _es_adjudicacion_rol = any(
                    any(kw in _normalize_name(v.get("rol_en_hoja") or "")
                        for kw in ("adjudicatari", "cesionari", "conyuge", "viuda"))
                    for v in _vendedores_ctl_list
                )
                _es_adjudicacion_kw = any(kw in _segundo_ctl.upper()
                                          for kw in ("ADJUDICAD", "SUCESI", "CAUSANTE", "HERENCIA"))
                _es_adjudicacion_personas = any(
                    any(kw in _normalize_name(p.get("rol_en_hoja") or "")
                        for kw in ("adjudicatari", "cesionari", "conyuge", "viuda"))
                    for p in (contexto.get("PERSONAS_ACTIVOS") or [])
                )
                _es_adjudicacion = _es_adjudicacion_rol or _es_adjudicacion_kw or _es_adjudicacion_personas
                # E-01 post-proc: guardar vendedoras con viudez para post-processing al final del pipeline
                # (más robusto que mutación: actúa aunque ROLES_ACTO esté vacío o DataBinder ignore instrucción)
                if _es_adjudicacion:
                    _pp_vend_src = _vendedores_ctl_list or []
                    if not _pp_vend_src:
                        # Fallback: buscar en PERSONAS_ACTIVOS por rol_en_hoja
                        for _pa_pp2 in (contexto.get("PERSONAS_ACTIVOS") or []):
                            _pa_pp2_rol_n = _normalize_name(_pa_pp2.get("rol_en_hoja") or "")
                            if any(kw in _pa_pp2_rol_n for kw in ("adjudicatari", "cesionari", "conyuge", "viuda")):
                                _pp_vend_src = [_pa_pp2]
                    for _v_pp in _pp_vend_src:
                        _v_pp_nom = (_v_pp.get("nombre") or "").strip()
                        if _v_pp_nom:
                            _v_pp_mujer = "de" in _normalize_name(_v_pp_nom).split()
                            _v_pp_ec = "Soltera por viudez" if _v_pp_mujer else "Soltero por viudez"
                            contexto.setdefault("_POST_PROC_VIUDEZ", []).append((_v_pp_nom, _v_pp_ec))
                _compradores_names = " | ".join(
                    c.get("nombre") or "" for c in (
                        (_roles_acto_ctl.get("COMPRADORES") or [])))
                ctx_acto["TIPO_ADQUISICION_ANTERIOR"] = (
                    "ADJUDICACIÓN EN SUCESIÓN" if _es_adjudicacion else ctx_acto.get("TIPO_ADQUISICION_ANTERIOR", "COMPRAVENTA"))
                # Causante: VENDEDOR_ANTERIOR es más fiable que _ctl_causante_primary
                _causante_final = (ctx_acto.get("VENDEDOR_ANTERIOR") or "").strip() or _ctl_causante_primary
                # E-02 causante validation: el causante NO puede ser la misma persona que la vendedora
                # (bug: Gemini a veces extrae el nombre del adquirente como "causante" al revés)
                _vend_words_c = set()
                for _vc in _vendedores_ctl_list:
                    for _w in _normalize_name(_vc.get("nombre") or "").split():
                        if len(_w) > 4: _vend_words_c.add(_w)
                def _causante_es_vendedora(c: str) -> bool:
                    if not c or not _vend_words_c: return False
                    c_words = {w for w in _normalize_name(c).split() if len(w) > 4}
                    return len(c_words & _vend_words_c) >= 2
                # Fix 1A v33: también disparar scan cuando _causante_final es un placeholder [[PENDIENTE:...]]
                # Root cause: "[[PENDIENTE: VENDEDOR_ANTERIOR]]" es truthy → no dispara _causante_es_vendedora
                _causante_is_placeholder = bool(re.search(r'\[\[PENDIENTE', _causante_final or ""))
                if _causante_is_placeholder or (_causante_final and _causante_es_vendedora(_causante_final)):
                    # Buscar causante en CTL anotaciones: campo "de" de la anotación de adjudicación
                    _hist_for_c = contexto.get("HISTORIA") or {}
                    _causante_final = ""
                    for _hck_c in ("anotaciones_registrales", "anotaciones_historicas", "anotaciones"):
                        _hcv_c = _hist_for_c.get(_hck_c)
                        if isinstance(_hcv_c, list):
                            for _ha_c in _hcv_c:
                                _ha_spec_c = (_ha_c.get("especificacion") or "").upper()
                                if any(kw in _ha_spec_c for kw in ("ADJUDICACI", "SUCES", "CAUSANTE")):
                                    _de_val_c = (_ha_c.get("de") or "").strip()
                                    # FIX A1 v38: cuando "de" vacío, parsear campo "intervinientes"
                                    # Formato: "DE: CORNEJO GONZALEZ GABRIEL, A: MARTINEZ DE CORNEJO HILDE"
                                    if not _de_val_c:
                                        _intv_c = (_ha_c.get("intervinientes") or "").strip()
                                        _intv_m = re.search(
                                            r'\bDE:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ &\.,]+?)(?:,?\s*\bA:|\s*$)',
                                            _intv_c, re.IGNORECASE
                                        )
                                        if _intv_m:
                                            _de_val_c = _intv_m.group(1).strip()
                                    if _de_val_c and not _causante_es_vendedora(_de_val_c):
                                        _causante_final = _de_val_c
                                        break
                            if _causante_final: break
                    if not _causante_final:
                        # Fix A v14: HISTORIA como dict descriptivo (no lista de anotaciones)
                        # Ej: {"descripcion": "adquirido por GABRIEL CORNEJO GONZALEZ...",
                        #       "servidumbre_acueducto": "...", "adquisicion_coldamparos": "..."}
                        for _hck_all, _hcv_all in (_hist_for_c.items() if isinstance(_hist_for_c, dict) else []):
                            if not isinstance(_hcv_all, str):
                                continue
                            _hcv_up = _hcv_all.upper()
                            if not any(kw in _hcv_up for kw in ("ADJUDICACI", "SUCES", "CAUSANTE", "ADQUIRIDO POR")):
                                continue
                            _caus_m = re.search(
                                r'(?:(DE:\s*)|(adquirido\s+por\s+))([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{5,50}?)(?:\s+(?:A[:\s]|mediante|→)|[,\.])',
                                _hcv_all, re.IGNORECASE
                            )
                            if _caus_m:
                                _caus_cand = _caus_m.group(3).strip()
                                _is_ctl_de_fmt = bool(_caus_m.group(1))  # True = "DE: X" → CTL, apellidos-first
                                # Fix 1A v34: solo reordenar si es formato narrativo "adquirido por NOMBRE APELLIDOS"
                                # NO reordenar si es "DE: APELLIDOS NOMBRE" (CTL ya en orden notarial correcto)
                                # Fix 2 v32 aplicaba reorder SIEMPRE → double-reversión en CTL format
                                if not _is_ctl_de_fmt:
                                    _caus_parts_ro = _caus_cand.split()
                                    if len(_caus_parts_ro) == 3:
                                        _caus_cand = f"{_caus_parts_ro[1]} {_caus_parts_ro[2]} {_caus_parts_ro[0]}"
                                    elif len(_caus_parts_ro) == 4:
                                        _caus_cand = f"{_caus_parts_ro[2]} {_caus_parts_ro[3]} {_caus_parts_ro[0]} {_caus_parts_ro[1]}"
                                # 2 palabras o >4: dejar sin cambio (ambiguo)
                                if _caus_cand and not _causante_es_vendedora(_caus_cand):
                                    _causante_final = _caus_cand
                                    break
                            # Fix 2 v35: patrón adicional para "causante NOMBRE APELLIDOS" (sucesion_causante key)
                            # El regex anterior (DE: | adquirido por) no matchea "El causante X falleció"
                            if not _causante_final:
                                _caus_m2 = re.search(
                                    r'causante\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{5,50}?)(?:\s+falleci|\s*,|\s*\(|\s+trami)',
                                    _hcv_all, re.IGNORECASE
                                )
                                if _caus_m2:
                                    _caus_cand2 = _caus_m2.group(1).strip()
                                    # Formato narrativo → nombre primero → reordenar a notarial (apellidos primero)
                                    _caus_parts2 = _caus_cand2.split()
                                    if len(_caus_parts2) == 3:
                                        _caus_cand2 = f"{_caus_parts2[1]} {_caus_parts2[2]} {_caus_parts2[0]}"
                                    elif len(_caus_parts2) == 4:
                                        _caus_cand2 = f"{_caus_parts2[2]} {_caus_parts2[3]} {_caus_parts2[0]} {_caus_parts2[1]}"
                                    if _caus_cand2 and not _causante_es_vendedora(_caus_cand2):
                                        _causante_final = _caus_cand2
                                        break
                    if not _causante_final:
                        _causante_final = "[[PENDIENTE: CAUSANTE_SUCESION]]"
                # Fix 1B v33: Sync ctx_acto["VENDEDOR_ANTERIOR"] con el causante computado
                # Previene que DataBinder vea "[[PENDIENTE: VENDEDOR_ANTERIOR]]" en ctx_acto
                ctx_acto["VENDEDOR_ANTERIOR"] = _causante_final
                # Fix C1: detectar formato raw CTL "Anotación N:" → generar narrativa estructurada
                # _force_narrative: cuando adjudicación detectada pero _segundo_ctl dice "compraventa" (CTL mal clasificado)
                _is_ctl_raw = bool(re.search(r'Anotaci[oó]n\s+\d+', _segundo_ctl))
                _force_narrative = _es_adjudicacion and "compraventa" in _segundo_ctl.lower()
                # Fix 1C v34: adjudicación + EP conocido + causante conocido → forzar narrativa Python (_s2_narr)
                # _s2_narr incluye ORIP y folio completos desde ep_antecedente_pacto + ctx_acto.
                # Evita que _segundo_ctl proveniente de fuente parcial (tradicion hallucinated, anotacion_NNN
                # sin doc completo) llegue a DataBinder vía "TRANSCRIBIR EXACTAMENTE" con datos incorrectos.
                if (not _is_ctl_raw
                        and _es_adjudicacion
                        and ctx_acto.get("EP_ANTECEDENTE_NUMERO")
                        and "[[PENDIENTE" not in ctx_acto.get("EP_ANTECEDENTE_NUMERO", "")
                        and _causante_final
                        and "[[PENDIENTE" not in _causante_final):
                    _force_narrative = True
                if _is_ctl_raw or _force_narrative:
                    _s2_vend = next((v.get("nombre") or "" for v in _vendedores_ctl_list), "LA PARTE VENDEDORA")
                    _s2_tipo = "adjudicación en sucesión" if _es_adjudicacion else "compraventa"
                    _s2_prep = (f"del causante {_causante_final or 'el causante'}"
                                if _es_adjudicacion
                                else f"de {_causante_final or 'el anterior propietario'}")
                    _s2_ep   = ctx_acto.get("EP_ANTECEDENTE_NUMERO") or ""
                    _s2_fec  = ctx_acto.get("FECHA_ESCRITURA_ANTERIOR") or ""
                    _s2_not  = ctx_acto.get("EP_ANTECEDENTE_NOTARIA") or ""
                    # FIX A2 v38: normalizar nombre de notaría
                    # "del Círculo Notarial de" → "del Círculo de"
                    _s2_not = re.sub(r'\bC[ií]rculo\s+Notarial\s+de\b', 'Círculo de',
                                     _s2_not, flags=re.IGNORECASE)
                    # "Notaría N De/de/del X" → "Notaría [Ordinal] del Círculo de X"
                    _ORDINALS_S2 = {"1":"Primera","2":"Segunda","3":"Tercera","4":"Cuarta",
                                    "5":"Quinta","6":"Sexta","7":"Séptima","8":"Octava",
                                    "9":"Novena","10":"Décima"}
                    _s2_not = re.sub(
                        r'\bNotar[ií]a\s+(\d+)\s+(?:de|del?)\s+',
                        lambda m: f"Notaría {_ORDINALS_S2.get(m.group(1), m.group(1))} del Círculo de ",
                        _s2_not, flags=re.IGNORECASE
                    )
                    _s2_orip = ctx_acto.get("OFICINA_REGISTRO") or inmueble.get("oficina_registro") or ""
                    _s2_mat  = ctx_acto.get("MATRICULA_INMOBILIARIA") or inmueble.get("matricula") or ""
                    _s2_narr = (
                        f"{_s2_vend} adquirió el inmueble mediante {_s2_tipo} {_s2_prep}, "
                        f"según escritura pública número {_s2_ep} de fecha {_s2_fec} "
                        f"de la {_s2_not}, debidamente registrada en la {_s2_orip} "
                        f"al folio de matrícula inmobiliaria número {_s2_mat}"
                    )
                    instruccion_acto += (
                        f"SEGUNDO (TÍTULO DE ADQUISICIÓN): Redactar narrativa en voz activa EXACTAMENTE: "
                        f"'{_s2_narr}. ----------' "
                        "PROHIBIDO formato 'Anotación N: ...'. "
                        "PROHIBIDO 'pacto de retroventa' o texto de otro acto. "
                        f"ADVERTENCIA CRÍTICA: '{_compradores_names}' es el COMPRADOR ACTUAL — NUNCA en SEGUNDO. "
                    )
                    if _force_narrative:
                        # FIX 1A v39: pop tradicion para que DataBinder no use la cadena completa
                        # y respete la instrucción _s2_narr (solo EP de la adjudicación)
                        (ctx_acto.get("INMUEBLE") or {}).pop("tradicion", None)
                        ctx_acto.pop("TRADICION_INMUEBLE", None)
                else:
                    instruccion_acto += (
                        f"SEGUNDO (TÍTULO DE ADQUISICIÓN): TRANSCRIBIR EXACTAMENTE el siguiente texto "
                        f"(fuente: Certificado de Tradición y Libertad): "
                        f"'{_segundo_ctl}. ----------' "
                        "PROHIBIDO agregar CUALQUIER texto adicional antes o después de esta frase. "
                        "PROHIBIDO añadir: 'en la cual X le transfirió', 'pacto cancelado', 'Primer Acto', "
                        "'cancelado en', 'pacto de retroventa', u otro. "
                        "El campo 'ep_antecedente_pacto' en el contexto es SOLO referencia del EP antecedente "
                        "del vendedor — NO indica que exista un pacto de retroventa en este caso. "
                        f"ADVERTENCIA CRÍTICA: '{_compradores_names}' es el COMPRADOR ACTUAL — NUNCA en SEGUNDO. "
                    )
                if _es_adjudicacion:
                    instruccion_acto += (
                        "El tipo de adquisición ES ADJUDICACIÓN EN SUCESIÓN, NO compraventa ni retroventa. "
                        "ABSOLUTAMENTE PROHIBIDO añadir 'por compra', 'mediante compraventa' u otras "
                        "frases de compraventa a este texto — la adquisición es exclusivamente "
                        "ADJUDICACIÓN EN SUCESIÓN. Si el texto base ya contiene 'por compra', eliminarlo. "
                        "ABSOLUTAMENTE PROHIBIDO mencionar 'pacto de retroventa' — este caso NO tiene pacto de retroventa. "
                        f"PROHIBIDO mencionar a '{_compradores_names}' en el SEGUNDO — "
                        "el comprador actual NO tuvo ninguna relación con el EP antecedente del vendedor. "
                    )
                    if _compradores_names:
                        instruccion_acto += (
                            f"ADVERTENCIA CRÍTICA: '{_compradores_names}' es/son el/la COMPRADOR(A) ACTUAL "
                            "en ESTE instrumento — NUNCA fue(ron) propietario(s) anterior(es) del inmueble. "
                            "PROHIBIDO mencionar su nombre en el SEGUNDO como vendedor, cedente, "
                            "otorgante o parte del antecedente. Aparecer en SEGUNDO = ERROR GRAVE. "
                        )
                    if _ctl_causante_primary:
                        instruccion_acto += (
                            f"CAUSANTE (anterior propietario cuyo inmueble se adjudicó): '{_causante_final or _ctl_causante_primary}'. "
                        )
                    # Si no es rural, prohibir explícitamente Ley 160 / baldíos
                    _rural_aplica_ctl = bool(contexto.get("DATOS_EXTRA", {}).get("RURAL_APLICA"))
                    if not _rural_aplica_ctl:
                        instruccion_acto += (
                            "PROHIBIDO ABSOLUTAMENTE insertar cláusulas de 'Ley 160', 'baldío', 'INCODER', "
                            "'UAF' o 'unidad agrícola familiar' — este es un inmueble URBANO. "
                        )
            # EC-2 (revisado): escanear TODOS los valores de HISTORIA para contexto de sucesión
            # — Gemini renombra la clave diferente cada run; no depender de "antecedentes_sucesion".
            _hist_ec = contexto.get("HISTORIA") or {}
            _hist_suc_ec_parts = []
            for _hec_v in _hist_ec.values():
                if isinstance(_hec_v, str) and _hec_v:
                    _hist_suc_ec_parts.append(_hec_v)
                elif isinstance(_hec_v, dict):
                    _hist_suc_ec_parts.extend(v for v in _hec_v.values() if isinstance(v, str) and v)
            _hist_suc_ec = " ".join(_hist_suc_ec_parts)
            _hist_suc_ec_has_suc = bool(re.search(
                r'\b(sucesion|sucesor|adjudicaci|herencia|causante)\b', _hist_suc_ec, re.IGNORECASE))
            # Fix A-v3: Standalone EC-2 — detectar viudez directamente desde rol_en_hoja del vendedor
            # (independiente de _hist_suc_ec_has_suc — funciona cuando HISTORIA está vacía o con claves inesperadas)
            for _v_ec_st in _vendedores_ctl_list:
                # E-01: usar _normalize_name (elimina tildes, lowercasea) + substring — sin word boundary
                # La regex anterior \b(ADJUDICATARI)\b fallaba en "Adjudicataria" y CONYUGE≠CÓNYUGE
                _rol_ec_st_n = _normalize_name(_v_ec_st.get("rol_en_hoja") or "")
                if any(kw in _rol_ec_st_n for kw in ("adjudicatari", "cesionari", "conyuge", "viuda")):
                    _nombre_st = _v_ec_st.get("nombre") or ""
                    _es_mujer_st = "de" in _normalize_name(_nombre_st).split()
                    _ec_viudez_st = "Soltera por viudez" if _es_mujer_st else "Soltero por viudez"
                    _v_ec_st["estado_civil"] = _ec_viudez_st  # E-01: mutar el dato directamente en ctx_acto
                    # FIX 1B v39: también actualizar flat key que DataBinder prioriza sobre el nested dict
                    _v_idx_v39 = (_vendedores_ctl_list.index(_v_ec_st) + 1
                                  if _v_ec_st in _vendedores_ctl_list else 1)
                    for _flat_ec_v39 in (f"ESTADO_CIVIL_VENDEDOR_{_v_idx_v39}",
                                         "ESTADO_CIVIL_VENDEDOR"):
                        ctx_acto[_flat_ec_v39] = _ec_viudez_st
                    # Fix 1 v32: instrucción con género explícito para evitar "Soltero" vs "Soltera" erróneo
                    _genero_label_st = "MUJER (GÉNERO FEMENINO)" if _es_mujer_st else "HOMBRE (GÉNERO MASCULINO)"
                    _prohib_st = (
                        "ABSOLUTAMENTE PROHIBIDO escribir 'Soltero por viudez' — es MUJER, no hombre."
                        if _es_mujer_st else
                        "ABSOLUTAMENTE PROHIBIDO escribir 'Soltera por viudez' — es HOMBRE, no mujer."
                    )
                    instruccion_acto += (
                        f"ESTADO CIVIL {_genero_label_st} — {_nombre_st}: "
                        f"escribir OBLIGATORIAMENTE '{_ec_viudez_st}'. {_prohib_st} "
                        "PROHIBIDO 'que declara bajo juramento'. El estado civil ES CONOCIDO y DETERMINADO. "
                    )
                    _hist_suc_ec_has_suc = True  # asegurar que MMFL8 también dispare
            # FIX 2 v39: normalizar dirección — insertar "DE LA" entre MANZANA y URBANIZACION/BARRIO
            _inmueble_ctx_v39 = ctx_acto.get("INMUEBLE") or {}
            _dir_raw_v39 = _inmueble_ctx_v39.get("direccion") or ""
            if _dir_raw_v39:
                _dir_fix_v39 = re.sub(
                    r'\bMANZANA\s+(\w+)\s*,\s*(URBANIZACION|BARRIO|CONJUNTO|SECTOR)',
                    r'MANZANA \1 DE LA \2',
                    _dir_raw_v39, flags=re.IGNORECASE
                )
                if _dir_fix_v39 != _dir_raw_v39:
                    _inmueble_ctx_v39["direccion"] = _dir_fix_v39
                    ctx_acto["DIRECCION_INMUEBLE"] = _dir_fix_v39
            # FIX 1C v39: rellenar OCUPACION desde PERSONAS_ACTIVOS cuando placeholder
            for _idx_oc_v39, _vend_oc_v39 in enumerate(_vendedores_ctl_list, start=1):
                _k_oc_v39 = f"OCUPACION_VENDEDOR_{_idx_oc_v39}"
                if "[[PENDIENTE" in (ctx_acto.get(_k_oc_v39) or ""):
                    _vn_v39 = _normalize_name(_vend_oc_v39.get("nombre") or "")
                    _vw_v39 = {w for w in _vn_v39.split() if len(w) > 3}
                    for _pa_v39 in (contexto.get("PERSONAS_ACTIVOS") or []):
                        _pan_v39 = _normalize_name(_pa_v39.get("nombre") or "")
                        _pw_v39 = {w for w in _pan_v39.split() if len(w) > 3}
                        if len(_pw_v39 & _vw_v39) >= 2:
                            _oc_v39 = (_pa_v39.get("ocupacion") or "").strip()
                            if _oc_v39:
                                ctx_acto[_k_oc_v39] = _oc_v39
                                break
            # FIX v41-OCUPACION: Fallback cuando _vendedores_ctl_list vacío
            # Scan PERSONAS_ACTIVOS buscando cualquier persona con rol vendedor y ocupacion
            if not _vendedores_ctl_list:
                _k_oc_fallback = "OCUPACION_VENDEDOR_1"
                if "[[PENDIENTE" in (ctx_acto.get(_k_oc_fallback) or ""):
                    for _pa_oc in (contexto.get("PERSONAS_ACTIVOS") or []):
                        _rol_oc_f = _normalize_name(_pa_oc.get("rol_en_hoja") or "")
                        # Excluir compradores, APs, acreedores
                        if any(kw in _rol_oc_f for kw in ("comprad", "beneficiari", "acreedor",
                                                            " ap ", "apoderado", "poder")):
                            continue
                        _oc_f = (_pa_oc.get("ocupacion") or "").strip()
                        if _oc_f:
                            ctx_acto[_k_oc_fallback] = _oc_f
                            break
            # Fix v47: cuando ocupación sigue PENDIENTE tras todos los fallbacks → "Independiente"
            if "[[PENDIENTE" in (ctx_acto.get("OCUPACION_VENDEDOR_1") or ""):
                ctx_acto["OCUPACION_VENDEDOR_1"] = "Independiente"
            if _hist_suc_ec_has_suc:
                _hist_suc_norm = _normalize_name(_hist_suc_ec)
                for _v_ec in _vendedores_ctl_list:
                    _v_ec_val = (_v_ec.get("estado_civil") or "").strip().upper()
                    # Fix A capa 2: rol_en_hoja explícito de viudez (Cónyuge Sobreviviente, Adjudicataria)
                    # E-01: misma fix normalize — evitar word-boundary + accent bug
                    _rol_ec_n = _normalize_name(_v_ec.get("rol_en_hoja") or "")
                    _ec_override_by_rol = any(kw in _rol_ec_n for kw in ("adjudicatari", "cesionari", "conyuge", "viuda"))
                    # Fix A capa 1: allowlist ampliado con CASADA/CASADO
                    if _ec_override_by_rol or _v_ec_val in (
                            "", "NO_DETECTADO", "ILEGIBLE", "PENDIENTE",
                            "SOLTERA", "SOLTERO", "CASADA", "CASADO"):
                        _v_nom_ec = _normalize_name(_v_ec.get("nombre") or "")
                        _v_parts_ec = [w for w in _v_nom_ec.split() if len(w) > 3]
                        if _v_parts_ec and sum(1 for p in _v_parts_ec if p in _hist_suc_norm) >= 2:
                            # Determinar género: keyword explícito → fallback a "DE" patronímico español
                            # "MARTINEZ DE CORNEJO HILDE": "de" en nombre normalizado = mujer casada
                            _es_mujer_ec = bool(re.search(r'\b(conyuge|conyugue|viuda|esposa)\b', _hist_suc_norm))
                            if not _es_mujer_ec:
                                _es_mujer_ec = "de" in _v_nom_ec.split()
                            _ec_viudez = "Soltera por viudez" if _es_mujer_ec else "Soltero por viudez"
                            # Fix 1 v32: género explícito en segundo bloque (igual que en standalone EC-2)
                            _genero_label_ec = "MUJER (GÉNERO FEMENINO)" if _es_mujer_ec else "HOMBRE (GÉNERO MASCULINO)"
                            _prohib_ec = (
                                "ABSOLUTAMENTE PROHIBIDO escribir 'Soltero por viudez' — es MUJER."
                                if _es_mujer_ec else
                                "ABSOLUTAMENTE PROHIBIDO escribir 'Soltera por viudez' — es HOMBRE."
                            )
                            instruccion_acto += (
                                f"ESTADO CIVIL {_genero_label_ec} — {_v_ec.get('nombre')}: "
                                f"escribir OBLIGATORIAMENTE '{_ec_viudez}'. {_prohib_ec} "
                                "Esta persona es cónyuge sobreviviente. NO usar 'que declara bajo juramento'. "
                            )
            # MMFL8: si hay contexto de sucesión, inyectar estado civil explícito para cada COMPRADOR
            # para impedir que DataBinder generalice la instrucción de viudez del vendedor al acto.
            if _hist_suc_ec_has_suc:
                _ya_cubiertos_mmfl8: set = set()
                # Compradores del acto
                for _c_mmfl8 in (_roles_acto_ctl.get("COMPRADORES") or []):
                    _c_nombre_mmfl8 = (_c_mmfl8.get("nombre") or "").strip()
                    _c_ec_mmfl8 = (_c_mmfl8.get("estado_civil") or "").strip().upper()
                    if not _c_nombre_mmfl8 or _c_ec_mmfl8 in ("VIUDO", "VIUDA"):
                        continue
                    instruccion_acto += (
                        f"ESTADO CIVIL de {_c_nombre_mmfl8}: 'soltero(a) con unión marital de hecho' "
                        f"(NO 'Soltero/a por viudez' — la viudez aplica ÚNICAMENTE a la PARTE VENDEDORA). "
                    )
                    _ya_cubiertos_mmfl8.add(_c_nombre_mmfl8)
                # Fix B: Cubrir también APs (rol_en_hoja "AP") — el AP comparece físicamente
                for _p_mmfl8 in (contexto.get("PERSONAS_ACTIVOS") or []):
                    _p_nom_mmfl8 = (_p_mmfl8.get("nombre") or "").strip()
                    _p_ec_mmfl8 = (_p_mmfl8.get("estado_civil") or "").strip().upper()
                    _p_rol_mmfl8 = (_p_mmfl8.get("rol_en_hoja") or "").upper()
                    if not _p_nom_mmfl8 or _p_nom_mmfl8 in _ya_cubiertos_mmfl8:
                        continue
                    if re.search(r'\bAP\b', _p_rol_mmfl8) and _p_ec_mmfl8 not in ("VIUDO", "VIUDA"):
                        instruccion_acto += (
                            f"ESTADO CIVIL de {_p_nom_mmfl8}: 'soltero(a) con unión marital de hecho' "
                            "(NO 'Soltero/a por viudez'). "
                        )
            # Fix B-v2: Standalone — emitir instrucción para CUALQUIER persona con "unión libre"
            # (independiente de _hist_suc_ec_has_suc — siempre corre para COMPRAVENTA)
            _ya_cubiertos_ul: set = set()
            for _p_ul in (contexto.get("PERSONAS_ACTIVOS") or []):
                _p_ec_ul = (_p_ul.get("estado_civil") or "").strip().lower()
                _p_nom_ul = (_p_ul.get("nombre") or "").strip()
                if not _p_nom_ul or _p_nom_ul in _ya_cubiertos_ul:
                    continue
                if "uni" in _p_ec_ul and "libre" in _p_ec_ul:
                    instruccion_acto += (
                        f"ESTADO CIVIL de {_p_nom_ul}: usar 'soltero(a) con unión marital de hecho' "
                        "(NO 'que declara bajo juramento', NO 'Soltero/a por viudez'). "
                    )
                    _ya_cubiertos_ul.add(_p_nom_ul)
            # CTL STATE ENGINE — Point 3 (TERCERO / NI-08): usar CTLState como fuente autoritativa
            # de gravámenes. Si el engine resolvió, inyectar instrucción directa a DataBinder.
            _ctl_deed_ctx_g = contexto.get("_ctl_deed_ctx") or {}
            if _ctl_deed_ctx_g:
                if _ctl_deed_ctx_g.get("clausula_libertad_libre"):
                    instruccion_acto += (
                        "TERCERO-LIBERTAD (CTL-ENGINE): Según el CTL State Engine el inmueble se "
                        "encuentra LIBRE de hipotecas, embargos, condiciones resolutorias y demás "
                        "gravámenes. ABSOLUTAMENTE PROHIBIDO mencionar 'gravamen de valorización' "
                        "como activo o vigente. Escribir que el inmueble está libre de gravámenes "
                        "y que el/la vendedor/a se obliga al saneamiento de Ley. "
                    )
                elif _ctl_deed_ctx_g.get("active_gravamenes_text") or _ctl_deed_ctx_g.get("active_embargos_text"):
                    _grav_eng = "; ".join(filter(None, [
                        _ctl_deed_ctx_g.get("active_gravamenes_text"),
                        _ctl_deed_ctx_g.get("active_embargos_text"),
                    ]))
                    instruccion_acto += (
                        f"TERCERO-LIBERTAD (CTL-ENGINE NI-08): El inmueble tiene gravámenes ACTIVOS "
                        f"según el CTL State Engine: {_grav_eng}. En TERCERO indicar estos gravámenes "
                        f"vigentes exactamente como se mencionan. "
                    )

            # NI-05/NI-08: Detectar gravámenes activos en CTL — soporta lista, dict Y strings HISTORIA
            _hist_grav = contexto.get("HISTORIA") or {}
            _anotas_grav_src: list = []
            for _grav_key in ("anotaciones_registrales", "anotaciones_historicas",
                              "anotaciones", "anotaciones_relevantes"):
                _grav_cand = _hist_grav.get(_grav_key)
                if isinstance(_grav_cand, list) and _grav_cand:
                    _anotas_grav_src = _grav_cand
                    break
                elif isinstance(_grav_cand, dict) and _grav_cand:
                    # Gemini a veces devuelve dict {"001_compraventa": "...", "008_gravamen": "..."}
                    _anotas_grav_src = [{"especificacion": v, "de": ""} for v in _grav_cand.values()
                                        if isinstance(v, str)]
                    break
            # FIX v41-MMFL12: Pre-compute text scan de HISTORIA ANTES del loop estructurado
            # para poder usarlo dentro del loop y evitar añadir VALORIZ cuando ya está cancelada
            _hist_valoriz_txt_cancelled = any(
                ("VALORIZ" in str(v).upper() and "CANCEL" in str(v).upper())
                for v in (_hist_grav.values() if isinstance(_hist_grav, dict) else [])
                if v
            )
            _active_gravamenes: list = []
            _nios_valoriz_cancelled = False  # FIX B v38: True cuando valorización CTL fue cancelada
            for _idx_g, _anota_g in enumerate(_anotas_grav_src):
                _espec_g = (_anota_g.get("especificacion") or "").upper()
                if not any(kw in _espec_g for kw in ("GRAVAMEN", "EMBARGO", "AFECTACION VIVIENDA", "VALORIZ")):
                    continue
                # Auto-cancelado: el propio texto de la anotación dice "CANCELADA POR ANOTACION X"
                _self_cancelled_g = bool(re.search(r'\bCANCEL', _espec_g))
                # v13 Fix-1: Cancelado por anotación posterior VÁLIDA en la lista.
                # Las cancelaciones declaradas inválidas ("Esta anotación no tiene validez",
                # Art. 59 Ley 1579/2012 salvedad registral) NO cancelan el gravamen.
                _VOID_CANCEL_KW = (
                    "NO TIENE VALIDEZ", "SIN VALIDEZ", "SIN EFECTO",
                    "NO CORRESPONDE", "SALVEDAD REGISTRAL",
                )
                _later_cancelled_g = False
                for _later_g in _anotas_grav_src[_idx_g + 1:]:
                    _later_spec_g = (_later_g.get("especificacion") or "").upper()
                    if "CANCEL" not in _later_spec_g:
                        continue
                    # Si la propia cancelación está declarada inválida/sin efecto → no cancela nada
                    if any(kw in _later_spec_g for kw in _VOID_CANCEL_KW):
                        continue
                    _later_cancelled_g = True
                    break
                # FIX B v38 + FIX v41: registrar cuando valorización fue cancelada (struct o texto)
                if "VALORIZ" in _espec_g and (_self_cancelled_g or _later_cancelled_g
                                               or _hist_valoriz_txt_cancelled):
                    _nios_valoriz_cancelled = True
                if not _self_cancelled_g and not _later_cancelled_g:
                    # FIX v41-MMFL12: no añadir VALORIZ si texto HISTORIA confirma cancelación
                    if "VALORIZ" in _espec_g and _hist_valoriz_txt_cancelled:
                        _nios_valoriz_cancelled = True  # asegurar flag
                    else:
                        _grav_desc = (_anota_g.get("especificacion") or "").strip()
                        _grav_de = (_anota_g.get("de") or "").strip()
                        _active_gravamenes.append(
                            f"{_grav_desc} a favor de {_grav_de}" if _grav_de else _grav_desc
                        )
            # Fallback 2: escanear strings HISTORIA cuando no hay lista/dict de anotaciones
            # CTL ENGINE GUARD: si el engine ya resolvió "libre", no escanear texto histórico
            # (el texto histórico menciona gravámenes del pasado → falsos positivos).
            if not _active_gravamenes and not (_ctl_deed_ctx_g or {}).get("clausula_libertad_libre"):
                for _hv_str in _hist_grav.values():
                    if not isinstance(_hv_str, str):
                        continue
                    _hv_up = _hv_str.upper()
                    if any(kw in _hv_up for kw in ("GRAVAMEN DE VALORIZACIÓN", "GRAVAMEN VIGENTE",
                                                    "VALORIZACIÓN MUNICIPAL", "PLAN VIAL",
                                                    "GRAVAMEN DE VALORIZACION", "EMBARGO",
                                                    "MEDIDA CAUTELAR")):
                        _active_gravamenes.append("gravamen de valorización municipal vigente")
                        break
            # Fix E/I-03 — Fallback 3: PERSONAS_ACTIVOS con rol de acreedor/gravamen/embargo
            if not _active_gravamenes:
                for _pa_g in (contexto.get("PERSONAS_ACTIVOS") or []):
                    _rol_g = (_pa_g.get("rol_en_hoja") or "").upper()
                    if any(kw in _rol_g for kw in ("VALORIZ", "GRAVAMEN", "ACREEDOR",
                                                    "EMBARGO", "CAUTELAR", "MEDIDA")):
                        _grav_nom = (_pa_g.get("nombre") or "").strip()
                        _grav_tipo_pa = ("embargo" if any(kw in _rol_g for kw in ("EMBARGO", "CAUTELAR", "MEDIDA"))
                                         else "gravamen de valorización")
                        # FIX v42: valorización con paz y salvo aportado → marcada cancelada, no añadir
                        if ("VALORIZ" in _rol_g and (
                                datos_extra.get("PAZ_SALVO_VALORIZACION") or
                                _hist_valoriz_txt_cancelled or _nios_valoriz_cancelled)):
                            _nios_valoriz_cancelled = True  # confirmado cancelada por paz y salvo
                            continue  # NO añadir a _active_gravamenes
                        _active_gravamenes.append(
                            f"{_grav_tipo_pa} a favor de {_grav_nom}" if _grav_nom
                            else f"{_grav_tipo_pa} vigente"
                        )
            # CTL ENGINE OVERRIDE: si el engine dice "libre", descartar gravámenes encontrados
            # por Fallback 2 / Fallback 3 (escanean texto histórico y producen falsos positivos).
            if (_ctl_deed_ctx_g or {}).get("clausula_libertad_libre"):
                _active_gravamenes = []
                _nios_valoriz_cancelled = True   # asegurar que NI-LIBRE-F3 dispara si aplica
            if _active_gravamenes:
                _grav_str = "; ".join(_active_gravamenes)
                instruccion_acto += (
                    f"TERCERO-LIBERTAD (NI-08): El inmueble tiene gravámenes ACTIVOS: {_grav_str}. "
                    "En TERCERO: NO escribir 'se encuentra libre de embargo [...] de cualquier otro gravamen'. "
                    f"En cambio: indicar que el inmueble tiene las siguientes anotaciones vigentes: {_grav_str}. "
                )
            # FIX B v38 — NI-LIBRE: cuando NI-05 procesó anotaciones y todas están canceladas
            # → el inmueble está libre de gravámenes → instrucción explícita
            if not _active_gravamenes and _anotas_grav_src:
                instruccion_acto += (
                    "TERCERO-LIBERTAD (NI-LIBRE): El inmueble se encuentra LIBRE de hipotecas, embargos, "
                    "condiciones resolutorias y demás gravámenes (todas las anotaciones de gravamen en el "
                    "Certificado de Tradición y Libertad tienen cancelación registral posterior). "
                    "En TERCERO escribir: 'El inmueble se encuentra libre de hipotecas, embargos, condiciones "
                    "resolutorias y demás gravámenes, salvo las servidumbres que constan en el folio de matrícula. "
                    "En todo caso la parte vendedora se obliga al saneamiento en los casos previstos por la Ley.' "
                    "ABSOLUTAMENTE PROHIBIDO mencionar 'gravamen de valorización' como activo o vigente. "
                )
            # FIX v42: NI-LIBRE-F3 — valorización cancelada detectada por Fallback 3 (PERSONAS_ACTIVOS)
            # _anotas_grav_src vacío → NI-LIBRE no dispara; E-03 no dispara gracias a _nios_valoriz_cancelled.
            # Sin instrucción explícita DataBinder podría mencionar el gravamen al ver PAZ_SALVO en contexto.
            if _nios_valoriz_cancelled and not _active_gravamenes and not _anotas_grav_src:
                instruccion_acto += (
                    "TERCERO-LIBERTAD (NI-LIBRE-F3): La valorización municipal del folio fue CANCELADA "
                    "(existe Paz y Salvo de Valorización aportado al instrumento). "
                    "ABSOLUTAMENTE PROHIBIDO mencionar 'gravamen de valorización' como activo o vigente. "
                    "En TERCERO escribir que el inmueble se encuentra libre de hipotecas, embargos, "
                    "condiciones resolutorias y demás gravámenes, salvo las servidumbres que constan en "
                    "el folio de matrícula. La parte vendedora se obliga al saneamiento de Ley. "
                )
            # E-03: Si NI-05 no detectó gravamen pero existe paz y salvo de valorización,
            # inferir gravamen de valorización activo en el folio (paz y salvo ≠ cancelación registral)
            # FIX B v38: NO disparar si NI-05 encontró la valorización y la confirmó cancelada en CTL
            # FIX v40: tampoco disparar si el texto de HISTORIA menciona valorización cancelada
            if (not _active_gravamenes
                    and not _nios_valoriz_cancelled
                    and not _hist_valoriz_txt_cancelled
                    and datos_extra.get("PAZ_SALVO_VALORIZACION")):
                instruccion_acto += (
                    "TERCERO-LIBERTAD (E-03): El folio registra anotación de VALORIZACIÓN MUNICIPAL VIGENTE. "
                    "La presentación del Paz y Salvo de Valorización confirma que el gravamen existe en el folio "
                    "pero el impuesto está pagado (paz y salvo NO equivale a cancelación registral). "
                    "En TERCERO escribir: 'El inmueble registra anotación de gravamen de valorización municipal "
                    "vigente en el folio de matrícula, cuyo paz y salvo fue aportado al presente instrumento. "
                    "En todo caso la parte vendedora se obliga al saneamiento en los casos previstos por la Ley.' "
                    "PROHIBIDO afirmar que el inmueble está LIBRE de todo gravamen o limitación. "
                )
            # v13 Fix-2: Servidumbre estable desde DATOS_EXTRA / HISTORIA.complementacion
            # DataBinder solo la incluye cuando Gemini la extrae ese run — no determinístico.
            # Inyectar instrucción explícita para garantizar presencia en TERCERO siempre.
            # Fix B v14: Scan dinámico de TODAS las keys de HISTORIA que contengan "servidumbre"
            # La key real varía por run: puede ser "servidumbre_acueducto", "servidumbres_pasivas", etc.
            _hist_for_serv = contexto.get("HISTORIA") or {}
            # FIX C v38: COLDAMPAROS en HISTORIA tiene prioridad sobre "Acueducto" genérico de DATOS_EXTRA.
            # El "or" chain cortocircuitaba: datos_extra["servidumbres_pasivas"]="Acueducto" (truthy)
            # → nunca llegaba al scan de HISTORIA donde está el texto completo con COLDAMPAROS.
            _serv_from_historia_cold = next(
                (v for k, v in (_hist_for_serv.items() if isinstance(_hist_for_serv, dict) else [])
                 if isinstance(v, str) and "COLDAMPAROS" in v.upper()),
                None
            )
            _serv_pasivas_raw = (
                _serv_from_historia_cold or                      # HISTORIA COLDAMPAROS (prioridad)
                datos_extra.get("servidumbres_pasivas") or
                datos_extra.get("SERVIDUMBRES_PASIVAS") or
                # Fix B v14: keys HISTORIA con "servidumbre" en el nombre
                next(
                    (v for k, v in (_hist_for_serv.items() if isinstance(_hist_for_serv, dict) else [])
                     if "servidumbre" in k.lower() and isinstance(v, str) and v.strip()),
                    None
                ) or
                # Fix 3 v32: VALORES de HISTORIA que mencionen servidumbre/acueducto/COLDAMPAROS
                # (cuando Gemini no usa key con "servidumbre" pero el contenido lo menciona)
                next(
                    (v for k, v in (_hist_for_serv.items() if isinstance(_hist_for_serv, dict) else [])
                     if isinstance(v, str) and any(
                         kw in v.upper() for kw in ("SERVIDUMBRE", "COLDAMPAROS", "ACUEDUCTO")
                     )),
                    None
                ) or ""
            ).strip()
            if _serv_pasivas_raw and not _is_garbage(_serv_pasivas_raw):
                _serv_text_v32 = _serv_pasivas_raw if len(_serv_pasivas_raw) < 300 else _serv_pasivas_raw[:300]
                # Fix 3 v32: si COLDAMPAROS detectado → cita legal completa (Escritura 1424/1978)
                if "COLDAMPAROS" in _serv_text_v32.upper():
                    _serv_text_v32 = (
                        "Servidumbre de Acueducto a favor de Cooperativa Colombiana de Previsión y Amparos "
                        "Limitada (COLDAMPAROS), constituida mediante Escritura 1424 del 8 de junio de 1978, "
                        "Notaría 3 de Bucaramanga."
                    )
                instruccion_acto += (
                    f"TERCERO-SERVIDUMBRE (v32): OBLIGATORIO agregar al final de TERCERO, antes del cierre: "
                    f"'Así mismo, el inmueble tiene las siguientes servidumbres pasivas: {_serv_text_v32}' "
                    "NUNCA omitir esta servidumbre. Es un gravamen real registrado en el folio de matrícula. "
                )

            # A-01: Standalone "unión marital de hecho" — SIEMPRE corre (independiente de _hist_suc_ec_has_suc)
            # Fix B-v2 está dentro del if _hist_suc_ec_has_suc, así que no corre en versiones impares.
            # Esta mutación garantiza el formato correcto en TODO run.
            for _p_ul_sa in (contexto.get("PERSONAS_ACTIVOS") or []):
                _p_ec_ul_sa = (_p_ul_sa.get("estado_civil") or "").strip().lower()
                _p_nom_ul_sa = (_p_ul_sa.get("nombre") or "").strip()
                if not _p_nom_ul_sa:
                    continue
                if "uni" in _p_ec_ul_sa and ("libre" in _p_ec_ul_sa or "marital" in _p_ec_ul_sa):
                    _p_ul_sa["estado_civil"] = "soltero(a) con unión marital de hecho"
                    instruccion_acto += (
                        f"ESTADO CIVIL de {_p_nom_ul_sa}: usar EXACTAMENTE 'soltero(a) con unión marital "
                        "de hecho' — incluir el paréntesis (a). NUNCA 'soltero' sin paréntesis. "
                    )
            # PH-1 + MMFL17: si el inmueble es CASA (no PH), prohibir secciones de PH
            # y el texto "LOTE SIN VIVIENDA" en la constancia notarial de afectación.
            _tipo_inm_compra = (inmueble.get("tipo_inmueble") or "").upper().strip()
            if _tipo_inm_compra in ("CASA", "LOTE", "LOTE_BALDIO", "RURAL") or (
                _tipo_inm_compra not in ("PROPIEDAD_HORIZONTAL", "APARTAMENTO")
                and "PH" not in (inmueble.get("descripcion") or "").upper()
            ):
                instruccion_acto += (
                    "RESTRICCIÓN DE FORMATO: Este inmueble NO está en propiedad horizontal. "
                    "PROHIBIDO agregar: (1) sección o párrafo 'RÉGIMEN DE PROPIEDAD HORIZONTAL', "
                    "(2) 'NOTA DE ADMINISTRACIÓN', (3) 'LINDEROS GENERALES' de la urbanización, "
                    "(4) frase 'reglamento de propiedad horizontal' en el texto del PRESENTE(S). "
                    "Generar ÚNICAMENTE los numerales del template de referencia. "
                )
            if _tipo_inm_compra == "CASA":
                # Fix F: usar nombre real del comprador en CONSTANCIA
                _compradores_constancia = _roles_acto_ctl.get("COMPRADORES") or []
                _id_compradora_f = (
                    (_compradores_constancia[0].get("nombre") or "").strip()
                    if _compradores_constancia else ""
                ) or "el/la comprador/a"
                instruccion_acto += (
                    "CONSTANCIA NOTARIAL (MMFL17): Transcribir EXACTAMENTE los dos bloques del template: "
                    "(1) 'CONSTANCIA NOTARIAL PARA EL(LA)(LOS) VENDEDOR(A)(ES): La suscrita notaria "
                    "indagó a la parte vendedora, sobre lo dispuesto en el art. 6 de la ley 258 de 1996 "
                    "modificada por la ley 854 de 2.003 y manifestó: que su estado civil es como se "
                    "contempló al comienzo de este instrumento y que el inmueble que transfiere no se "
                    "encuentra afectado a vivienda familiar. ----------' "
                    "(2) 'CONSTANCIA NOTARIAL PARA EL(LA)(LOS) COMPRADOR(A)(ES): La suscrita notaria "
                    "indagó a la parte compradora, sobre lo dispuesto en el art. 6 de la ley 258 de 1996 "
                    "modificada por la ley 854 de 2.003 y bajo la gravedad de juramento manifestó: que "
                    f"el inmueble que adquieren SÍ constituye vivienda familiar para {_id_compradora_f}. ----------' "
                    "PROHIBIDO sustituir por 'CONSTANCIA NOTARIAL DE AFECTACIÓN A VIVIENDA FAMILIAR'. "
                    "PROHIBIDO el texto 'POR TRATARSE DE UN LOTE SIN VIVIENDA'. "
                    "NI-06: Si el template contiene [[PENDIENTE: CONSTITUYE_VIVIENDA_FAMILIAR]], "
                    f"reemplazar EXACTAMENTE con: 'SÍ constituye vivienda familiar para {_id_compradora_f}'. "
                )
                # Fix E v14: Instrucción para AFECTACIÓN/PATRIMONIO en encabezado CASA
                instruccion_acto += (
                    "ENCABEZADO INMUEBLE (Fix E v14): El template tiene placeholders "
                    "[[PENDIENTE: AFECTACION_VIVIENDA_FAMILIAR]] y [[PENDIENTE: PATRIMONIO_FAMILIA_INEMBARGABLE]]. "
                    "Resolver con: 'AFECTACIÓN A VIVIENDA FAMILIAR: NO.' (la parte vendedora declaró que el "
                    "inmueble NO está afectado a vivienda familiar) y "
                    "'PATRIMONIO DE FAMILIA INEMBARGABLE: NO.' (ninguna de las partes tiene patrimonio de familia). "
                    "NUNCA dejar 'INCLUIR SEGÚN CONSTANCIA NOTARIAL' — reemplazar siempre con el valor correcto. "
                )
            # Bug 8 / H-C: Inject forma_de_pago into COMPRAVENTA context
            fdp = datos_extra.get("FORMA_DE_PAGO") or ""
            if not fdp or _is_garbage(fdp):
                instruccion_acto += (
                    "MMFL13-CUARTO: No hay datos de FORMA_DE_PAGO disponibles. "
                    "Después del precio en CUARTO añadir: "
                    "'La forma de pago se realizó [[PENDIENTE: FORMA_DE_PAGO]]. ----------' "
                    "NO inventar ni asumir modalidad de pago (contado, cuotas, hipoteca). "
                )
            # Fix I-04 ajustado por feedback Word: el PARÁGRAFO SEGUNDO de condición resolutoria
            # solo se conserva cuando la forma de pago evidencia saldo pendiente o financiación.
            if _should_keep_condicion_resolutoria_paragraph(fdp):
                instruccion_acto += (
                    "NI-04-CUARTO: CONSERVAR ÍNTEGRAMENTE el PARÁGRAFO PRIMERO (Ley 2010/2019) y el "
                    "PARÁGRAFO SEGUNDO (condición resolutoria) tal como aparecen en la minuta de referencia, "
                    "porque la forma de pago evidencia saldo pendiente, cuotas, crédito o financiación. "
                    "PROHIBIDO eliminar o resumir cualquiera de los dos párrafos. "
                )
            else:
                instruccion_acto += (
                    "NI-04-CUARTO: CONSERVAR ÍNTEGRAMENTE el PARÁGRAFO PRIMERO (Ley 2010/2019). "
                    "OMITIR el PARÁGRAFO SEGUNDO de condición resolutoria cuando el pago es de contado "
                    "o cuando no existe evidencia de saldo pendiente, cuotas, crédito o hipoteca. "
                    "PROHIBIDO inventar financiación para justificar ese parágrafo. "
                )
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
            # J-1/I-4: Si hay ep_antecedente_pacto Y existe un acto de CANCELACION en este instrumento,
            # aclarar contexto de retroventa. Fix C2: sin cancelacion no generar instrucción de retroventa.
            ep_ant_trad = inmueble.get("ep_antecedente_pacto") or {}
            if ep_ant_trad.get("numero_ep") and not _is_garbage(ep_ant_trad.get("numero_ep") or ""):
                _has_cancelacion_j1 = bool(contexto.get("_ACTO_CANCELACION_REFERENCIA"))
                if _has_cancelacion_j1:
                    _ref_canc = contexto.get("_ACTO_CANCELACION_REFERENCIA")
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
                # else: EP antecedente existe pero es EP de sucesión/compraventa sin retroventa — no generar instrucción
            # J-2: Registrar vendedores de esta compraventa para uso en HIPOTECA (excluye empresa NIT)
            _vendedores_cv = [
                v.get("nombre") for v in _vendedores_ctl_list
                if v.get("nombre") and "NIT" not in (v.get("identificacion") or "").upper()
            ]
            if _vendedores_cv:
                contexto["_VENDEDOR_COMPRAVENTA_NOMBRES"] = " y ".join(_vendedores_cv)
            # Fix 25278-B2: COMPRAVENTA con CANCELACIÓN de pacto de retroventa antecedente en mismo instrumento
            # → EP_ANTECEDENTE desde ep_antecedente_pacto + VENDEDOR_ANTERIOR = parte NIT de la CANCELACIÓN
            if (contexto.get("_ACTO_CANCELACION_REFERENCIA")
                    and ep_ant_global.get("numero_ep")
                    and not _is_garbage(ep_ant_global.get("numero_ep") or "")):
                # EP_ANTECEDENTE: propagar siempre (no solo si [[PENDIENTE]]) — los datos son autoritativos
                ctx_acto["EP_ANTECEDENTE_NUMERO"] = ep_ant_global["numero_ep"]
                ctx_acto["EP_ANTECEDENTE_FECHA"] = ep_ant_global.get("fecha") or ctx_acto.get("EP_ANTECEDENTE_FECHA", "")
                ctx_acto["EP_ANTECEDENTE_NOTARIA"] = ep_ant_global.get("notaria") or ctx_acto.get("EP_ANTECEDENTE_NOTARIA", "")
                # VENDEDOR_ANTERIOR = parte NIT del acto CANCELACIÓN (quien tenía el inmueble bajo el pacto)
                # Override cuando el valor actual ES la vendedora actual (error semántico) O si es [[PENDIENTE]]
                _vendedora_cv_norm = _normalize_name(
                    next((v.get("nombre", "") for v in _vendedores_ctl_list), "")
                )
                _va_actual_norm = _normalize_name(ctx_acto.get("VENDEDOR_ANTERIOR") or "")
                _va_es_vendedora = (_vendedora_cv_norm and _va_actual_norm == _vendedora_cv_norm)
                if _va_es_vendedora or "[[PENDIENTE" in (ctx_acto.get("VENDEDOR_ANTERIOR") or ""):
                    for _a_canc in actos_list:
                        _a_canc_nom = (_a_canc.get("nombre") if isinstance(_a_canc, dict) else "").upper()
                        if "CANCELAC" in _a_canc_nom:
                            for _ot_canc in (_a_canc.get("otorgantes") or []):
                                if _normalize_name(_ot_canc) != _vendedora_cv_norm:
                                    # Buscar nombre completo en PERSONAS_ACTIVOS por word-overlap
                                    _full_va = _ot_canc
                                    for _pa_va in (contexto.get("PERSONAS_ACTIVOS") or []):
                                        _pa_ws = frozenset(_normalize_name(_pa_va.get("nombre","")).split())
                                        _ot_ws = frozenset(_normalize_name(_ot_canc).split())
                                        if len(_pa_ws & _ot_ws) >= 2:
                                            _full_va = _pa_va.get("nombre", _ot_canc)
                                            break
                                    ctx_acto["VENDEDOR_ANTERIOR"] = _full_va.upper()
                                    break
                            break
                # Instrucción dominante para DataBinder (override CTL-1) con ABSOLUTAMENTE PROHIBIDO [[PENDIENTE]]
                _va_final = ctx_acto.get("VENDEDOR_ANTERIOR") or ""
                _ep_num   = ctx_acto.get("EP_ANTECEDENTE_NUMERO") or ""
                _ep_fecha = ctx_acto.get("EP_ANTECEDENTE_FECHA") or ""
                _ep_not   = ctx_acto.get("EP_ANTECEDENTE_NOTARIA") or ""
                if _va_final and _ep_num and "[[PENDIENTE" not in _va_final:
                    instruccion_acto += (
                        "SEGUNDO — TÍTULO DE ADQUISICIÓN DEFINITIVO (OVERRIDE de cualquier otro texto): "
                        f"La vendedora adquirió el inmueble por compraventa con pacto de retroventa "
                        f"a {_va_final}, mediante escritura pública número {_ep_num}"
                        + (f" de fecha {_ep_fecha}" if _ep_fecha else "")
                        + (f" de la {_ep_not}" if _ep_not else "")
                        + f". El pacto de retroventa fue cancelado en "
                        f"{contexto.get('_ACTO_CANCELACION_REFERENCIA', 'el Primer Acto')} "
                        "del presente instrumento. "
                        "ABSOLUTAMENTE PROHIBIDO usar [[PENDIENTE:...]] en el SEGUNDO — "
                        f"todos los datos están disponibles: EP {_ep_num}, fecha {_ep_fecha}, notaría: {_ep_not}, "
                        f"vendedor anterior: {_va_final}. "
                        "TIPO_ADQUISICION_ANTERIOR = 'compraventa con pacto de retroventa'. "
                    )

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
            # S-1: precio_compraventa_original = precio TOTAL de la venta EP526
            # Puede diferir del valor del pacto de retroventa (ej: $605M vs $600M)
            _precio_cv_ant_raw = (ep_ant.get("precio_compraventa_original") or "").strip()
            _precio_compraventa_ant = (
                _precio_cv_ant_raw if _precio_cv_ant_raw and not _is_garbage(_precio_cv_ant_raw)
                else _ep_ant_valor  # fallback: si Gemini no separa, usar el mismo valor
            )
            # S-2: Si Gemini no separó precio_compraventa_original del valor del pacto,
            # usar la cuantía de la COMPRAVENTA del mismo instrumento (más fiable).
            if (not _precio_cv_ant_raw or _is_garbage(_precio_cv_ant_raw)
                    or _precio_compraventa_ant == _ep_ant_valor):
                for _a_cv in actos_list:
                    _a_cv_nombre = (_a_cv.get("nombre") if isinstance(_a_cv, dict) else "").upper()
                    _a_cv_cuantia = int(_a_cv.get("cuantia") or 0) if isinstance(_a_cv, dict) else 0
                    if "COMPRAVENTA" in _a_cv_nombre and _a_cv_cuantia:
                        _precio_compraventa_ant = f"${_a_cv_cuantia:,.0f}".replace(",", ".")
                        break
            ctx_acto["VALOR_ANTECEDENTE"] = _valor_canc_autoritativo          # para CUARTO
            ctx_acto["PRECIO_CANCELACION"] = _valor_canc_autoritativo         # para CUARTO
            ctx_acto["PRECIO_COMPRAVENTA_ANTECEDENTE"] = _precio_compraventa_ant  # para SEGUNDO
            # Suprimir total_venta_hoy para que no contamine el valor del pacto
            for _k_tvh in ("total_venta_hoy", "TOTAL_VENTA_HOY"):
                ctx_acto.pop(_k_tvh, None)
                (ctx_acto.get("DATOS_EXTRA") or {}).pop(_k_tvh, None)
            instruccion_acto += (
                f"SEGUNDO: El precio de la compraventa original (EP antecedente) fue EXACTAMENTE "
                f"{_precio_compraventa_ant} — usar PRECIO_COMPRAVENTA_ANTECEDENTE en la cláusula SEGUNDO. "
                f"CUARTO: El valor del pacto de retroventa que se cancela fue EXACTAMENTE "
                f"{_valor_canc_autoritativo} — usar PRECIO_CANCELACION en la cláusula CUARTO. "
                "ESTOS SON DOS VALORES DISTINTOS — NO usar el mismo número en ambas cláusulas. "
                "NO usar total_venta_hoy ni ninguna suma de cuotas de la compraventa actual. "
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
            # Fix 25278-B3: si nombre_nuevo_asignado no disponible, buscar soporte que muestre
            # explícitamente el cambio FROM nombre_actual (nombre_anterior_predio == inmueble.nombre_nuevo)
            if not _nombre_asignado_comprador:
                _nombre_actual_inmueble_norm = _normalize_name(inmueble.get("nombre_nuevo") or "")
                # Pasada 1: soporte con nombre_anterior_predio == nombre_actual → usar su nombre_nuevo_predio
                for _sp_hv in (contexto.get("_SOPORTES_HALLAZGOS") or []):
                    _ant_sp = _normalize_name((_sp_hv or {}).get("nombre_anterior_predio") or "")
                    _nuevo_sp = ((_sp_hv or {}).get("nombre_nuevo_predio") or "").strip()
                    if (_ant_sp and _nombre_actual_inmueble_norm
                            and _ant_sp == _nombre_actual_inmueble_norm
                            and _nuevo_sp and not _is_garbage(_nuevo_sp)):
                        _nombre_asignado_comprador = _nuevo_sp.upper()
                        break
                # Pasada 2: fallback al primer nombre_nuevo_predio no-nulo (comportamiento anterior)
                if not _nombre_asignado_comprador:
                    for _sp_hv in (contexto.get("_SOPORTES_HALLAZGOS") or []):
                        _nv2 = ((_sp_hv or {}).get("nombre_nuevo_predio") or "").strip()
                        if _nv2 and not _is_garbage(_nv2):
                            _nombre_asignado_comprador = _nv2.upper()
                            break
            # B2: Nombre nuevo del predio — prioridad: comentario > soportes > radicación > inmueble
            _nombre_nuevo_b2 = (
                comentario_overrides.get("NOMBRE_NUEVO_PREDIO")  # H-A: prioridad máxima (usuario)
                or _nombre_asignado_comprador                    # G3B: nombre asignado por comprador
                or datos_extra.get("NOMBRE_NUEVO_PREDIO")        # radicación
                or inmueble.get("nombre_nuevo") or ""
            )
            # Sanity fix 25278: si el "nuevo nombre" == nombre actual/anterior (no hay cambio real),
            # buscar en soportes cualquier nombre distinto a los nombres históricos conocidos del predio
            _nombres_historicos_b2 = {
                _normalize_name(inmueble.get("nombre_nuevo") or ""),
                _normalize_name(inmueble.get("nombre_anterior") or ""),
            }
            _nombres_historicos_b2.discard("")  # no comparar contra string vacío
            if (_nombre_nuevo_b2
                    and _normalize_name(_nombre_nuevo_b2) in _nombres_historicos_b2):
                for _sp_alt in (contexto.get("_SOPORTES_HALLAZGOS") or []):
                    _nv_alt = ((_sp_alt or {}).get("nombre_nuevo_predio") or "").strip()
                    if (_nv_alt
                            and not _is_garbage(_nv_alt)
                            and _normalize_name(_nv_alt) not in _nombres_historicos_b2):
                        _nombre_nuevo_b2 = _nv_alt.upper()
                        break
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

        # NI-04/MMFL11: Eliminar ep_antecedente_pacto del ctx_acto antes de enviar a DataBinder.
        # El campo "pacto" en el nombre confunde a DataBinder → inventa "COMPRADOR le transfirió
        # con pacto de retroventa". Los datos del EP antecedente ya están en instruccion_acto CTL-1.
        _ctx_inmueble_to_clean = ctx_acto.get("INMUEBLE") or {}
        _ctx_inmueble_to_clean.pop("ep_antecedente_pacto", None)
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
    _pz_metro = datos_extra.get("PAZ_SALVO_AREA_METRO") or "[[PENDIENTE: PAZ_SALVO_AREA_METRO]]"
    # NI-04: Filtrar empresa_rl_map a solo empresas reales (NIT).
    # empresa_rl_map también incluye relaciones AP (persona natural → apoderado), lo que causa
    # que se generen líneas de "Cámara de Comercio" para personas naturales. Excluir esas entradas.
    _cc_ins_word_sets = {
        frozenset(_normalize_name(p.get("nombre") or "").split())
        for p in personas_activos or []
        if p.get("nombre") and not re.search(r"\bNIT\b|\bNI\b", (p.get("identificacion") or "").upper())
    }
    _empresa_rl_map_nit = {
        k: v for k, v in (empresa_rl_map or {}).items()
        if not any(frozenset(_normalize_name(k).split()) == cc_ws for cc_ws in _cc_ins_word_sets)
    }
    # CERTIFICADOS_PAZ_Y_SALVO_DETALLE: construir lista de todos los documentos protocolizados
    _cert_lines = build_certificados_paz_y_salvo_detalle(
        _pz_predial if not _is_garbage(_pz_predial) else "",
        _pz_valoriz if not _is_garbage(_pz_valoriz) else "",
        _pz_metro if not _is_garbage(_pz_metro) else "",
    )
    # Agregar cámaras de comercio de empresas reales (solo NIT — excluye personas naturales AP)
    _disp_map_ins = contexto.get("EMPRESA_DISPLAY_MAP") or {}
    _seen_cert_emp: set = set()
    for _emp_k_ins, _rl_ins in _empresa_rl_map_nit.items():
        _emp_name_ins = _disp_map_ins.get(_emp_k_ins) or _emp_k_ins.upper()
        _emp_norm_key = _normalize_name(_emp_name_ins)[:30]
        if _emp_norm_key in _seen_cert_emp:
            continue
        _seen_cert_emp.add(_emp_norm_key)
        _cert_lines.append(f"Certificado de Existencia y Representación Legal de {_emp_name_ins}, expedido por la Cámara de Comercio.")
    # Pre-resolver [[CAMARAS_COMERCIO]] usando solo empresas NIT (evita Cámara para personas naturales)
    _camaras_str_ins = _build_camaras_text(_empresa_rl_map_nit, contexto.get("EMPRESA_DISPLAY_MAP")) or ""
    # Añadir poder(es) AP protocolizados
    _poder_ins_lines = []
    for _p_ap_ins in personas_activos or []:
        if re.search(r"\bAP\b", (_p_ap_ins.get("rol_en_hoja") or "").upper()):
            _rep_ap_ins = (_p_ap_ins.get("representa_a") or "").strip()
            if _rep_ap_ins:
                _poder_ins_lines.append(
                    f"Poder otorgado por {_rep_ap_ins} a {(_p_ap_ins.get('nombre') or '').strip()}."
                )
    # NI-09/MMFL27: incluir poder también en _cert_lines (sección "ME FUERON PRESENTADOS")
    _cert_lines.extend(_poder_ins_lines)
    _certificados_detalle = "\n".join(_cert_lines) if _cert_lines else datos_extra.get("CERTIFICADOS_PAZ_Y_SALVO_DETALLE") or "[[PENDIENTE: CERTIFICADOS_PAZ_Y_SALVO_DETALLE]]"
    _poder_str_ins = "\n".join(_poder_ins_lines)
    _extra_ins = "\n".join(filter(None, [_poder_str_ins, _camaras_str_ins]))
    # Reemplazar placeholder en template antes de enviarlo a DataBinder
    _insertos_tmpl_resolved = EP_INSERTOS_TEMPLATE.replace(
        "[[CAMARAS_COMERCIO]]",
        _extra_ins if _extra_ins else ""
    )
    misiones.append({
        "orden": 91,
        "descripcion": "EP_INSERTOS",
        "plantilla_con_huecos": _insertos_tmpl_resolved,
        "contexto_datos": {
            **contexto,
            "MATRICULA_INMOBILIARIA": matricula,
            "PAZ_SALVO_PREDIAL": _pz_predial or "[[PENDIENTE: PAZ_SALVO_PREDIAL]]",
            "PAZ_SALVO_VALORIZACION": _pz_valoriz or "no aplica",
            "PAZ_SALVO_AREA_METRO": _pz_metro,
            "CERTIFICADOS_PAZ_Y_SALVO_DETALLE": _certificados_detalle,
        },
        "instrucciones": (
            "Completa insertos usando EXACTAMENTE los valores del contexto. "
            "PAZ_SALVO_PREDIAL, PAZ_SALVO_VALORIZACION, PAZ_SALVO_AREA_METRO y "
            "CERTIFICADOS_PAZ_Y_SALVO_DETALLE ya están resueltos — transcríbelos sin modificar. "
            "Si PAZ_SALVO_VALORIZACION es 'no aplica', OMITIR ESA LÍNEA por completo. "
            "CAMARAS_COMERCIO ya está resuelto en el template — "
            "NO añadir ni inventar líneas de Cámara de Comercio de personas naturales ni de personas fallecidas. "
            "Sin markdown. No inventes números ni documentos no listados."
        ),
    })

    # OTORGAMIENTO
    _email_notificaciones = datos_extra.get("EMAIL_NOTIFICACIONES") or "tramites@notaria3bga.com"
    misiones.append({
        "orden": 80,
        "descripcion": "EP_OTORGAMIENTO",
        "plantilla_con_huecos": EP_OTORGAMIENTO_TEMPLATE,
        "passthrough_text": _resolve_static_placeholders(
            EP_OTORGAMIENTO_TEMPLATE,
            {"EMAIL_NOTIFICACIONES": _email_notificaciones},
        ),
        "contexto_datos": {
            **contexto,
            "EMAIL_NOTIFICACIONES": _email_notificaciones,
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
            "contexto_datos": {},  # sin datos personales — campos deben quedar en blanco
            "instrucciones": (
                "Bloques UIAF ya construidos. TRANSCRIBE EXACTAMENTE sin modificar nada. "
                "Todos los campos (NOMBRE, DOCUMENTOS DE IDENTIFICACIÓN, CELULAR, DIRECCIÓN, EMAIL, "
                "PROFESIÓN, ACTIVIDAD ECONÓMICA, ESTADO CIVIL, etc.) deben quedar EN BLANCO — "
                "NO insertes ningún dato personal aunque lo conozcas del contexto. "
                "El cliente completa estos campos en la notaría. "
                "Solo mantén los marcadores ###TABLE_START### y ###TABLE_END###."
            ),
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

    radicado = _extract_radicado_from_radicacion_json(rad_json, raw_text=rad_text)

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
    contexto = _build_universal_context(
        rad_json,
        soportes_json_list,
        cedulas_json_list,
        resolved_radicado=radicado,
    )
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
    # Limpiar outputs de runs anteriores para evitar archivos stale en el DOCX
    debug.clear_binder_outputs()

    async def _run_mision(m: Dict[str, Any]) -> Dict[str, Any]:
        if m.get("passthrough_text"):
            out = str(m["passthrough_text"])
            debug.dump_binder_output(m["orden"], m["descripcion"], out)
            return {"orden": m["orden"], "descripcion": m["descripcion"], "texto": out}

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

    # E-01 post-processing: reemplazar estado civil inválido para vendedores con viudez
    # Actúa DESPUÉS de todos los pasos — robusto aunque DataBinder ignore instrucciones o ROLES_ACTO esté vacío
    _pp_viudez_list = contexto.get("_POST_PROC_VIUDEZ") or []
    for _pv_nom, _pv_ec in _pp_viudez_list:
        _pv_words = [w for w in _normalize_name(_pv_nom).split() if len(w) > 4]
        if not _pv_words:
            continue
        _pv_anchor = re.escape(_pv_words[0])  # primer apellido/nombre largo como ancla
        # FIX v40-MMFL7-B: incluir forma de género incorrecto (Soltero↔Soltera por viudez)
        _pv_bad_forms = ["que declara bajo juramento", "Soltero/a", "soltero/a", "casada", "casado",
                         "mujer mayor de edad", "hombre mayor de edad"]
        if "Soltera" in _pv_ec:
            _pv_bad_forms.append("Soltero por viudez")
        elif "Soltero" in _pv_ec:
            _pv_bad_forms.append("Soltera por viudez")
        for _bad_ec_pp in _pv_bad_forms:
            _pp_pattern = rf'({_pv_anchor}[\s\S]{{0,500}}?de estado civil\s+){re.escape(_bad_ec_pp)}'
            cuerpo_escritura = re.sub(_pp_pattern, rf'\1{_pv_ec}', cuerpo_escritura, flags=re.IGNORECASE, count=1)

    # FIX v40-MMFL7-A: Scan PERSONAS_ACTIVOS — independiente de _POST_PROC_VIUDEZ
    # Cubre cuando _vendedores_ctl_list vacío y _POST_PROC_VIUDEZ queda sin datos
    for _pa_vdz in (contexto.get("PERSONAS_ACTIVOS") or []):
        _pa_rol_n = _normalize_name(_pa_vdz.get("rol_en_hoja") or "")
        if not any(kw in _pa_rol_n for kw in ("adjudicatari", "cesionari", "conyuge", "viuda")):
            continue
        _pa_nom = (_pa_vdz.get("nombre") or "").strip()
        if not _pa_nom:
            continue
        _pa_mujer = "de" in _normalize_name(_pa_nom).split()
        _pa_ec = "Soltera por viudez" if _pa_mujer else "Soltero por viudez"
        _pa_words = [w for w in _normalize_name(_pa_nom).split() if len(w) > 4]
        if not _pa_words:
            continue
        for _anchor in _pa_words[:2]:
            # FIX v41-MMFL7-A: lista ampliada con formas nuevas
            _pa_bad_explicit = [
                "que declara bajo juramento", "Soltero por viudez",
                "Soltero/a", "soltero/a", "casada", "casado",
                "mujer mayor de edad", "hombre mayor de edad",
            ]
            for _bad in _pa_bad_explicit:
                _pp_pat = rf'({re.escape(_anchor)}[\s\S]{{0,600}}?de estado civil\s+){re.escape(_bad)}'
                cuerpo_escritura = re.sub(_pp_pat, rf'\1{_pa_ec}',
                                          cuerpo_escritura, flags=re.IGNORECASE, count=1)
            # FIX v41-MMFL7-A: patrón genérico — reemplaza CUALQUIER EC incorrecto si la forma
            # correcta no está ya presente cerca del ancla
            _anchor_pos = cuerpo_escritura.lower().find(_anchor)
            if _anchor_pos >= 0 and _pa_ec not in cuerpo_escritura[max(0, _anchor_pos - 10):_anchor_pos + 700]:
                _pp_generic = (rf'({re.escape(_anchor)}[\s\S]{{0,600}}?de estado civil\s+)'
                               rf'(?!{re.escape(_pa_ec)})[^\n,;]{{3,50}}')
                cuerpo_escritura = re.sub(_pp_generic, rf'\1{_pa_ec}',
                                          cuerpo_escritura, flags=re.IGNORECASE, count=1)

    # Fix-Final-Gender: nombres patronímicos colombianos con " DE " → género femenino.
    # Cubre casos donde rol_en_hoja = "VENDEDORA"/"Propietario Actual" (no "adjudicataria"),
    # por lo que los bloques EC-2 / E-01 no detectaron a la persona y DataBinder usó el género
    # masculino por defecto. Aplica SOLO cuando "Soltero por viudez" sigue a un nombre con DE.
    cuerpo_escritura = re.sub(
        r'(\b\w+\s+DE\s+\w+[\s\S]{0,400}?de\s+estado\s+civil\s+)Soltero\s+por\s+viudez',
        r'\1Soltera por viudez',
        cuerpo_escritura,
        flags=re.IGNORECASE,
    )

    # Fix-PorCompra: DataBinder a veces inserta "por compra" de la plantilla de compraventa
    # aunque la instrucción diga "adjudicación en sucesión". Eliminar si aparece dentro de
    # los 120 chars siguientes a "adjudicación en sucesión".
    cuerpo_escritura = re.sub(
        r'(adjudicaci[oó]n\s+en\s+sucesi[oó]n\b[^\n]{0,120}?)\s*,?\s*por\s+compra\b[^,.\n]{0,60}',
        r'\1',
        cuerpo_escritura,
        flags=re.IGNORECASE,
    )

    debug.dump_stage_text("08_cuerpo_escritura.txt", cuerpo_escritura)
    debug.write_checklist()

    # 8) RENDER
    docx_path = str(case_dir / f"Minuta_Caso_{radicado}.docx")
    pdf_path = str(case_dir / f"Escritura_Caso_{radicado}.pdf")

    # Fix 5 v32: Normalizar DECONOCIMIENTO → DE CONOCIMIENTO con NFC + regex word-boundary
    # Reemplaza Fix C v14 (str.replace exacto) — cubre variantes Unicode decomposed y espacios dobles
    import unicodedata as _ud_orch
    cuerpo_escritura = _ud_orch.normalize('NFC', cuerpo_escritura)
    # Fix 2 v33: sin \b — captura DECONOCIMIENTO aunque esté pegado a otro carácter (encoding edge cases)
    cuerpo_escritura = re.sub(r'DECONOCIMIENTO', 'DE CONOCIMIENTO', cuerpo_escritura, flags=re.IGNORECASE)

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
