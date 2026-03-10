"""
CTL State Engine — Parser de anotaciones.

Convierte el JSON del CTL (salida variable de Gemini) en List[Annotation].
Maneja los 7+ formatos observados en producción de forma determinista.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .classifier import classify_annotation
from .models import Annotation, EventType, Party

# ──────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ──────────────────────────────────────────────────────────────────────────────

def _party(name: str, role: Optional[str] = None, identifier: Optional[str] = None) -> Party:
    return Party(name=name.strip(), role=role, identifier=identifier)


def _clean(v: Any) -> str:
    return (str(v) if v is not None else "").strip()


_EP_RE = re.compile(
    r'ESCRITURA\s+(?:P[ÚU]BLICA\s+)?'
    r'N?[°º]?\s*(\d[\d.]*)\s+DEL?\s+'
    r'(\d{2}[-/\s]\d{2}[-/\s]\d{4}|\d+\s+DE\s+\w+\s+DE\s+\d{4})'
    r'(?:[\s,]*(?:DE\s+LA\s+)?NOT[AÁ]RI[AO]\s+(.+?))?(?:[\.,]|$)',
    re.IGNORECASE,
)

_NOTA_RE = re.compile(
    r'NOT[AÁ]RI[AO]\s+(?:PRIMERA?|SEGUNDA?|TERCERA?|CUARTA?|QUINTA?|\d+)\s+DE\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑA-Za-záéíóúñ ]+',
    re.IGNORECASE,
)

_ADJ_SUC_RE = re.compile(
    r'[Aa]djudicaci[oó]n\s+en\s+sucesi[oó]n\s+de\s+'
    r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{4,}?)\s+[Aa]\s+'
    r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{4,}?)(?:\.|,|\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ])',
)

_ACQUIRED_RE = re.compile(
    r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{4,}?)\s+ADQUIRI[OÓ]\s+(?:POR\s+)?'
    r'(COMPRA|ADJUDICACI[OÓ]N|DONACI[OÓ]N|PERMUTA|REMATE|APORTE)[^\n]*?'
    r'(?:\s+[AÁ]\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]{4,?}))?',
    re.IGNORECASE,
)

# Roles de personas_detalle que implican lien / gravamen
_LIEN_ROLE_MAP: dict[tuple, EventType] = {
    ("VALORIZ",): EventType.VALORIZACION_CREATE,
    ("GRAVAMEN",): EventType.VALORIZACION_CREATE,
    ("PLAN VIAL",): EventType.VALORIZACION_CREATE,
    ("HIPOTECA",): EventType.MORTGAGE_CREATE,
    ("EMBARGO",): EventType.EMBARGO_CREATE,
    ("CAUTELAR",): EventType.EMBARGO_CREATE,
    ("SECUESTRO",): EventType.SECUESTRO_CREATE,
    ("SERVIDUMBRE",): EventType.SERVITUDE_CREATE,
}

_OWNER_ROLE_KW = ("PROPIETARIO ACTUAL", "PROPIETARIA ACTUAL", "TITULAR", "DUENO", "DUEÑO")


def _extract_ep(text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extrae (número_ep, fecha, notaría) de un texto."""
    m = _EP_RE.search(text.upper())
    if m:
        return m.group(1), m.group(2), (m.group(3) or "").strip() or None
    return None, None, None


def _seq_number(key: str) -> int:
    """Extrae número de una clave tipo 'anotacion_007' → 7."""
    m = re.search(r'(\d+)', key)
    return int(m.group(1)) if m else 0


# ──────────────────────────────────────────────────────────────────────────────
# Parsers por formato
# ──────────────────────────────────────────────────────────────────────────────

def _parse_structured_list(items: list, source: str) -> List[Annotation]:
    """
    Formato 1: lista de dicts con campos de/a/especificacion/documento/fecha.
    También Formato 2: lista con campo 'intervinientes: "DE: X, A: Y"'.
    """
    anns: List[Annotation] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue

        spec = _clean(item.get("especificacion") or item.get("tipo") or "")
        from_name = _clean(item.get("de") or item.get("from") or "")
        to_name = _clean(item.get("a") or item.get("to") or item.get("para") or "")

        # Intervinientes como string "DE: X, A: Y"
        if not from_name and not to_name and item.get("intervinientes"):
            intv = _clean(item["intervinientes"]).upper()
            m_de = re.search(r'\bDE:\s*(.+?)(?:,\s*A:|$)', intv)
            m_a  = re.search(r'\bA:\s*(.+?)(?:\s*\(|\s*,|$)', intv)
            if m_de:
                from_name = m_de.group(1).strip()
            if m_a:
                to_name = m_a.group(1).strip()

        doc_raw = _clean(item.get("documento") or item.get("doc") or "")
        fecha_raw = _clean(item.get("fecha") or item.get("date") or "")
        num_raw = _clean(item.get("numero") or item.get("number") or "")

        ep_num, ep_date, ep_nota = _extract_ep(doc_raw) if doc_raw else (None, None, None)
        if not ep_date:
            ep_date = fecha_raw or None

        ann = Annotation(
            number=int(_clean(item.get("anotacion") or item.get("numero_anotacion") or str(i + 1)) or i + 1),
            raw_specification=spec,
            from_parties=[_party(from_name)] if from_name else [],
            to_parties=[_party(to_name)] if to_name else [],
            doc_number=ep_num or (num_raw if num_raw else None),
            doc_date=ep_date,
            authority=ep_nota,
            source=source,
        )
        classify_annotation(ann)
        anns.append(ann)
    return anns


def _parse_resumen_eventos(events: list) -> List[Annotation]:
    """
    Formato 3: lista de strings narrativos tipo:
    "Anotación 007 (Adjudicación en sucesión): Fecha 24-06-2003, Escritura 1579..."
    """
    anns: List[Annotation] = []
    for ev in events:
        if not isinstance(ev, str):
            continue
        ev_up = ev.upper()

        # Número de anotación
        m_num = re.search(r'ANOTACI[OÓ]N\s*(?:N[°º]?)?\s*(\d+)', ev_up)
        num = int(m_num.group(1)) if m_num else 0

        # Tipo
        spec = ""
        if "ADJUDICACI" in ev_up and "SUCESI" in ev_up:
            spec = "ADJUDICACION EN SUCESION"
        elif "COMPRAVENTA" in ev_up:
            spec = "COMPRAVENTA"
        elif "HIPOTECA" in ev_up:
            spec = "HIPOTECA"
        elif "EMBARGO" in ev_up:
            spec = "EMBARGO"
        elif "VALORIZ" in ev_up:
            spec = "VALORIZACION"
        else:
            spec = ev[:80]

        # Partes (adjudicación)
        m_adj = _ADJ_SUC_RE.search(ev)
        from_name = m_adj.group(1).strip() if m_adj else ""
        to_name = m_adj.group(2).strip() if m_adj else ""

        ep_num, ep_date, ep_nota = _extract_ep(ev)
        ann = Annotation(
            number=num,
            raw_specification=spec,
            from_parties=[_party(from_name)] if from_name else [],
            to_parties=[_party(to_name)] if to_name else [],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="resumen_eventos",
        )
        classify_annotation(ann)
        anns.append(ann)
    return anns


def _parse_anotacion_nnn(hist_dict: dict) -> List[Annotation]:
    """
    Formato 4: dict con claves 'anotacion_001', 'anotacion_007' → strings.
    Texto: "Modo de Adquisición: X. DE: PEDRO. A: JUAN. Valor: $X."
    """
    anns: List[Annotation] = []
    items = sorted(
        [(k, v) for k, v in hist_dict.items()
         if re.match(r'anotacion[_\s]*\d+', k, re.IGNORECASE) and isinstance(v, str)],
        key=lambda x: _seq_number(x[0]),
    )
    for key, val in items:
        val_up = val.upper()
        m_modo = re.search(r'MODO DE ADQUISI[CÇ]I[OÓ]N[:\s]+(.+?)(?:\.\s*DE:|$)', val_up)
        m_de   = re.search(r'\bDE:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?)(?:\.\s*A:|$)', val)
        m_a    = re.search(r'\bA:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?)(?:\s*\(CC|\.\s*Valor|$)', val)
        spec = m_modo.group(1).strip() if m_modo else val[:80]
        from_name = m_de.group(1).strip() if m_de else ""
        to_name   = m_a.group(1).strip() if m_a else ""
        ep_num, ep_date, ep_nota = _extract_ep(val)
        ann = Annotation(
            number=_seq_number(key),
            raw_specification=spec,
            from_parties=[_party(from_name)] if from_name else [],
            to_parties=[_party(to_name)] if to_name else [],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="anotacion_nnn",
        )
        classify_annotation(ann)
        anns.append(ann)
    return anns


def _parse_cadena(cadena: Any) -> List[Annotation]:
    """
    Formato 5: cadena_de_tradicion (string o lista de strings).
    """
    if isinstance(cadena, str):
        items = [cadena]
    elif isinstance(cadena, list):
        items = [s for s in cadena if isinstance(s, str)]
    else:
        return []

    anns: List[Annotation] = []
    for text in items:
        ep_num, ep_date, ep_nota = _extract_ep(text)
        spec = text[:120]
        ann = Annotation(
            number=0,
            raw_specification=spec,
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="cadena_de_tradicion",
        )
        classify_annotation(ann)
        anns.append(ann)
    return anns


def _parse_personas_detalle(personas: list, ep_antecedente: Optional[dict]) -> List[Annotation]:
    """
    Formato 6: personas_detalle con roles.
    Siempre disponible; sirve como último recurso.
    """
    anns: List[Annotation] = []
    for p in personas:
        if not isinstance(p, dict):
            continue
        name = _clean(p.get("nombre") or "")
        rol = _clean(p.get("rol_en_hoja") or p.get("rol") or "").upper()
        identifier = _clean(p.get("identificacion") or "")
        if not name:
            continue

        # Propietario actual → anotación de adquisición sintética
        if any(kw in rol for kw in _OWNER_ROLE_KW):
            ep_num = ep_date = ep_nota = None
            if ep_antecedente and isinstance(ep_antecedente, dict):
                ep_num  = _clean(ep_antecedente.get("numero_ep") or "")
                ep_date = _clean(ep_antecedente.get("fecha") or "")
                ep_nota = _clean(ep_antecedente.get("notaria") or "")
            ann = Annotation(
                number=0,
                raw_specification="ADQUISICION",
                to_parties=[_party(name, rol, identifier)],
                doc_number=ep_num or None,
                doc_date=ep_date or None,
                authority=ep_nota or None,
                source="personas_detalle_owner",
            )
            # Dejar como UNKNOWN para que el resolver lo complete con context
            ann.event_type = EventType.ACQUISITION_UNKNOWN
            anns.append(ann)
            continue

        # Acreedor / gravamen → anotación de lien sintética
        for kws, etype in _LIEN_ROLE_MAP.items():
            if any(kw in rol for kw in kws):
                ann = Annotation(
                    number=0,
                    raw_specification=" ".join(kws),
                    from_parties=[_party(name, rol, identifier)],
                    source="personas_detalle_lien",
                )
                ann.event_type = etype
                anns.append(ann)
                break

    return anns


def _parse_complementacion_string(text: str) -> List[Annotation]:
    """
    Formato 7: historia_y_antecedentes como string largo (complementación).
    Extrae adquisiciones del texto narrativo con regex.
    """
    anns: List[Annotation] = []
    # Buscar patrones de adjudicación en sucesión
    for m in _ADJ_SUC_RE.finditer(text):
        ep_num, ep_date, ep_nota = _extract_ep(text[max(0, m.start() - 50):m.end() + 200])
        ann = Annotation(
            number=0,
            raw_specification="ADJUDICACION EN SUCESION",
            from_parties=[_party(m.group(1).strip())],
            to_parties=[_party(m.group(2).strip())],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="historia_string",
        )
        ann.event_type = EventType.ACQUISITION_SUCCESSION
        anns.append(ann)

    # Buscar "ADQUIRIO POR COMPRA"
    for m in _ACQUIRED_RE.finditer(text):
        spec_word = m.group(2).upper()
        if "SUCESI" in spec_word or "ADJUDICACI" in spec_word:
            continue  # ya capturado arriba
        ep_num, ep_date, ep_nota = _extract_ep(text[m.start():m.end() + 200])
        spec_map = {
            "COMPRA": "COMPRAVENTA",
            "DONACI": "DONACION",
            "REMATE": "REMATE",
            "PERMUTA": "PERMUTA",
            "APORTE": "APORTE",
        }
        spec_str = next((v for k, v in spec_map.items() if k in spec_word.upper()), spec_word)
        ann = Annotation(
            number=0,
            raw_specification=spec_str,
            to_parties=[_party(m.group(1).strip())],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="historia_string",
        )
        classify_annotation(ann)
        anns.append(ann)

    return anns


# ──────────────────────────────────────────────────────────────────────────────
# Función principal
# ──────────────────────────────────────────────────────────────────────────────

def parse_ctl_json(soporte_json: dict) -> List[Annotation]:
    """
    Convierte el JSON del CTL (salida de Gemini) en List[Annotation].

    Estrategia: multi-formato por prioridad decreciente de fiabilidad.
    Las anotaciones de mayor prioridad sobreescriben o complementan las sintéticas.
    """
    hist = soporte_json.get("historia_y_antecedentes") or {}
    personas = soporte_json.get("personas_detalle") or []
    ep_ant = soporte_json.get("ep_antecedente") or {}

    # Intento 1: anotaciones estructuradas con de/a
    if isinstance(hist, dict):
        for key in ("anotaciones_registrales", "anotaciones_historicas"):
            cand = hist.get(key)
            if isinstance(cand, list) and cand:
                return _parse_structured_list(cand, key)

        # Intento 2: anotaciones con intervinientes string
        cand = hist.get("anotaciones")
        if isinstance(cand, list) and cand:
            result = _parse_structured_list(cand, "anotaciones")
            if result:
                return result

        # Intento 3: resumen_eventos
        cand = hist.get("resumen_eventos")
        if isinstance(cand, list) and cand:
            result = _parse_resumen_eventos(cand)
            if result:
                return result

        # Intento 4: anotacion_NNN keys
        result = _parse_anotacion_nnn(hist)
        if result:
            return result

        # Intento 5: cadena_de_tradicion
        cand = hist.get("cadena_de_tradicion")
        if cand:
            result = _parse_cadena(cand)
            if result:
                return result

        # Intento 6: texto libre o descripcion
        for key in ("texto", "descripcion"):
            cand = hist.get(key)
            if isinstance(cand, str) and cand.strip():
                result = _parse_complementacion_string(cand)
                if result:
                    # Complementar con personas_detalle
                    result += _parse_personas_detalle(personas, ep_ant)
                    return result

    # Intento 7: historia_y_antecedentes ES un string (complementación directa)
    if isinstance(hist, str) and hist.strip():
        result = _parse_complementacion_string(hist)
        # Siempre complementar con personas_detalle (propietario actual / acreedor)
        result += _parse_personas_detalle(personas, ep_ant)
        return result

    # Fallback: solo personas_detalle
    return _parse_personas_detalle(personas, ep_ant)
