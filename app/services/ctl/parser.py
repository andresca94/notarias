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
    r'(?:[\s,]*(?:DE\s+LA\s+)?NOT[AÁ]R[IÍ][AO]\s+(.+?))?(?:[\.,]|$)',
    re.IGNORECASE,
)

_EP_SHORT_RE = re.compile(
    r'\bEP\s*(\d[\d.]*)\s+DE\s+'
    r'(\d{2}[-/\s]\d{2}[-/\s]\d{4}|\d+\s+DE\s+\w+\s+DE\s+\d{4})'
    r'(?:[\s,]*(?:DE\s+LA\s+)?NOT[AÁ]R[IÍ][AO]\s+(.+?))?(?:[\.,]|$)',
    re.IGNORECASE,
)

_NOTA_RE = re.compile(
    r'NOT[AÁ]R[IÍ][AO]\s+(?:PRIMERA?|SEGUNDA?|TERCERA?|CUARTA?|QUINTA?|\d+)\s+DE\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑA-Za-záéíóúñ ]+',
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
    m = _EP_RE.search(text)
    if not m:
        m = _EP_SHORT_RE.search(text)
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

        # Intervinientes/personas como string "DE: X, A: Y"
        if not from_name and not to_name and isinstance(item.get("personas"), list):
            for persona in item.get("personas") or []:
                if not isinstance(persona, dict):
                    continue
                role = _clean(persona.get("rol") or persona.get("rol_en_hoja") or "").upper()
                name = _clean(persona.get("nombre") or "")
                if not name:
                    continue
                if not from_name and role.startswith("DE"):
                    from_name = name
                elif not to_name and role.startswith("A"):
                    to_name = name

        if not from_name and not to_name:
            raw_parties = _clean(item.get("intervinientes") or item.get("personas") or "")
            if raw_parties:
                m_de = re.search(r'\bDE:\s*(.+?)(?:,\s*A:|\.?\s+A:|$)', raw_parties, re.IGNORECASE)
                m_a = re.search(r'\bA:\s*(.+?)(?:\s*\(|\.|$)', raw_parties, re.IGNORECASE)
                if m_de:
                    from_name = m_de.group(1).strip(" .")
                if m_a:
                    to_name = m_a.group(1).strip(" .")

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


def _parse_string_annotations(items: list, source: str) -> List[Annotation]:
    """
    Formato híbrido: anotaciones_registrales/anotaciones_historicas como lista de strings.
    Ejemplo:
    "Anotación 002: Fecha: 21-06-2007 ... Documento: ESCRITURA 2676 ... Especificación: COMPRAVENTA.
     Personas: DE: X A: Y."
    """
    anns: List[Annotation] = []
    for item in items:
        if not isinstance(item, str):
            continue
        text = item.strip()
        text_up = text.upper()
        if not text:
            continue

        m_num = re.search(r'ANOTACI[OÓ]N\s*(?:N[°º]?)?\s*(\d+)', text_up)
        num = int(m_num.group(1)) if m_num else 0

        m_spec = re.search(
            r'ESPECIFICACI[OÓ]N:\s*(.+?)(?:\.\s*PERSONAS:|\.?\s*$)',
            text,
            re.IGNORECASE,
        )
        spec = (m_spec.group(1).strip() if m_spec else text[:120]).upper()

        m_de = re.search(r'\bDE:\s*(.+?)(?:\s+A:|\.|$)', text, re.IGNORECASE)
        m_a = re.search(r'\bA:\s*(.+?)(?:\.\s*\(|\.|$)', text, re.IGNORECASE)
        from_name = (m_de.group(1).strip(" .") if m_de else "")
        to_name = (m_a.group(1).strip(" .") if m_a else "")

        ep_num, ep_date, ep_nota = _extract_ep(text)
        if not ep_date:
            m_fecha = re.search(r'FECHA:\s*([0-9]{2}[-/][0-9]{2}[-/][0-9]{4})', text, re.IGNORECASE)
            ep_date = m_fecha.group(1) if m_fecha else None

        ann = Annotation(
            number=num,
            raw_specification=spec,
            from_parties=[_party(from_name)] if from_name else [],
            to_parties=[_party(to_name)] if to_name else [],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source=source,
        )
        classify_annotation(ann)
        anns.append(ann)
    return anns


def _parse_eventos_strings(items: list) -> List[Annotation]:
    """
    Formato historia_y_antecedentes.eventos como lista de strings narrativos.
    Ejemplo:
    "FLOREZ IBAÑEZ CARMELINA adquirió por compraventa de LESMES ..., según EP 2676..."
    """
    anns: List[Annotation] = []
    for item in items:
        if not isinstance(item, str):
            continue
        text = item.strip()
        text_up = text.upper()
        if not text:
            continue

        m_num = re.search(r'ANOTACI[OÓ]N\s*(?:NRO|N[°º])?\s*0*(\d+)', text_up)
        num = int(m_num.group(1)) if m_num else 0
        m_leading_date = re.match(r'^\s*(\d{2}-\d{2}-\d{4})\s*:', text)
        leading_date = m_leading_date.group(1) if m_leading_date else None

        m_to = re.match(
            r'^\s*(?:\d{2}-\d{2}-\d{4}:\s*)?([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ ]+?)\s+(?:adquiere|adquiri[oó]|efectu[oó]|efectua|fue)\b',
            text,
            re.IGNORECASE,
        )
        to_name = m_to.group(1).strip() if m_to else ""

        spec = ""
        from_name = ""
        if "COMPRAVENTA" in text_up:
            spec = "COMPRAVENTA"
            m_from = re.search(
                r'COMPRAVENTA\s+DE\s+(.+?)(?:\s*\(EP|,\s*SEG[ÚU]N|$)',
                text,
                re.IGNORECASE,
            )
            from_name = m_from.group(1).strip(" .") if m_from else ""
        elif "ADJUDICACI" in text_up and "SUCESI" in text_up:
            spec = "ADJUDICACION EN SUCESION"
            m_from = re.search(
                r'ADJUDICACI[OÓ]N(?:\s+QUE\s+SE\s+LE\s+HIZO)?\s+EN\s+EL\s+JUICIO\s+DE\s+SUCESI[OÓ]N\s+(?:DEL\s+CAUSANTE\s+|DE\s+)(.+?)(?:\s*\(EP|,\s*SEG[ÚU]N|$)',
                text,
                re.IGNORECASE,
            )
            from_name = m_from.group(1).strip(" .") if m_from else ""
        elif "PERMUTA" in text_up:
            spec = "PERMUTA"
            m_from = re.search(r'PERMUTA.+?\s+CON\s+(.+?)(?:,\s*SEG[ÚU]N|$)', text, re.IGNORECASE)
            from_name = m_from.group(1).strip(" .") if m_from else ""
        elif "DONACI" in text_up:
            spec = "DONACION"
            m_from = re.search(r'DONACI[OÓ]N\s+DE\s+(.+?)(?:,\s*SEG[ÚU]N|$)', text, re.IGNORECASE)
            from_name = m_from.group(1).strip(" .") if m_from else ""
        elif "HIPOTECA" in text_up:
            spec = "HIPOTECA"
        elif "EMBARGO" in text_up:
            spec = "EMBARGO"
        elif "VALORIZ" in text_up:
            spec = "VALORIZACION"
        else:
            continue

        ep_num, ep_date, ep_nota = _extract_ep(text)
        if not ep_num:
            m_ep_inline = re.search(
                r'\(EP\s*(\d[\d.]*)\s*,\s*Notar[íi]a\s+(.+?)\)',
                text,
                re.IGNORECASE,
            )
            if m_ep_inline:
                ep_num = m_ep_inline.group(1)
                ep_nota = m_ep_inline.group(2).strip()
        if not ep_date:
            ep_date = leading_date
        ann = Annotation(
            number=num,
            raw_specification=spec,
            from_parties=[_party(from_name)] if from_name else [],
            to_parties=[_party(to_name)] if to_name else [],
            doc_number=ep_num,
            doc_date=ep_date,
            authority=ep_nota,
            source="historia_eventos",
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
                if any(isinstance(item, dict) for item in cand):
                    result = _parse_structured_list(cand, key)
                else:
                    result = _parse_string_annotations(cand, key)
                if result:
                    return result

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

        # Intento 3b: historia_y_antecedentes.eventos
        for key in ("eventos", "eventos_historicos"):
            cand = hist.get(key)
            if isinstance(cand, list) and cand:
                result = _parse_eventos_strings(cand)
                if result:
                    result += _parse_personas_detalle(personas, ep_ant)
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
        for key in ("texto", "descripcion", "COMPLEMENTACION"):
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
