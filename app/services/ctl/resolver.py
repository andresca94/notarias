"""
CTL State Engine — State Resolver.

Toma List[Annotation] + metadata + paz_salvo_data y produce CTLState determinista.
"""
from __future__ import annotations

import re
from typing import List, Optional

from .models import (
    ACQUISITION_TYPES,
    ActiveLien,
    Annotation,
    CTLState,
    EventType,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_ACQUISITION_MODE_LABELS = {
    EventType.ACQUISITION_SALE:         "compraventa",
    EventType.ACQUISITION_SUCCESSION:   "adjudicacion_sucesion",
    EventType.ACQUISITION_ADJUDICATION: "adjudicacion",
    EventType.ACQUISITION_DONATION:     "donacion",
    EventType.ACQUISITION_UNKNOWN:      "adquisicion",
}

_LIEN_TYPE_LABELS = {
    EventType.MORTGAGE_CREATE:      "hipoteca",
    EventType.EMBARGO_CREATE:       "embargo",
    EventType.SECUESTRO_CREATE:     "secuestro",
    EventType.VALORIZACION_CREATE:  "valorizacion",
    EventType.PATRIMONIO_FAMILIA_CREATE: "patrimonio_familia",
    EventType.VIVIENDA_FAMILIAR_CREATE:  "vivienda_familiar",
    EventType.USUFRUCT_CREATE:      "usufructo",
    EventType.SERVITUDE_CREATE:     "servidumbre",
}

_LIEN_CANCEL_MAP = {
    EventType.MORTGAGE_CANCEL:      EventType.MORTGAGE_CREATE,
    EventType.EMBARGO_CANCEL:       EventType.EMBARGO_CREATE,
    EventType.SECUESTRO_CANCEL:     EventType.SECUESTRO_CREATE,
    EventType.VALORIZACION_CANCEL:  EventType.VALORIZACION_CREATE,
    EventType.PATRIMONIO_FAMILIA_CANCEL: EventType.PATRIMONIO_FAMILIA_CREATE,
    EventType.VIVIENDA_FAMILIAR_CANCEL:  EventType.VIVIENDA_FAMILIAR_CREATE,
    EventType.USUFRUCT_CANCEL:      EventType.USUFRUCT_CREATE,
    EventType.SERVITUDE_CANCEL:     EventType.SERVITUDE_CREATE,
}

_INVALIDATION_EVENTS = {EventType.INVALIDATION, EventType.RECTIFICATION, EventType.CLARIFICATION}


def _build_title_text(ann: Annotation, etype: EventType) -> str:
    """Genera el texto narrativo del título de adquisición para la cláusula SEGUNDO."""
    to_name  = ann.to_parties[0].name  if ann.to_parties  else ""
    from_name = ann.from_parties[0].name if ann.from_parties else "el causante"

    if etype == EventType.ACQUISITION_SUCCESSION:
        base = (f"{to_name} adquirió el inmueble mediante adjudicación en sucesión"
                f" de {from_name}")
    elif etype == EventType.ACQUISITION_SALE:
        base = f"{to_name} adquirió el inmueble por compraventa de {from_name}"
    elif etype == EventType.ACQUISITION_DONATION:
        base = f"{to_name} adquirió el inmueble por donación de {from_name}"
    elif etype == EventType.ACQUISITION_ADJUDICATION:
        base = f"{to_name} adquirió el inmueble por adjudicación de {from_name}"
    else:
        base = f"{to_name} adquirió el inmueble de {from_name}"

    parts = [base]
    if ann.doc_number:
        parts.append(f"mediante escritura pública número {ann.doc_number}")
    if ann.doc_date:
        parts.append(f"de fecha {ann.doc_date}")
    if ann.authority:
        parts.append(f"de la {ann.authority}")

    return ", ".join(parts[:1]) + ("".join(f", {p}" for p in parts[1:]) if len(parts) > 1 else "")


def _has_paz_salvo(paz_salvo_data: dict, keyword: str) -> bool:
    """Verifica si existe paz y salvo para un tipo de gravamen."""
    for k, v in paz_salvo_data.items():
        if keyword.upper() in k.upper() and v:
            return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Función principal
# ──────────────────────────────────────────────────────────────────────────────

def resolve_state(
    annotations: List[Annotation],
    metadata: dict,
    paz_salvo_data: dict,
    hallazgos: Optional[dict] = None,
) -> CTLState:
    """
    Convierte List[Annotation] en CTLState determinista.

    Args:
        annotations: Lista de anotaciones clasificadas (ya con event_type).
        metadata: dict de datos_inmueble del CTL JSON.
        paz_salvo_data: dict con paz y salvos aportados (DATOS_EXTRA de orchestrator).
        hallazgos: dict hallazgos_variables del CTL JSON (servidumbres, etc.).
    """
    matricula = metadata.get("matricula") or ""
    state = CTLState(
        matricula=matricula,
        folio_status=metadata.get("folio_status") or metadata.get("estado_folio"),
        property_address=metadata.get("direccion") or metadata.get("direccion_inmueble"),
    )

    # ── 1. Construir índice por número de anotación ─────────────────────────
    # Sólo numeradas (number > 0) para cancelaciones cruzadas
    ann_by_num: dict[int, Annotation] = {}
    for ann in annotations:
        if ann.number > 0:
            ann_by_num[ann.number] = ann

    # ── 2. Resolver cancelaciones cruzadas ──────────────────────────────────
    for ann in annotations:
        if ann.cancels_annotation and ann.cancels_annotation in ann_by_num:
            ann_by_num[ann.cancels_annotation].invalidated = True

    # También cruzar por tipo: si hay CANCEL de tipo X, buscar el CREATE de tipo X más reciente
    cancel_create_pairs_used: set[int] = set()
    for ann in sorted(annotations, key=lambda a: a.number):
        cancel_etype = _LIEN_CANCEL_MAP.get(ann.event_type)
        if cancel_etype is None or ann.invalidated:
            continue
        # Encontrar el lien más reciente de ese tipo no cancelado
        for candidate in reversed(sorted(annotations, key=lambda a: a.number)):
            if (candidate.event_type == cancel_etype
                    and not candidate.invalidated
                    and candidate.number not in cancel_create_pairs_used
                    and candidate.number != ann.number):
                candidate.invalidated = True
                cancel_create_pairs_used.add(candidate.number)
                break

    # ── 3. Resolución de propietario / título ───────────────────────────────
    # Última adquisición válida (número más alto o sintética si no hay numeradas)
    acquisitions = [
        a for a in sorted(annotations, key=lambda x: x.number)
        if a.event_type in ACQUISITION_TYPES and not a.invalidated
    ]

    last_acq: Optional[Annotation] = None
    # Preferir anotaciones numeradas (más fiables)
    numbered_acqs = [a for a in acquisitions if a.number > 0]
    if numbered_acqs:
        last_acq = numbered_acqs[-1]
    elif acquisitions:
        # Solo sintéticas (número=0, de personas_detalle o historia_string)
        last_acq = acquisitions[-1]

    if last_acq:
        etype = last_acq.event_type
        state.title_acquisition_mode = _ACQUISITION_MODE_LABELS.get(etype, etype.value)
        state.title_from_party = (last_acq.from_parties[0].name
                                  if last_acq.from_parties else None)
        state.title_doc_number = last_acq.doc_number
        state.title_doc_date   = last_acq.doc_date
        state.title_authority  = last_acq.authority
        state.title_acquisition_text = _build_title_text(last_acq, etype)

        # Propietario: to_parties de la última adquisición
        if last_acq.to_parties:
            state.current_owner_name = last_acq.to_parties[0].name
            state.current_owner_id   = last_acq.to_parties[0].identifier
    else:
        state.warnings.append("No se encontró evento de adquisición en el CTL.")
        state.needs_human_review = True

    # Complementar propietario desde personas_detalle si no se obtuvo
    if not state.current_owner_name:
        owner_ann = next(
            (a for a in annotations
             if a.source == "personas_detalle_owner" and a.to_parties),
            None,
        )
        if owner_ann:
            state.current_owner_name = owner_ann.to_parties[0].name
            state.current_owner_id   = owner_ann.to_parties[0].identifier

    # ── 4. Gravámenes activos ────────────────────────────────────────────────
    paz_valoriz = _has_paz_salvo(paz_salvo_data, "VALORIZ")
    paz_predial = _has_paz_salvo(paz_salvo_data, "PREDIAL")

    if paz_valoriz:
        state.paz_salvo_valorizacion = paz_salvo_data.get("PAZ_SALVO_VALORIZACION") or "aportado"

    for ann in annotations:
        if ann.invalidated:
            continue

        if ann.event_type == EventType.VALORIZACION_CREATE:
            if paz_valoriz:
                state.warnings.append(
                    "Valorización registrada en el CTL — CANCELADA por Paz y Salvo aportado."
                )
            else:
                creditor = ann.from_parties[0].name if ann.from_parties else "Municipio"
                state.active_liens.append(
                    ActiveLien("valorizacion", creditor, ann.number)
                )

        elif ann.event_type == EventType.MORTGAGE_CREATE:
            creditor = ann.from_parties[0].name if ann.from_parties else "acreedor hipotecario"
            state.active_liens.append(
                ActiveLien("hipoteca", creditor, ann.number)
            )

        elif ann.event_type == EventType.EMBARGO_CREATE:
            creditor = ann.from_parties[0].name if ann.from_parties else "acreedor embargante"
            state.active_embargos.append(
                ActiveLien("embargo", creditor, ann.number)
            )

        elif ann.event_type == EventType.SECUESTRO_CREATE:
            creditor = ann.from_parties[0].name if ann.from_parties else "secuestrador"
            state.active_embargos.append(
                ActiveLien("secuestro", creditor, ann.number)
            )

        elif ann.event_type == EventType.PATRIMONIO_FAMILIA_CREATE:
            state.active_limitations.append("Afectación a patrimonio de familia vigente")

        elif ann.event_type == EventType.VIVIENDA_FAMILIAR_CREATE:
            state.active_limitations.append("Afectación a vivienda familiar vigente")

        elif ann.event_type == EventType.USUFRUCT_CREATE:
            creditor = ann.from_parties[0].name if ann.from_parties else "usufructuario"
            state.active_limitations.append(f"Usufructo a favor de {creditor}")

        elif ann.event_type == EventType.SERVITUDE_CREATE:
            desc = ann.raw_specification or "servidumbre"
            state.active_servitudes.append(desc)

    # ── 5. Servidumbres desde hallazgos_variables ───────────────────────────
    if hallazgos and isinstance(hallazgos, dict):
        srv_raw = hallazgos.get("servidumbres_pasivas") or ""
        if isinstance(srv_raw, str) and srv_raw.strip():
            srv_items = [s.strip() for s in re.split(r'[;,]', srv_raw) if s.strip()]
            for srv in srv_items:
                if not any(srv.lower() in s.lower() for s in state.active_servitudes):
                    state.active_servitudes.append(srv)
        elif isinstance(srv_raw, list):
            for srv in srv_raw:
                if srv and str(srv).strip() not in state.active_servitudes:
                    state.active_servitudes.append(str(srv).strip())

    # ── 6. Advertencias de revisión ─────────────────────────────────────────
    if not acquisitions:
        # No se pudo clasificar ninguna adquisición — revisar manualmente
        state.needs_human_review = True
        state.warnings.append(
            "No se clasificaron eventos de adquisición. "
            "Verificar formato del CTL o completar manualmente."
        )

    return state
