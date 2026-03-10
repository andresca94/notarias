"""
CTL State Engine — Clasificador jurídico de eventos.

Mapea anotaciones CTL a tipos de evento normalizados de forma DETERMINISTA
(basado en keywords, sin LLM). También extrae referencias de cancelación.
"""
from __future__ import annotations

import re
from typing import Optional

from .models import Annotation, EventType


# ──────────────────────────────────────────────────────────────────────────────
# Mapa de clasificación: tupla de keywords (todos deben estar en spec.upper())
# → EventType correspondiente.
# Orden IMPORTA: las reglas más específicas primero.
# ──────────────────────────────────────────────────────────────────────────────
_SPEC_MAP: list[tuple[tuple[str, ...], EventType]] = [
    # Cancelaciones (verificar primero → más específico)
    (("CANCEL", "HIPOTECA"),        EventType.MORTGAGE_CANCEL),
    (("CANCEL", "EMBARGO"),         EventType.EMBARGO_CANCEL),
    (("CANCEL", "SECUESTRO"),       EventType.SECUESTRO_CANCEL),
    (("CANCEL", "VALORIZ"),         EventType.VALORIZACION_CANCEL),
    (("CANCEL", "PATRIMONIO"),      EventType.PATRIMONIO_FAMILIA_CANCEL),
    (("CANCEL", "VIVIENDA"),        EventType.VIVIENDA_FAMILIAR_CANCEL),
    (("CANCEL", "USUFRUCT"),        EventType.USUFRUCT_CANCEL),
    (("CANCEL", "SERVIDUMBRE"),     EventType.SERVITUDE_CANCEL),
    # Adquisición — con matiz de tipo
    (("ADJUDICACI", "SUCESI"),      EventType.ACQUISITION_SUCCESSION),
    (("ADJUDICACI",),               EventType.ACQUISITION_ADJUDICATION),
    (("COMPRAVENTA",),              EventType.ACQUISITION_SALE),
    (("VENTA",),                    EventType.ACQUISITION_SALE),
    (("DONACION",),                 EventType.ACQUISITION_DONATION),
    (("DONACIÓN",),                 EventType.ACQUISITION_DONATION),
    (("PERMUTA",),                  EventType.ACQUISITION_UNKNOWN),
    (("APORTE",),                   EventType.ACQUISITION_UNKNOWN),
    (("DACION",),                   EventType.ACQUISITION_UNKNOWN),
    (("REMATE",),                   EventType.ACQUISITION_ADJUDICATION),
    # Gravámenes
    (("HIPOTECA",),                 EventType.MORTGAGE_CREATE),
    (("EMBARGO",),                  EventType.EMBARGO_CREATE),
    (("SECUESTRO",),                EventType.SECUESTRO_CREATE),
    (("VALORIZ",),                  EventType.VALORIZACION_CREATE),
    (("GRAVAMEN",),                 EventType.VALORIZACION_CREATE),   # genérico
    (("PLAN VIAL",),                EventType.VALORIZACION_CREATE),
    # Limitaciones
    (("PATRIMONIO", "FAMILIA"),     EventType.PATRIMONIO_FAMILIA_CREATE),
    (("AFECTACION", "VIVIENDA"),    EventType.VIVIENDA_FAMILIAR_CREATE),
    (("USUFRUCT",),                 EventType.USUFRUCT_CREATE),
    (("SERVIDUMBRE",),              EventType.SERVITUDE_CREATE),
    # Administrativos
    (("ACLARACION",),               EventType.CLARIFICATION),
    (("ACLARACIÓN",),               EventType.CLARIFICATION),
    (("RECTIFICA",),                EventType.RECTIFICATION),
    (("CORREC",),                   EventType.RECTIFICATION),
    (("INVALIDACI",),               EventType.INVALIDATION),
    (("NO TIENE VALIDEZ",),         EventType.INVALIDATION),
]

# ──────────────────────────────────────────────────────────────────────────────
# Regex para extraer referencias de cancelación:
# "CANCELA ANOTACION 7", "CANCELA LA ANOTACIÓN 007", "CANCELA ASIENTO 009"
# ──────────────────────────────────────────────────────────────────────────────
_CANCELS_RE = re.compile(
    r'CANCEL[AÁ]\s+(?:LA\s+)?(?:ANOTACI[OÓ]N|ASIENTO)\s+(?:N[°º]?)?\s*(\d+)',
    re.IGNORECASE,
)


def classify_annotation(annotation: Annotation) -> EventType:
    """
    Clasifica la anotación de forma determinista.
    Actualiza `annotation.event_type` y `annotation.cancels_annotation` in place.
    Retorna el EventType resultante.
    """
    spec = annotation.raw_specification.upper()

    # 1. Clasificar tipo de evento
    event_type = EventType.UNKNOWN
    for keywords, etype in _SPEC_MAP:
        if all(kw in spec for kw in keywords):
            event_type = etype
            break
    annotation.event_type = event_type

    # 2. Extraer referencia de cancelación
    m = _CANCELS_RE.search(spec)
    if m:
        annotation.cancels_annotation = int(m.group(1))

    return event_type


def is_lien_cancel(annotation: Annotation) -> bool:
    """True si la anotación cancela un gravamen."""
    return annotation.event_type in (
        EventType.MORTGAGE_CANCEL,
        EventType.EMBARGO_CANCEL,
        EventType.SECUESTRO_CANCEL,
        EventType.VALORIZACION_CANCEL,
        EventType.PATRIMONIO_FAMILIA_CANCEL,
        EventType.VIVIENDA_FAMILIAR_CANCEL,
        EventType.USUFRUCT_CANCEL,
        EventType.SERVITUDE_CANCEL,
    )
