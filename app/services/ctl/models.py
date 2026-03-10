"""
CTL State Engine — Modelos de datos.

Representa el estado jurídico vigente de un folio de matrícula inmobiliaria
derivado del Certificado de Tradición y Libertad (CTL).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class EventType(str, Enum):
    """Tipos de evento jurídico normalizados."""

    # Adquisición de dominio
    ACQUISITION_SALE         = "acquisition_sale"
    ACQUISITION_SUCCESSION   = "acquisition_succession"
    ACQUISITION_ADJUDICATION = "acquisition_adjudication"
    ACQUISITION_DONATION     = "acquisition_donation"
    ACQUISITION_UNKNOWN      = "acquisition_unknown"

    # Hipoteca
    MORTGAGE_CREATE      = "mortgage_create"
    MORTGAGE_CANCEL      = "mortgage_cancel"
    MORTGAGE_SUBROGATION = "mortgage_subrogation"
    MORTGAGE_MODIFICATION = "mortgage_modification"

    # Embargo / medidas cautelares
    EMBARGO_CREATE = "embargo_create"
    EMBARGO_CANCEL = "embargo_cancel"
    SECUESTRO_CREATE = "secuestro_create"
    SECUESTRO_CANCEL = "secuestro_cancel"

    # Valorización
    VALORIZACION_CREATE = "valorizacion_create"
    VALORIZACION_CANCEL = "valorizacion_cancel"

    # Servidumbres
    SERVITUDE_CREATE = "servitude_create"
    SERVITUDE_CANCEL = "servitude_cancel"

    # Limitaciones al dominio
    PATRIMONIO_FAMILIA_CREATE = "patrimonio_familia_create"
    PATRIMONIO_FAMILIA_CANCEL = "patrimonio_familia_cancel"
    VIVIENDA_FAMILIAR_CREATE  = "vivienda_familiar_create"
    VIVIENDA_FAMILIAR_CANCEL  = "vivienda_familiar_cancel"
    USUFRUCT_CREATE = "usufruct_create"
    USUFRUCT_CANCEL = "usufruct_cancel"

    # Administrativos
    RECTIFICATION = "rectification"
    CLARIFICATION = "clarification"
    INVALIDATION  = "invalidation"
    UNKNOWN       = "unknown"


# Tipos que transfieren dominio
ACQUISITION_TYPES = {
    EventType.ACQUISITION_SALE,
    EventType.ACQUISITION_SUCCESSION,
    EventType.ACQUISITION_ADJUDICATION,
    EventType.ACQUISITION_DONATION,
    EventType.ACQUISITION_UNKNOWN,
}

# Tipos que crean gravamen
LIEN_CREATE_TYPES = {
    EventType.MORTGAGE_CREATE,
    EventType.EMBARGO_CREATE,
    EventType.SECUESTRO_CREATE,
    EventType.VALORIZACION_CREATE,
}

# Tipos que cancelan gravamen
LIEN_CANCEL_TYPES = {
    EventType.MORTGAGE_CANCEL,
    EventType.EMBARGO_CANCEL,
    EventType.SECUESTRO_CANCEL,
    EventType.VALORIZACION_CANCEL,
}


@dataclass
class Party:
    """Parte interviniente en una anotación."""
    name: str
    role: Optional[str] = None
    identifier: Optional[str] = None


@dataclass
class Annotation:
    """
    Evento jurídico unitario extraído del CTL.
    Un número de anotación = un evento.
    """
    number: int             # 0 = sintetizada (sin número en el CTL)
    event_type: EventType = EventType.UNKNOWN
    from_parties: List[Party] = field(default_factory=list)
    to_parties: List[Party] = field(default_factory=list)
    doc_number: Optional[str] = None
    doc_date: Optional[str] = None
    authority: Optional[str] = None     # Notaría
    raw_specification: str = ""
    cancels_annotation: Optional[int] = None   # número que cancela
    invalidated: bool = False
    source: str = ""   # qué clave/formato de Gemini generó esta anotación


@dataclass
class ActiveLien:
    """Gravamen o carga activa sobre el inmueble."""
    lien_type: str          # "hipoteca" | "embargo" | "valorizacion" | "secuestro" | "cautelar"
    creditor: str           # acreedor o beneficiario del gravamen
    annotation_number: int
    cancelled: bool = False
    cancel_reason: Optional[str] = None


@dataclass
class CTLState:
    """
    Estado jurídico vigente del folio de matrícula.
    Producto final del CTL State Engine.
    """
    matricula: str
    folio_status: Optional[str] = None
    property_address: Optional[str] = None

    # ── Titular actual ──────────────────────────────────────────────
    current_owner_name: Optional[str] = None
    current_owner_id: Optional[str] = None

    # ── Título de adquisición vigente ───────────────────────────────
    title_acquisition_mode: Optional[str] = None   # valor de EventType.value
    title_acquisition_text: Optional[str] = None   # texto listo para cláusula SEGUNDO
    title_from_party: Optional[str] = None          # causante / vendedor anterior
    title_doc_number: Optional[str] = None
    title_doc_date: Optional[str] = None
    title_authority: Optional[str] = None

    # ── Gravámenes activos ──────────────────────────────────────────
    active_liens: List[ActiveLien] = field(default_factory=list)
    active_embargos: List[ActiveLien] = field(default_factory=list)

    # ── Limitaciones vigentes ───────────────────────────────────────
    active_servitudes: List[str] = field(default_factory=list)
    active_limitations: List[str] = field(default_factory=list)

    # ── Diagnóstico ─────────────────────────────────────────────────
    warnings: List[str] = field(default_factory=list)
    needs_human_review: bool = False
    paz_salvo_valorizacion: Optional[str] = None
