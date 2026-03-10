"""
CTL State Engine — Render Adapter.

Convierte CTLState en un dict plano listo para inyectar en el contexto
del orchestrator (instrucciones para DataBinder).
"""
from __future__ import annotations

from .models import CTLState


def to_deed_context(state: CTLState) -> dict:
    """
    Transforma CTLState en un dict de contexto para orchestrator.py.

    Keys resultantes:
        titulo_adquisicion_text  — texto listo para SEGUNDO
        title_acquisition_mode   — "compraventa" | "adjudicacion_sucesion" | ...
        title_from_party         — causante / vendedor anterior
        title_doc_number         — número EP de adquisición
        title_doc_date           — fecha EP de adquisición
        title_authority          — notaría de adquisición
        current_owner_name       — nombre propietario actual
        current_owner_id         — cédula propietario actual
        clausula_libertad_libre  — True si libre de hipotecas y embargos
        active_gravamenes_text   — descripción de gravámenes activos (str)
        active_embargos_text     — descripción de embargos activos (str)
        active_limitations_text  — limitaciones al dominio (str)
        servidumbres_text        — servidumbres (str)
        warnings                 — List[str] de advertencias
        needs_human_review       — bool
        paz_salvo_valorizacion   — número o "aportado" si existe
    """
    is_libre = (
        len(state.active_liens) == 0
        and len(state.active_embargos) == 0
    )

    gravamenes_text = "; ".join(
        f"{l.lien_type} a favor de {l.creditor}" for l in state.active_liens
    )
    embargos_text = "; ".join(
        f"{e.lien_type} a favor de {e.creditor}" for e in state.active_embargos
    )
    limitations_text = "; ".join(state.active_limitations)
    servidumbres_text = "; ".join(state.active_servitudes)

    return {
        "titulo_adquisicion_text": state.title_acquisition_text,
        "title_acquisition_mode": state.title_acquisition_mode,
        "title_from_party": state.title_from_party,
        "title_doc_number": state.title_doc_number,
        "title_doc_date": state.title_doc_date,
        "title_authority": state.title_authority,
        "current_owner_name": state.current_owner_name,
        "current_owner_id": state.current_owner_id,
        "clausula_libertad_libre": is_libre,
        "active_gravamenes_text": gravamenes_text,
        "active_embargos_text": embargos_text,
        "active_limitations_text": limitations_text,
        "servidumbres_text": servidumbres_text,
        "warnings": state.warnings,
        "needs_human_review": state.needs_human_review,
        "paz_salvo_valorizacion": state.paz_salvo_valorizacion,
    }
