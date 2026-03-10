"""
CTL State Engine — Public API.

Convierte el JSON del CTL (salida de Gemini) en un CTLState determinista
y en un dict de contexto listo para el orchestrator.

Uso:
    from app.services.ctl import resolve_ctl, to_deed_context

    ctl_state = resolve_ctl(soporte_json, paz_salvo_data)
    deed_ctx = to_deed_context(ctl_state)
"""
from __future__ import annotations

from typing import Optional

from .adapter import to_deed_context
from .classifier import classify_annotation
from .models import CTLState
from .parser import parse_ctl_json
from .resolver import resolve_state


def resolve_ctl(soporte_json: dict, paz_salvo_data: Optional[dict] = None) -> CTLState:
    """
    Punto de entrada principal del CTL State Engine.

    Args:
        soporte_json: JSON del CTL tal como lo devuelve Gemini (el contenido
                      del archivo CTL.pdf__raw.txt, ya parseado como dict).
        paz_salvo_data: dict con paz y salvos del caso (DATOS_EXTRA de orchestrator).
                        Claves esperadas: "PAZ_SALVO_VALORIZACION", "PAZ_SALVO_PREDIAL", etc.

    Returns:
        CTLState con el estado jurídico determinista del folio.
    """
    if paz_salvo_data is None:
        paz_salvo_data = {}

    # 1. Parsear todas las anotaciones del JSON
    annotations = parse_ctl_json(soporte_json)

    # 2. Clasificar (classify_annotation ya fue llamado dentro del parser,
    #    pero re-clasificamos por si alguna quedó UNKNOWN)
    for ann in annotations:
        if ann.event_type.value == "unknown":
            classify_annotation(ann)

    # 3. Metadata del inmueble y hallazgos
    metadata  = soporte_json.get("datos_inmueble") or {}
    hallazgos = soporte_json.get("hallazgos_variables") or {}

    # 4. Resolver estado final
    return resolve_state(annotations, metadata, paz_salvo_data, hallazgos)


__all__ = ["resolve_ctl", "to_deed_context", "CTLState"]
