from pathlib import Path

import pytest

from app.pipeline.act_engine import _acto_kind
from app.pipeline.orchestrator import (
    _generated_act_has_body,
    _validate_generated_act_sections,
)
from app.services.rag.local_rag import LocalRAG


def test_acto_kind_distinguishes_codigo_catastral_from_nomenclatura():
    assert _acto_kind("ACTUALIZACION DE CODIGO CATASTRAL") == "ACTUALIZACION_CODIGO_CATASTRAL"
    assert _acto_kind("ACTUALIZACION DE NOMENCLATURA") == "ACTUALIZACION_NOMENCLATURA"


def test_local_rag_returns_specific_template_for_codigo_catastral():
    store_dir = Path(__file__).resolve().parents[1] / "app" / "rag_store"
    rag = LocalRAG(store_dir=str(store_dir))

    raw = rag.retrieve_acto_text("ACTUALIZACION DE CODIGO CATASTRAL")

    assert "ACTUALIZAR EL CÓDIGO CATASTRAL" in raw
    assert "ACTUALIZAR LA NOMENCLATURA" not in raw


def test_generated_act_has_body_accepts_structured_act_output():
    text = """
---PRIMER ACTO---
---ACTUALIZACION DE CODIGO CATASTRAL---
Comparecieron: JAIME GOMEZ FLOREZ, mayor de edad, y manifestó:
PRIMERO: Que es propietario del inmueble.
SEGUNDO: TRADICIÓN: Que el inmueble fue adquirido por compraventa.
"""

    assert _generated_act_has_body(text) is True


def test_validate_generated_act_sections_rejects_missing_body():
    acts = [{"nombre": "ACTUALIZACION DE CODIGO CATASTRAL"}]
    results = [
        {"orden": 10, "descripcion": "EP_APERTURA", "texto": "RADICADO: 25356"},
        {
            "orden": 21,
            "descripcion": "EP_ACTO_1",
            "texto": "---PRIMER ACTO---\n---ACTUALIZACION DE CODIGO CATASTRAL---\nOTORGAMIENTO y AUTORIZACIÓN",
        },
    ]

    with pytest.raises(RuntimeError, match="perdió el cuerpo de acto"):
        _validate_generated_act_sections(results, acts)
