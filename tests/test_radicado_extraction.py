from app.pipeline.orchestrator import (
    _build_universal_context,
    _extract_radicado_from_radicacion_json,
)


def test_extract_radicado_prefers_structured_fields():
    rad_json = {
        "radicacion": {"numero": "26485"},
        "negocio_actual": {"numero_radicado": "25963"},
    }

    assert _extract_radicado_from_radicacion_json(rad_json) == "25963"


def test_extract_radicado_falls_back_to_raw_text_when_json_is_placeholder():
    rad_json = {
        "radicacion": {"numero": "EXTRAER"},
        "negocio_actual": {"numero_radicado": "EXTRAER"},
    }
    raw_text = """
    {
      "es_hoja_radicacion": true,
      "radicacion": {"numero": "25963"},
      "negocio_actual": {"numero_radicado": "25963"}
    }
    """

    assert _extract_radicado_from_radicacion_json(rad_json, raw_text=raw_text) == "25963"


def test_build_universal_context_uses_resolved_radicado_when_json_failed():
    rad_json = {
        "radicacion": {"numero": "EXTRAER"},
        "negocio_actual": {"numero_radicado": "EXTRAER"},
    }

    contexto = _build_universal_context(
        rad_json,
        soportes_json_list=[],
        cedulas_json_list=[],
        resolved_radicado="25963",
    )

    assert contexto["RADICACION"] == "25963"
