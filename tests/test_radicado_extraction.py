from app.pipeline.orchestrator import (
    _build_universal_context,
    _pick_radicacion_file,
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


def test_extract_radicado_from_plain_radicacion_pdf_text():
    rad_json = {
        "radicacion": {"numero": "EXTRAER"},
        "negocio_actual": {"numero_radicado": "EXTRAER"},
    }
    raw_text = """
    NOTARIA TERCERA DE BUCARAMANGA

    Radicacion Nro. 26485

    Fecha: 26/02/2026 10:42:37 AM
    Matriculas: 300-62293
    Escritura Nro.
    01250000
    """

    assert _extract_radicado_from_radicacion_json(rad_json, raw_text=raw_text) == "26485"


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


def test_pick_radicacion_file_prefers_rad_token_over_other_pdfs():
    paths = [
        "/tmp/REDAM.pdf",
        "/tmp/ANEXOS.pdf",
        "/tmp/CLT.pdf",
        "/tmp/RAD.pdf",
        "/tmp/PAZ Y SALVO.pdf",
    ]

    assert _pick_radicacion_file(paths) == "/tmp/RAD.pdf"
