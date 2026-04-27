from app.pipeline.boilerplate import (
    EP_CARATULA_TEMPLATE,
    EP_OTORGAMIENTO_TEMPLATE,
    REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT,
    build_certificados_paz_y_salvo_detalle,
)


def test_caratula_template_keeps_property_header_fields():
    assert "NÚMERO PREDIAL NACIONAL: [[NUMERO_PREDIAL_NACIONAL]]" in EP_CARATULA_TEMPLATE
    assert "CÓDIGO CATASTRAL ANTERIOR: [[CODIGO_CATASTRAL_ANTERIOR]]" in EP_CARATULA_TEMPLATE
    assert "AFECTACIÓN A VIVIENDA FAMILIAR: [[AFECTACION_VIVIENDA_FAMILIAR]]" in EP_CARATULA_TEMPLATE
    assert "PATRIMONIO DE FAMILIA INEMBARGABLE: [[PATRIMONIO_FAMILIA_INEMBARGABLE]]" in EP_CARATULA_TEMPLATE


def test_otorgamiento_template_keeps_manifestacion_y_advertencia_clauses():
    assert "MANIFESTACIÓN DE LOS COMPARECIENTES:" in EP_OTORGAMIENTO_TEMPLATE
    assert "Ley de Víctimas y Restitución de Tierras" in EP_OTORGAMIENTO_TEMPLATE
    assert "artículo 1947 del Código Civil Colombiano" in EP_OTORGAMIENTO_TEMPLATE


def test_redam_protocolizacion_text_matches_feedback_wording():
    assert "artículo 6 numeral 3" in REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT
    assert "MINTIC" in REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT
    assert "artículo 2 de la Ley 2097" not in REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT


def test_build_certificados_paz_y_salvo_detalle_includes_area_metro():
    assert build_certificados_paz_y_salvo_detalle("2615433", "2615434", "646295E99F") == [
        "Paz y Salvo Predial N° 2615433.",
        "Paz y Salvo de Valorización N° 2615434.",
        "Paz y salvo de Área Metropolitana N° 646295E99F.",
    ]


def test_build_certificados_paz_y_salvo_detalle_supports_no_cobro_valorizacion():
    assert build_certificados_paz_y_salvo_detalle("2615433", "NO COBRA", None) == [
        "Paz y Salvo Predial N° 2615433.",
        "Constancia de no cobro de valorización: NO COBRA.",
    ]
