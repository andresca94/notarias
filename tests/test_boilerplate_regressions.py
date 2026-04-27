from app.pipeline.boilerplate import EP_CARATULA_TEMPLATE, EP_OTORGAMIENTO_TEMPLATE


def test_caratula_template_keeps_property_header_fields():
    assert "NÚMERO PREDIAL NACIONAL: [[NUMERO_PREDIAL_NACIONAL]]" in EP_CARATULA_TEMPLATE
    assert "CÓDIGO CATASTRAL ANTERIOR: [[CODIGO_CATASTRAL_ANTERIOR]]" in EP_CARATULA_TEMPLATE
    assert "AFECTACIÓN A VIVIENDA FAMILIAR: [[AFECTACION_VIVIENDA_FAMILIAR]]" in EP_CARATULA_TEMPLATE
    assert "PATRIMONIO DE FAMILIA INEMBARGABLE: [[PATRIMONIO_FAMILIA_INEMBARGABLE]]" in EP_CARATULA_TEMPLATE


def test_otorgamiento_template_keeps_manifestacion_y_advertencia_clauses():
    assert "MANIFESTACIÓN DE LOS COMPARECIENTES:" in EP_OTORGAMIENTO_TEMPLATE
    assert "Ley de Víctimas y Restitución de Tierras" in EP_OTORGAMIENTO_TEMPLATE
    assert "artículo 1947 del Código Civil Colombiano" in EP_OTORGAMIENTO_TEMPLATE
