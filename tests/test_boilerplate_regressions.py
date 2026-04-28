from app.pipeline.boilerplate import (
    EP_CARATULA_TEMPLATE,
    EP_INSERTOS_TEMPLATE,
    EP_OTORGAMIENTO_TEMPLATE,
    REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT,
    build_certificados_paz_y_salvo_detalle,
)
from app.pipeline.orchestrator import (
    _build_universal_context,
    _prepare_ep_sections,
    _should_keep_condicion_resolutoria_paragraph,
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


def test_insertos_template_avoids_duplicate_paz_y_salvo_placeholders():
    assert "[[CERTIFICADOS_PAZ_Y_SALVO_DETALLE]]" in EP_INSERTOS_TEMPLATE
    assert "Paz y Salvo Predial N° [[PAZ_SALVO_PREDIAL]]." not in EP_INSERTOS_TEMPLATE
    assert "Paz y Salvo de Valorización N° [[PAZ_SALVO_VALORIZACION]]." not in EP_INSERTOS_TEMPLATE
    assert "Paz y salvo de Área Metropolitana N° [[PAZ_SALVO_AREA_METRO]]." not in EP_INSERTOS_TEMPLATE


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


def test_condicion_resolutoria_only_stays_for_deferred_payment_terms():
    assert _should_keep_condicion_resolutoria_paragraph(None) is False
    assert _should_keep_condicion_resolutoria_paragraph("Pago de contado contra firma") is False
    assert _should_keep_condicion_resolutoria_paragraph(
        "30 cuotas mensuales de $1.000.000 con saldo financiado por crédito hipotecario"
    ) is True


def test_otorgamiento_section_bypasses_binder_and_keeps_full_static_clauses():
    misiones = _prepare_ep_sections(
        {
            "RADICACION": "25963",
            "INMUEBLE": {"matricula": "300-366931"},
            "DATOS_EXTRA": {"EMAIL_NOTIFICACIONES": "cliente@example.com"},
        },
        [],
    )

    otorgamiento = next(m for m in misiones if m["descripcion"] == "EP_OTORGAMIENTO")
    assert otorgamiento["passthrough_text"].startswith("OTORGAMIENTO y AUTORIZACIÓN")
    assert "MANIFESTACIÓN DE LOS COMPARECIENTES:" in otorgamiento["passthrough_text"]
    assert "ADVERTENCIA NOTARIAL:" in otorgamiento["passthrough_text"]
    assert "cliente@example.com" in otorgamiento["passthrough_text"]


def test_universal_context_ignores_ant_support_paz_y_salvo_numbers():
    contexto = _build_universal_context(
        {},
        [
            {
                "_fileName": "ANT.pdf",
                "hallazgos_variables": {
                    "paz_salvo_predial": "2585075",
                    "paz_salvo_valorizacion": "2585077",
                    "paz_salvo_area_metro": "2585077",
                },
            },
            {
                "_fileName": "PAZ Y SALVO.pdf",
                "hallazgos_variables": {
                    "paz_salvo_predial": "2615433",
                    "paz_salvo_valorizacion": "2615434",
                    "paz_salvo_area_metro": "PIN.646295E99F",
                },
            },
        ],
        [],
        resolved_radicado="25963",
    )

    assert contexto["DATOS_EXTRA"]["PAZ_SALVO_PREDIAL"] == "2615433"
    assert contexto["DATOS_EXTRA"]["PAZ_SALVO_VALORIZACION"] == "2615434"
    assert contexto["DATOS_EXTRA"]["PAZ_SALVO_AREA_METRO"] == "PIN.646295E99F"
