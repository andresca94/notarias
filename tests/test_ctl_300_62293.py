"""
Tests del CTL State Engine con datos reales del CASE-26485.
Folio de matrícula 300-62293 (Bucaramanga).

CTL real analizado:
  - Propietaria actual: MARTINEZ DE CORNEJO HILDE (CC 27952821)
  - Acreedor valorización: MUNICIPIO DE BUCARAMANGA (NIT 8902012220)
  - Historia: narrativa como string (complementación)
  - Servidumbre pasiva: Servidumbre de acueducto

Ejecución:
    python -m pytest tests/test_ctl_300_62293.py -v
"""
import pytest

from app.services.ctl import resolve_ctl, to_deed_context

# ──────────────────────────────────────────────────────────────────────────────
# Datos del CTL.pdf__raw.txt de CASE-26485
# (simplificados para prueba; preservan la estructura real de Gemini)
# ──────────────────────────────────────────────────────────────────────────────

CTL_SOPORTE_JSON = {
    "datos_inmueble": {
        "matricula": "300-62293",
        "ciudad_registro": "Bucaramanga",
        "oficina_registro": "OFICINA DE REGISTRO DE INSTRUMENTOS PUBLICOS DE BUCARAMANGA",
        "direccion": "KR 35 # 11 - 23 URB LOS PINOS II 2 ETAPA, BUCARAMANGA",
        "afectacion_vivienda": "NO_DETECTADO",
        "patrimonio_familia": "NO_DETECTADO",
    },
    "historia_y_antecedentes": (
        "CON BASE EN LA MATRICULA # 300-0011.582.- #300-0006.290.- "
        "\"COOPERATIVA COLOMBIANA DE PREVISION Y AMPAROS LIMITADA\"(\"COLDAMPAROS\"), "
        "ADQUIRIO EN MAYOR EXTENSION, CON OTRO INMUEBLE POR COMPRA QUE HIZO A "
        "\"INVERSIONES MARPICO * & CIA. S. EN C., SEGUN ESCRITURA # 1788 DEL 19 DE JULIO DE 1.976"
    ),
    "personas_detalle": [
        {
            "nombre": "MARTINEZ DE CORNEJO HILDE",
            "identificacion": "CC 27952821",
            "rol_en_hoja": "Propietario Actual",
            "estado_civil": None,
        },
        {
            "nombre": "MUNICIPIO DE BUCARAMANGA",
            "identificacion": "NIT 8902012220",
            "rol_en_hoja": "Acreedor (por Valorización)",
            "estado_civil": None,
        },
    ],
    "hallazgos_variables": {
        "servidumbres_pasivas": "Servidumbre de acueducto",
        "paz_salvo_valorizacion": None,
    },
}

# Paz y salvo real aportado en el caso
PAZ_SALVO_DATA_CON = {"PAZ_SALVO_VALORIZACION": "2619752"}
PAZ_SALVO_DATA_SIN = {}

# ──────────────────────────────────────────────────────────────────────────────
# CTL con formato estructurado (para tests de adjudicación)
# ──────────────────────────────────────────────────────────────────────────────

CTL_ESTRUCTURADO_JSON = {
    "datos_inmueble": {
        "matricula": "300-62293",
    },
    "historia_y_antecedentes": {
        "anotaciones_registrales": [
            {
                "anotacion": 7,
                "de": "CORNEJO GONZALEZ GABRIEL",
                "a": "MARTINEZ DE CORNEJO HILDE",
                "especificacion": "ADJUDICACION EN SUCESION",
                "documento": "ESCRITURA 1579 DEL 16 DE JUNIO DE 2003 NOTARIA 1 DE BUCARAMANGA",
            }
        ]
    },
    "personas_detalle": [],
    "hallazgos_variables": {},
}

CTL_ESTRUCTURADO_JSON_CANCEL = {
    "datos_inmueble": {"matricula": "300-62293"},
    "historia_y_antecedentes": {
        "anotaciones_registrales": [
            {
                "anotacion": 7,
                "de": "CORNEJO GONZALEZ GABRIEL",
                "a": "MARTINEZ DE CORNEJO HILDE",
                "especificacion": "ADJUDICACION EN SUCESION",
                "documento": "ESCRITURA 1579 DEL 16 DE JUNIO DE 2003 DE LA NOTARIA 1 DE BUCARAMANGA",
            },
            {
                "anotacion": 8,
                "de": "MUNICIPIO DE BUCARAMANGA",
                "a": "MARTINEZ DE CORNEJO HILDE",
                "especificacion": "VALORIZACION MUNICIPAL",
                "documento": "RESOLUCION 1234",
            },
            {
                "anotacion": 9,
                "de": "MUNICIPIO DE BUCARAMANGA",
                "a": "",
                "especificacion": "CANCELACION VALORIZACION CANCELA ANOTACION 8",
                "documento": "RESOLUCION 5678",
            },
        ]
    },
    "personas_detalle": [],
    "hallazgos_variables": {},
}


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestOwnerResolution:
    def test_owner_detected_from_personas_detalle(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_SIN)
        assert state.current_owner_name is not None
        assert "MARTINEZ" in state.current_owner_name.upper()

    def test_owner_from_structured_annotations(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON, {})
        assert state.current_owner_name is not None
        assert "MARTINEZ" in state.current_owner_name.upper()


class TestTitleAcquisition:
    def test_structured_adjudication_mode(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON, {})
        assert state.title_acquisition_mode == "adjudicacion_sucesion"

    def test_structured_adjudication_from_party(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON, {})
        assert state.title_from_party is not None
        assert "GABRIEL" in (state.title_from_party or "").upper()

    def test_structured_adjudication_doc_number(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON, {})
        assert state.title_doc_number == "1579"

    def test_structured_adjudication_text_contains_keyword(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON, {})
        text = state.title_acquisition_text or ""
        assert "adjudicaci" in text.lower()
        assert "sucesi" in text.lower()


class TestValorizacionWithPazSalvo:
    def test_valorizacion_not_active_when_paz_salvo_present(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_CON)
        valoriz_liens = [l for l in state.active_liens if "valoriz" in l.lien_type.lower()]
        assert valoriz_liens == [], "Valorización no debe estar activa con Paz y Salvo"

    def test_paz_salvo_recorded_in_state(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_CON)
        assert state.paz_salvo_valorizacion is not None

    def test_deed_context_libre_with_paz_salvo(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_CON)
        ctx = to_deed_context(state)
        assert ctx["clausula_libertad_libre"] is True


class TestValorizacionWithoutPazSalvo:
    def test_valorizacion_active_without_paz_salvo(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_SIN)
        has_valoriz = (
            any("valoriz" in l.lien_type.lower() for l in state.active_liens)
            or any("valoriz" in w.lower() for w in state.warnings)
        )
        assert has_valoriz, "Valorización debe reportarse activa o como warning sin Paz y Salvo"

    def test_deed_context_not_libre_without_paz_salvo(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_SIN)
        ctx = to_deed_context(state)
        assert ctx["clausula_libertad_libre"] is False


class TestServitudes:
    def test_servitude_from_hallazgos(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_SIN)
        assert any("acueducto" in s.lower() for s in state.active_servitudes)

    def test_servitudes_in_deed_context(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_SIN)
        ctx = to_deed_context(state)
        assert "acueducto" in (ctx["servidumbres_text"] or "").lower()


class TestCancelacion:
    def test_cancelled_lien_not_in_active(self):
        """Valorización con anotación de cancelación posterior no debe estar activa."""
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON_CANCEL, {})
        valoriz = [l for l in state.active_liens if "valoriz" in l.lien_type.lower()]
        assert valoriz == [], "Valorización cancelada por anotación no debe ser activa"

    def test_acquisition_still_valid_after_lien_cancel(self):
        state = resolve_ctl(CTL_ESTRUCTURADO_JSON_CANCEL, {})
        assert state.title_acquisition_mode == "adjudicacion_sucesion"


class TestAdapterOutput:
    def test_deed_context_keys_present(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_CON)
        ctx = to_deed_context(state)
        required = [
            "titulo_adquisicion_text", "title_acquisition_mode",
            "clausula_libertad_libre", "active_gravamenes_text",
            "servidumbres_text", "warnings",
        ]
        for key in required:
            assert key in ctx, f"Clave '{key}' ausente del deed_context"

    def test_warnings_list_type(self):
        state = resolve_ctl(CTL_SOPORTE_JSON, PAZ_SALVO_DATA_CON)
        ctx = to_deed_context(state)
        assert isinstance(ctx["warnings"], list)
