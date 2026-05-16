from pathlib import Path
import unicodedata

import pytest

from app.pipeline.act_engine import _acto_kind, build_act_context
from app.pipeline.orchestrator import (
    _generated_act_has_body,
    _pick_best_ctl_support,
    _pick_preferred_previous_cadastral_code,
    _recover_missing_actos_a_firmar,
    _validate_generated_act_sections,
)
from app.services.rag.local_rag import LocalRAG
from app.services.ctl import resolve_ctl, to_deed_context


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


def test_validate_generated_act_sections_rejects_zero_detected_acts():
    with pytest.raises(RuntimeError, match="no produjo ningún acto válido"):
        _validate_generated_act_sections([], [])


def test_recover_missing_actos_from_radicacion_text_and_filenames():
    rad_json = {
        "negocio_actual": {
            "numero_radicado": "25356",
            "total_venta_hoy": 0,
            "actos_a_firmar": [],
        },
        "mapeo_roles": {"vendedores": [], "compradores": []},
    }

    acts = _recover_missing_actos_a_firmar(
        [],
        radicacion_json=rad_json,
        radicacion_raw_text="Solicitud de actualizacion de codigo catastral para el radicado 25356",
        documentos_paths=[
            "/tmp/Correo de Notaria Tercera de Bucaramanga - RAD 25356 INFORMACION IMPORTANTE DE SOLICITUD DE ACTUALIZACION DE CODIGO CATASTRAL.pdf",
        ],
    )

    assert acts == [
        {
            "nombre": "ACTUALIZACION DE CODIGO CATASTRAL",
            "cuantia": 0,
            "otorgantes": [],
            "beneficiarios": [],
        }
    ]


def test_build_act_context_uses_support_people_when_radicacion_only_has_interesado():
    contexto = {
        "PERSONAS_ACTIVOS": [
            {"nombre": "JAIME GOMEZ FLOREZ", "rol_detectado": "INTERESADO", "_source": "radicacion"},
        ],
        "PERSONAS": [
            {"nombre": "JAIME GOMEZ FLOREZ", "rol_detectado": "INTERESADO", "_source": "radicacion"},
            {"nombre": "JAIME GOMEZ FLOREZ", "rol_detectado": "APODERADO ESPECIAL DE LA VENDEDORA"},
            {"nombre": "CARMELINA FLOREZ IBAÑEZ", "rol_detectado": "TITULAR DE DERECHO REAL DE DOMINIO"},
        ],
        "EMPRESA_RL_MAP": {},
        "INMUEBLE": {},
    }

    ctx = build_act_context(
        contexto,
        {"nombre": "ACTUALIZACION DE CODIGO CATASTRAL", "cuantia": 0, "otorgantes": [], "beneficiarios": []},
        0,
    )

    solicitantes = (ctx.get("ROLES_ACTO") or {}).get("SOLICITANTES") or []
    assert solicitantes
    assert "APODERADO" in str(solicitantes[0].get("rol_detectado") or "").upper()


def test_pick_best_ctl_support_prefers_structured_annotations():
    supports = [
        {
            "_fileName": "CLT.pdf",
            "datos_inmueble": {"tradicion": "Historia amplia sin anotaciones estructuradas."},
            "historia_y_antecedentes": {},
        },
        {
            "_fileName": "CLT0001.pdf",
            "datos_inmueble": {
                "tradicion": "Adquirido por compraventa según Escritura Pública 2676 del 08-06-2007."
            },
            "historia_y_antecedentes": {
                "anotaciones_registrales": [
                    {
                        "anotacion": 2,
                        "especificacion": "COMPRAVENTA",
                        "de": "LESMES ARENGAS NELLY ESPERANZA",
                        "a": "FLOREZ IBAÑEZ CARMELINA",
                        "documento": "ESCRITURA 2676 del 08-06-2007 Notaría Séptima de BUCARAMANGA",
                    }
                ]
            },
        },
    ]

    best = _pick_best_ctl_support(supports)

    assert best is not None
    assert best["_fileName"] == "CLT0001.pdf"


def test_pick_preferred_previous_cadastral_code_prefers_paz_y_salvo_legacy_code():
    contexto = {
        "INMUEBLE": {
            "predial_nacional": "68-001-01-07-00-00-0149-0021-0-00-00-0000",
            "codigo_catastral_anterior": "00-00-00-00-0003-1004-0-000-00-0000",
        },
        "DATOS_EXTRA": {
            "PAZ_SALVO_PREDIAL_DETALLE": {
                "codigo_catastral": "",
                "predial_nacional": "00-00-0003-0738-000",
            },
            "PAZ_SALVO_VALORIZACION_DETALLE": {
                "codigo_catastral": "",
                "predial_nacional": "00-00-0003-0738-000",
            },
        },
    }

    assert _pick_preferred_previous_cadastral_code(contexto) == "00-00-0003-0738-000"


def test_ctl_resolver_parses_string_annotations_for_title_acquisition():
    soporte = {
        "datos_inmueble": {"matricula": "300-311985"},
        "historia_y_antecedentes": {
            "anotaciones_registrales": [
                "Anotación 001: Fecha: 05-06-2007, Radicación: 2007-300-6-26371. Documento: ESCRITURA 2054 del 04-05-2007 Notaría 7 de BUCARAMANGA. Especificación: DIVISION MATERIAL. Personas: DE: LESMES ARENGAS NELLY ESPERANZA.",
                "Anotación 002: Fecha: 21-06-2007, Radicación: 2007-300-6-28645. Documento: ESCRITURA 2676 del 08-06-2007 Notaría Séptima de BUCARAMANGA. Especificación: COMPRAVENTA. Personas: DE: LESMES ARENGAS NELLY ESPERANZA A: FLOREZ IBAÑEZ CARMELINA.",
                "Anotación 003: Fecha: 19-02-2009, Radicación: 2009-300-6-7621. Documento: ESCRITURA 822 del 18-02-2009 Notaría Tercera de BUCARAMANGA. Especificación: HIPOTECA. Valor Acto: $10,000,000. Personas: DE: FLOREZ IBAÑEZ CARMELINA A: ANAYA HILARIO.",
            ]
        },
        "personas_detalle": [
            {
                "nombre": "FLOREZ IBAÑEZ CARMELINA",
                "identificacion": "CC 63285526",
                "rol_en_hoja": "Propietario, Deudor (Hipoteca)",
            }
        ],
    }

    state = resolve_ctl(soporte, {})
    deed = to_deed_context(state)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    authority_norm = unicodedata.normalize("NFKD", deed["title_authority"] or "").encode("ascii", "ignore").decode()
    assert "SEPTIMA" in authority_norm.upper()
    assert deed["title_acquisition_mode"] == "compraventa"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert "2676" in (deed["titulo_adquisicion_text"] or "")
