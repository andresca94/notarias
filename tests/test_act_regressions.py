from pathlib import Path
import unicodedata

import pytest

from app.pipeline.act_engine import _acto_kind, build_act_context
from app.pipeline.orchestrator import (
    _canonicalize_act_name,
    _derive_deed_context_from_history,
    _derive_deed_context_from_supports,
    _generated_act_has_body,
    _pick_best_ctl_support,
    _pick_preferred_previous_cadastral_code,
    _recover_missing_actos_a_firmar,
    _score_ctl_support_candidate,
    _stabilize_generated_act_output,
    _validate_generated_act_sections,
)
from app.services.rag.local_rag import LocalRAG
from app.services.ctl import resolve_ctl, to_deed_context


def test_acto_kind_distinguishes_codigo_catastral_from_nomenclatura():
    assert _acto_kind("ACTUALIZACION DE CODIGO CATASTRAL") == "ACTUALIZACION_CODIGO_CATASTRAL"
    assert _acto_kind("ACTUALIZACION DE NOMENCLATURA") == "ACTUALIZACION_NOMENCLATURA"


def test_canonicalize_act_name_normalizes_solicitud_prefix():
    assert _canonicalize_act_name("SOLICITUD DE ACTUALIZACION DE CODIGO CATASTRAL") == (
        "ACTUALIZACION DE CODIGO CATASTRAL"
    )


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


def test_score_ctl_support_candidate_rewards_historia_eventos():
    weak = {
        "_fileName": "CLT.pdf",
        "datos_inmueble": {"tradicion": "Historia amplia sin anotaciones estructuradas."},
        "historia_y_antecedentes": {},
    }
    rich = {
        "_fileName": "CLT0001.pdf",
        "datos_inmueble": {"tradicion": "Adquirido por compraventa según EP 2676 de 08-06-2007."},
        "historia_y_antecedentes": {
            "eventos": [
                "LESMES ARENGAS NELLY ESPERANZA adquirió el 50% por adjudicación en el juicio de sucesión del causante MURALLAS JUAN DE DIOS, según EP 1914 de 23-05-2005 de la Notaría Séptima de Bucaramanga.",
                "FLOREZ IBAÑEZ CARMELINA adquirió por compraventa de LESMES ARENGAS NELLY ESPERANZA, según EP 2676 de 08-06-2007 de la Notaría Séptima de Bucaramanga.",
            ]
        },
    }

    assert _score_ctl_support_candidate(rich) > _score_ctl_support_candidate(weak)


def test_score_ctl_support_candidate_prefers_eventos_historicos_with_parties():
    weak = {
        "_fileName": "CLT.pdf",
        "datos_inmueble": {
            "tradicion": "LESMES ARENGAS NELLY ESPERANZA ADQUIRIO ASI: 1.- EL 50% POR ADJUDICACION..."
        },
        "historia_y_antecedentes": {
            "COMPLEMENTACION": (
                "LESMES ARENGAS NELLY ESPERANZA ADQUIRIO ASI: 1.- EL 50% POR ADJUDICACION "
                "QUE SE LE HIZO EN EL JUICIO DE SUCESION DEL CAUSANTE MURALLAS JUAN DE DIOS."
            )
        },
    }
    rich = {
        "_fileName": "CLT0001.pdf",
        "datos_inmueble": {"tradicion": ""},
        "historia_y_antecedentes": {
            "eventos_historicos": [
                "04-05-2007: LESMES ARENGAS NELLY ESPERANZA efectúa división material (EP 2054, Notaría Séptima Bucaramanga) (Anotación 001).",
                "08-06-2007: FLOREZ IBAÑEZ CARMELINA adquiere por compraventa de LESMES ARENGAS NELLY ESPERANZA (EP 2676, Notaría Séptima Bucaramanga) (Anotación 002).",
                "18-02-2009: Hipoteca constituida por FLOREZ IBAÑEZ CARMELINA a favor de ANAYA HILARIO (EP 822, Notaría Tercera Bucaramanga) (Anotación 003).",
            ]
        },
    }

    assert _score_ctl_support_candidate(rich) > _score_ctl_support_candidate(weak)
    assert _pick_best_ctl_support([weak, rich])["_fileName"] == "CLT0001.pdf"


def test_derive_deed_context_from_history_recovers_sale_title():
    contexto = {
        "HISTORIA": {
            "narrativa_historica": (
                "Finalmente, FLOREZ IBAÑEZ CARMELINA adquirió el inmueble por compraventa de "
                "LESMES ARENGAS NELLY ESPERANZA (EP 2676, 08-06-2007, Notaría Séptima de Bucaramanga)."
            )
        }
    }

    deed = _derive_deed_context_from_history(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert deed["title_acquisition_mode"] == "compraventa"


def test_derive_deed_context_from_history_reads_list_entries_and_por_compra_a():
    contexto = {
        "HISTORIA": {
            "antecedente_adquisicion_deudora": (
                "El inmueble fue adquirido por la deudora CARMELINA FLOREZ IBAÑEZ por compra a "
                "NELLY ESPERANZA LESMES ARENGAS mediante Escritura Pública número 2676 de fecha "
                "8 de junio de 2007, otorgada por la Notaría Séptima de Bucaramanga."
            ),
            "cadena_de_tradicion": [
                "LESMES ARENGAS NELLY ESPERANZA efectuó división material según EP 2054 de 04-05-2007.",
                "FLOREZ IBAÑEZ CARMELINA adquirió por compraventa de LESMES ARENGAS NELLY ESPERANZA, según EP 2676 del 08-06-2007 de la Notaría Séptima de Bucaramanga.",
            ],
        }
    }

    deed = _derive_deed_context_from_history(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_from_party"] in {
        "NELLY ESPERANZA LESMES ARENGAS",
        "LESMES ARENGAS NELLY ESPERANZA",
    }
    assert deed["current_owner_name"] in {
        "CARMELINA FLOREZ IBAÑEZ",
        "FLOREZ IBAÑEZ CARMELINA",
    }


def test_derive_deed_context_from_supports_recovers_sale_title():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "CLT0001.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "Adquirido por FLOREZ IBAÑEZ CARMELINA mediante compraventa según "
                            "Escritura Pública 2676 del 08-06-2007 de la Notaría Séptima de Bucaramanga, "
                            "de LESMES ARENGAS NELLY ESPERANZA."
                        )
                    },
                    "historia_y_antecedentes": {"cadena_de_tradicion": "..."},  # richer support
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert deed["title_acquisition_mode"] == "compraventa"


def test_derive_deed_context_from_supports_chooses_best_title_across_multiple_sources():
    contexto = {
        "DATOS_EXTRA": {},
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "CLT.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "LESMES ARENGAS NELLY ESPERANZA ADQUIRIO ASI: 1.- EL 50% POR ADJUDICACION "
                            "QUE SE LE HIZO EN EL JUICIO DE SUCESION DEL CAUSANTE MURALLAS JUAN DE DIOS."
                        )
                    },
                    "historia_y_antecedentes": {
                        "complementacion": (
                            "LESMES ARENGAS NELLY ESPERANZA ADQUIRIO ASI: 1.- EL 50% POR ADJUDICACION "
                            "QUE SE LE HIZO EN EL JUICIO DE SUCESION DEL CAUSANTE MURALLAS JUAN DE DIOS."
                        )
                    },
                },
                {
                    "_fileName": "CLT0001.pdf",
                    "datos_inmueble": {"tradicion": ""},
                    "historia_y_antecedentes": {
                        "anotaciones": [
                            {
                                "numero": "002",
                                "fecha": "21-06-2007",
                                "documento": "ESCRITURA 2676 DEL 08-06-2007 NOTARIA SEPTIMA DE BUCARAMANGA",
                                "especificacion": "MODO DE ADQUISICION: 0125 COMPRAVENTA",
                                "personas": [
                                    {"nombre": "LESMES ARENGAS NELLY ESPERANZA", "rol": "DE (Titular de derecho real de dominio)"},
                                    {"nombre": "FLOREZ IBAÑEZ CARMELINA", "rol": "A (Titular de derecho real de dominio)"},
                                ],
                            }
                        ]
                    },
                },
            ]
        },
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert deed["_source_file"] == "CLT0001.pdf"


def test_derive_deed_context_from_supports_prefers_rich_anexo_over_owner_only_support():
    contexto = {
        "DATOS_EXTRA": {"PAZ_SALVO_VALORIZACION": "784"},
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "PAZ Y SALVO0001.pdf",
                    "personas_detalle": [
                        {
                            "nombre": "CARMELINA FLOREZ IBANEZ",
                            "identificacion": "CC 63285526",
                            "rol_en_hoja": "Propietario Actual",
                        }
                    ],
                },
                {
                    "_fileName": "ANEXOS.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "Anotación 002: Compraventa mediante ESCRITURA 2676 del 08-06-2007 "
                            "de la Notaría Séptima de Bucaramanga, de LESMES ARENGAS NELLY ESPERANZA "
                            "a FLOREZ IBAÑEZ CARMELINA."
                        )
                    },
                    "historia_y_antecedentes": {
                        "adquisicion_deudora": (
                            "La deudora CARMELINA FLOREZ IBAÑEZ adquirió el inmueble por compra a "
                            "NELLY ESPERANZA LESMES ARENGAS mediante Escritura Pública número 2676 "
                            "de fecha 8 de junio de 2007, otorgada por la Notaría Séptima de Bucaramanga."
                        )
                    },
                    "documento_ep_info": {
                        "numero_ep": "822",
                        "fecha": "18 de febrero de 2009",
                        "notaria": "Notaría Tercera del Círculo de Bucaramanga",
                        "vendedor": "CARMELINA FLOREZ IBAÑEZ",
                    },
                },
            ]
        },
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_from_party"] in {
        "LESMES ARENGAS NELLY ESPERANZA",
        "NELLY ESPERANZA LESMES ARENGAS",
    }
    assert deed["current_owner_name"] in {
        "CARMELINA FLOREZ IBAÑEZ",
        "FLOREZ IBAÑEZ CARMELINA",
    }
    assert deed["_source_file"] == "ANEXOS.pdf"


def test_derive_deed_context_from_supports_handles_runtime_list_shape_and_clt_phrase():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "CLT0001.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "Actualmente, la propietaria es FLOREZ IBAÑEZ CARMELINA, quien adquirió "
                            "por compraventa a LESMES ARENGAS NELLY ESPERANZA según Escritura Pública "
                            "2676 del 08-06-2007 de la Notaría Séptima de Bucaramanga."
                        )
                    },
                    "historia_y_antecedentes": {
                        "cadena_de_tradicion": (
                            "El predio tiene su origen en las sucesiones de MURALLAS JUAN DE DIOS "
                            "y MURALLAS JUSTO."
                        )
                    },
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert deed["_source_file"] == "CLT0001.pdf"


def test_derive_deed_context_from_supports_matches_runtime_clt_comma_segun_phrase():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "CLT.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "El inmueble fue adquirido por FLOREZ IBAÑEZ CARMELINA mediante compraventa "
                            "a LESMES ARENGAS NELLY ESPERANZA, según Escritura Pública 2676 del "
                            "08-06-2007 de la Notaría Séptima de Bucaramanga."
                        )
                    },
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert deed["_source_file"] == "CLT.pdf"


def test_derive_deed_context_from_supports_matches_runtime_anexos_phrase():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "ANEXOS.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "Adquirido por CARMELINA FLOREZ IBAÑEZ por compra a "
                            "NELLY ESPERANZA LESMES ARENGAS mediante Escritura Pública número 2676 "
                            "de fecha 8 de Junio de 2007, Notaría Séptima de Bucaramanga."
                        )
                    },
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_from_party"] == "NELLY ESPERANZA LESMES ARENGAS"
    assert deed["current_owner_name"] == "CARMELINA FLOREZ IBAÑEZ"
    assert "SEPTIMA" in unicodedata.normalize("NFKD", deed["title_authority"] or "").encode("ascii", "ignore").decode().upper()


def test_derive_deed_context_from_supports_matches_runtime_anexos_phrase_with_ids():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "ANEXOS.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "El inmueble con Matrícula Inmobiliaria 300-311985 fue adquirido por "
                            "CARMELINA FLOREZ IBAÑEZ (CC 63.285.526) mediante compraventa a "
                            "NELLY ESPERANZA LESMES ARENGAS (CC 63.320.186) según Escritura Pública "
                            "2676 del 08 de junio de 2007 de la Notaría Séptima de Bucaramanga, "
                            "registrada el 21 de junio de 2007 (Anotación 002)."
                        )
                    },
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08 de junio de 2007"
    assert deed["title_from_party"] == "NELLY ESPERANZA LESMES ARENGAS"
    assert deed["current_owner_name"] == "CARMELINA FLOREZ IBAÑEZ"
    assert deed["_source_file"] == "ANEXOS.pdf"


def test_derive_deed_context_from_supports_matches_runtime_anexos_phrase_without_owner():
    contexto = {
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "ANEXOS.pdf",
                    "datos_inmueble": {
                        "tradicion": (
                            "Adquirido por compra efectuada a NELLY ESPERANZA LESMES ARENGAS mediante "
                            "escritura pública número 2676 de fecha 8 de Junio de 2007 otorgada por la "
                            "Notaría Séptima de Bucaramanga, debidamente Registrada al folio de Matrícula "
                            "Inmobiliaria número 300-311985."
                        )
                    },
                    "documento_ep_info": {
                        "vendedor": "CARMELINA FLOREZ IBAÑEZ",
                    },
                }
            ]
        }
    }

    deed = _derive_deed_context_from_supports(contexto)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_from_party"] == "NELLY ESPERANZA LESMES ARENGAS"
    assert deed["_source_file"] == "ANEXOS.pdf"


def test_stabilize_generated_act_output_enforces_authoritative_title_and_code():
    mision = {
        "descripcion": "EP_ACTO_1",
        "contexto_datos": {
            "ORDINAL_ACTO": "PRIMER",
            "NOMBRE_ACTO_ACTUAL": "ACTUALIZACION DE CODIGO CATASTRAL",
            "TRADICION_AUTORITATIVA": (
                "FLOREZ IBAÑEZ CARMELINA adquirió el inmueble por compraventa de "
                "LESMES ARENGAS NELLY ESPERANZA, mediante escritura pública número 2676, "
                "de fecha 08-06-2007, de la SEPTIMA DE BUCARAMANGA"
            ),
            "EP_ANTECEDENTE_NUMERO": "2676",
            "EP_ANTECEDENTE_FECHA": "08-06-2007",
            "EP_ANTECEDENTE_NOTARIA": "SEPTIMA DE BUCARAMANGA",
            "CODIGO_CATASTRAL_ANTERIOR": "00-00-0003-0738-000",
        },
    }
    raw = """---PRIMER ACTO---
---COMPRAVENTA DE BIENES INMUEBLES---
SEGUNDO: TRADICIÓN: Que el inmueble antes mencionado fue adquirido por CARMELINA FLOREZ IBAÑEZ por adjudicación en sucesión de el causante.
TERCERO: Por el presente instrumento, el exponente procede a ACTUALIZAR EL CÓDIGO CATASTRAL del inmueble de su propiedad, de conformidad con la Escritura Pública número 822 de fecha 18 de febrero de 2009 otorgada en la Notaría Tercera del Círculo de Bucaramanga, documento el cual se anexa a la presente escritura para su debida protocolización y su contenido se inserta en las copias que de este instrumento se expidan.
y cédula catastral anterior 01-07-0149-0021-000
"""

    fixed = _stabilize_generated_act_output(mision, raw)

    assert "---ACTUALIZACION DE CODIGO CATASTRAL---" in fixed
    assert "2676" in fixed
    assert "LESMES ARENGAS NELLY ESPERANZA" in fixed
    assert "00-00-0003-0738-000" in fixed
    assert "822" not in fixed


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


def test_pick_preferred_previous_cadastral_code_formats_short_predial_digits():
    contexto = {
        "INMUEBLE": {
            "predial_nacional": "68-001-01-07-00-00-0149-0021-0-00-00-0000",
        },
        "DATOS_EXTRA": {
            "PAZ_SALVO_PREDIAL_DETALLE": {
                "codigo_catastral": "",
                "predial_nacional": "000000030738000",
            },
        },
    }

    assert _pick_preferred_previous_cadastral_code(contexto) == "00-00-0003-0738-000"


def test_pick_preferred_previous_cadastral_code_falls_back_to_supports_list():
    contexto = {
        "INMUEBLE": {
            "predial_nacional": "68-001-01-07-00-00-0149-0021-0-00-00-0000",
        },
        "DATOS_EXTRA": {},
        "FUENTES": {
            "soportes": [
                {
                    "_fileName": "ANEXOS.pdf",
                    "datos_inmueble": {
                        "codigo_catastral_anterior": "00-00-0003-0738-000",
                    },
                },
                {
                    "_fileName": "ANEXOS0002.pdf",
                    "datos_inmueble": {
                        "codigo_catastral_anterior": "01-07-0149-0021-000",
                    },
                },
            ]
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


def test_ctl_resolver_parses_structured_annotations_with_personas_field():
    soporte = {
        "datos_inmueble": {"matricula": "300-311985"},
        "historia_y_antecedentes": {
            "anotaciones": [
                {
                    "numero": "001",
                    "fecha": "05-06-2007",
                    "documento": "ESCRITURA 2054 DEL 04-05-2007 NOTARIA 7 DE BUCARAMANGA",
                    "especificacion": "OTRO: 0918 DIVISION MATERIAL",
                    "personas": "DE: LESMES ARENGAS NELLY ESPERANZA",
                },
                {
                    "numero": "002",
                    "fecha": "21-06-2007",
                    "documento": "ESCRITURA 2676 DEL 08-06-2007 NOTARIA SEPTIMA DE BUCARAMANGA",
                    "especificacion": "MODO DE ADQUISICION: 0125 COMPRAVENTA",
                    "personas": "DE: LESMES ARENGAS NELLY ESPERANZA, A: FLOREZ IBAÑEZ CARMELINA",
                },
                {
                    "numero": "003",
                    "fecha": "19-02-2009",
                    "documento": "ESCRITURA 822 DEL 18-02-2009 NOTARIA TERCERA DE BUCARAMANGA",
                    "especificacion": "GRAVAMEN: 0203 HIPOTECA",
                    "personas": "DE: FLOREZ IBAÑEZ CARMELINA, A: ANAYA HILARIO",
                },
            ]
        },
    }

    state = resolve_ctl(soporte, {})
    deed = to_deed_context(state)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    authority_norm = unicodedata.normalize("NFKD", deed["title_authority"] or "").encode("ascii", "ignore").decode()
    assert "SEPTIMA" in authority_norm.upper()
    assert deed["title_acquisition_mode"] == "compraventa"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert "LESMES ARENGAS NELLY ESPERANZA" in (deed["titulo_adquisicion_text"] or "")


def test_ctl_resolver_parses_structured_annotations_with_personas_list():
    soporte = {
        "datos_inmueble": {"matricula": "300-311985"},
        "historia_y_antecedentes": {
            "anotaciones": [
                {
                    "numero": "001",
                    "fecha": "05-06-2007",
                    "documento": "ESCRITURA 2054 DEL 04-05-2007 NOTARIA 7 DE BUCARAMANGA",
                    "especificacion": "OTRO: 0918 DIVISION MATERIAL",
                    "personas": [
                        {"nombre": "LESMES ARENGAS NELLY ESPERANZA", "rol": "DE (Titular de derecho real de dominio)"},
                    ],
                },
                {
                    "numero": "002",
                    "fecha": "21-06-2007",
                    "documento": "ESCRITURA 2676 DEL 08-06-2007 NOTARIA SEPTIMA DE BUCARAMANGA",
                    "especificacion": "MODO DE ADQUISICION: 0125 COMPRAVENTA",
                    "personas": [
                        {"nombre": "LESMES ARENGAS NELLY ESPERANZA", "rol": "DE (Titular de derecho real de dominio)"},
                        {"nombre": "FLOREZ IBAÑEZ CARMELINA", "rol": "A (Titular de derecho real de dominio)"},
                    ],
                },
                {
                    "numero": "003",
                    "fecha": "19-02-2009",
                    "documento": "ESCRITURA 822 DEL 18-02-2009 NOTARIA TERCERA DE BUCARAMANGA",
                    "especificacion": "GRAVAMEN: 0203 HIPOTECA",
                    "personas": [
                        {"nombre": "FLOREZ IBAÑEZ CARMELINA", "rol": "DE (Titular de derecho real de dominio)"},
                        {"nombre": "ANAYA HILARIO", "rol": "A (Titular de derecho real de dominio)"},
                    ],
                },
            ]
        },
    }

    state = resolve_ctl(soporte, {})
    deed = to_deed_context(state)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    authority_norm = unicodedata.normalize("NFKD", deed["title_authority"] or "").encode("ascii", "ignore").decode()
    assert "SEPTIMA" in authority_norm.upper()
    assert deed["title_acquisition_mode"] == "compraventa"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
    assert "LESMES ARENGAS NELLY ESPERANZA" in (deed["titulo_adquisicion_text"] or "")


def test_ctl_resolver_parses_historia_eventos_for_title_acquisition():
    soporte = {
        "datos_inmueble": {"matricula": "300-311985"},
        "historia_y_antecedentes": {
            "eventos": [
                "LESMES ARENGAS NELLY ESPERANZA adquirió el 50% por adjudicación en el juicio de sucesión del causante MURALLAS JUAN DE DIOS, según EP 1914 de 23-05-2005 de la Notaría Séptima de Bucaramanga, registrada el 03-06-2005.",
                "FLOREZ IBAÑEZ CARMELINA adquirió por compraventa de LESMES ARENGAS NELLY ESPERANZA, según EP 2676 de 08-06-2007 de la Notaría Séptima de Bucaramanga, registrada el 21-06-2007 (Anotación Nro 002).",
                "FLOREZ IBAÑEZ CARMELINA constituyó hipoteca abierta sin límite de cuantía a favor de ANAYA HILARIO, según EP 822 de 18-02-2009 de la Notaría Tercera de Bucaramanga.",
            ]
        },
        "personas_detalle": [
            {
                "nombre": "FLOREZ IBAÑEZ CARMELINA",
                "identificacion": "CC 63285526",
                "rol_en_hoja": "Propietario Actual",
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
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"


def test_ctl_resolver_parses_eventos_historicos_for_title_acquisition():
    soporte = {
        "datos_inmueble": {"matricula": "300-311985"},
        "historia_y_antecedentes": {
            "eventos_historicos": [
                "23-05-2005: LESMES ARENGAS NELLY ESPERANZA adquiere 50% por adjudicación de sucesión de MURALLAS JUAN DE DIOS (EP 1914, Notaría Séptima Bucaramanga).",
                "04-05-2007: LESMES ARENGAS NELLY ESPERANZA efectúa división material (EP 2054, Notaría Séptima Bucaramanga) (Anotación 001).",
                "08-06-2007: FLOREZ IBAÑEZ CARMELINA adquiere por compraventa de LESMES ARENGAS NELLY ESPERANZA (EP 2676, Notaría Séptima Bucaramanga) (Anotación 002).",
                "18-02-2009: Hipoteca constituida por FLOREZ IBAÑEZ CARMELINA a favor de ANAYA HILARIO (EP 822, Notaría Tercera Bucaramanga) (Anotación 003).",
            ]
        },
    }

    state = resolve_ctl(soporte, {})
    deed = to_deed_context(state)

    assert deed["title_doc_number"] == "2676"
    assert deed["title_doc_date"] == "08-06-2007"
    authority_norm = unicodedata.normalize("NFKD", deed["title_authority"] or "").encode("ascii", "ignore").decode()
    assert "SEPTIMA" in authority_norm.upper()
    assert deed["title_acquisition_mode"] == "compraventa"
    assert deed["title_from_party"] == "LESMES ARENGAS NELLY ESPERANZA"
    assert deed["current_owner_name"] == "FLOREZ IBAÑEZ CARMELINA"
