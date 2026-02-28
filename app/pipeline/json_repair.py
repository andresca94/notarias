# app/pipeline/json_repair.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional


REPAIR_SYSTEM = """Eres un asistente que REPARA JSON.
Te voy a dar una salida de OCR/LLM que intenta ser JSON pero puede estar truncada o malformada.
Tu tarea: producir UN JSON VÁLIDO y COMPLETO que siga el ESQUEMA proporcionado.
Reglas:
- Devuelve SOLO JSON (sin markdown).
- Si un campo no se puede recuperar, usa "NO_DETECTADO" o [] o {} según corresponda.
- NO inventes valores específicos: conserva solo lo que esté en el texto.
"""

REPAIR_USER = """TEXTO (posiblemente truncado):
\"\"\"{raw}\"\"\"

ESQUEMA OBJETIVO (usa exactamente estas llaves principales):
{schema}

Devuelve SOLO JSON válido.
"""


def _schema_min_radicacion() -> Dict[str, Any]:
    return {
        "es_hoja_radicacion": True,
        "radicacion": {"numero": "EXTRAER", "fecha": "EXTRAER", "notaria": "EXTRAER", "notario_encargado": "EXTRAER"},
        "datos_inmueble": {"direccion": "EXTRAER", "matricula": "EXTRAER", "ciudad_registro": "EXTRAER"},
        "negocio_actual": {
            "numero_radicado": "EXTRAER",
            "total_venta_hoy": 0,
            "actos_a_firmar": [
                {"nombre": "CANCELACION PACTO DE RETROVENTA", "cuantia": 0},
                {"nombre": "COMPRAVENTA DE BIENES INMUEBLES", "cuantia": 0},
                {"nombre": "CAMBIO DE NOMBRE DE PREDIO RURAL", "cuantia": 0},
                {"nombre": "HIPOTECA ABIERTA SIN LIMITE DE CUANTIA", "cuantia": 0},
            ],
        },
        "personas_detalle": [],
        "actos_juridicos": [],
        "mapeo_roles": {"vendedores": [], "compradores": [], "representantes": []},
    }


def _schema_min_docs() -> Dict[str, Any]:
    return {
        "datos_inmueble": {
            "matricula": "EXTRAER",
            "predial_nacional": "EXTRAER",
            "direccion": "EXTRAER",
            "cabida_area": "EXTRAER",
            "linderos": "EXTRAER",
            "tradicion": "EXTRAER",
            "afectacion_vivienda": "NO_DETECTADO",
            "patrimonio_familia": "NO_DETECTADO",
        },
        "historia_y_antecedentes": {},
        "personas_detalle": [],
        "hallazgos_variables": {},
    }


def _schema_min_cedula() -> Dict[str, Any]:
    return {
        "cedulas": [
            {
                "nombre": "ILEGIBLE",
                "cedula": "ILEGIBLE",
                "fecha_nacimiento": "ILEGIBLE",
                "lugar_nacimiento": "ILEGIBLE",
                "fecha_expedicion": "ILEGIBLE",
                "lugar_expedicion": "ILEGIBLE",
                "estado_civil": "ILEGIBLE",
            }
        ]
    }


def _strip_fences(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("```json", "").replace("```", "").strip()
    return s


def parse_json_strict_or_none(text: str, *, allow_array: bool = False):
    """Returns parsed JSON (dict or list) or None.
    allow_array=True enables detecting JSON arrays as start position (only for cedula kind).
    """
    s = _strip_fences(text)
    if not s:
        return None
    obj_pos = s.find("{")
    arr_pos = s.find("[") if allow_array else -1
    candidates = [p for p in [obj_pos, arr_pos] if p != -1]
    if not candidates:
        return None
    start = min(candidates)
    try:
        return json.loads(s[start:])
    except Exception:
        return None


def parse_json_with_repair(
    raw_text: str,
    *,
    kind: str,
    openai_client,  # OpenAIClient (tu wrapper)
    temperature: float = 0.0,
    max_tokens: int = 4000,
) -> Dict[str, Any]:
    """
    1) intenta parse estricto
    2) si falla, manda a OpenAI para 'repair'
    """
    parsed = parse_json_strict_or_none(raw_text, allow_array=(kind == "cedula"))
    if parsed is not None:
        return parsed

    if kind == "radicacion":
        schema = _schema_min_radicacion()
    elif kind == "docs":
        schema = _schema_min_docs()
    elif kind == "cedula":
        schema = _schema_min_cedula()
    else:
        schema = {}

    prompt_user = REPAIR_USER.format(raw=raw_text[:20000], schema=json.dumps(schema, ensure_ascii=False))

    repaired = openai_client.chat(
        REPAIR_SYSTEM,
        prompt_user,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    parsed2 = parse_json_strict_or_none(repaired, allow_array=(kind == "cedula"))
    if parsed2 is None:
        return {}
    return parsed2