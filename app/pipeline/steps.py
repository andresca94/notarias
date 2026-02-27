from __future__ import annotations
from typing import Any, Dict, List, Optional
import json
import re
import uuid

def new_run_id() -> str:
    return "RUN-" + uuid.uuid4().hex[:10].upper()

def safe_json_loads(x: Any, default: Any) -> Any:
    if isinstance(x, (dict, list)):
        return x
    if not x:
        return default
    s = str(x).strip()
    # limpia fences
    s = s.replace("```json", "").replace("```", "").strip()
    # extrae primer {...}
    i, j = s.find("{"), s.rfind("}")
    if i != -1 and j != -1 and j > i:
        s2 = s[i:j+1]
        try:
            return json.loads(s2)
        except Exception:
            return default
    try:
        return json.loads(s)
    except Exception:
        return default

# ---------- PROMPTS (copiados/adaptados de tu n8n) ----------

PROMPT_ARQUITECTO = lambda plantilla_texto: f"""ACTÚA COMO UN ARQUITECTO DE DATOS JURÍDICOS.

TU TAREA PRINCIPAL:
Recibes una plantilla notarial donde los espacios vacíos han sido marcados como `[[VARIABLE_POR_DEFINIR]]`. Tu trabajo es darles un NOMBRE TÉCNICO basado en el contexto inmediato.

INPUT:
\"\"\"{plantilla_texto}\"\"\"

INSTRUCCIONES DE BAUTIZO:
1. Lee el texto ANTES de cada `[[VARIABLE_POR_DEFINIR]]`.
2. Asigna un nombre de variable descriptivo.
   - Ejemplo: "...expedida en [[VARIABLE_POR_DEFINIR]]" -> `LUGAR_EXPEDICION_VENDEDOR`
   - Ejemplo: "...estado civil [[VARIABLE_POR_DEFINIR]]" -> `ESTADO_CIVIL_COMPRADOR`
   - Ejemplo: "...ocupación: [[VARIABLE_POR_DEFINIR]]" -> `OCUPACION_OTORGANTE`

3. MANTÉN las variables que ya tienen nombre (ej: `[[NOMBRE_VENDEDOR]]`).

SALIDA JSON ESTRICTA:
{{
 "tipo_acto": "Escritura Pública Compleja",
 "lista_variables_requeridas": [
    "NUMERO_RADICADO",
    "NOMBRE_VENDEDOR"
 ],
 "resumen_requisitos": "..."
}}

⛔ REGLA DE ORO: Responde ÚNICAMENTE con el JSON. NO uses Markdown ni explicaciones.
"""

PROMPT_SCAN_ID = """Eres un asistente OCR experto. Extrae todos los campos de esta cédula en JSON estricto
(nombre, cedula, FECHA DE NACIMIENTO, LUGAR DE NACIMIENTO, FECHA Y LUGAR DE EXPEDICION):
{ "nombre": "NOMBRE COMPLETO", "cedula": "NUMERO", "fecha_nacimiento": "...", "lugar_nacimiento": "...", "fecha_expedicion": "...", "lugar_expedicion": "..." }.
Si no se lee, usa "ILEGIBLE".
"""

PROMPT_DOCS_LEGAL1 = """ERES UN ANALISTA JURÍDICO NOTARIAL.
TU OBJETIVO: Procesar TODOS los documentos para crear un contexto completo, pero diferenciando claramente el "Negocio Actual" de la "Historia".

1. NEGOCIO ACTUAL (Fuente: Hoja de Radicación):
- Actos que se van a firmar HOY. Extrae lista de actos, cuantías actuales, compradores y vendedores de hoy.
- "Paz y Salvo" NO es un acto.

2. CONTEXTO HISTÓRICO (Fuente: Certificados, escrituras viejas):
- Extrae historia y guárdala como antecedentes.

3. PERSONAS Y DATOS:
- Cruza info: si en radicación dice "Sandra", busca su cédula en scans.

4. DATOS DEL INMUEBLE (OBLIGATORIO):
Extrae: matricula, predial_nacional, direccion, afectacion_vivienda, patrimonio_familia, cabida_area, linderos, tradicion (número/fecha/notaría).

SALIDA JSON ESTRICTA con:
{
  "es_hoja_radicacion": false,
  "negocio_actual": {...},
  "datos_inmueble": {...},
  "historia_y_antecedentes": {...},
  "personas_detalle": [...],
  "mapeo_roles": {...},
  "actos_juridicos": [...]
}
"""

PROMPT_RADICACION_LEGAL2 = """ERES UN ANALISTA DE DATOS NOTARIALES EXPERTO EN "HOJAS DE RADICACIÓN".
Estructura la información del OCR adjunto.

DICCIONARIO:
- "DE": Otorgante/Vendedor
- "A": Beneficiario/Comprador
- "RL": Representante Legal
- "NI": NIT (Jurídica)
- "CC": Cédula (Natural)
- "SS": Estado civil

SALIDA JSON ESTRICTA:
{
  "es_hoja_radicacion": true,
  "radicacion": { "numero": "...", "fecha": "...", "notaria": "...", "notario_encargado": "..." },
  "datos_inmueble": { "direccion": "...", "matricula": "...", "ciudad_registro": "..." },
  "negocio_actual": { "numero_radicado": "...", "total_venta_hoy": 0, "actos_a_firmar": [{ "nombre": "...", "cuantia": 0 }] },
  "personas_detalle": [...],
  "actos_juridicos": ["..."],
  "mapeo_roles": { "vendedores": [...], "compradores": [...], "representantes": [...] }
}
"""

# ---------- MERGE INTELIGENTE (tu misma idea) ----------

def es_basura(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if not s:
        return True
    u = s.upper()
    return (
        u in {"NULL", "UNDEFINED"} or
        "NO_DETECTADO" in u or
        "NO_APLICA" in u or
        "PENDIENTE" in u or
        len(s) < 3
    )

def merge_inteligente(target: Dict[str, Any], source: Optional[Dict[str, Any]]) -> None:
    if not source:
        return
    for k, vnew in source.items():
        if es_basura(vnew):
            continue
        vold = target.get(k)
        if vold is not None and (not es_basura(vold)) and len(str(vnew)) < len(str(vold)):
            # conserva el viejo si es más “rico” (ej: linderos)
            continue
        target[k] = vnew

def build_contexto_universal(parsed_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = {
        "RADICACION": "PENDIENTE",
        "INMUEBLE": {},
        "NEGOCIO": {},
        "PERSONAS": [],
        "DATOS_EXTRA": {}
    }

    for item in parsed_items:
        # Radicación
        if item.get("negocio_actual", {}).get("numero_radicado"):
            ctx["RADICACION"] = item["negocio_actual"]["numero_radicado"]
        if item.get("radicacion", {}).get("numero"):
            ctx["RADICACION"] = item["radicacion"]["numero"]

        # Inmueble
        if item.get("datos_inmueble"):
            merge_inteligente(ctx["INMUEBLE"], item["datos_inmueble"])
        if item.get("linderos") and len(str(item["linderos"])) > 50:
            ctx["INMUEBLE"]["linderos"] = item["linderos"]

        # Negocio
        if item.get("negocio_actual"):
            merge_inteligente(ctx["NEGOCIO"], item["negocio_actual"])

        # Personas
        p = item.get("personas_detalle") or item.get("personas") or item.get("intervinientes") or []
        if isinstance(p, list):
            for persona in p:
                rol_raw = persona.get("rol") or persona.get("rol_en_hoja") or "INTERVINIENTE"
                rol = str(rol_raw).upper()
                if "DE " in rol:
                    rol = "VENDEDOR/OTORGANTE"
                if "A " in rol:
                    rol = "COMPRADOR/BENEFICIARIO"
                ctx["PERSONAS"].append({
                    "nombre": (persona.get("nombre") or persona.get("nombre_completo") or "").upper(),
                    "identificacion": persona.get("identificacion") or persona.get("cedula") or "PENDIENTE",
                    "rol_detectado": rol,
                    "datos_contacto": persona.get("datos") or {}
                })

        # Extras
        if item.get("hallazgos_variables"):
            merge_inteligente(ctx["DATOS_EXTRA"], item["hallazgos_variables"])

    return ctx

# ---------- Misiones tipo n8n (encabezado + actos + cierre) ----------

SYSTEM_NOTARIO_REDACTOR = """ERES UN MOTOR DE IA EXPERTO EN "DATA BINDING" (FUSIÓN DE DATOS JURÍDICOS).
No inventes: usa SOLO el JSON. Si falta algo: [[PENDIENTE: ...]].
No saludes. No expliques. Devuelve texto final.
"""

USER_NOTARIO = lambda contexto_datos, plantilla_con_huecos, extra_instr: f"""ERES UN AGENTE DE PROCESAMIENTO DOCUMENTAL (DATA BINDER).

BASE DE DATOS (HECHOS):
{json.dumps(contexto_datos, ensure_ascii=False)}

PLANTILLA (CON HUECOS):
\"\"\"{plantilla_con_huecos}\"\"\"

INSTRUCCIONES:
{extra_instr}

Devuelve ÚNICAMENTE el texto completado.
"""

def preparar_misiones(plantilla_base: str, templates_rag: List[Dict[str, Any]], contexto_universal: Dict[str, Any]) -> List[Dict[str, Any]]:
    misiones: List[Dict[str, Any]] = []

    misiones.append({
        "orden": 1,
        "extra_instr": "Rellena el encabezado y la comparecencia.",
        "plantilla": plantilla_base
    })

    actos_list = contexto_universal.get("NEGOCIO", {}).get("actos_a_firmar") or [{"nombre": "ACTO GENERAL", "cuantia": 0}]
    if isinstance(actos_list, dict):
        actos_list = [actos_list]
    if isinstance(actos_list, str):
        try:
            actos_list = json.loads(actos_list)
        except Exception:
            actos_list = [{"nombre": "ACTO", "cuantia": 0}]

    ordinales = ["PRIMER", "SEGUNDO", "TERCER", "CUARTO", "QUINTO"]

    for idx, acto in enumerate(actos_list):
        nombre_acto = (acto.get("nombre") or "ACTO").strip()
        cuantia = acto.get("cuantia") or 0

        # toma el texto RAG correspondiente (si no, deja placeholder)
        texto_crudo = "TEXTO NO ENCONTRADO"
        if idx < len(templates_rag) and templates_rag[idx].get("contenido_legal"):
            texto_crudo = templates_rag[idx]["contenido_legal"]
        else:
            # fallback: busca por nombre dentro del texto
            for t in templates_rag:
                if t.get("contenido_legal") and nombre_acto.lower() in t["contenido_legal"].lower():
                    texto_crudo = t["contenido_legal"]
                    break

        ctx = json.loads(json.dumps(contexto_universal, ensure_ascii=False))
        ctx["VALOR_ACTO_ACTUAL"] = cuantia

        misiones.append({
            "orden": 20 + idx,
            "extra_instr": f'Título: **-{ordinales[idx] if idx < len(ordinales) else "SIGUIENTE"} ACTO-** **{nombre_acto.upper()}**',
            "plantilla": texto_crudo,
            "contexto_override": ctx
        })

    cierre = """
**OTORGAMIENTO Y AUTORIZACIÓN**
EL(LOS) COMPARECIENTE(S) HACE(N) CONSTAR QUE...
**EL(LOS) OTORGANTE(S),**
[[BLOQUE_DE_FIRMAS]]
**LA NOTARIA,**
""".strip()

    misiones.append({
        "orden": 99,
        "extra_instr": "Genera el bloque de firmas usando PERSONAS del contexto.",
        "plantilla": cierre
    })

    return misiones
