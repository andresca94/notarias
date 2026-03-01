"""
KnowledgeRAG: recupera conocimiento legal del derecho notarial colombiano
para enriquecer las instrucciones del DataBinder por tipo de acto.

El contenido está basado en el "Mapa Conceptual Derecho Notarial Escritura
Pública" (D.L 960 de 1970, Ley 258/1996, Ley 854/2003 y demás normas).
"""
from __future__ import annotations

from typing import List

try:
    from rank_bm25 import BM25Okapi  # type: ignore
except Exception:
    BM25Okapi = None


# ── Chunks de conocimiento extraídos del mapa conceptual ────────────────────

KNOWLEDGE_CHUNKS = [
    {
        "id": "requisitos_formales",
        "tags": ["general", "escritura", "formal", "registro", "plazo"],
        "text": (
            "REQUISITOS FORMALES DE LA ESCRITURA PÚBLICA:\n"
            "- Redactar en idioma castellano, sin abreviaturas.\n"
            "- Cantidades en letras y números.\n"
            "- Extender en papel de seguridad autorizado, letra Arial No 12.\n"
            "- La escritura debe registrarse en la Oficina de Registro correspondiente "
            "dentro de los DOS (2) MESES contados a partir de la fecha de otorgamiento; "
            "el incumplimiento causa intereses moratorios sobre la boleta fiscal.\n"
            "- Hipotecas y patrimonio de familia deben registrarse dentro de los "
            "90 días siguientes a la fecha de la escritura.\n"
            "ACTOS QUE REQUIEREN ESCRITURA PÚBLICA: compraventa de inmuebles, "
            "venta de servidumbres, venta de derechos sucesorales (Art. 13 Dcto 960/1970)."
        ),
    },
    {
        "id": "comparecencia",
        "tags": ["comparecencia", "identificacion", "personas", "estado_civil", "domicilio"],
        "text": (
            "IDENTIFICACIÓN DE COMPARECIENTES (Art. 24-25 D.L 960/1970):\n"
            "Persona natural colombiana: cédula de ciudadanía.\n"
            "Extranjero: pasaporte o cédula de extranjería (Decreto 834 de 2013, arts. 17 y 33).\n"
            "Persona jurídica: certificado de existencia y representación legal "
            "(si es entidad bancaria, también certificado de la Superfinanciera).\n"
            "DATOS QUE DEBEN CONSTAR POR CADA COMPARECIENTE:\n"
            "1. Nombre completo y apellidos.\n"
            "2. Calidad en que comparece (contratante, representante legal, apoderado, etc.).\n"
            "3. Estado civil.\n"
            "4. Domicilio.\n"
            "5. Edad (solo se expresa el número de años para menores adultos, "
            "adoptantes y adoptados en escritura de adopción; para mayores basta decir 'mayor de edad')."
        ),
    },
    {
        "id": "compraventa",
        "tags": ["compraventa", "inmueble", "venta", "precio", "vendedor", "comprador", "rural", "apartamento"],
        "text": (
            "COMPRAVENTA DE BIENES INMUEBLES — DOCUMENTOS Y REQUISITOS:\n"
            "Documentos básicos: cédulas vendedor y comprador, paz y salvo predial, "
            "título antecedente, certificado de libertad y tradición, consulta de más bienes.\n"
            "Si quien firma lo hace por poder: cédula del poderdante, verificar repositorio "
            "de poderes; si no figura, escanear y confirmar con la notaría del poder.\n"
            "Si el comprador es menor: lo representan los padres; aportar registro civil "
            "y tarjeta de identidad.\n"
            "Si es persona jurídica: certificado de existencia y representación legal.\n"
            "COMPRAVENTA DE INMUEBLE RURAL: además, declaración extrajuicio del vendedor "
            "(vende voluntariamente), copia del acto de la junta autorizando, permiso INCODER, "
            "revisar en el VUR si el comprador tiene más predios rurales.\n"
            "COMPRAVENTA DE APARTAMENTOS: paz y salvo de la administración del edificio, "
            "copia del reglamento de propiedad horizontal si está radicado en otra notaría.\n"
            "PRECIO: expresarse en moneda legal colombiana en letras y números. "
            "Si se pacta en moneda extranjera, establecer equivalencia en moneda nacional."
        ),
    },
    {
        "id": "hipoteca",
        "tags": ["hipoteca", "gravamen", "constitucion", "garantia", "deudor", "acreedor", "cuantia"],
        "text": (
            "HIPOTECA — REQUISITOS LEGALES:\n"
            "La hipoteca debe registrarse dentro de los 90 días siguientes a la fecha "
            "de la escritura para ser oponible a terceros.\n"
            "Identificar claramente: hipotecante/deudor, acreedor, inmueble gravado "
            "(cédula catastral, matrícula inmobiliaria, linderos y nomenclatura).\n"
            "Cuantía: expresar en moneda legal colombiana en letras y números.\n"
            "Cláusulas típicas de hipoteca abierta sin límite de cuantía:\n"
            "- Extensión a mejoras, construcciones e indemnizaciones (Art. 2466 C.C.).\n"
            "- Condiciones de exigibilidad anticipada del crédito.\n"
            "- Copia con mérito ejecutivo para el acreedor (Decreto 1681/1996).\n"
            "- La hipoteca no se extingue por renovación o prórroga de obligaciones."
        ),
    },
    {
        "id": "cancelacion",
        "tags": ["cancelacion", "hipoteca", "gravamen", "pacto", "retroventa", "usufructo",
                 "fideicomiso", "arrendamiento", "patrimonio", "condicion", "reglamento"],
        "text": (
            "CANCELACIÓN DE GRAVÁMENES Y ACTOS — REQUISITOS:\n"
            "Referenciar explícitamente la escritura pública que se cancela: "
            "número, fecha y notaría.\n"
            "Para CANCELACION DE HIPOTECA: identificar al acreedor hipotecario que "
            "cancela y al deudor/hipotecante. El acreedor debe declarar a paz y salvo.\n"
            "Para CANCELACION PACTO DE RETROVENTA: señalar que la facultad de rescate "
            "fue cancelada de común acuerdo o por vencimiento del plazo, y ratificar "
            "al actual propietario como dueño pleno.\n"
            "Para CANCELACION USUFRUCTO/FIDEICOMISO/ARRENDAMIENTO: indicar el tipo de "
            "derecho que se extingue, las partes y la escritura constitutiva."
        ),
    },
    {
        "id": "dacion_pago",
        "tags": ["dacion", "pago", "deuda", "acreedor", "deudor", "obligacion", "extincion"],
        "text": (
            "DACIÓN EN PAGO — REQUISITOS LEGALES:\n"
            "El deudor (otorgante/VENDEDOR) transfiere el inmueble al acreedor "
            "(beneficiario/ACREEDOR) como forma de extinguir una deuda preexistente.\n"
            "Debe identificarse: la obligación que se extingue (monto, título, origen), "
            "el inmueble con todos sus datos registrales, y el valor por el que se da en pago.\n"
            "Si el valor del inmueble supera la deuda: el excedente se restituye al deudor "
            "o se imputa a otras obligaciones.\n"
            "Si el valor es inferior: señalar si el saldo queda pendiente o si se condona.\n"
            "El acreedor debe aceptar la dación en la misma escritura."
        ),
    },
    {
        "id": "donacion",
        "tags": ["donacion", "insinuacion", "donante", "donatario", "gratuito", "liberalidad"],
        "text": (
            "DONACIÓN DE INMUEBLES — REQUISITOS LEGALES:\n"
            "Donaciones de valor superior a 50 SMLMV requieren INSINUACIÓN "
            "(autorización judicial o notarial) antes de la escritura.\n"
            "Identificar: donante (VENDEDOR/otorgante), donatario (COMPRADOR/beneficiario), "
            "inmueble con cédula catastral, matrícula inmobiliaria y linderos.\n"
            "Si hay insinuación: referenciar la resolución de insinuación "
            "(número, fecha, despacho o notaría que la otorgó).\n"
            "La donación es a título gratuito; no hay precio de compraventa "
            "pero sí valor catastral/comercial para efectos notariales.\n"
            "La aceptación del donatario debe constar expresamente en la misma escritura."
        ),
    },
    {
        "id": "afectacion_vivienda",
        "tags": ["afectacion", "vivienda", "familiar", "conyuges", "compañeros", "inalienable"],
        "text": (
            "AFECTACIÓN A VIVIENDA FAMILIAR — MARCO LEGAL "
            "(Ley 258/1996 modificada por Ley 854/2003):\n"
            "Solo puede afectarse el inmueble destinado como vivienda principal "
            "y permanente de la familia.\n"
            "Ambos cónyuges o compañeros permanentes deben comparecer para constituirla.\n"
            "La afectación impide la enajenación o gravamen del inmueble sin el "
            "consentimiento de ambos titulares.\n"
            "Para levantar la afectación: ambos deben comparecer, o por muerte de uno, "
            "o disolución de la sociedad conyugal/patrimonial.\n"
            "CONSTANCIA NOTARIAL: en toda escritura de compraventa o hipoteca de "
            "inmueble residencial, la notaria deja constancia del estado de afectación "
            "a vivienda familiar al cierre del instrumento."
        ),
    },
    {
        "id": "aclaracion_actualizacion",
        "tags": ["aclaracion", "actualizacion", "nomenclatura", "error", "correccion", "orip"],
        "text": (
            "ACLARACIÓN / ACTUALIZACIÓN DE ESCRITURA PÚBLICA:\n"
            "Referenciar la escritura que se aclara: número, fecha, notaría y "
            "folio de matrícula inmobiliaria.\n"
            "Señalar explícitamente qué dato se corrige o actualiza: "
            "número predial, nomenclatura, linderos, nombre del compareciente, "
            "cédula catastral, matrícula inmobiliaria, área, etc.\n"
            "La aclaración NO modifica el negocio jurídico de fondo "
            "(precio, partes, objeto), solo corrige errores o actualiza datos formales.\n"
            "Si la ORIP devolvió la escritura: indicar la causal de devolución "
            "y el dato que se corrige para subsanarla."
        ),
    },
    {
        "id": "inmueble_identificacion",
        "tags": ["inmueble", "identificacion", "matricula", "linderos", "predial", "catastral", "nomenclatura"],
        "text": (
            "IDENTIFICACIÓN DEL INMUEBLE (Art. 23 D.L 960/1970):\n"
            "El inmueble objeto del contrato debe identificarse por:\n"
            "- Cédula catastral / código catastral anterior.\n"
            "- Paraje, vereda o localidad donde está ubicado.\n"
            "- Matrícula inmobiliaria (número de folio en la ORIP).\n"
            "- Número predial nacional.\n"
            "- Linderos usando el sistema métrico decimal "
            "(POR EL NORTE / SUR / ORIENTE / OCCIDENTE).\n"
            "- Nomenclatura (dirección oficial).\n"
            "- Cabida/área: en hectáreas y metros cuadrados.\n"
            "El precio o cuantía se expresa en moneda legal colombiana, "
            "en letras y números."
        ),
    },
    {
        "id": "clausulas_especiales",
        "tags": ["clausulas", "especiales", "subsidio", "vivienda", "interes_social",
                 "redam", "uiaf", "origen_fondos", "condicion_resolutoria"],
        "text": (
            "ADVERTENCIAS Y CLÁUSULAS ESPECIALES:\n"
            "- UIAF / Origen de fondos: declaración anti-lavado de activos.\n"
            "- REDAM: cláusula sobre Registro de Deudores Alimentarios Morosos "
            "(obligatoria en compraventas).\n"
            "- Condición resolutoria expresa: si aplica al negocio.\n"
            "- Inalienabilidad: si aplica.\n"
            "- Derecho de preferencia: si aplica.\n"
            "- Vivienda con subsidio: incluir cláusulas especiales de la entidad otorgante.\n"
            "- Elegibilidad como Vivienda de Interés Social (VIS): indicar si aplica.\n"
            "- Registro: plazo perentorio de DOS (2) MESES desde el otorgamiento "
            "para registrar la escritura; vencido el plazo causan intereses moratorios."
        ),
    },
    {
        "id": "cambio_razon_social",
        "tags": ["cambio", "razon", "social", "empresa", "sociedad", "persona_juridica", "nit"],
        "text": (
            "CAMBIO DE RAZÓN SOCIAL CON INMUEBLES:\n"
            "La sociedad acredita el cambio de razón social mediante escritura pública "
            "o documento privado reconocido ante notario.\n"
            "Identificar: razón social anterior, nueva razón social, NIT (permanece igual), "
            "inmuebles afectados con su matrícula inmobiliaria.\n"
            "Adjuntar certificado de existencia y representación legal "
            "expedido con la nueva razón social.\n"
            "El cambio de razón social no altera la titularidad ni las cargas del inmueble."
        ),
    },
    # ── Chunks incorporados del Mapa Conceptual Derecho Notarial (PDF 5 págs.) ──
    {
        "id": "fe_publica_notario",
        "tags": ["notario", "fe", "publica", "autenticidad", "instrumento", "protocolo",
                 "etapas", "recepcion", "extension", "otorgamiento", "autorizacion"],
        "text": (
            "EL NOTARIO Y LA FE PÚBLICA (Art. 123 C.P.; Ley 29/1973):\n"
            "El notario es un particular que presta un servicio público delegado por el estado, "
            "bajo el principio de descentralización por colaboración "
            "(Art. 123 C.P.; Sentencia C-1508 del 2000). "
            "Es guardián de la fe pública, garante de la autenticidad y la confianza "
            "de la validez de documentos o actos de interés público.\n"
            "La fe pública notarial otorga plena autenticidad a las declaraciones emitidas ante "
            "el Notario y a lo que este exprese respecto a los hechos percibidos por él en el "
            "ejercicio de sus funciones (Art. 1 Ley 29/1973; Sentencia C-037 del 2003).\n"
            "ESCRITURA PÚBLICA (Art. 13 D.L 960/1970): instrumento que contiene declaraciones "
            "de actos jurídicos emitidas ante notario, con los requisitos previstos en la ley "
            "e incorporadas al protocolo notarial.\n"
            "ETAPAS DE LA ESCRITURA PÚBLICA:\n"
            "1. RECEPCIÓN: El notario percibe las declaraciones de los interesados.\n"
            "2. EXTENSIÓN: Versión escrita de lo declarado por los comparecientes.\n"
            "3. OTORGAMIENTO: Asentimiento expreso de los comparecientes al instrumento extendido; "
            "se materializa con la impresión de la firma de los comparecientes.\n"
            "4. AUTORIZACIÓN: Fe que imprime el notario cuando se han llenado los requisitos legales "
            "y las declaraciones han sido emitidas; se materializa con la firma del notario."
        ),
    },
    {
        "id": "recepcion_documentos",
        "tags": ["recepcion", "documentos", "solicitar", "requisitos", "compraventa",
                 "poder", "persona_juridica", "menor", "rural", "apartamento",
                 "instruccion", "junta", "camara", "paz_salvo", "ctl"],
        "text": (
            "DOCUMENTOS A SOLICITAR EN RECEPCIÓN — COMPRAVENTA (Mapa Notarial / Instr. 13/2018):\n"
            "Documentos BASE (toda compraventa):\n"
            "- Cédulas vendedor y comprador (fotocopias).\n"
            "- Título antecedente (escritura anterior de adquisición del vendedor).\n"
            "- Consulta de 'más bienes' (VUR o Superintendencia de Notariado).\n"
            "- Paz y salvo predial o comprobantes fiscales al día.\n"
            "- Certificado de libertad y tradición (CTL) actualizado.\n"
            "- Vigencia de la cédula de ciudadanía cuando aplique.\n"
            "SI QUIEN FIRMA LO HACE POR PODER:\n"
            "- Buscar en el repositorio de poderes; si no figura: escanear el poder, "
            "enviarlo a la notaría del poder y obtener confirmación de vigencia.\n"
            "- Aportar cédula del poderdante.\n"
            "PERSONA JURÍDICA (vendedor o comprador sociedad):\n"
            "- Certificado de existencia y representación legal (Cámara de Comercio vigente).\n"
            "- Copia del acto de la junta directiva o asamblea autorizando la celebración "
            "del contrato (si los estatutos o la ley lo exigen).\n"
            "SI ES MENOR EL COMPRADOR:\n"
            "- Lo representan los padres o representante legal.\n"
            "- Aportar registro civil del menor y tarjeta de identidad.\n"
            "COMPRAVENTA DE INMUEBLE RURAL:\n"
            "- Declaración extrajuicio del vendedor que vende voluntariamente "
            "(sin presiones ni desplazamiento).\n"
            "- Permiso INCODER/ANT si el predio es de origen baldío adjudicado.\n"
            "- Revisar en el VUR si el comprador ya tiene más predios rurales "
            "(restricción UAF — Ley 160/1994).\n"
            "COMPRAVENTA DE APARTAMENTOS / PROPIEDAD HORIZONTAL:\n"
            "- Paz y salvo de la administración del edificio.\n"
            "- Copia del reglamento de propiedad horizontal si está radicado en otra notaría.\n"
            "DIVISIONES MATERIALES / PH / ENGLOBES:\n"
            "- El folio de matrícula debe tener área registrada.\n"
            "- Aplica Instrucción Administrativa Conjunta 13 de 2018 (Catastro y Registro)."
        ),
    },
    {
        "id": "rural_ley160",
        "tags": ["rural", "ley160", "uaf", "incoder", "ant", "baldio", "vur", "acumulacion",
                 "predios", "extrajuicio", "voluntariamente", "restriction", "adjudicacion"],
        "text": (
            "COMPRAVENTA DE INMUEBLE RURAL — REQUISITOS LEY 160/1994:\n"
            "La Ley 160 de 1994 regula la reforma agraria y el régimen de tierras rurales.\n"
            "UNIDAD AGRÍCOLA FAMILIAR (UAF):\n"
            "- No se puede acumular predios rurales que superen la UAF establecida para la zona "
            "por el INCODER/ANT. El notario debe advertir esta limitación a los comparecientes.\n"
            "- Revisar en el VUR si el comprador ya tiene predios rurales que, sumados al que "
            "compra, superarían la UAF. La acumulación de baldíos está expresamente prohibida.\n"
            "DECLARACIÓN EXTRAJUICIO DEL VENDEDOR:\n"
            "- En toda compraventa rural, el vendedor declara bajo juramento que vende "
            "voluntariamente, sin presiones, intimidaciones ni desplazamiento forzado.\n"
            "PERMISO INCODER / ANT:\n"
            "- Para predios de origen baldío adjudicado: verificar si se requiere permiso de la "
            "Agencia Nacional de Tierras (ANT, antes INCODER) para la transferencia.\n"
            "- Los bienes baldíos adjudicados tienen restricción de enajenación durante 12 años "
            "desde la adjudicación (Art. 72 Ley 160/1994).\n"
            "CONSULTA VUR:\n"
            "- Obligatoria en compraventas rurales para verificar UAF y acumulación de predios.\n"
            "FORMALIDADES DEL PREDIO RURAL EN LA ESCRITURA:\n"
            "- Denominación del predio (nombre propio).\n"
            "- Vereda, corregimiento y municipio.\n"
            "- Cabida y linderos detallados por puntos cardinales (Norte, Sur, Oriente, Occidente).\n"
            "- Cédula catastral, matrícula inmobiliaria, NPN.\n"
            "- La venta 'como cuerpo cierto' no exime de mencionar cabida y linderos.\n"
            "- Si la cabida supera 50 ha: especial cuidado con restricciones UAF y VUR."
        ),
    },
]


class KnowledgeRAG:
    """
    RAG de conocimiento notarial colombiano basado en el Mapa Conceptual de
    Derecho Notarial y Escritura Pública (D.L 960/1970 y normas concordantes).

    Recupera los chunks más relevantes para un tipo de acto dado y los incluye
    como contexto legal en las instrucciones del DataBinder.
    """

    def __init__(self) -> None:
        self._chunks = KNOWLEDGE_CHUNKS
        self._bm25 = None
        self._tokens: List[List[str]] = []
        self._build_index()

    # ── Indexación ───────────────────────────────────────────────────────────

    def _tokenize(self, s: str) -> List[str]:
        s = s.lower()
        for ch in ",.;:()[]{}<>/\\\n\r\t-_":
            s = s.replace(ch, " ")
        return [w for w in s.split() if w.strip() and len(w) > 2]

    def _build_index(self) -> None:
        if not BM25Okapi:
            return
        texts = [
            c["id"] + " " + " ".join(c["tags"]) + " " + c["text"]
            for c in self._chunks
        ]
        self._tokens = [self._tokenize(t) for t in texts]
        self._bm25 = BM25Okapi(self._tokens)

    # ── Recuperación ─────────────────────────────────────────────────────────

    def retrieve(self, acto_nombre: str, top_k: int = 2) -> str:
        """
        Retorna el texto de los top_k chunks más relevantes para el acto,
        precedidos siempre por el chunk de identificación del inmueble.

        Si BM25 no está disponible, retorna el chunk de inmueble + el de
        requisitos formales como fallback útil para cualquier acto.
        """
        # Chunk base siempre incluido: identificación del inmueble
        base_id = "inmueble_identificacion"
        base_chunk = next((c for c in self._chunks if c["id"] == base_id), None)

        if not self._bm25:
            # fallback: inmueble + requisitos_formales
            fallback_ids = [base_id, "requisitos_formales"]
            parts = [c["text"] for c in self._chunks if c["id"] in fallback_ids]
            return "\n\n".join(parts)

        q = self._tokenize(acto_nombre)
        scores = self._bm25.get_scores(q)
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

        seen: set = {base_id}
        result_parts: List[str] = []
        if base_chunk:
            result_parts.append(base_chunk["text"])

        collected = 0
        for idx, sc in ranked:
            if collected >= top_k:
                break
            chunk = self._chunks[idx]
            if chunk["id"] in seen or sc < 0.1:
                continue
            result_parts.append(chunk["text"])
            seen.add(chunk["id"])
            collected += 1

        return "\n\n".join(result_parts)
