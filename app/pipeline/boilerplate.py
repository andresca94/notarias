# app/pipeline/boilerplate.py
from __future__ import annotations

# Nota: aquí dejamos placeholders tipo [[...]] para que OpenAI DataBinder los complete o los deje como [[PENDIENTE:...]].

REDAM_COMPRAVENTA_PROTOCOLIZACION_TEXT = (
    "De conformidad con el artículo 6 numeral 3 de la Ley 2097 del 02 de julio de 2021, "
    "LA PARTE VENDEDORA presenta para su protocolización CERTIFICACIÓN DE LA CONSULTA "
    "REALIZADA ANTE EL MINISTERIO DE TECNOLOGÍAS DE LA INFORMACIÓN Y COMUNICACIONES - "
    "MINTIC, Registro de Deudores Alimentarios Morosos. (Circular 355 de fecha 20 septiembre "
    "de 2023 SNR)."
)


def build_certificados_paz_y_salvo_detalle(
    paz_salvo_predial: str | None,
    paz_salvo_valorizacion: str | None,
    paz_salvo_area_metro: str | None,
) -> list[str]:
    def _norm(value: str | None) -> str:
        return str(value or "").strip()

    def _usable(value: str) -> bool:
        if not value:
            return False
        upper = value.upper()
        return upper not in {"NO APLICA", "N/A", "NULL", "NONE"} and "[[PENDIENTE:" not in upper

    predial = _norm(paz_salvo_predial)
    valorizacion = _norm(paz_salvo_valorizacion)
    area_metro = _norm(paz_salvo_area_metro)

    lines: list[str] = []
    if _usable(predial):
        lines.append(f"Paz y Salvo Predial N° {predial}.")
    if _usable(valorizacion):
        if "NO COBRA" in valorizacion.upper():
            lines.append(f"Constancia de no cobro de valorización: {valorizacion}.")
        else:
            lines.append(f"Paz y Salvo de Valorización N° {valorizacion}.")
    if _usable(area_metro):
        lines.append(f"Paz y salvo de Área Metropolitana N° {area_metro}.")
    return lines


EP_CARATULA_TEMPLATE = """
ESCRITURA PUBLICA NUMERO: [[PENDIENTE: NUMERO_EP]]
RADICADO: [[NUMERO_RADICADO]]
OTORGADA EN LA NOTARIA: [[NOTARIA_NOMBRE]]
CIRCULO NOTARIAL: [[CIUDAD]]
SUPERINTENDENCIA DE NOTARIADO Y REGISTRO
FECHA: [[PENDIENTE: FECHA_OTORGAMIENTO]]

NATURALEZA JURIDICA DEL ACTO:

[[RESUMEN_ACTOS]]

INMUEBLE: [[DESCRIPCION_INMUEBLE]]
MATRICULA INMOBILIARIA: [[MATRICULA_INMOBILIARIA]]
NÚMERO PREDIAL NACIONAL: [[NUMERO_PREDIAL_NACIONAL]]
CÓDIGO CATASTRAL ANTERIOR: [[CODIGO_CATASTRAL_ANTERIOR]]
AFECTACIÓN A VIVIENDA FAMILIAR: [[AFECTACION_VIVIENDA_FAMILIAR]]
PATRIMONIO DE FAMILIA INEMBARGABLE: [[PATRIMONIO_FAMILIA_INEMBARGABLE]]
""".strip()


EP_INSERTOS_TEMPLATE = """
INSERTOS Y COMPROBANTES: Se protocolizan:

Certificado de Tradición y Libertad N° [[MATRICULA_INMOBILIARIA]].
Fotocopias de cédulas / documentos de identificación de los otorgantes.
[[CAMARAS_COMERCIO]]
Paz y Salvo Predial N° [[PAZ_SALVO_PREDIAL]].
Paz y Salvo de Valorización N° [[PAZ_SALVO_VALORIZACION]].
Paz y salvo de Área Metropolitana N° [[PAZ_SALVO_AREA_METRO]].
Certificado REDAM (si aplica).

ME FUERON PRESENTADOS LOS SIGUIENTES COMPROBANTES LEGALES QUE SE PROTOCOLIZAN ASÍ:
[[CERTIFICADOS_PAZ_Y_SALVO_DETALLE]]

ADVERTENCIA NOTARIAL: Los comparecientes hacen constar que han verificado cuidadosamente sus nombres completos, estado civil y número de su documento de identidad; declaran que todas las informaciones consignadas en el presente instrumento son correctas, y que, en consecuencia, asumen la responsabilidad que se derive de cualquier inexactitud.

ADVERTENCIA DE REGISTRO: Se advirtió a los otorgantes la obligación de registrar esta escritura dentro de los dos (2) meses siguientes.

ADVERTIDOS: Los otorgantes de la formalidad del registro se les leyó la presente escritura y la aprobaron.
""".strip()


# Tu bloque (estático) de OTORGAMIENTO y AUTORIZACIÓN — lo dejamos tal cual, solo con placeholders puntuales.
EP_OTORGAMIENTO_TEMPLATE = """
OTORGAMIENTO y AUTORIZACIÓN

EL(LOS) COMPARECIENTE(S) HACE(N) CONSTAR QUE:
1. Declaran bajo la gravedad de juramento que su presencia física y jurídica, así como las manifestaciones en las diferentes cláusulas de este instrumento, obedece a la autonomía de su voluntad y que no se ha ejercido sobre ellos dolo, fuerza física o psicológica, que los datos consignados en la comparecencia del presente instrumento público como lo son sus nombres y apellidos, la titularidad del documento de identificación exhibido, así como su estado civil corresponden a su actual realidad jurídica, los cuales han sido confirmados de viva voz a los funcionarios notariales y transcritos de su puño y letra al momento de plasmar su firma en señal de aceptación del presente acto notarial, hechos que dejan plenamente establecida su asistencia en este despacho notarial.
2. Han verificado cuidadosamente su nombre y apellido, su real estado civil, numero correcto de sus documentos de identificación, y aprueban este instrumento sin reserva alguna, en forma como quedo redactado.
3. Las declaraciones consignadas en este instrumento corresponde a la verdad el(los) otorgantes lo aprueba totalmente, sin reserva alguna, en consecuencia, asumen la responsabilidad de cualquier inexactitud.
4. La Notaria no puede dar fe sobre la voluntad real del(los) comparecientes y beneficiaros, salvo lo expresado en este instrumento, que fue aprobado sin reserva alguna por el(los) comparecientes y beneficiarios en la forma como quedo redactado.
5. Conocen la ley y saben que la Notaría responde de la regularidad formal de los instrumentos que autoriza, pero no de la veracidad de las declaraciones del(los) otorgantes ni de la autenticidad de los documentos que forman parte de este instrumento.
6. Serán responsables civil, penal y físicamente, en caso de utilizarse esta escritura con fines ilegales.
7. Solo solicitaran correcciones, aclaraciones, o modificaciones al texto de la presente escritura en la forma y casos previstos por la ley
8. La presente escritura se Autoriza a Insistencia de los interesados (ART. 6 Decreto 960 de 1970).

DE LA CAPACIDAD: El (la, los) compareciente(s) manifiesta(n) que son plenamente capaces para contratar y obligarse, que no tiene ningún tipo de impedimento legal que vicie de nulidad las declaraciones que dentro del acto o negocio jurídico se han consignado. Que gozan de forma absoluta del ejercicio de sus derechos y que las declaraciones redactadas en este instrumento son su real voluntad y de esta forma busca la eficacia del acto o negocio otorgado. Que sus condiciones mentales e intelectuales son las idóneas y en razón a ello han conllevado a la notaria a través de un juicio de valores, a determinar su capacidad para comparecer. Que han entendido el clausulado que conforma la presente escritura pública y que la aprueban en su totalidad.

DEL OBJETO LICITO: El (la, los) compareciente(s) manifiesta(n) que el objeto del presente negocio o acto jurídico se encuentra enmarcado dentro de las normas legales vigentes, que no contraviene la ley.

DE LOS RECURSOS: Manifiesta(n) El (la, los) compareciente(s) que para efectos de las leyes 333 de 1996, 335 de 1997 y 793 de 2000, los dineros que componen la cuantía del acto o negocio jurídico contenido en la presente escritura pública son recursos que provienen de la práctica de actividades licitas.

DE LA IDENTIFICACIÓN: El (la, los) compareciente(s) manifiesta(n) que los documentos de identidad que exhiben para suscribir el presente instrumento son auténticos, se encuentran vigentes y corresponden a su verdadera identidad, en todo caso que el (la, los) compareciente(s) presente(n) para su identificación contraseña que señale el trámite de duplicado, corrección o rectificación, el ciudadano afirma bajo la gravedad de juramento que el sello que certifica el estado de su trámite ha sido estampado en una oficina de la registraduría nacional del estado civil, en todo caso los titulares de contraseñas de expedición de cédula de ciudadanía por primera vez, o no certificadas, las cédulas de extranjería, pasaportes o visas que no pueden ser sometidas al control de captura de identificación biométrica, manifiestan que estos documentos han sido tramitados y expedidos por la entidad competente y legítimamente constituida para ello (registraduría, consulado, das, embajadas, etc.) y que no ha sido adulterada o modificada dolosamente.

CLÁUSULA DE CONOCIMIENTO: La Notaría en ejercicio del control de legalidad que se le asiste advierte a las partes intervinientes en el negocio jurídico de la importancia de verificar previamente la identidad, condiciones legales de los intervinientes, que los bienes, cosas y derechos que se comprometen en esta transacción jurídica, están dentro del mercado comercial y de la vida jurídica y están sujetos al principio de la oferta y la demanda por no existir sobre los mismos anotaciones o decisiones que prohíban la enajenación o gravamen, embargo o medida cautelares vigente, que no contraviene la ley, que los bienes, cosas y derechos que se comprometen en esta transacción jurídica, están dentro del mercado comercial y de la vida jurídica y están sujetos al principio de la oferta y la demanda.

MANIFESTACIÓN DE LOS COMPARECIENTES: El(Los) compareciente(s) manifiesta(n) bajo la gravedad del juramento que hasta la fecha este inmueble no se encuentra inmerso en causales o motivos de la Ley de Víctimas y Restitución de Tierras (Ley 1448 de 2011), de igual forma manifiestan que en el inmueble objeto del presente negocio jurídico no ha existido desplazamiento forzoso, despojo o abandono forzado de tierras, así como tampoco acción ilegal que genere aprovechamiento de la situación de violencia, ni ha sido fruto de ser privado arbitrariamente a una persona de su propiedad, posesión por vía de hecho y/o negocios apócrifos, actos administrativos, sentencia o mediante comisión de delitos asociados a la situación de violencia.

ADVERTENCIA NOTARIAL: A los otorgantes se les advierte que una vez firmado este instrumento público la Notaría no asumirá correcciones o modificaciones sino en la forma y casos previstos por la ley, siendo esto solo responsabilidad de los otorgantes. Además, la Notaría les advierte a EL(LOS) COMPARECIENTE(S) que cualquier aclaración a la presente escritura implica el otorgamiento de una nueva escritura pública de aclaración cuyos costos serán asumidos única y exclusivamente por EL(LOS) COMPARECIENTE(S). EL(LOS) COMPARECIENTE(S) hacen constar que han verificado cuidadosamente sus nombres completos y el número de su documento de identidad. Declaran que todas las informaciones consignadas en el presente instrumento son CORRECTAS y, en consecuencia, asumen la responsabilidad que se derive de cualquier inexactitud en las mismas. Conocen la ley y saben que la Notaría responde de la regularidad formal de los instrumentos que autoriza, pero no de la veracidad de las declaraciones de los interesados. Se les advierte a los otorgantes de la formalidad del registro en la oficina correspondiente dentro del término perentorio de dos (2) meses contados a partir de la fecha de otorgamiento de este instrumento, cuyo incumplimiento causará interés moratorio por mes o fracción de mes de retardo, de conformidad con el artículo 37 del Decreto Ley 960 de 1970. De igual forma se les advierte a los otorgantes que deben tener en cuenta lo señalado en el artículo 1947 del Código Civil Colombiano.

LEY DE CRECIMIENTO ECONÓMICO: La notaría ilustró a los otorgantes sobre el contenido, alcance y consecuencias del artículo 61 de la ley 2010 del 27 de diciembre de 2019, que modificó el artículo 90 del estatuto tributario. Los otorgantes declaran bajo gravedad de juramento que el precio de la venta contenido en la presente escritura es real y no ha sido objeto de pactos privados en los que se señale un precio diferente, ni existen sumas que se hayan convenido o facturado por fuera del valor aquí estipulado.

PROTECCIÓN DE DATOS DE CARÁCTER PERSONAL: Los intervinientes aceptan la incorporación de sus datos y la copia del documento de identidad en el presente instrumento, con la finalidad de realizar las funciones propias de la actividad Notarial y efectuar las comunicaciones de datos previstas en la ley a la administración pública.

ACEPTACIÓN DE NOTIFICACIONES ELECTRÓNICAS: El(los) interesado(s) manifiesta(n) su consentimiento el cual se entiende otorgado con la firma de la presente escritura pública que NO (   ) SI ( X ) acepta(n) ser notificado(s) por medio electrónico al correo: [[EMAIL_NOTIFICACIONES]], sobre el estado del trámite del presente instrumento público una vez haya calificación y anotación en el folio de matrícula inmobiliaria correspondiente, todo de conformidad con el artículo 15 del decreto 1579 del 1 de octubre de 2012 y artículo 56 del código de procedimiento administrativo y de lo contencioso administrativo.
""".strip()


EP_DERECHOS_TEMPLATE = """
DERECHOS NOTARIALES: Derechos Notariales, según Resolución número [[RESOLUCION_DERECHOS]]; por valor de:

Gastos Notariales: $ [[VALOR_DERECHOS]]
IVA: $ [[VALOR_IVA]]
Retención en la Fuente: $ [[VALOR_RETEFUENTE]]
Superintendencia de Notariado: $ [[VALOR_SUPER]]
Fondo Nacional del Notariado: $ [[VALOR_FONDO]]

NOTA: ESTA ESCRITURA FUE EXTENDIDA EN LAS HOJAS DE PAPEL NOTARIAL NÚMEROS:
[[NUMEROS_PAPEL_NOTARIAL]]
""".strip()


EP_UIAF_TEMPLATE = """
NOMBRE:

DOCUMENTOS DE IDENTIFICACIÓN:

TELEFONO FIJO:

CELULAR:

DIRECCIÓN:

CIUDAD:

EMAIL:

PROFESIÓN U OFICIO:

ACTIVIDAD ECONÓMICA:

ESTADO CIVIL:

PERSONA EXPUESTA POLÍTICAMENTE DECRETO 1674 DE 2016: SI: ___ / NO: X
CARGO:
FECHA DE VINCULACIÓN:
LA ANTERIOR INFORMACIÓN SE TOMA CONFORME LO ORDENADO EN LA INSTRUCCIÓN ADMINISTRATIVA N° 08 DE 07 DE ABRIL DE 2017 DE LA SUPERINTENDENCIA DE NOTARIADO Y REGISTRO.
""".strip()


EP_APERTURA_TEMPLATE = """
--------------------------------------------------------------------------------------------------------------
En la ciudad de [[CIUDAD]], [[DEPARTAMENTO]], República de Colombia, a los [[PENDIENTE: DIA_LETRAS]] ([[PENDIENTE: DIA_NUMERO]]) días del mes de [[PENDIENTE: MES_LETRAS]] del año [[PENDIENTE: ANO_LETRAS]], ante mí, [[NOTARIA_ENCARGADO]], [[NOTARIA_NOMBRE]]. ---------------------------------------------------------------------------------------------------------------
""".strip()


EP_FIRMAS_TEMPLATE = """
EL(LOS) OTORGANTE(S),

[[BLOQUE_FIRMAS]]

TODO LO ESCRITO EN OTRO TIPO DE LETRA Y MAQUINA VALE.
""".strip()
