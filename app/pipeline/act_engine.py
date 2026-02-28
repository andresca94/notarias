# app/pipeline/act_engine.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _normalize_id(x: str) -> str:
    if not x:
        return ""
    return re.sub(r"[^0-9]", "", str(x))


def _normalize_name(x: str) -> str:
    if not x:
        return ""
    s = str(x).upper()
    s = re.sub(r"[^A-ZÁÉÍÓÚÜÑ0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def dedupe_personas(personas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplica por:
      - ID numérico si existe
      - si no, por nombre normalizado
    Y hace merge conservando datos "más ricos" (no pisa con vacíos).
    """
    if not personas:
        return []

    by_id: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []

    def is_empty(v: Any) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        if not s:
            return True
        u = s.upper()
        return u in {"NULL", "UNDEFINED"} or "ILEGIBLE" in u or "PENDIENTE" in u

    def merge(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
        for k, v in (src or {}).items():
            if is_empty(v):
                continue
            old = dst.get(k)
            # conserva el que tenga más info
            if old is not None and (not is_empty(old)) and len(str(v)) < len(str(old)):
                continue
            dst[k] = v

    for p in personas:
        if not isinstance(p, dict):
            continue

        pid = _normalize_id(p.get("identificacion") or "")
        pname = _normalize_name(p.get("nombre") or "")

        if pid:
            if pid in by_id:
                merge(by_id[pid], p)
            else:
                base = dict(p)
                # normaliza nombre final
                if pname:
                    base["nombre"] = pname
                by_id[pid] = base
                out.append(base)
            continue

        if pname:
            if pname in by_name:
                merge(by_name[pname], p)
            else:
                base = dict(p)
                base["nombre"] = pname
                by_name[pname] = base
                out.append(base)
            continue

        # si no hay nada, lo dejamos
        out.append(dict(p))

    return out


def _acto_kind(acto_nombre: str) -> str:
    a = (acto_nombre or "").upper()
    a = re.sub(r"\s+", " ", a).strip()

    if "COMPRAVENTA" in a:
        return "COMPRAVENTA"
    if "HIPOTECA" in a:
        return "HIPOTECA"
    if "CANCELACION" in a or "CANCELACIÓN" in a:
        return "CANCELACION"
    if "CAMBIO" in a and "NOMBRE" in a:
        return "CAMBIO_NOMBRE"
    return "GENERIC"


def infer_roles_por_acto(
    acto_nombre: str,
    personas_globales: List[Dict[str, Any]],
    radicacion_personas: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Devuelve un diccionario de roles -> lista de personas para ESTE acto.

    Regla principal:
      - Si en radicación vienen roles DE/A, los usamos como señal fuerte
      - Si no, usamos heurísticas por tipo de acto + fallback "INTERVINIENTES"
    """
    radicacion_personas = radicacion_personas or []
    kind = _acto_kind(acto_nombre)

    # helpers
    def has_role(p: Dict[str, Any], needle: str) -> bool:
        r = (p.get("rol_detectado") or p.get("rol_en_hoja") or p.get("rol") or "").upper()
        return needle in r

    # señales DE/A globales (si existen)
    vendedores = [p for p in personas_globales if has_role(p, "VENDEDOR") or has_role(p, "OTORGANTE") or re.search(r"\bDE\b", (p.get("rol_detectado") or "").upper())]
    compradores = [p for p in personas_globales if has_role(p, "COMPRADOR") or has_role(p, "BENEFICIARIO") or re.search(r"\bA\b", (p.get("rol_detectado") or "").upper())]

    # fallback: si no hay nada, todos son intervinientes
    intervinientes = list(personas_globales or [])

    if kind == "COMPRAVENTA":
        return {
            "VENDEDORES": vendedores or [],
            "COMPRADORES": compradores or [],
            "INTERVINIENTES": intervinientes,
        }

    if kind == "HIPOTECA":
        # DEUDOR: el comprador/adquirente (quien hipoteca su nuevo bien)
        # ACREEDOR: el vendedor/otorgante (quien otorga el crédito) u otra entidad
        deudores = compradores or []
        deudor_ids = {_normalize_id(d.get("identificacion") or "") for d in deudores if d.get("identificacion")}

        acreedores = []
        for p in intervinientes:
            pid = _normalize_id(p.get("identificacion") or "")
            if pid and pid in deudor_ids:
                continue  # no puede ser acreedor y deudor a la vez
            n = (p.get("nombre") or "").upper()
            if any(x in n for x in ["BANCO", "FONDO", "SAS", "S.A.S", "S.A.", "LTDA", "FINANCI", "INSELEM", "SOLUCIONES"]):
                acreedores.append(p)

        # fallback: vendedores son acreedores si no encontramos otros
        if not acreedores:
            acreedores = [v for v in (vendedores or []) if _normalize_id(v.get("identificacion") or "") not in deudor_ids]

        return {
            "DEUDORES": deudores,
            "ACREEDORES": acreedores,
            "INTERVINIENTES": intervinientes,
        }

    if kind == "CANCELACION":
        # Cancelación pacto retroventa: los otorgantes del pacto (vendedor original + quien recibió el predio)
        # En la práctica son los mismos vendedores quienes cancelan; el comprador no es parte del acto.
        return {
            "SOLICITANTES": vendedores or intervinientes[:2],
            "INTERVINIENTES": intervinientes,
        }

    if kind == "CAMBIO_NOMBRE":
        # El nuevo propietario (comprador) solicita el cambio de nombre
        return {
            "SOLICITANTES": compradores or intervinientes[:2],
            "INTERVINIENTES": intervinientes,
        }

    return {
        "INTERVINIENTES": intervinientes,
    }


def _format_cc(digits: str) -> str:
    """63501152 → 63.501.152  (puntos cada 3 desde la derecha)."""
    if not digits or len(digits) < 6:
        return digits
    parts = []
    d = digits
    while d:
        parts.append(d[-3:])
        d = d[:-3]
    return ".".join(reversed(parts))


def build_act_context(
    contexto_universal: Dict[str, Any],
    acto_obj: Any,
    idx: int,
) -> Dict[str, Any]:
    """
    Construye un contexto listo para DataBinder, por acto:
      - PERSONAS deduplicadas
      - ROLES_ACTO (diccionario)
      - PERSONAS_ACTO (lista "priorizada" para ese acto)
      - variables convenientes
    """
    # Usar PERSONAS_ACTIVOS (ya filtrado a source=radicacion) para evitar duplicados
    # de soportes (ej: INSELEM aparece también en cámara de comercio con NIT distinto).
    personas = (contexto_universal.get("PERSONAS_ACTIVOS")
                or dedupe_personas(contexto_universal.get("PERSONAS") or []))
    acto_nombre = ""
    cuantia = 0

    if isinstance(acto_obj, dict):
        acto_nombre = str(acto_obj.get("nombre") or "ACTO")
        cuantia = int(acto_obj.get("cuantia") or 0)
    else:
        acto_nombre = str(acto_obj or "ACTO")
        cuantia = 0

    roles = infer_roles_por_acto(acto_nombre, personas, None)

    # "personas acto" priorizadas: junta roles relevantes y deduplica por id/nombre
    flat: List[Dict[str, Any]] = []
    for _, plist in roles.items():
        for p in (plist or []):
            if isinstance(p, dict):
                flat.append(p)
    flat = dedupe_personas(flat) if flat else personas

    # variables útiles
    out = dict(contexto_universal)
    out["PERSONAS"] = personas  # deduplicadas globales
    out["NOMBRE_ACTO_ACTUAL"] = acto_nombre
    out["IDX_ACTO"] = idx + 1
    out["VALOR_ACTO_ACTUAL"] = f"{cuantia:,}".replace(",", ".")
    out["ROLES_ACTO"] = roles
    out["PERSONAS_ACTO"] = flat

    # ── Variables explícitas por rol (para que DataBinder no tenga que inferirlas) ──
    # Evita que [[LUGAR_EXP_CEDULA_VENDEDOR]], [[ESTADO_CIVIL_COMPRADOR]], etc. queden [[PENDIENTE]]
    empresa_rl_map = contexto_universal.get("EMPRESA_RL_MAP") or {}

    def _p(field: str, suf: str = "") -> str:
        return f"[[PENDIENTE: {field}{suf}]]"

    def _effective(p: Dict[str, Any]) -> Dict[str, Any]:
        """Para empresas, usa los datos del RL si existe."""
        raw_id = (p.get("identificacion") or "").upper()
        if "NIT" in raw_id or raw_id.startswith("NI "):
            rl = empresa_rl_map.get(_normalize_name(p.get("nombre") or ""))
            return rl if rl else p
        return p

    # Valores LLM que deben tratarse como vacíos
    _GARBAGE_VALS = {"N/A", "NA", "EXTRAER", "S/I", "ILEGIBLE", "NULL", "UNDEFINED",
                     "NO APLICA", "NO DISPONIBLE", "DESCONOCIDO"}

    def _clean(v: Any) -> str:
        """Devuelve el valor como string o vacío si es garbage."""
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.upper() in _GARBAGE_VALS else s

    def _set_person_vars(persons: List[Dict[str, Any]], role_key: str) -> None:
        for i, raw_p in enumerate(persons[:2]):
            ep = _effective(raw_p)
            suf = f"_{i + 1}"            # e.g. "_1" or "_2"

            raw_id_upper = (raw_p.get("identificacion") or "").upper()
            is_empresa = "NIT" in raw_id_upper or raw_id_upper.startswith("NI ")

            if is_empresa:
                # ── Empresa con Representante Legal ──
                empresa_nombre = raw_p.get("nombre") or _p(f"EMPRESA_{role_key}", suf)
                empresa_raw    = re.sub(r"[^0-9]", "", raw_p.get("identificacion") or "")
                empresa_nit    = _format_cc(empresa_raw) if empresa_raw else _p(f"NIT_{role_key}", suf)

                rl_nombre = ep.get("nombre") or _p(f"RL_{role_key}_NOMBRE", suf)
                rl_raw    = re.sub(r"[^0-9]", "", ep.get("identificacion") or "")
                rl_ced    = _format_cc(rl_raw) if rl_raw else _p(f"RL_{role_key}_CEDULA", suf)
                rl_loc    = ep.get("lugar_expedicion") or _p(f"RL_{role_key}_LUG_EXP", suf)
                rl_dom    = ep.get("direccion") or ""
                rl_ciudad = rl_dom.split(",")[0].strip() if rl_dom else _p(f"RL_{role_key}_CIUDAD", suf)
                rl_ec     = _clean(ep.get("estado_civil"))   # vacío para empresa/RL sin estado civil
                rl_oc     = _clean(ep.get("ocupacion"))      # vacío para empresa

                out[f"EMPRESA_{role_key}{suf}"]           = empresa_nombre
                out[f"NIT_{role_key}{suf}"]               = empresa_nit
                out[f"RL_{role_key}_NOMBRE{suf}"]         = rl_nombre
                out[f"RL_{role_key}_CEDULA{suf}"]         = rl_ced
                out[f"RL_{role_key}_LUG_EXP{suf}"]       = rl_loc
                out[f"RL_{role_key}_CIUDAD{suf}"]         = rl_ciudad
                # Alias compatible con templates que usan [[LUGAR_EXP_CEDULA_VENDEDOR_1]]
                out[f"LUGAR_EXP_CEDULA_{role_key}{suf}"]  = rl_loc

                # NOMBRE y CEDULA del rol = empresa (para encabezado carátula)
                nombre = empresa_nombre
                ced    = empresa_nit
                loc    = rl_loc
                ec     = rl_ec
                oc     = rl_oc
                ciudad = rl_ciudad

                if i == 0:
                    out[f"EMPRESA_{role_key}"]            = empresa_nombre
                    out[f"NIT_{role_key}"]                = empresa_nit
                    out[f"RL_{role_key}_NOMBRE"]          = rl_nombre
                    out[f"RL_{role_key}_CEDULA"]          = rl_ced
                    out[f"RL_{role_key}_LUG_EXP"]         = rl_loc
                    out[f"RL_{role_key}_CIUDAD"]          = rl_ciudad
                    out[f"LUGAR_EXP_CEDULA_{role_key}"]   = rl_loc
            else:
                # ── Persona natural ──
                nombre  = ep.get("nombre") or _p(f"NOMBRE_{role_key}", suf)
                raw_dig = re.sub(r"[^0-9]", "", ep.get("identificacion") or "")
                ced     = _format_cc(raw_dig) if raw_dig else _p(f"CEDULA_{role_key}", suf)
                loc     = ep.get("lugar_expedicion") or _p(f"LUGAR_EXP_CEDULA_{role_key}", suf)
                ec      = _clean(ep.get("estado_civil")) or _p(f"ESTADO_CIVIL_{role_key}", suf)
                oc      = _clean(ep.get("ocupacion"))    or _p(f"OCUPACION_{role_key}", suf)
                dom     = ep.get("direccion") or ""
                ciudad  = dom.split(",")[0].strip() if dom else _p(f"CIUDAD_DOMICILIO_{role_key}", suf)

            out[f"NOMBRE_{role_key}{suf}"]           = nombre
            out[f"CEDULA_{role_key}{suf}"]           = ced
            out[f"LUGAR_EXP_CEDULA_{role_key}{suf}"] = loc
            out[f"ESTADO_CIVIL_{role_key}{suf}"]     = ec
            out[f"OCUPACION_{role_key}{suf}"]        = oc
            out[f"CIUDAD_DOMICILIO_{role_key}{suf}"] = ciudad

            # Versión sin sufijo numérico para el primer compareciente
            if i == 0:
                out[f"NOMBRE_{role_key}"]           = nombre
                out[f"CEDULA_{role_key}"]           = ced
                out[f"LUGAR_EXP_CEDULA_{role_key}"] = loc
                out[f"ESTADO_CIVIL_{role_key}"]     = ec
                out[f"OCUPACION_{role_key}"]        = oc
                out[f"CIUDAD_DOMICILIO_{role_key}"] = ciudad

    vendedores   = roles.get("VENDEDORES", []) or roles.get("SOLICITANTES", [])
    compradores  = roles.get("COMPRADORES", [])
    solicitantes = roles.get("SOLICITANTES", [])
    deudores     = roles.get("DEUDORES", [])
    acreedores   = roles.get("ACREEDORES", [])

    _set_person_vars(vendedores,   "VENDEDOR")
    _set_person_vars(compradores,  "COMPRADOR")
    _set_person_vars(solicitantes, "SOLICITANTE")
    _set_person_vars(deudores,     "DEUDOR")
    _set_person_vars(acreedores,   "ACREEDOR")

    # Si hay menos de 2 personas en un rol, vaciar variables _2 → DataBinder omite el bloque
    _ROLES_WITH_2 = [
        ("VENDEDOR", vendedores), ("COMPRADOR", compradores),
        ("SOLICITANTE", solicitantes), ("DEUDOR", deudores), ("ACREEDOR", acreedores),
    ]
    _PERSON_FIELDS_2 = ["NOMBRE", "CEDULA", "LUGAR_EXP_CEDULA", "ESTADO_CIVIL", "OCUPACION", "CIUDAD_DOMICILIO"]
    for _role, _persons in _ROLES_WITH_2:
        if len(_persons) < 2 and f"NOMBRE_{_role}_2" not in out:
            for _field in _PERSON_FIELDS_2:
                out[f"{_field}_{_role}_2"] = ""

    # Propagar campos catastrales desde el contexto universal
    inmueble_ctx = contexto_universal.get("INMUEBLE") or {}
    out["CODIGO_CATASTRAL_ANTERIOR"] = inmueble_ctx.get("CODIGO_CATASTRAL_ANTERIOR") or "[[PENDIENTE: CODIGO_CATASTRAL_ANTERIOR]]"
    out["CEDULA_CATASTRAL"]          = inmueble_ctx.get("CEDULA_CATASTRAL") or "[[PENDIENTE: CEDULA_CATASTRAL]]"
    out["NUMERO_PREDIAL_NACIONAL"]   = inmueble_ctx.get("predial_nacional") or "[[PENDIENTE: NUMERO_PREDIAL_NACIONAL]]"

    return out