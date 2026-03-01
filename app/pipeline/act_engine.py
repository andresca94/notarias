# app/pipeline/act_engine.py
from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple


# Nombres normalizados (sin tildes) de departamentos colombianos para extracción de ciudad
_DEPT_NAMES: frozenset = frozenset({
    "SANTANDER", "CUNDINAMARCA", "ANTIOQUIA", "ATLANTICO",
    "BOLIVAR", "VALLE", "TOLIMA", "NARINO", "CAUCA",
    "BOYACA", "CALDAS", "RISARALDA", "QUINDIO",
    "HUILA", "CORDOBA", "CESAR", "MAGDALENA", "SUCRE",
    "LA GUAJIRA", "CHOCO", "ARAUCA", "CASANARE", "VICHADA",
    "META", "GUAINIA", "VAUPES", "AMAZONAS", "PUTUMAYO",
    "CAQUETA", "GUAVIARE", "NORTE DE SANTANDER",
})
_STREET_KEYWORDS: frozenset = frozenset({
    "TRV", "CR ", "CLL", "KM ", " N. ", "AVE", "AV ", "BRR", "VIA A",
    "CALLE", "CARRERA", "TRANSVERSAL", "DIAGONAL", "CIRCULAR",
})
_LOCALITY_KEYWORDS: frozenset = frozenset({
    "VEREDA ", "CORREGIMIENTO", "SECTOR ", "BARRIO ", "FINCA ", "VIA A",
})


def _normalize_accents(s: str) -> str:
    """Elimina tildes para comparación con _DEPT_NAMES."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _extract_city_from_address(address: str) -> str:
    """Extrae el municipio de una dirección colombiana.

    Maneja dos formatos habituales:
    - "Ciudad [dep], Calle X"          → toma el primer segmento
    - "Calle X, Barrio, Ciudad, Dpto"  → toma el segmento antes del dpto
    - "SECTOR, VEREDA – CIUDAD - DPTO" → extrae ciudad del segmento con guiones
    """
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",")]
    if not parts:
        return ""

    def _is_dept(seg: str) -> bool:
        clean = re.sub(r"\s*\(.*?\)", "", seg).strip().upper()
        norm = _normalize_accents(clean)
        return norm in _DEPT_NAMES or any(d in norm for d in _DEPT_NAMES)

    def _is_street_or_locality(seg: str) -> bool:
        u = seg.upper()
        return (any(c.isdigit() for c in u)
                or any(kw in u for kw in _STREET_KEYWORDS)
                or any(kw in u for kw in _LOCALITY_KEYWORDS))

    # Buscar departamento en los segmentos (de atrás hacia adelante)
    for i in range(len(parts) - 1, 0, -1):
        if not _is_dept(parts[i]):
            continue

        # El dpto encontrado puede tener ciudad interna separada por guiones
        # ej. "VEREDA RIO FRIO – FLORIDABLANCA - SANTANDER"
        dash_parts = re.split(r"\s+[–\-]\s+", parts[i])
        if len(dash_parts) >= 2:
            for dp in reversed(dash_parts):
                dp_clean = re.sub(r"\s*\(.*?\)", "", dp).strip()
                if _is_dept(dp_clean):
                    continue
                if _is_street_or_locality(dp_clean):
                    continue
                if dp_clean:
                    return dp_clean

        # Ciudad = segmento anterior al dpto
        city_candidate = parts[i - 1].strip()
        if _is_street_or_locality(city_candidate) and i - 2 >= 0:
            city_candidate = parts[i - 2].strip()
        return re.sub(r"\s*\(.*?\)", "", city_candidate).strip() or city_candidate

    # Sin dpto reconocido → primer segmento (formato "Ciudad, Calle")
    first = parts[0].strip()
    return re.sub(r"\s*\(.*?\)", "", first).strip() or first


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
                # Fuzzy match: mismo nombre con variaciones de apellidos (ej: "GARNICA" vs "GARNICA ORDOÑEZ")
                # Requiere >= 3 palabras significativas en común para evitar falsos positivos
                _STOP = {"DE", "LA", "EL", "LOS", "LAS", "Y", "E", "DEL"}
                def _sig(name: str) -> set:
                    return {w for w in name.split() if len(w) >= 4 and w not in _STOP}
                sig_p = _sig(pname)
                fuzzy_match = None
                if len(sig_p) >= 3:
                    for ename, ep_dict in by_name.items():
                        if len(_sig(ename) & sig_p) >= 3:
                            fuzzy_match = ep_dict
                            break
                if fuzzy_match is not None:
                    merge(fuzzy_match, p)
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
    a = re.sub(r"\s+", " ", (acto_nombre or "").upper()).strip()

    if "COMPRAVENTA" in a:
        if "DERECHOS" in a and ("CUOTA" in a or "PARTE" in a):
            return "COMPRAVENTA_CUOTA"
        return "COMPRAVENTA"
    if "HIPOTECA" in a:
        if "CANCELAC" in a:
            return "CANCELACION_HIPOTECA"
        return "HIPOTECA"
    if "CANCELAC" in a or "CANCELACIÓN" in a:
        if "PACTO" in a and "RETRO" in a:
            return "CANCELACION"
        if "USUFRUCTO" in a:
            return "CANCELACION_USUFRUCTO"
        if "FIDEICOMISO" in a:
            return "CANCELACION_FIDEICOMISO"
        if "ARRENDAMIENTO" in a:
            return "CANCELACION_ARRENDAMIENTO"
        if "PATRIMONIO" in a:
            return "CANCELACION_PATRIMONIO"
        if "HORIZONTAL" in a or "REGLAMENTO" in a:
            return "CANCELACION_PH"
        if "CONDICION" in a or "CONDICIÓN" in a:
            return "CANCELACION_CONDICION"
        return "CANCELACION_GENERICA"
    if "CAMBIO" in a and "NOMBRE" in a:
        return "CAMBIO_NOMBRE"
    if ("CAMBIO" in a or "MODIFICACION" in a) and ("RAZON" in a or "RAZÓN" in a):
        return "CAMBIO_RAZON_SOCIAL"
    if "DACION" in a or "DACIÓN" in a:
        return "DACION_PAGO"
    if "DONACION" in a or "DONACIÓN" in a or "INSINUACION" in a:
        return "DONACION"
    if "AFECTAC" in a and "VIVIENDA" in a:
        return "AFECTACION_VF"
    if "ACTUALIZACION" in a or "ACTUALIZACIÓN" in a or "NOMENCLATURA" in a:
        return "ACTUALIZACION_NOMENCLATURA"
    if "ACLARACION" in a or "ACLARACIÓN" in a:
        return "ACLARACION"
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

    # señales de rol por nombre normalizado
    # Nota: \bDE\b y \bA\b se eliminaron por false-positives (ej: "REPRESENTANTE LEGAL DE EMPRESA")
    # La normalización DE→VENDEDOR/OTORGANTE y A→COMPRADOR/BENEFICIARIO se hace en orchestrator.py
    vendedores = [p for p in personas_globales if has_role(p, "VENDEDOR") or has_role(p, "OTORGANTE")]
    compradores = [p for p in personas_globales if has_role(p, "COMPRADOR") or has_role(p, "BENEFICIARIO")]

    # fallback: si no hay nada, todos son intervinientes
    intervinientes = list(personas_globales or [])

    if kind == "COMPRAVENTA":
        # Multi-acto safety: cuando rol_detectado global contamina roles de COMPRAVENTA,
        # se corrige usando tipo CC/NIT. Si todos los vendedores son NIT (empresas) pero
        # hay personas CC en compradores, los roles están invertidos por contaminación
        # de otro acto (ej: CANCELACION donde la empresa es "otorgante" global).
        def _is_nit(p: Dict[str, Any]) -> bool:
            id_up = (p.get("identificacion") or "").upper()
            return "NIT" in id_up or id_up.startswith("NI ")

        if vendedores and all(_is_nit(p) for p in vendedores):
            cc_in_compradores = [p for p in compradores if not _is_nit(p)]
            if cc_in_compradores:
                # Roles invertidos: personas CC en compradores pasan a vendedores.
                # Empresas NIT en compradores permanecen como compradores.
                # Empresas NIT que estaban en vendedores (incorrectamente) se descartan
                # (no son parte de este acto de compraventa).
                nit_in_compradores = [p for p in compradores if _is_nit(p)]
                vendedores = cc_in_compradores
                compradores = nit_in_compradores

        return {
            "VENDEDORES": vendedores or [],
            "COMPRADORES": compradores or [],
            "INTERVINIENTES": intervinientes,
        }

    if kind == "HIPOTECA":
        # build_act_context asigna: otorgantes (hipotecantes) → VENDEDOR/OTORGANTE → vendedores
        #                           beneficiarios (acreedores) → COMPRADOR/BENEFICIARIO → compradores
        # Cuando hay extracción per-acto: vendedores = hipotecantes/deudores, compradores = acreedores
        #
        # Detección de contaminación global: si compradores incluye personas CC (naturales),
        # significa que los roles globales contaminaron (ej: Sandra aparece como COMPRADOR
        # global por ser beneficiaria del CANCELACION). En ese caso usar lógica NIT:
        # - NIT en compradores = deudores (empresa que compró el inmueble y da garantía)
        # - NIT en vendedores = acreedores (empresa que financia / recibe hipoteca)
        def _is_nit_h(p: Dict[str, Any]) -> bool:
            id_up = (p.get("identificacion") or "").upper()
            return "NIT" in id_up or id_up.startswith("NI ")

        _has_cc_in_compradores = any(not _is_nit_h(p) for p in compradores)

        if _has_cc_in_compradores:
            # Contaminación global: extraer solo NIT y asignar por lógica económica
            # comprador NIT = deudor/hipotecante; vendedor NIT = acreedor/financiador
            nit_compradores = [p for p in compradores if _is_nit_h(p)]
            nit_vendedores = [p for p in vendedores if _is_nit_h(p)]
            deudores = nit_compradores or []
            deudor_ids = {_normalize_id(d.get("identificacion") or "") for d in deudores if d.get("identificacion")}
            acreedores = [p for p in nit_vendedores if _normalize_id(p.get("identificacion") or "") not in deudor_ids]
        elif vendedores:
            # Per-acto extraction: vendedores = hipotecantes/deudores, compradores = acreedores
            deudores = vendedores
            deudor_ids = {_normalize_id(d.get("identificacion") or "") for d in deudores if d.get("identificacion")}
            acreedores = [p for p in compradores if _normalize_id(p.get("identificacion") or "") not in deudor_ids]
        else:
            # Sin extracción per-acto y sin CC contamination: compradores → deudores
            deudores = compradores or []
            deudor_ids = {_normalize_id(d.get("identificacion") or "") for d in deudores if d.get("identificacion")}
            acreedores = []

        # Fallback keyword match si acreedores sigue vacío
        if not acreedores:
            for p in intervinientes:
                pid = _normalize_id(p.get("identificacion") or "")
                if pid and pid in deudor_ids:
                    continue
                n = (p.get("nombre") or "").upper()
                if any(x in n for x in ["BANCO", "FONDO", "SAS", "S.A.S", "S.A.", "LTDA", "FINANCI", "SOLUCIONES", "INVERSIONES", "INMOBILI"]):
                    acreedores.append(p)

        # Último fallback: vendedores como acreedores (solo cuando deudores viene de compradores)
        if not acreedores and not vendedores:
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

    if kind == "COMPRAVENTA_CUOTA":
        return {
            "VENDEDORES": vendedores or personas_globales[:1],
            "COMPRADORES": compradores or personas_globales[1:2],
            "INTERVINIENTES": intervinientes,
        }

    if kind in (
        "CANCELACION_HIPOTECA", "CANCELACION_GENERICA", "CANCELACION_USUFRUCTO",
        "CANCELACION_FIDEICOMISO", "CANCELACION_ARRENDAMIENTO",
        "CANCELACION_PATRIMONIO", "CANCELACION_PH", "CANCELACION_CONDICION",
    ):
        return {
            "SOLICITANTES": (vendedores or compradores or intervinientes)[:2],
            "INTERVINIENTES": intervinientes,
        }

    if kind == "DACION_PAGO":
        return {
            "VENDEDORES": vendedores or personas_globales[:1],
            "ACREEDORES": compradores or personas_globales[1:2],
            "INTERVINIENTES": intervinientes,
        }

    if kind == "DONACION":
        return {
            "VENDEDORES": vendedores or personas_globales[:1],
            "COMPRADORES": compradores or personas_globales[1:2],
            "INTERVINIENTES": intervinientes,
        }

    if kind in ("AFECTACION_VF", "ACTUALIZACION_NOMENCLATURA", "ACLARACION", "CAMBIO_RAZON_SOCIAL"):
        return {
            "SOLICITANTES": (vendedores or compradores or intervinientes)[:2],
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

    # Filtrar personas al conjunto de ESTE acto si el prompt extrajo otorgantes/beneficiarios por acto
    # y marcar rol_detectado per-acto (VENDEDOR/OTORGANTE vs COMPRADOR/BENEFICIARIO) en copias.
    if isinstance(acto_obj, dict):
        _otor_raw = [n for n in (acto_obj.get("otorgantes") or []) if n]
        _benef_raw = [n for n in (acto_obj.get("beneficiarios") or []) if n]
        _act_parts = _otor_raw + _benef_raw
        if _act_parts:
            _otor_norms = {_normalize_name(n) for n in _otor_raw}
            _benef_norms = {_normalize_name(n) for n in _benef_raw}
            _all_norms = _otor_norms | _benef_norms
            _sw_act = {"DE", "LA", "EL", "LOS", "LAS", "Y", "E", "S", "A", "SAS", "LTDA", "SA"}

            def _name_words(s: str) -> set:
                return {w for w in s.split() if w not in _sw_act and len(w) > 2}

            def _name_in_set(pname: str, nset: set) -> bool:
                if pname in nset:
                    return True
                pw = _name_words(pname)
                for an in nset:
                    if pw and len(pw & _name_words(an)) >= 2:
                        return True
                return False

            def _acto_role_for(pname: str) -> Optional[str]:
                in_o = _name_in_set(pname, _otor_norms)
                in_b = _name_in_set(pname, _benef_norms)
                if in_o and not in_b:
                    return "VENDEDOR/OTORGANTE"
                if in_b and not in_o:
                    return "COMPRADOR/BENEFICIARIO"
                return None

            _filtered_raw = [p for p in personas if _name_in_set(
                _normalize_name(p.get("nombre") or ""), _all_norms
            ) or not _normalize_name(p.get("nombre") or "")]

            if _filtered_raw:
                # Crear copias con rol_detectado correcto para ESTE acto
                _filtered = []
                for p in _filtered_raw:
                    pname = _normalize_name(p.get("nombre") or "")
                    acto_role = _acto_role_for(pname)
                    if acto_role:
                        p_copy = dict(p)
                        p_copy["rol_detectado"] = acto_role
                        _filtered.append(p_copy)
                    else:
                        _filtered.append(p)
                personas = _filtered

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
                _raw_nit_str   = raw_p.get("identificacion") or ""
                _nit_vd_m      = re.search(r"(\d[\d.]*)-(\d)\s*$", _raw_nit_str.strip())
                empresa_raw    = re.sub(r"[^0-9]", "", _nit_vd_m.group(1) if _nit_vd_m else _raw_nit_str)
                _nit_verif     = _nit_vd_m.group(2) if _nit_vd_m else ""
                empresa_nit    = ((_format_cc(empresa_raw) + "-" + _nit_verif)
                                  if empresa_raw and _nit_verif
                                  else (_format_cc(empresa_raw) if empresa_raw
                                        else _p(f"NIT_{role_key}", suf)))

                rl_nombre = ep.get("nombre") or _p(f"RL_{role_key}_NOMBRE", suf)
                rl_raw    = re.sub(r"[^0-9]", "", ep.get("identificacion") or "")
                rl_ced    = _format_cc(rl_raw) if rl_raw else _p(f"RL_{role_key}_CEDULA", suf)
                rl_loc    = ep.get("lugar_expedicion") or _p(f"RL_{role_key}_LUG_EXP", suf)
                rl_dom    = ep.get("direccion") or ""
                rl_ciudad = _extract_city_from_address(rl_dom) if rl_dom else _p(f"RL_{role_key}_CIUDAD", suf)
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
                ciudad  = _extract_city_from_address(dom) if dom else _p(f"CIUDAD_DOMICILIO_{role_key}", suf)

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