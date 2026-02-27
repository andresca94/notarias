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
    personas = dedupe_personas((contexto_universal.get("PERSONAS") or []))
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

    return out