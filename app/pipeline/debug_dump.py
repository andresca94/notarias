# app/pipeline/debug_dump.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    p = Path(path)
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8", errors="ignore")


def _safe_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _sanitize_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    out = []
    for ch in s:
        if ch.isalnum() or ch in ["_", "-", ".", "ñ", "Ñ"]:
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:120] or "item"


@dataclass
class DebugDumper:
    case_dir: Path
    enabled: bool = True

    @property
    def debug_dir(self) -> Path:
        return self.case_dir / "debug"

    def write_manifest(self, scanner_paths: List[str], documentos_paths: List[str]) -> None:
        if not self.enabled:
            return
        manifest = {
            "cwd": str(Path.cwd()),
            "scanner_files": [
                {
                    "name": Path(p).name,
                    "path": p,
                    "size_bytes": Path(p).stat().st_size if Path(p).exists() else None,
                    "sha256": _sha256_file(p) if Path(p).exists() else None,
                }
                for p in (scanner_paths or [])
            ],
            "documentos_files": [
                {
                    "name": Path(p).name,
                    "path": p,
                    "size_bytes": Path(p).stat().st_size if Path(p).exists() else None,
                    "sha256": _sha256_file(p) if Path(p).exists() else None,
                }
                for p in (documentos_paths or [])
            ],
        }
        _safe_write_json(self.debug_dir / "00_inputs_manifest.json", manifest)

    def dump_stage_json(self, filename: str, data: Any) -> None:
        if not self.enabled:
            return
        _safe_write_json(self.debug_dir / filename, data)

    def dump_stage_text(self, filename: str, text: str) -> None:
        if not self.enabled:
            return
        _safe_write_text(self.debug_dir / filename, text)

    def dump_gemini_output(self, stage_dir: str, src_path: str, raw_text: str, parsed_json: Any) -> None:
        """
        stage_dir ejemplos:
          - "01_radicacion"
          - "02_soportes"
          - "03_cedulas"
        """
        if not self.enabled:
            return
        name = Path(src_path).name
        safe = _sanitize_filename(name)

        _safe_write_text(self.debug_dir / stage_dir / f"{safe}.raw.txt", raw_text or "")
        _safe_write_json(self.debug_dir / stage_dir / f"{safe}.json", parsed_json or {})

    def dump_rag_hit(self, acto_nombre: str, raw_text: str, meta_body: Dict[str, str]) -> None:
        if not self.enabled:
            return
        safe = _sanitize_filename(acto_nombre)
        _safe_write_text(self.debug_dir / "05_rag_raw" / f"{safe}.txt", raw_text or "")
        _safe_write_json(self.debug_dir / "05_rag_splits" / f"{safe}.json", meta_body or {})

    def dump_misiones(self, misiones: List[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        _safe_write_json(self.debug_dir / "06_misiones.json", misiones)

    def dump_gemini_output(self, stage_prefix: str, file_path: str, raw_text: str, parsed_json: Any) -> None:
        """
        Guarda por archivo:
        - raw del modelo (txt)
        - json parseado (json)
        Ej: stage_prefix="02_soportes" => debug/02_soportes/<nombre_archivo>_raw.txt y _json.json
        """
        if not self.enabled:
            return

        fname = _sanitize_filename(Path(file_path).name)
        out_dir = self.debug_dir / stage_prefix
        out_dir.mkdir(parents=True, exist_ok=True)

        _safe_write_text(out_dir / f"{fname}__raw.txt", raw_text or "")
        _safe_write_json(out_dir / f"{fname}__json.json", parsed_json)

    def clear_binder_outputs(self) -> None:
        """Elimina todos los archivos del directorio 07_binder_outputs para evitar
        contaminación de runs anteriores con secciones de órden diferente."""
        if not self.enabled:
            return
        import shutil
        _bo_dir = self.debug_dir / "07_binder_outputs"
        if _bo_dir.exists():
            shutil.rmtree(_bo_dir)
        _bo_dir.mkdir(parents=True, exist_ok=True)

    def dump_binder_output(self, orden: int, descripcion: str, text: str) -> None:
        if not self.enabled:
            return
        safe = _sanitize_filename(descripcion)
        _safe_write_text(self.debug_dir / "07_binder_outputs" / f"{orden:03d}_{safe}.txt", text or "")

    def build_ep_checklist_md(self) -> str:
        return """# EP Debug Checklist (comparación 1:1)

## 0) Carátula / Resumen (EP real: página 1)
- RADICACIÓN
- ESCRITURA PÚBLICA NÚMERO
- FECHA
- NOTARÍA / CÍRCULO / CIUDAD
- NATURALEZA JURÍDICA
- LISTA DE ACTOS (ACTO 1..4) + partes por acto + cuantías
- Identificación del inmueble (ubicación, matrícula, etc.)

## 1) Actos (EP real: páginas siguientes)
Por cada acto:
- Título “PRIMER/SEGUNDO/TERCER/CUARTO ACTO”
- Comparecencia específica del acto (quién comparece y en qué calidad)
- Texto legal por numerales (PRIMERO/SEGUNDO/…)
- Valores y forma de pago (si aplica)
- Representación legal (si aplica)
- NIT / CC correctos

## 2) Otorgamiento y autorización + Insertos
- Bloque “OTORGAMIENTO Y AUTORIZACIÓN”
- INSERTOS (tradición, cédulas, paz y salvo, etc.)

## 3) UIAF / Biometría + Firmas
- Sección UIAF (otorgante 1, otorgante 2, etc.)
- Firmas con CC y huella

---

## Señales de alerta típicas
- Un acto salió con plantilla equivocada (RAG hit incorrecto)
- Los roles por acto quedaron mal (p.ej. hipoteca: deudor/acreedor)
- Campos críticos quedaron [[PENDIENTE: ...]]
"""

    def write_checklist(self) -> None:
        if not self.enabled:
            return
        _safe_write_text(self.debug_dir / "99_ep_checklist.md", self.build_ep_checklist_md())