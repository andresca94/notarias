from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, UploadFile, HTTPException
from fastapi.responses import FileResponse

from app.pipeline.orchestrator import run_pipeline
from app.core.config import settings

router = APIRouter()


def save_uploads(files: List[UploadFile], out_dir: Path) -> List[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved = []
    for f in files:
        content = f.file.read()
        dst = out_dir / f.filename
        dst.write_bytes(content)
        saved.append(str(dst))
    return saved


@router.post("/notaria-v63-universal")
async def notaria_v63_universal(
    cedula: Optional[List[UploadFile]] = File(default=None),
    documentos: List[UploadFile] = File(...),
    comentario: str = Form("(Sin comentarios)"),
    template_id: Optional[str] = Form(None),
):
    cedula = cedula or []

    base_in = Path("inputs")
    scans_dir = base_in / "scanner"
    docs_dir = base_in / "documentos"

    scan_paths = save_uploads(cedula, scans_dir)
    doc_paths = save_uploads(documentos, docs_dir)

    result = await run_pipeline(
        scanner_paths=scan_paths,
        documentos_paths=doc_paths,
        comentario=comentario,
        template_id=template_id,
    )

    rad = result["radicado"]
    return {
        "ok": True,
        "radicado": rad,
        "docx_path": result["docx_path"],
        "pdf_path": result["pdf_path"],
        "download_url": f"/download/{rad}",
        # debug opcional:
        # "debug": result["debug"],
    }


@router.get("/download/{radicado}")
async def download_pdf(radicado: str):
    case_dir = Path(settings.OUTPUT_DIR) / f"CASE-{radicado}"
    pdf_path = case_dir / f"Escritura_Caso_{radicado}.pdf"

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF no encontrado para ese radicado.")

    return FileResponse(
        path=str(pdf_path),
        media_type="application/pdf",
        filename=pdf_path.name,
    )
