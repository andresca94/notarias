from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.core.config import settings
from app.pipeline.orchestrator import run_pipeline
from app.services.case_manager import (
    DOCX_MIME_TYPE,
    PDF_MIME_TYPE,
    CaseLockError,
    CaseStateError,
    append_feedback_corpus_event,
    artifact_path_for_response,
    build_case_response,
    case_dir,
    case_lock,
    compose_iteration_commentary,
    create_staging_dir,
    finalize_generation,
    list_case_inputs,
    load_case_state,
    mark_iteration_in_progress,
    record_feedback,
    save_case_state,
    save_upload_file,
    save_uploads,
    utc_now_iso,
)
from app.services.docx_feedback import parse_docx_comments
from app.services.openclaw_maintenance import run_auto_tune_for_feedback, trigger_backend_maintenance

router = APIRouter()


class OpenClawMaintenanceRequest(BaseModel):
    radicado: Optional[str] = None
    prompt: Optional[str] = None


def _http_404(detail: str) -> HTTPException:
    return HTTPException(status_code=404, detail=detail)


def _load_state_or_404(radicado: str) -> dict:
    try:
        return load_case_state(radicado)
    except CaseStateError as exc:
        raise _http_404(str(exc)) from exc


def _ensure_file_exists(path: Path, detail: str) -> Path:
    if not path.exists():
        raise _http_404(detail)
    return path


def _require_admin_token(x_admin_token: Optional[str]) -> None:
    expected = settings.INTERNAL_ADMIN_TOKEN
    if not expected:
        raise HTTPException(status_code=503, detail="INTERNAL_ADMIN_TOKEN no configurado.")
    if x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Token administrativo inválido.")


@router.post("/notaria-v63-universal")
async def notaria_v63_universal(
    cedula: Optional[List[UploadFile]] = File(default=None),
    documentos: List[UploadFile] = File(...),
    comentario: str = Form("(Sin comentarios)"),
    template_id: Optional[str] = Form(None),
):
    cedula = cedula or []
    staging_dir = create_staging_dir()
    scans_dir = staging_dir / "scanner"
    docs_dir = staging_dir / "documentos"

    try:
        scan_paths = await save_uploads(cedula, scans_dir)
        doc_paths = await save_uploads(documentos, docs_dir)

        result = await run_pipeline(
            scanner_paths=[str(path) for path in scan_paths],
            documentos_paths=[str(path) for path in doc_paths],
            comentario=comentario,
            template_id=template_id,
        )

        radicado = str(result["radicado"])
        state = finalize_generation(
            radicado=radicado,
            iteration=1,
            result=result,
            comentario=comentario,
            template_id=template_id,
            staged_scans=scan_paths,
            staged_docs=doc_paths,
        )
        return build_case_response(state, iteration=1)
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)


@router.get("/cases/{radicado}")
async def get_case(radicado: str):
    state = _load_state_or_404(radicado)
    return build_case_response(state)


@router.post("/cases/{radicado}/feedback")
async def upload_feedback(
    radicado: str,
    background_tasks: BackgroundTasks,
    feedback_docx: UploadFile = File(...),
):
    if not (feedback_docx.filename or "").lower().endswith(".docx"):
        raise HTTPException(status_code=400, detail="El feedback debe ser un archivo .docx.")

    state = _load_state_or_404(radicado)
    current_iteration = int(state.get("latest_iteration") or 0)
    if current_iteration < 1:
        raise HTTPException(status_code=400, detail="El caso no tiene iteraciones generadas.")
    if state.get("status") == "iteration_in_progress":
        raise HTTPException(status_code=409, detail="Ya hay una iteración en curso.")

    feedback_dir = case_dir(radicado) / "iterations" / str(current_iteration) / "feedback"
    if feedback_dir.exists():
        shutil.rmtree(feedback_dir)
    feedback_dir.mkdir(parents=True, exist_ok=True)

    reviewed_docx_path = await save_upload_file(feedback_docx, feedback_dir)
    try:
        comments = parse_docx_comments(reviewed_docx_path)
    except ValueError as exc:
        if reviewed_docx_path.exists():
            reviewed_docx_path.unlink()
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    comments_path = feedback_dir / "comments.json"
    comments_path.write_text(json.dumps(comments, ensure_ascii=False, indent=2), encoding="utf-8")

    state = record_feedback(
        radicado=radicado,
        iteration=current_iteration,
        reviewed_docx_path=reviewed_docx_path,
        comments_path=comments_path,
        comments=comments,
    )
    append_feedback_corpus_event(
        radicado=radicado,
        iteration=current_iteration,
        comments=comments,
    )
    background_tasks.add_task(
        run_auto_tune_for_feedback,
        radicado=radicado,
        iteration=current_iteration,
        comments_count=len(comments),
    )
    return build_case_response(state, iteration=current_iteration)


@router.post("/cases/{radicado}/iterations/next")
async def create_next_iteration(
    radicado: str,
    comentario: Optional[str] = None,
    template_id: Optional[str] = None,
):
    state = _load_state_or_404(radicado)
    current_iteration = int(state.get("latest_iteration") or 0)
    if current_iteration < 1:
        raise HTTPException(status_code=400, detail="No existe una iteración base para continuar.")

    current_entry = (state.get("iterations") or {}).get(str(current_iteration)) or {}
    if not current_entry.get("feedback"):
        raise HTTPException(
            status_code=400,
            detail="Primero debes subir el DOCX revisado con comentarios de Word.",
        )

    scan_paths, doc_paths = list_case_inputs(radicado)
    if not doc_paths:
        raise HTTPException(status_code=400, detail="No existen documentos base guardados para este caso.")

    try:
        with case_lock(radicado):
            mark_iteration_in_progress(radicado)
            fresh_state = load_case_state(radicado)
            composed_commentary = compose_iteration_commentary(fresh_state, comentario)
            next_iteration = current_iteration + 1

            try:
                result = await run_pipeline(
                    scanner_paths=scan_paths,
                    documentos_paths=doc_paths,
                    comentario=composed_commentary,
                    template_id=template_id or fresh_state.get("template_id"),
                )
                if str(result["radicado"]) != radicado:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "La radicación detectada al regenerar no coincide con el caso actual. "
                            f"Esperado {radicado}, recibido {result['radicado']}."
                        ),
                    )
                final_state = finalize_generation(
                    radicado=radicado,
                    iteration=next_iteration,
                    result=result,
                    comentario=composed_commentary,
                    template_id=template_id or fresh_state.get("template_id"),
                )
                return build_case_response(final_state, iteration=next_iteration)
            except Exception:
                rollback_state = load_case_state(radicado)
                rollback_state["status"] = "feedback_uploaded"
                rollback_state["updated_at"] = utc_now_iso()
                save_case_state(rollback_state)
                raise
    except CaseLockError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/cases/{radicado}/artifacts/docx")
async def download_docx(radicado: str, iteration: Optional[int] = Query(default=None)):
    try:
        docx_path = artifact_path_for_response(radicado, "docx", iteration)
    except CaseStateError as exc:
        raise _http_404(str(exc)) from exc
    _ensure_file_exists(docx_path, "DOCX no encontrado para ese radicado.")
    return FileResponse(
        path=str(docx_path),
        media_type=DOCX_MIME_TYPE,
        filename=docx_path.name,
    )


@router.get("/cases/{radicado}/artifacts/pdf")
async def download_pdf(radicado: str, iteration: Optional[int] = Query(default=None)):
    try:
        pdf_path = artifact_path_for_response(radicado, "pdf", iteration)
    except CaseStateError as exc:
        raise _http_404(str(exc)) from exc
    _ensure_file_exists(pdf_path, "PDF no encontrado para ese radicado.")
    return FileResponse(
        path=str(pdf_path),
        media_type=PDF_MIME_TYPE,
        filename=pdf_path.name,
    )


@router.get("/download/{radicado}")
async def download_pdf_legacy(radicado: str):
    return await download_pdf(radicado)


@router.post("/admin/openclaw/backend-maintenance")
async def trigger_openclaw_backend_maintenance(
    payload: OpenClawMaintenanceRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    _require_admin_token(x_admin_token)
    response = await trigger_backend_maintenance(
        radicado=payload.radicado,
        prompt=payload.prompt,
        trigger="admin_endpoint",
    )
    return {"ok": True, "openclaw": response}
