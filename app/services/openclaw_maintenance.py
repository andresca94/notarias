from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import settings
from app.services.case_manager import (
    CaseStateError,
    build_case_response,
    case_dir,
    feedback_corpus_runs_path,
    load_case_state,
    utc_now_iso,
)
from app.services.openclaw_client import OpenClawClient


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_prompt_template() -> str:
    path = Path(settings.OPENCLAW_MAINTENANCE_PROMPT_FILE)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _build_context(
    *,
    radicado: Optional[str],
    prompt: Optional[str],
    trigger: str,
    comments_count: Optional[int] = None,
) -> str:
    context_lines = [
        "Trabaja SOLO sobre el backend de Notar-IA.",
        "Lee el feedback acumulado de Word y las regresiones del backend.",
        "Haz cambios pequenos, respaldados por tests o verificaciones locales.",
        "No modifiques el frontend.",
        "Si vas a proponer un deploy, verifica antes los checks aplicables.",
        f"Origen del disparo: {trigger}.",
    ]

    if comments_count is not None:
        context_lines.append(f"Comentarios detectados en este disparo: {comments_count}.")

    prompt_template = _load_prompt_template()
    if prompt_template:
        context_lines.append(f"Instrucciones permanentes:\n{prompt_template}")

    if radicado:
        try:
            state = load_case_state(radicado)
            context_lines.append(f"Radicado objetivo: {radicado}.")
            context_lines.append(f"Directorio del caso: {case_dir(radicado).resolve()}.")
            context_lines.append(
                "Resume primero los comentarios cargados y utilízalos para priorizar ajustes del backend."
            )
            context_lines.append(
                f"Estado del caso: {json.dumps(build_case_response(state), ensure_ascii=False)}"
            )
        except CaseStateError:
            context_lines.append(f"Radicado objetivo informado, pero sin estado local: {radicado}.")

    if prompt and prompt.strip():
        context_lines.append(prompt.strip())

    return "\n".join(context_lines)


async def trigger_backend_maintenance(
    *,
    radicado: Optional[str],
    prompt: Optional[str],
    trigger: str,
    comments_count: Optional[int] = None,
) -> Dict[str, Any]:
    message = _build_context(
        radicado=radicado,
        prompt=prompt,
        trigger=trigger,
        comments_count=comments_count,
    )
    client = OpenClawClient()
    response = await client.trigger_agent_task(
        name="Notar-IA backend maintenance",
        message=message,
        model=settings.OPENCLAW_MAINTENANCE_MODEL,
    )
    _append_jsonl(
        feedback_corpus_runs_path(),
        {
            "timestamp": utc_now_iso(),
            "trigger": trigger,
            "radicado": radicado,
            "comments_count": comments_count,
            "ok": True,
            "openclaw": response,
        },
    )
    return response


async def run_auto_tune_for_feedback(
    *,
    radicado: str,
    iteration: int,
    comments_count: int,
) -> None:
    if not settings.OPENCLAW_AUTO_TUNE_ENABLED:
        return
    if comments_count < int(settings.OPENCLAW_AUTO_TUNE_MIN_COMMENTS or 1):
        return

    prompt = (
        "Este disparo fue activado automaticamente despues de subir feedback experto en Word. "
        f"Iteracion objetivo: {iteration}. "
        "Busca patrones corregibles en prompts, reglas, parsers, validaciones o tests del backend. "
        "Si el cambio es seguro y verificable, aplicalo. Si no, deja un no-op claro."
    )

    try:
        await trigger_backend_maintenance(
            radicado=radicado,
            prompt=prompt,
            trigger="feedback_upload_auto_tune",
            comments_count=comments_count,
        )
    except Exception as exc:
        _append_jsonl(
            feedback_corpus_runs_path(),
            {
                "timestamp": utc_now_iso(),
                "trigger": "feedback_upload_auto_tune",
                "radicado": radicado,
                "iteration": iteration,
                "comments_count": comments_count,
                "ok": False,
                "error": str(exc),
            },
        )
