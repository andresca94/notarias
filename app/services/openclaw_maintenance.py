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


def _record_maintenance_failure(
    *,
    trigger: str,
    radicado: Optional[str],
    comments_count: Optional[int],
    iteration: Optional[int] = None,
    error: str,
) -> None:
    payload: Dict[str, Any] = {
        "timestamp": utc_now_iso(),
        "trigger": trigger,
        "radicado": radicado,
        "comments_count": comments_count,
        "ok": False,
        "error": error,
    }
    if iteration is not None:
        payload["iteration"] = iteration
    _append_jsonl(feedback_corpus_runs_path(), payload)


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
        *_build_workspace_guardrails(),
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


def _build_workspace_guardrails() -> list[str]:
    workspace = Path(settings.OPENCLAW_MAINTENANCE_WORKSPACE).resolve()
    live_checkout = Path(settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT).resolve()
    outputs_root = Path(settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT).resolve()
    guardrails = [
        f"Workspace de mantenimiento esperado: {workspace}.",
        f"Rama de trabajo esperada: {settings.OPENCLAW_MAINTENANCE_BRANCH}.",
        f"Ruta absoluta de salidas a consultar: {outputs_root}.",
        f"Lee el corpus desde {outputs_root / '_feedback_corpus' / 'feedback_events.jsonl'} cuando exista.",
        f"Lee los casos desde {outputs_root / 'CASE-*' / 'case_state.json'} y {outputs_root / 'CASE-*' / 'iterations' / '*' / 'feedback' / 'comments.json'} cuando existan.",
        "Antes de cambiar nada, ejecuta `git status --short` y si el worktree no esta limpio, aborta sin cambios.",
    ]
    if workspace != live_checkout:
        guardrails.append(
            f"No modifiques {live_checkout}; ese checkout queda reservado para pull y deploy."
        )
    return guardrails


def _build_auto_tune_prompt(*, iteration: int) -> str:
    instructions = [
        "Este disparo fue activado automaticamente despues de subir feedback experto en Word.",
        f"Iteracion objetivo: {iteration}.",
        "Busca patrones corregibles en prompts, reglas, parsers, validaciones o tests del backend.",
        "Si el cambio es seguro y verificable, aplicalo. Si no, deja un no-op claro.",
        "No toques el frontend ni nginx.",
        *_build_workspace_guardrails(),
        f"Trabaja solamente dentro de {Path(settings.OPENCLAW_MAINTENANCE_WORKSPACE).resolve()}.",
        "Si modificas archivos, ejecuta los checks mas pequenos y relevantes antes de continuar.",
        "Si el worktree termina con archivos inesperados o fuera de alcance, aborta sin commit ni push.",
        "Si hay cambios backend-only validos, haz git add y git commit con un mensaje corto y especifico.",
    ]

    if settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED:
        instructions.append(
            "Despues del commit, haz git push origin main. Si push falla, deja el commit local y explica el error."
        )
    else:
        instructions.append(
            "No hagas git push; deja el commit local y resume el cambio aplicado."
        )

    if settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED:
        instructions.extend(
            [
                "Despues del cambio verificado, redespliega solo el backend.",
                f"Usa exactamente este comando de deploy: {settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND}",
                "Si el deploy falla, no toques el frontend y resume el error exacto.",
            ]
        )

    return " ".join(instructions)


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


async def trigger_backend_maintenance_logged(
    *,
    radicado: Optional[str],
    prompt: Optional[str],
    trigger: str,
    comments_count: Optional[int] = None,
    iteration: Optional[int] = None,
) -> None:
    try:
        await trigger_backend_maintenance(
            radicado=radicado,
            prompt=prompt,
            trigger=trigger,
            comments_count=comments_count,
        )
    except Exception as exc:
        _record_maintenance_failure(
            trigger=trigger,
            radicado=radicado,
            comments_count=comments_count,
            iteration=iteration,
            error=str(exc),
        )


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

    prompt = _build_auto_tune_prompt(iteration=iteration)

    try:
        await trigger_backend_maintenance(
            radicado=radicado,
            prompt=prompt,
            trigger="feedback_upload_auto_tune",
            comments_count=comments_count,
        )
    except Exception as exc:
        _record_maintenance_failure(
            trigger="feedback_upload_auto_tune",
            radicado=radicado,
            comments_count=comments_count,
            iteration=iteration,
            error=str(exc),
        )
