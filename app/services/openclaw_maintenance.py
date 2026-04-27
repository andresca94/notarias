from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import settings
from app.services.case_manager import (
    CaseStateError,
    backend_maintenance_state_path,
    build_case_response,
    case_dir,
    feedback_corpus_runs_path,
    load_case_state,
    set_backend_maintenance_state,
    set_iteration_maintenance_status,
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


def record_maintenance_status_update(
    *,
    trigger: str,
    radicado: str,
    iteration: int,
    status: str,
    message: Optional[str] = None,
    run_id: Optional[str] = None,
) -> None:
    payload: Dict[str, Any] = {
        "timestamp": utc_now_iso(),
        "trigger": trigger,
        "radicado": radicado,
        "iteration": iteration,
        "status": status,
        "ok": status not in {"failed"},
    }
    if message:
        payload["message"] = message
    if run_id:
        payload["run_id"] = run_id
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


def _build_maintenance_callback_instructions(*, radicado: str, iteration: int) -> list[str]:
    callback_url = "http://127.0.0.1:8080/admin/openclaw/backend-maintenance/status"
    token_source = (
        "ADMIN_TOKEN=\"$(grep '^INTERNAL_ADMIN_TOKEN=' /srv/notar-ia/backend/shared/backend.env | cut -d= -f2-)\""
    )
    callback_prefix = (
        f"{token_source} && curl -fsS -X POST {callback_url} "
        "-H \"x-admin-token: ${ADMIN_TOKEN}\" "
        "-H \"Content-Type: application/json\" "
    )

    def _payload(status: str, message: str) -> str:
        escaped_message = json.dumps(message, ensure_ascii=False)
        return (
            f"'{json.dumps({'radicado': radicado, 'iteration': iteration, 'status': status}, ensure_ascii=False)[:-1]}, "
            f"\"message\": {escaped_message}}}'"
        )

    return [
        "Nunca marques la actualización como finalizada antes de tener una verificación real del resultado.",
        (
            "Cuando completes un deploy exitoso y `curl -fsS http://127.0.0.1:8080/docs >/dev/null` responda bien, "
            f"reporta completion con este comando exacto: {callback_prefix}-d {_payload('completed', 'Backend actualizado y verificado para la siguiente iteración.')}."
        ),
        (
            "Si concluyes que no corresponde aplicar un cambio global seguro o decides un no-op explícito, "
            f"reporta skip con este comando exacto: {callback_prefix}-d {_payload('skipped', 'No se aplicó un cambio global seguro; la siguiente iteración solo validará el feedback del caso.')}."
        ),
        (
            "Si fallan checks, push o deploy y el backend sigue accesible por loopback, "
            f"reporta failure con este comando exacto: {callback_prefix}-d {_payload('failed', 'La actualización automática del backend falló; revisa logs antes de reintentar.')}."
        ),
        (
            "Si el backend no queda accesible para reportar failure, no inventes completion; "
            f"el sistema dejará registro pendiente y habrá que revisar {backend_maintenance_state_path().resolve()} y los logs."
        ),
    ]


def _build_auto_tune_prompt(*, radicado: str, iteration: int) -> str:
    instructions = [
        "Este disparo fue activado automaticamente despues de subir feedback experto en Word.",
        f"Radicado objetivo: {radicado}.",
        f"Iteracion objetivo: {iteration}.",
        "Busca patrones corregibles en prompts, reglas, parsers, validaciones o tests del backend.",
        "Si el cambio es seguro y verificable, aplicalo. Si no, deja un no-op claro.",
        "No toques el frontend ni nginx.",
        *_build_workspace_guardrails(),
        *_build_maintenance_callback_instructions(radicado=radicado, iteration=iteration),
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
        set_iteration_maintenance_status(
            radicado,
            iteration,
            status="skipped",
            message="Auto-tune deshabilitado en este entorno.",
        )
        set_backend_maintenance_state(
            status="skipped",
            radicado=radicado,
            iteration=iteration,
            message="Auto-tune deshabilitado en este entorno.",
        )
        return
    if comments_count < int(settings.OPENCLAW_AUTO_TUNE_MIN_COMMENTS or 1):
        set_iteration_maintenance_status(
            radicado,
            iteration,
            status="skipped",
            message="No se alcanzó el mínimo de comentarios para auto-tune.",
        )
        set_backend_maintenance_state(
            status="skipped",
            radicado=radicado,
            iteration=iteration,
            message="No se alcanzó el mínimo de comentarios para auto-tune.",
        )
        return

    prompt = _build_auto_tune_prompt(radicado=radicado, iteration=iteration)
    set_iteration_maintenance_status(
        radicado,
        iteration,
        status="running",
        message="Codex está actualizando el backend con este feedback.",
    )
    set_backend_maintenance_state(
        status="running",
        radicado=radicado,
        iteration=iteration,
        message="Codex está actualizando el backend con este feedback.",
    )

    try:
        response = await trigger_backend_maintenance(
            radicado=radicado,
            prompt=prompt,
            trigger="feedback_upload_auto_tune",
            comments_count=comments_count,
        )
        run_id = ((response or {}).get("runId") if isinstance(response, dict) else None)
        set_iteration_maintenance_status(
            radicado,
            iteration,
            status="running",
            message=(
                "La tarea de Codex fue aceptada y ahora espera una confirmación final "
                "después del deploy del backend."
            ),
            run_id=run_id,
        )
        set_backend_maintenance_state(
            status="running",
            radicado=radicado,
            iteration=iteration,
            message=(
                "La tarea de Codex fue aceptada y ahora espera una confirmación final "
                "después del deploy del backend."
            ),
            run_id=run_id,
        )
    except Exception as exc:
        set_iteration_maintenance_status(
            radicado,
            iteration,
            status="failed",
            message=f"Actualización automática fallida: {exc}",
        )
        set_backend_maintenance_state(
            status="failed",
            radicado=radicado,
            iteration=iteration,
            message=f"Actualización automática fallida: {exc}",
        )
        _record_maintenance_failure(
            trigger="feedback_upload_auto_tune",
            radicado=radicado,
            comments_count=comments_count,
            iteration=iteration,
            error=str(exc),
        )
