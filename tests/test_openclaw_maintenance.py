from fastapi.testclient import TestClient

from app.core.config import settings
from app.main import app
from app.services.openclaw_maintenance import _build_auto_tune_prompt, trigger_backend_maintenance


def test_build_auto_tune_prompt_respects_push_and_deploy_flags():
    original_push = settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED
    original_deploy = settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED
    original_command = settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND
    original_workspace = settings.OPENCLAW_MAINTENANCE_WORKSPACE
    original_live_checkout = settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT
    original_branch = settings.OPENCLAW_MAINTENANCE_BRANCH
    original_outputs_root = settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT
    original_agent_timeout = settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS
    try:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = "deploy-backend-command"
        settings.OPENCLAW_MAINTENANCE_WORKSPACE = "/srv/notar-ia/backend/autotune"
        settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT = "/srv/notar-ia/backend/current"
        settings.OPENCLAW_MAINTENANCE_BRANCH = "main"
        settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT = "/srv/notar-ia/data/outputs"
        settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS = 840

        prompt = _build_auto_tune_prompt(radicado="26485", iteration=7)

        assert "Radicado objetivo: 26485." in prompt
        assert "Iteracion objetivo: 7." in prompt
        assert "/srv/notar-ia/backend/autotune" in prompt
        assert "/srv/notar-ia/backend/current" in prompt
        assert "/srv/notar-ia/data/outputs" in prompt
        assert "Rama de trabajo esperada: main." in prompt
        assert "git status --short" in prompt
        assert "git fetch origin main" in prompt
        assert "git pull --ff-only origin main" in prompt
        assert "git push origin main" in prompt
        assert "deploy-backend-command" in prompt
        assert "report_backend_maintenance_status.sh" in prompt
        assert "si falta, reporta failure y aborta" in prompt
        assert "reporta running" in prompt
        assert "reporta completion" in prompt
        assert "Objetivo principal" in prompt
        assert "una sola mejora backend principal" in prompt
        assert "un solo test o check focalizado" in prompt
        assert "Evita tocar archivos del rag_store" in prompt
        assert "No uses `skipped` como salida por defecto." in prompt
        assert "Comentarios Word relevantes" in prompt or "si todos los comentarios son puramente especificos" in prompt
    finally:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = original_push
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = original_deploy
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = original_command
        settings.OPENCLAW_MAINTENANCE_WORKSPACE = original_workspace
        settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT = original_live_checkout
        settings.OPENCLAW_MAINTENANCE_BRANCH = original_branch
        settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT = original_outputs_root
        settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS = original_agent_timeout


def test_trigger_backend_maintenance_uses_bounded_agent_timeout(monkeypatch):
    original_agent_timeout = settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS
    original_pending_timeout = settings.OPENCLAW_MAINTENANCE_PENDING_TIMEOUT_SECONDS
    captured = {}

    class FakeOpenClawClient:
        async def trigger_agent_task(self, **kwargs):
            captured.update(kwargs)
            return {"ok": True, "runId": "run-123"}

    monkeypatch.setattr("app.services.openclaw_maintenance.OpenClawClient", FakeOpenClawClient)

    try:
        settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS = 1200
        settings.OPENCLAW_MAINTENANCE_PENDING_TIMEOUT_SECONDS = 900

        import asyncio

        asyncio.run(
            trigger_backend_maintenance(
                radicado="25963",
                prompt="smoke",
                trigger="feedback_upload_auto_tune",
                comments_count=11,
                iteration=1,
            )
        )

        assert captured["timeout_seconds"] == 840
        assert captured["name"] == "Notar-IA backend maintenance"
    finally:
        settings.OPENCLAW_MAINTENANCE_AGENT_TIMEOUT_SECONDS = original_agent_timeout
        settings.OPENCLAW_MAINTENANCE_PENDING_TIMEOUT_SECONDS = original_pending_timeout


def test_admin_endpoint_queues_background_maintenance(monkeypatch):
    original_admin_token = settings.INTERNAL_ADMIN_TOKEN
    settings.INTERNAL_ADMIN_TOKEN = "secret-token"
    captured = {}

    async def fake_trigger_backend_maintenance_logged(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        "app.api.routes.trigger_backend_maintenance_logged",
        fake_trigger_backend_maintenance_logged,
    )

    with TestClient(app) as client:
        response = client.post(
            "/admin/openclaw/backend-maintenance",
            headers={"x-admin-token": "secret-token"},
            json={"prompt": "smoke prompt"},
        )

    settings.INTERNAL_ADMIN_TOKEN = original_admin_token

    assert response.status_code == 200
    assert response.json() == {"ok": True, "queued": True}
    assert captured["prompt"] == "smoke prompt"
    assert captured["trigger"] == "admin_endpoint"
