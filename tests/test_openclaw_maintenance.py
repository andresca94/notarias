from fastapi.testclient import TestClient

from app.core.config import settings
from app.main import app
from app.services.openclaw_maintenance import _build_auto_tune_prompt


def test_build_auto_tune_prompt_respects_push_and_deploy_flags():
    original_push = settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED
    original_deploy = settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED
    original_command = settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND
    original_workspace = settings.OPENCLAW_MAINTENANCE_WORKSPACE
    original_live_checkout = settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT
    original_branch = settings.OPENCLAW_MAINTENANCE_BRANCH
    original_outputs_root = settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT
    try:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = "deploy-backend-command"
        settings.OPENCLAW_MAINTENANCE_WORKSPACE = "/srv/notar-ia/backend/autotune"
        settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT = "/srv/notar-ia/backend/current"
        settings.OPENCLAW_MAINTENANCE_BRANCH = "main"
        settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT = "/srv/notar-ia/data/outputs"

        prompt = _build_auto_tune_prompt(iteration=7)

        assert "Iteracion objetivo: 7." in prompt
        assert "/srv/notar-ia/backend/autotune" in prompt
        assert "/srv/notar-ia/backend/current" in prompt
        assert "/srv/notar-ia/data/outputs" in prompt
        assert "Rama de trabajo esperada: main." in prompt
        assert "git status --short" in prompt
        assert "git push origin main" in prompt
        assert "deploy-backend-command" in prompt
    finally:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = original_push
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = original_deploy
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = original_command
        settings.OPENCLAW_MAINTENANCE_WORKSPACE = original_workspace
        settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT = original_live_checkout
        settings.OPENCLAW_MAINTENANCE_BRANCH = original_branch
        settings.OPENCLAW_MAINTENANCE_OUTPUTS_ROOT = original_outputs_root


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
