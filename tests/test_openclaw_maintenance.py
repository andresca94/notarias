from app.core.config import settings
from app.services.openclaw_maintenance import _build_auto_tune_prompt


def test_build_auto_tune_prompt_respects_push_and_deploy_flags():
    original_push = settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED
    original_deploy = settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED
    original_command = settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND
    original_workspace = settings.OPENCLAW_MAINTENANCE_WORKSPACE
    original_live_checkout = settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT
    original_branch = settings.OPENCLAW_MAINTENANCE_BRANCH
    try:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = "deploy-backend-command"
        settings.OPENCLAW_MAINTENANCE_WORKSPACE = "/srv/notar-ia/backend/autotune"
        settings.OPENCLAW_MAINTENANCE_LIVE_CHECKOUT = "/srv/notar-ia/backend/current"
        settings.OPENCLAW_MAINTENANCE_BRANCH = "main"

        prompt = _build_auto_tune_prompt(iteration=7)

        assert "Iteracion objetivo: 7." in prompt
        assert "/srv/notar-ia/backend/autotune" in prompt
        assert "/srv/notar-ia/backend/current" in prompt
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
