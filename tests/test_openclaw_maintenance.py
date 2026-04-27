from app.core.config import settings
from app.services.openclaw_maintenance import _build_auto_tune_prompt


def test_build_auto_tune_prompt_respects_push_and_deploy_flags():
    original_push = settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED
    original_deploy = settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED
    original_command = settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND
    try:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = True
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = "deploy-backend-command"

        prompt = _build_auto_tune_prompt(iteration=7)

        assert "Iteracion objetivo: 7." in prompt
        assert "git push origin main" in prompt
        assert "deploy-backend-command" in prompt
    finally:
        settings.OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED = original_push
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED = original_deploy
        settings.OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND = original_command
