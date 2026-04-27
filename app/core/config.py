from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    GEMINI_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None

    GEMINI_MODEL_VISION: str = "models/gemini-2.5-flash"
    GEMINI_MODEL_TEXT: str = "models/gemini-2.5-pro"
    OPENAI_MODEL: str = "gpt-4.1"

    TEMPLATE_DOCX_PATH: str = "app/templates/plantilla_notarial.docx"
    OUTPUT_DIR: str = "outputs"
    PDF_ENGINE: str = "libreoffice"

    RAG_STORE_DIR: str = "app/rag_store"
    CORS_ALLOW_ORIGINS: str = "http://localhost:5173"

    OPENCLAW_BASE_URL: str | None = None
    OPENCLAW_HOOK_TOKEN: str | None = None
    OPENCLAW_AGENT_HOOK_PATH: str = "/hooks/agent"
    OPENCLAW_HOOK_HTTP_TIMEOUT_SECONDS: int = 360
    OPENCLAW_MAINTENANCE_MODEL: str | None = None
    OPENCLAW_MAINTENANCE_PROMPT_FILE: str = "ops/openclaw/backend-maintenance-prompt.md"
    OPENCLAW_MAINTENANCE_WORKSPACE: str = "/srv/notar-ia/backend/current"
    OPENCLAW_MAINTENANCE_LIVE_CHECKOUT: str = "/srv/notar-ia/backend/current"
    OPENCLAW_MAINTENANCE_BRANCH: str = "main"
    OPENCLAW_MAINTENANCE_OUTPUTS_ROOT: str = "/srv/notar-ia/data/outputs"
    OPENCLAW_MAINTENANCE_PENDING_TIMEOUT_SECONDS: int = 900
    OPENCLAW_AUTO_TUNE_ENABLED: bool = False
    OPENCLAW_AUTO_TUNE_MIN_COMMENTS: int = 1
    OPENCLAW_AUTO_TUNE_GIT_PUSH_ENABLED: bool = False
    OPENCLAW_AUTO_TUNE_DEPLOY_ENABLED: bool = False
    OPENCLAW_AUTO_TUNE_DEPLOY_COMMAND: str = (
        "cd /srv/notar-ia/backend/current && "
        "git pull --ff-only origin main && "
        "docker build -t ghcr.io/andresca94/notarias-backend:latest . && "
        "docker compose -f /srv/notar-ia/backend/current/ops/deploy/docker-compose.yml up -d --force-recreate backend && "
        "curl -fsS http://127.0.0.1:8080/docs >/dev/null"
    )
    INTERNAL_ADMIN_TOKEN: str | None = None


settings = Settings()
