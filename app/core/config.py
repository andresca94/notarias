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
    OPENCLAW_MAINTENANCE_MODEL: str | None = None
    INTERNAL_ADMIN_TOKEN: str | None = None


settings = Settings()
