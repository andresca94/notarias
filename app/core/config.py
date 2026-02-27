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

    RAG_STORE_DIR: str = "rag_store"


settings = Settings()
