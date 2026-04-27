from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from app.api.routes import router
from app.core.config import settings

app = FastAPI(title="Notar-IA (FastAPI)")
app.include_router(router)

allowed_origins = [
    origin.strip()
    for origin in (settings.CORS_ALLOW_ORIGINS or "").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
