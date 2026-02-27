from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(title="Notar-IA (FastAPI)")
app.include_router(router)
