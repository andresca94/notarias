from __future__ import annotations

import os
from typing import Any, Dict, Optional

from app.core.config import settings

# OpenAI python SDK v1 style
from openai import OpenAI


class OpenAIClient:
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        api_key = api_key or settings.OPENAI_API_KEY or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY no está configurada en .env")
        self.client = OpenAI(api_key=api_key)
        self.model = model or settings.OPENAI_MODEL

    def chat(self, system: str, user: str, temperature: float = 0.1, max_tokens: int = 8000) -> str:
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return resp.choices[0].message.content or ""
