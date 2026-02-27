from __future__ import annotations

import base64
import json
import requests
from typing import Any, Dict, Optional

from app.core.config import settings


class GeminiClient:
    """
    Minimal REST client for Google Generative Language API v1beta
    Uses API key.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.GEMINI_API_KEY
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY no está configurada en .env")

    def _post(self, model: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://generativelanguage.googleapis.com/v1beta/{model}:generateContent"
        r = requests.post(url, params={"key": self.api_key}, json=payload, timeout=180)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _extract_text(resp: Dict[str, Any]) -> str:
        # candidates[0].content.parts[0].text
        try:
            return resp["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            return ""

    def generate_text(self, prompt: str, model: Optional[str] = None, temperature: float = 0.1, max_output_tokens: int = 8192) -> str:
        model = model or settings.GEMINI_MODEL_TEXT
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_output_tokens},
        }
        resp = self._post(model, payload)
        return self._extract_text(resp)

    def analyze_binary(
        self,
        binary_bytes: bytes,
        mime_type: str,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.1,
        max_output_tokens: int = 8192,
    ) -> str:
        model = model or settings.GEMINI_MODEL_VISION
        b64 = base64.b64encode(binary_bytes).decode("utf-8")
        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt},
                        {"inlineData": {"mimeType": mime_type, "data": b64}},
                    ]
                }
            ],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_output_tokens},
        }
        resp = self._post(model, payload)
        return self._extract_text(resp)


def parse_json_strict(maybe_json_text: str) -> Dict[str, Any]:
    """
    Limpia ```json ... ``` y parsea.
    """
    if not maybe_json_text:
        return {}
    s = maybe_json_text.strip()
    s = s.replace("```json", "").replace("```", "").strip()
    # extra: si viene texto antes/después, corta en {..}
    if not s.startswith("{"):
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            s = s[start : end + 1]
    try:
        return json.loads(s)
    except Exception:
        return {}
