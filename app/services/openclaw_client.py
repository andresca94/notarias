from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from app.core.config import settings


class OpenClawClient:
    def __init__(self) -> None:
        self.base_url = (settings.OPENCLAW_BASE_URL or "").rstrip("/")
        self.hook_token = settings.OPENCLAW_HOOK_TOKEN
        self.agent_hook_path = settings.OPENCLAW_AGENT_HOOK_PATH
        if not self.base_url or not self.hook_token:
            raise RuntimeError("OpenClaw no está configurado.")

    async def trigger_agent_task(
        self,
        *,
        message: str,
        name: str,
        model: Optional[str] = None,
        timeout_seconds: int = 300,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": name,
            "message": message,
            "timeoutSeconds": timeout_seconds,
        }
        if model:
            payload["model"] = model

        timeout = httpx.Timeout(float(settings.OPENCLAW_HOOK_HTTP_TIMEOUT_SECONDS))
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self.base_url}{self.agent_hook_path}",
                json=payload,
                headers={"Authorization": f"Bearer {self.hook_token}"},
            )
            response.raise_for_status()
            return response.json()
