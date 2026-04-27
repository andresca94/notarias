# OpenClaw Setup

This backend expects OpenClaw to run on the same VPS as a host-level service, not inside the FastAPI container.

## Install

1. SSH into the VPS.
2. Install OpenClaw with the official installer:

```bash
curl -fsSL https://openclaw.ai/install.sh | bash -s -- --no-onboard
```

3. Confirm the install:

```bash
openclaw --version
openclaw doctor
openclaw gateway status
```

## Configure hooks

1. Copy [openclaw.config.example.json](/Users/andrescarvajal/Documents/Notar-IA/notar-ia-fastapi/ops/openclaw/openclaw.config.example.json) into your OpenClaw config directory and adapt the secrets.
2. Export `OPENCLAW_WEBHOOK_SECRET` and `OPENCLAW_PLUGIN_SECRET` in the OpenClaw service environment.
3. Restart the gateway after saving the config.

## Workspace

- Point the OpenClaw workspace at the backend checkout on the VPS, for example `/srv/notar-ia/backend/current`.
- Keep the frontend repo out of the workspace instructions.
- Use [backend-maintenance-prompt.md](/Users/andrescarvajal/Documents/Notar-IA/notar-ia-fastapi/ops/openclaw/backend-maintenance-prompt.md) as the standing maintenance prompt.

## Trigger paths

- Backend admin endpoint: `POST http://127.0.0.1:8080/admin/openclaw/backend-maintenance`
- Native hook endpoint: `POST http://127.0.0.1:18789/hooks/agent`

Both should remain loopback-only on the VPS.
