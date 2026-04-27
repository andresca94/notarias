# Deployment Steps

## 1. Provision the VPS

Create these directories on the server:

```bash
mkdir -p /srv/notar-ia/backend/current
mkdir -p /srv/notar-ia/backend/shared/secrets
mkdir -p /srv/notar-ia/frontend/current
mkdir -p /srv/notar-ia/data/outputs
mkdir -p /srv/notar-ia/nginx
```

Create the nginx basic-auth file:

```bash
htpasswd -c /srv/notar-ia/nginx/.htpasswd <username>
```

Copy `ops/deploy/backend.env.example` to `/srv/notar-ia/backend/shared/backend.env` and fill in the real secrets.

## 2. Backend bootstrap

Clone the backend repo into `/srv/notar-ia/backend/current`, then start the stack:

```bash
cd /srv/notar-ia/backend/current
cp ops/deploy/backend.env.example /srv/notar-ia/backend/shared/backend.env
docker compose -f ops/deploy/docker-compose.yml up -d
```

The backend container binds loopback `127.0.0.1:8080`; nginx exposes the internal-team site on port `80`.

## 3. Frontend bootstrap

Clone the frontend repo locally for GitHub Actions only. The deploy workflow uploads the built `dist/` output to `/srv/notar-ia/frontend/current`.

Set these frontend repo secrets:

- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`
- `VPS_PORT`

## 4. Backend repo secrets

Set these backend repo secrets:

- `GHCR_USERNAME`
- `GHCR_TOKEN`
- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`
- `VPS_PORT`
- `INTERNAL_ADMIN_TOKEN`

## 5. OpenClaw

Follow [ops/openclaw/README.md](/Users/andrescarvajal/Documents/Notar-IA/notar-ia-fastapi/ops/openclaw/README.md) on the VPS.

Keep OpenClaw loopback-only and point its workspace to `/srv/notar-ia/backend/current`.

## 6. Smoke test

1. Open the site through nginx and pass the basic-auth prompt.
2. Generate the first draft for case `26485`.
3. Download the generated Word file.
4. Upload the reviewed Word file with native comments.
5. Submit the feedback.
6. Continue to the next iteration.
7. Confirm iteration `1` and `2` both exist under `outputs/CASE-26485/iterations/`.
