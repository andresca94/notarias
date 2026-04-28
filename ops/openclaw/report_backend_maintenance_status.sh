#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "Uso: bash ops/openclaw/report_backend_maintenance_status.sh <status> <radicado> <iteration> <message> [run_id]" >&2
  exit 2
fi

status="$1"
radicado="$2"
iteration="$3"
message="$4"
run_id="${5:-}"

case "$status" in
  running|completed|failed|skipped)
    ;;
  *)
    echo "Estado inválido: $status" >&2
    exit 2
    ;;
esac

backend_env="/srv/notar-ia/backend/shared/backend.env"
if [[ -n "${INTERNAL_ADMIN_TOKEN:-}" ]]; then
  admin_token="${INTERNAL_ADMIN_TOKEN}"
elif [[ -f "${backend_env}" ]]; then
  admin_token="$(grep '^INTERNAL_ADMIN_TOKEN=' "${backend_env}" | cut -d= -f2-)"
else
  echo "No se encontró INTERNAL_ADMIN_TOKEN ni ${backend_env}" >&2
  exit 2
fi

payload="$(python3 - "$radicado" "$iteration" "$status" "$message" "$run_id" <<'PY'
import json
import sys

radicado, iteration, status, message, run_id = sys.argv[1:]
payload = {
    "radicado": radicado,
    "iteration": int(iteration),
    "status": status,
    "message": message,
}
if run_id:
    payload["run_id"] = run_id
print(json.dumps(payload, ensure_ascii=False))
PY
)"

curl -fsS -X POST "http://127.0.0.1:8080/admin/openclaw/backend-maintenance/status" \
  -H "x-admin-token: ${admin_token}" \
  -H "Content-Type: application/json" \
  --data "${payload}"
