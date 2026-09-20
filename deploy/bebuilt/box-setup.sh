#!/usr/bin/env bash
# Bring one client's RAGFlow box to its intended state. Runs as root on the box, from the checkout it
# configures (/opt/ragflow, at the pinned ref). Safe to re-run: every step checks before it changes.
#
# Driven from the laptop by scripts/ragflow-provision.sh in bebuilt-platform-v2, which first writes
#   /etc/bebuilt/ragflow-secrets.env      (0600) MYSQL_PASSWORD, MINIO_PASSWORD, REDIS_PASSWORD,
#                                          OPENSEARCH_PASSWORD, ELASTIC_PASSWORD, ADMIN_DEFAULT_PASSWORD,
#                                          RAGFLOW_SECRET_KEY, RAGFLOW_APP_PASSWORD, RAGFLOW_APP_EMAIL — from the
#                                          client's own 1Password vault
#   /etc/bebuilt/cloudflared-token        (0600, optional) the client's tunnel token
#   /etc/bebuilt/worker.env               (0600, optional) WORKER_DB_URL, ORG_ID, COMPOSIO_API_KEY,
#                                          COMPOSIO_USER_ID — turns on the ingestion worker
# and then checks out the pinned ref. This script never fetches code and never generates a secret.
#
# The box leaves our hands (walk-away, D36), so nothing here may be multi-tenant: no platform key, no
# credential that reaches another client. Everything it needs is in the two files above.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$ROOT/deploy/bebuilt"
SECRETS=/etc/bebuilt/ragflow-secrets.env
TOKEN=/etc/bebuilt/cloudflared-token
PROJECT=ragflow
log() { printf '== %s\n' "$*"; }
die() { printf 'box-setup: %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" = 0 ] || die "run as root"
[ -f "$SECRETS" ] || die "$SECRETS missing; run scripts/ragflow-provision.sh from the laptop"
[ "$(stat -c %a "$SECRETS")" = 600 ] || die "$SECRETS must be mode 600"
export DEBIAN_FRONTEND=noninteractive

log "ssh: keys only"
cat > /etc/ssh/sshd_config.d/10-bebuilt.conf <<'CONF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
CONF
sshd -t && systemctl try-reload-or-restart ssh
[ "$(sshd -T | awk '/^passwordauthentication /{print $2}')" = no ] || die "sshd still allows passwords"

log "packages: unattended security upgrades, git, python3"
apt-get update -qq
apt-get install -y -qq unattended-upgrades git python3 python3-psycopg python3-requests ca-certificates curl gnupg >/dev/null

log "kernel: vm.max_map_count for OpenSearch"
echo 'vm.max_map_count=262144' > /etc/sysctl.d/60-opensearch.conf
sysctl -q --system

if ! command -v docker >/dev/null; then
  log "docker: installing"
  . /etc/os-release
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $VERSION_CODENAME stable" \
    > /etc/apt/sources.list.d/docker.list
  if apt-get update -qq 2>/dev/null && apt-cache policy docker-ce | grep -q 'Candidate: [0-9]'; then
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin >/dev/null
  else
    log "docker: no upstream packages for $VERSION_CODENAME yet; using Ubuntu's"
    rm -f /etc/apt/sources.list.d/docker.list; apt-get update -qq
    apt-get install -y -qq docker.io docker-compose-v2 >/dev/null
  fi
fi
# !reset / !override in compose.bebuilt.yml need Compose 2.24.4 or later.
python3 - "$(docker compose version --short)" <<'PY' || die "docker compose $(docker compose version --short) is older than 2.24.4"
import re, sys
m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", sys.argv[1])  # Ubuntu's reads like 2.40.3+ds1-0ubuntu1
sys.exit(0 if m and tuple(map(int, m.groups())) >= (2, 24, 4) else 1)
PY

log "docker: bounded logs"
DAEMON='{"log-driver":"json-file","log-opts":{"max-size":"50m","max-file":"3"}}'
if [ "$(cat /etc/docker/daemon.json 2>/dev/null)" != "$DAEMON" ]; then
  echo "$DAEMON" > /etc/docker/daemon.json; systemctl restart docker
fi

log "config: docker/.env = upstream at this ref + env.bebuilt + secrets"
git -C "$ROOT" show HEAD:docker/.env > "$ROOT/docker/.env.upstream"
python3 - "$ROOT/docker/.env.upstream" "$HERE/env.bebuilt" "$SECRETS" "$ROOT/docker/.env" <<'PY'
import os, re, sys
base, *layers, out = sys.argv[1:]
def pairs(path):
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            yield k.strip(), v.strip()
want = {}
for layer in layers:
    # RAGFLOW_APP_* belong to tenant-setup.py alone; docker/.env reaches every container's environment.
    want.update((k, v) for k, v in pairs(layer) if not k.startswith("RAGFLOW_APP_"))
lines, seen = [], set()
for line in open(base):
    m = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
    if m and m.group(1) in want:
        k = m.group(1)
        if k in seen:
            continue  # one definition per key, so interpolation and env_file agree
        lines.append(f"{k}={want[k]}\n"); seen.add(k)
    else:
        lines.append(line)
lines += [f"{k}={v}\n" for k, v in want.items() if k not in seen]
fd = os.open(out + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.writelines(lines)
os.replace(out + ".tmp", out)
PY
rm -f "$ROOT/docker/.env.upstream"
grep -q '^MYSQL_PASSWORD=infini_rag_flow$' "$ROOT/docker/.env" && die "docker/.env still carries a default password"

log "compose: up"
cd "$ROOT/docker"
COMPOSE=(docker compose -p "$PROJECT" -f docker-compose.yml -f "$HERE/compose.bebuilt.yml")
"${COMPOSE[@]}" pull -q
"${COMPOSE[@]}" up -d --remove-orphans

log "compose: nothing published beyond loopback"
# Our own stack only. A box can carry a neighbour through a migration — LaborTech's ran Onyx on :80/:443
# while RAGFlow was built beside it — and stopping on the neighbour's ports blocks the very migration
# that removes them. Ours must still publish nothing outside 127.0.0.1: everything arrives by tunnel.
OPEN="$(docker ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}} {{.Ports}}' | grep -E '(0\.0\.0\.0|\[::\]|:::)[0-9]*:' || true)"
[ -z "$OPEN" ] || die "a container publishes a port on a public interface: $OPEN"
FOREIGN="$(docker ps --format '{{.Label "com.docker.compose.project"}} {{.Names}} {{.Ports}}' | grep -vE "^$PROJECT " | grep -E '(0\.0\.0\.0|\[::\]|:::)[0-9]*:' || true)"
[ -z "$FOREIGN" ] || log "note: another stack on this box publishes public ports: $FOREIGN"

if [ -f "$TOKEN" ]; then
  if ! systemctl is-enabled cloudflared >/dev/null 2>&1; then
    log "cloudflared: installing"
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg -o /usr/share/keyrings/cloudflare-main.gpg
    echo 'deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main' \
      > /etc/apt/sources.list.d/cloudflared.list
    apt-get update -qq && apt-get install -y -qq cloudflared >/dev/null
    cloudflared service install "$(cat "$TOKEN")"
  fi
  systemctl is-active --quiet cloudflared || die "cloudflared is not running"
else
  log "cloudflared: skipped, no $TOKEN"
fi

log "tenant: app user, API key and the shared dataset"
# nginx answers before the API does (a 502 behind it), so wait for the API and the admin server themselves.
ready() { curl -sf http://127.0.0.1:8080/api/v1/system/config | grep -q '"code":0' && curl -sf -o /dev/null http://127.0.0.1:8080/api/v1/admin/ping; }
for i in $(seq 1 60); do ready && break; sleep 5; done
ready || die "RAGFlow's API did not come up within five minutes"
TENANT=/etc/bebuilt/ragflow-tenant.json
if grep -q '^RAGFLOW_APP_PASSWORD=' "$SECRETS"; then
  RF="$(docker ps -q -f label=com.docker.compose.project=$PROJECT -f label=com.docker.compose.service=ragflow-cpu)"
  ( set -a; . "$SECRETS"; set +a
    docker exec -i -e ADMIN_DEFAULT_PASSWORD -e RAGFLOW_APP_PASSWORD -e RAGFLOW_APP_EMAIL -e RAGFLOW_EMBEDDING_MODEL "$RF" /ragflow/.venv/bin/python - \
      < "$HERE/tenant-setup.py" > "$TENANT.tmp" ) || die "tenant setup failed"
  chmod 600 "$TENANT.tmp" && mv "$TENANT.tmp" "$TENANT"
  python3 -c "import json; d=json.load(open('$TENANT')); print('   dataset', d['dataset_id'] or 'not yet (no embedding model chosen)', d['embedding_model'] or '')"
else
  log "tenant: skipped, no RAGFLOW_APP_PASSWORD in $SECRETS"
fi

if [ -f /etc/bebuilt/worker.env ] && [ -f "$TENANT" ] && python3 -c "import json,sys; sys.exit(0 if json.load(open('$TENANT')).get('dataset_id') else 1)"; then
  log "ingestion: worker on a five-minute timer"
  [ "$(stat -c %a /etc/bebuilt/worker.env)" = 600 ] || die "/etc/bebuilt/worker.env must be mode 600"
  install -m 0644 "$HERE/bebuilt-ingest.service" "$HERE/bebuilt-ingest.timer" /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now bebuilt-ingest.timer >/dev/null 2>&1
else
  log "ingestion: skipped (needs /etc/bebuilt/worker.env and a dataset)"
fi

log "backups: nightly consistent MySQL dump onto this disk"
install -m 0700 -d /var/backups/ragflow
cat > /etc/cron.d/bebuilt-ragflow-dump <<CRON
# Written by deploy/bebuilt/box-setup.sh. Hetzner's daily backup then carries one clean copy.
17 3 * * * root $HERE/ragflow-dump.sh >> /var/log/bebuilt-ragflow-dump.log 2>&1
CRON

log "done"
"${COMPOSE[@]}" ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'
