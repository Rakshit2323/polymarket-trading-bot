#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_ssh.sh [command] [-- args...]

Deploys one of the Go CLIs to a remote Linux host over SSH (key or password auth),
using SSH details from a local .env file:

  SSH_SERVER=user@host
  SSH_PASSWORD=...
  # OR (recommended):
  # SSH_KEY_PATH=~/.ssh/id_ed25519
  # SSH_PORT=22
  # SSH_USE_SUDO=1  # if SSH user is not root (requires passwordless sudo)

Defaults:
  command: arbitrage
  remote dir: /opt/poly-gocopy (override with DEPLOY_REMOTE_DIR in .env)
  systemd unit: poly-gocopy-<command> (override with DEPLOY_SERVICE_NAME in .env)

Examples:
  scripts/deploy_ssh.sh
  scripts/deploy_ssh.sh arbitrage -- --token-a=... --token-b=...
  scripts/deploy_ssh.sh arbitrage-equal -- --event-slug=... --cap-a=... --cap-b=...
  scripts/deploy_ssh.sh arbitrage-weighted -- --event-slug=... --cap-a=... --cap-b=...
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

cmd_name="${1:-arbitrage}"
if [[ $# -gt 0 ]]; then shift; fi

binary_args=()
if [[ "${1:-}" == "--" ]]; then
  shift
  binary_args=("$@")
fi

case "$cmd_name" in
  benchmark|latency|arbitrage|arbitrage-equal|arbitrage-weighted) ;;
  *)
    echo "Unsupported command: $cmd_name" >&2
    echo "Expected one of: benchmark, latency, arbitrage, arbitrage-equal, arbitrage-weighted" >&2
    exit 2
    ;;
esac

env_file="${ENV_FILE:-.env}"
dotenv_py="scripts/dotenv.py"

if [[ ! -f "$dotenv_py" ]]; then
  echo "Missing helper: $dotenv_py" >&2
  exit 2
fi

ssh_server="${SSH_SERVER:-}"
if [[ -z "$ssh_server" ]]; then
  ssh_server="$(python3 "$dotenv_py" --file "$env_file" get SSH_SERVER || true)"
fi
ssh_password="${SSH_PASSWORD:-}"
if [[ -z "$ssh_password" ]]; then
  ssh_password="$(python3 "$dotenv_py" --file "$env_file" get SSH_PASSWORD || true)"
fi
ssh_key_path="${SSH_KEY_PATH:-}"
if [[ -z "$ssh_key_path" ]]; then
  ssh_key_path="$(python3 "$dotenv_py" --file "$env_file" get SSH_KEY_PATH || true)"
fi
ssh_port="${SSH_PORT:-}"
if [[ -z "$ssh_port" ]]; then
  ssh_port="$(python3 "$dotenv_py" --file "$env_file" get SSH_PORT || true)"
fi
ssh_use_sudo_raw="${SSH_USE_SUDO:-}"
if [[ -z "$ssh_use_sudo_raw" ]]; then
  ssh_use_sudo_raw="$(python3 "$dotenv_py" --file "$env_file" get SSH_USE_SUDO || true)"
fi
deploy_remote_dir="${DEPLOY_REMOTE_DIR:-}"
if [[ -z "$deploy_remote_dir" ]]; then
  deploy_remote_dir="$(python3 "$dotenv_py" --file "$env_file" get DEPLOY_REMOTE_DIR || true)"
fi
deploy_service_name="${DEPLOY_SERVICE_NAME:-}"
if [[ -z "$deploy_service_name" ]]; then
  deploy_service_name="$(python3 "$dotenv_py" --file "$env_file" get DEPLOY_SERVICE_NAME || true)"
fi

if [[ -z "$ssh_server" ]]; then
  echo "Missing SSH_SERVER in $env_file" >&2
  exit 2
fi

use_key_auth=false
if [[ -n "$ssh_key_path" ]]; then
  use_key_auth=true
  ssh_key_path="${ssh_key_path/#\~/$HOME}"
  if [[ ! -f "$ssh_key_path" ]]; then
    echo "SSH_KEY_PATH not found: $ssh_key_path" >&2
    exit 2
  fi
elif [[ -z "$ssh_password" ]]; then
  echo "Missing SSH_PASSWORD in $env_file (or set SSH_KEY_PATH)" >&2
  exit 2
fi

if [[ -n "$ssh_port" && ! "$ssh_port" =~ ^[0-9]+$ ]]; then
  echo "SSH_PORT must be an integer, got: $ssh_port" >&2
  exit 2
fi

use_sudo=false
case "${ssh_use_sudo_raw,,}" in
  "") use_sudo=false ;;
  0|false|no|off) use_sudo=false ;;
  1|true|yes|on) use_sudo=true ;;
  *)
    echo "SSH_USE_SUDO must be true/false (or 1/0), got: $ssh_use_sudo_raw" >&2
    exit 2
    ;;
esac

sudo_prefix=""
if $use_sudo; then
  sudo_prefix="sudo -n "
fi

remote_dir="${deploy_remote_dir:-/opt/poly-gocopy}"
service_name="${deploy_service_name:-poly-gocopy-$cmd_name}"

# Basic sanity checks to avoid nasty quoting surprises.
if [[ ! "$remote_dir" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "DEPLOY_REMOTE_DIR must be an absolute path with safe characters, got: $remote_dir" >&2
  exit 2
fi
if [[ ! "$service_name" =~ ^[A-Za-z0-9._@-]+$ ]]; then
  echo "DEPLOY_SERVICE_NAME must be a safe systemd unit name, got: $service_name" >&2
  exit 2
fi

out_dir="out/deploy/$cmd_name"
local_bin="$out_dir/$cmd_name"
local_env="$out_dir/.env"
local_unit="$out_dir/$service_name.service"

mkdir -p "$out_dir"

common_ssh_opts=(
  -o StrictHostKeyChecking=accept-new
)

ssh_opts=("${common_ssh_opts[@]}")
scp_opts=("${common_ssh_opts[@]}")

if [[ -n "$ssh_port" ]]; then
  ssh_opts+=(-p "$ssh_port")
  scp_opts+=(-P "$ssh_port")
fi

if $use_key_auth; then
  ssh_opts+=(
    -o BatchMode=yes
    -o PreferredAuthentications=publickey
    -o PasswordAuthentication=no
    -o PubkeyAuthentication=yes
    -o IdentitiesOnly=yes
    -i "$ssh_key_path"
  )
  scp_opts+=(
    -o BatchMode=yes
    -o PreferredAuthentications=publickey
    -o PasswordAuthentication=no
    -o PubkeyAuthentication=yes
    -o IdentitiesOnly=yes
    -i "$ssh_key_path"
  )
else
  askpass="$out_dir/ssh_askpass.sh"
  cat >"$askpass" <<'EOF'
#!/usr/bin/env bash
exec printf "%s\n" "${SSH_PASSWORD:?}"
EOF
  chmod 700 "$askpass"

  ssh_opts+=(
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
  )
  scp_opts+=(
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
  )
fi

ssh_cmd() {
  local remote_cmd="$1"
  if $use_key_auth; then
    ssh "${ssh_opts[@]}" "$ssh_server" "$remote_cmd"
  else
    SSH_PASSWORD="$ssh_password" DISPLAY=dummy SSH_ASKPASS="$askpass" SSH_ASKPASS_REQUIRE=force \
      setsid -w ssh "${ssh_opts[@]}" "$ssh_server" "$remote_cmd" < /dev/null
  fi
}

scp_file() {
  local src="$1"
  local dest="$2"
  if $use_key_auth; then
    scp "${scp_opts[@]}" "$src" "$ssh_server:$dest"
  else
    SSH_PASSWORD="$ssh_password" DISPLAY=dummy SSH_ASKPASS="$askpass" SSH_ASKPASS_REQUIRE=force \
      setsid -w scp "${scp_opts[@]}" "$src" "$ssh_server:$dest" < /dev/null
  fi
}

remote_arch="$(ssh_cmd 'uname -m' | tr -d '\r\n' || true)"
case "$remote_arch" in
  x86_64) goarch="amd64" ;;
  aarch64|arm64) goarch="arm64" ;;
  *)
    echo "Unsupported remote arch from uname -m: $remote_arch" >&2
    exit 2
    ;;
esac

echo "Remote: $ssh_server ($remote_arch) → GOARCH=$goarch"
echo "Building: ./cmd/$cmd_name"
CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" \
  go build -trimpath -ldflags="-s -w" -o "$local_bin" "./cmd/$cmd_name"

echo "Building: ./cmd/balance"
local_balance_bin="$out_dir/balance"
CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" \
  go build -trimpath -ldflags="-s -w" -o "$local_balance_bin" "./cmd/balance"

python3 "$dotenv_py" --file "$env_file" filter \
  --exclude-key SSH_SERVER \
  --exclude-key SSH_PASSWORD \
  --exclude-prefix SSH_ \
  >"$local_env"

chmod 600 "$local_env"

{
  echo "[Unit]"
  echo "Description=poly-gocopy ($cmd_name)"
  echo "After=network-online.target"
  echo "Wants=network-online.target"
  echo
  echo "[Service]"
  echo "Type=simple"
  echo "WorkingDirectory=$remote_dir"
  echo "EnvironmentFile=$remote_dir/.env"
  printf "ExecStart=%s/bin/%s" "$remote_dir" "$cmd_name"
  if [[ ${#binary_args[@]} -gt 0 ]]; then
    for a in "${binary_args[@]}"; do
      if [[ "$a" =~ [[:space:]] ]]; then
        echo >&2
        echo "Error: systemd ExecStart args must not contain whitespace (got: $a)" >&2
        echo "Tip: use --flag=value instead of --flag \"value with spaces\"" >&2
        exit 2
      fi
      printf " %s" "$a"
    done
  fi
  echo
  if [[ "$cmd_name" == "arbitrage" || "$cmd_name" == "arbitrage-equal" ]]; then
    # arbitrage supports hot-reloading its event slugs file via SIGHUP.
    echo 'ExecReload=/bin/kill -HUP $MAINPID'
  fi
  echo "Restart=on-failure"
  echo "RestartSec=2"
  echo "UMask=0077"
  echo "NoNewPrivileges=true"
  echo "PrivateTmp=true"
  echo "ProtectHome=true"
  echo "ProtectSystem=strict"
  echo "ReadWritePaths=$remote_dir/out"
  echo
  echo "[Install]"
  echo "WantedBy=multi-user.target"
} >"$local_unit"

echo "Uploading to: $remote_dir"
if $use_sudo; then
  # Stage uploads to a user-writable dir first, then sudo-move into place.
  remote_stage_dir="$(ssh_cmd "mktemp -d /tmp/poly-gocopy-deploy.${cmd_name}.XXXXXX" | tr -d '\r\n' || true)"
  if [[ -z "$remote_stage_dir" || ! "$remote_stage_dir" =~ ^/tmp/[A-Za-z0-9._/-]+$ ]]; then
    echo "Failed to create a safe remote staging dir, got: $remote_stage_dir" >&2
    exit 2
  fi

  remote_bin_stage="$remote_stage_dir/${cmd_name}.new"
  remote_balance_stage="$remote_stage_dir/balance.new"
  remote_env_stage="$remote_stage_dir/.env.new"
  remote_unit_stage="$remote_stage_dir/${service_name}.service.new"

  scp_file "$local_bin" "$remote_bin_stage"
  scp_file "$local_balance_bin" "$remote_balance_stage"
  scp_file "$local_env" "$remote_env_stage"
  scp_file "$local_unit" "$remote_unit_stage"

  echo "Configuring systemd (sudo): $service_name"
  ssh_cmd "set -euo pipefail; \
    ${sudo_prefix}mkdir -p '$remote_dir/bin' '$remote_dir/out'; \
    ${sudo_prefix}chmod 700 '$remote_dir' '$remote_dir/bin' '$remote_dir/out'; \
    ${sudo_prefix}install -m 700 '$remote_bin_stage' '$remote_dir/bin/.${cmd_name}.new'; \
    ${sudo_prefix}install -m 700 '$remote_balance_stage' '$remote_dir/bin/.balance.new'; \
    ${sudo_prefix}install -m 600 '$remote_env_stage' '$remote_dir/.env.new'; \
    ${sudo_prefix}install -m 644 '$remote_unit_stage' '/etc/systemd/system/.${service_name}.service.new'; \
    ${sudo_prefix}mv -f '$remote_dir/bin/.${cmd_name}.new' '$remote_dir/bin/$cmd_name'; \
    ${sudo_prefix}mv -f '$remote_dir/bin/.balance.new' '$remote_dir/bin/balance'; \
    ${sudo_prefix}mv -f '$remote_dir/.env.new' '$remote_dir/.env'; \
    ${sudo_prefix}mv -f '/etc/systemd/system/.${service_name}.service.new' '/etc/systemd/system/$service_name.service'; \
    ${sudo_prefix}systemctl daemon-reload; \
    ${sudo_prefix}systemctl enable --now '$service_name'; \
    ${sudo_prefix}systemctl restart '$service_name'; \
    rm -rf '$remote_stage_dir'"
else
  ssh_cmd "mkdir -p '$remote_dir/bin' '$remote_dir/out' && chmod 700 '$remote_dir' '$remote_dir/bin' '$remote_dir/out'"
  remote_bin_tmp="$remote_dir/bin/.${cmd_name}.new"
  remote_balance_tmp="$remote_dir/bin/.balance.new"
  remote_env_tmp="$remote_dir/.env.new"
  remote_unit_tmp="/etc/systemd/system/.${service_name}.service.new"

  # Upload to temp paths, then atomically swap into place (avoids ETXTBSY when the service is currently running).
  scp_file "$local_bin" "$remote_bin_tmp"
  scp_file "$local_balance_bin" "$remote_balance_tmp"
  scp_file "$local_env" "$remote_env_tmp"
  scp_file "$local_unit" "$remote_unit_tmp"

  echo "Configuring systemd: $service_name"
  ssh_cmd "chmod 700 '$remote_bin_tmp' && mv -f '$remote_bin_tmp' '$remote_dir/bin/$cmd_name' && chmod 700 '$remote_balance_tmp' && mv -f '$remote_balance_tmp' '$remote_dir/bin/balance' && chmod 600 '$remote_env_tmp' && mv -f '$remote_env_tmp' '$remote_dir/.env' && mv -f '$remote_unit_tmp' '/etc/systemd/system/$service_name.service' && systemctl daemon-reload && systemctl enable --now '$service_name' && systemctl restart '$service_name'"
fi

echo "Status:"
ssh_cmd "${sudo_prefix}systemctl status '$service_name' --no-pager || true"
echo
echo "Recent logs:"
ssh_cmd "${sudo_prefix}journalctl -u '$service_name' -n 80 --no-pager || true"
