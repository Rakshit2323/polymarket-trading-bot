#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/restart_services.sh [options]

Restarts poly-gocopy arbitrage services on the SSH host configured in .env:
  SSH_SERVER=user@host
  SSH_PASSWORD=...
  # OR (recommended):
  # SSH_KEY_PATH=~/.ssh/id_ed25519
  # SSH_PORT=22
  # SSH_USE_SUDO=1  # if SSH user is not root (requires passwordless sudo)

Defaults:
  Services: poly-gocopy-arbitrage, poly-gocopy-arbitrage-equal, poly-gocopy-arbitrage-weighted
  (Override via ARBITRAGE_SERVICE / ARBITRAGE_EQUAL_SERVICE / ARBITRAGE_WEIGHTED_SERVICE in .env)

Options:
  --deploy        Redeploy all services (build+upload+unit), then restart
  --reload        Reload-or-restart all (uses ExecReload if supported)
  --status        Print status only (no restart)
  --logs          Print recent logs after status
  --follow        Follow logs (journalctl -f) after restart/status
  --tail N        Number of log lines (default 80)
  -h, --help      Show help
EOF
}

show_status_only=false
print_logs=false
follow_logs=false
deploy=false
reload=false
tail_lines=80

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy) deploy=true ;;
    --reload) reload=true ;;
    --status) show_status_only=true ;;
    --logs) print_logs=true ;;
    --follow) follow_logs=true ;;
    --tail)
      shift
      tail_lines="${1:-}"
      if [[ -z "$tail_lines" ]]; then
        echo "Missing value for --tail" >&2
        exit 2
      fi
      ;;
    --tail=*)
      tail_lines="${1#*=}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if ! [[ "$tail_lines" =~ ^[0-9]+$ ]]; then
  echo "--tail must be an integer, got: $tail_lines" >&2
  exit 2
fi

env_file="${ENV_FILE:-.env}"
dotenv_py="scripts/dotenv.py"
if [[ ! -f "$dotenv_py" ]]; then
  echo "Missing helper: $dotenv_py" >&2
  exit 2
fi

ssh_server="$(python3 "$dotenv_py" --file "$env_file" get SSH_SERVER || true)"
ssh_password="$(python3 "$dotenv_py" --file "$env_file" get SSH_PASSWORD || true)"
ssh_key_path="$(python3 "$dotenv_py" --file "$env_file" get SSH_KEY_PATH || true)"
ssh_port="$(python3 "$dotenv_py" --file "$env_file" get SSH_PORT || true)"
ssh_use_sudo_raw="$(python3 "$dotenv_py" --file "$env_file" get SSH_USE_SUDO || true)"
arbitrage_service="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_SERVICE || true)"
arbitrage_equal_service="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_EQUAL_SERVICE || true)"
arbitrage_weighted_service="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_WEIGHTED_SERVICE || true)"
arbitrage_deploy_args="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_DEPLOY_ARGS || true)"
arbitrage_equal_deploy_args="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_EQUAL_DEPLOY_ARGS || true)"
arbitrage_weighted_deploy_args="$(python3 "$dotenv_py" --file "$env_file" get ARBITRAGE_WEIGHTED_DEPLOY_ARGS || true)"

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

arbitrage_service="${arbitrage_service:-poly-gocopy-arbitrage}"
arbitrage_equal_service="${arbitrage_equal_service:-poly-gocopy-arbitrage-equal}"
arbitrage_weighted_service="${arbitrage_weighted_service:-poly-gocopy-arbitrage-weighted}"

services=("$arbitrage_service" "$arbitrage_equal_service" "$arbitrage_weighted_service")
for svc in "${services[@]}"; do
  if [[ ! "$svc" =~ ^[A-Za-z0-9._@-]+$ ]]; then
    echo "Invalid service name: $svc" >&2
    exit 2
  fi
done

if $deploy && $show_status_only; then
  echo "Error: --deploy cannot be combined with --status" >&2
  exit 2
fi
if $reload && $show_status_only; then
  echo "Error: --reload cannot be combined with --status" >&2
  exit 2
fi

out_dir="out/restart"
mkdir -p "$out_dir"

common_ssh_opts=(
  -o StrictHostKeyChecking=accept-new
)

ssh_opts=("${common_ssh_opts[@]}")

if [[ -n "$ssh_port" ]]; then
  ssh_opts+=(-p "$ssh_port")
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

unit_with_suffix() {
  local unit="$1"
  if [[ "$unit" == *.service ]]; then
    printf '%s' "$unit"
    return 0
  fi
  printf '%s.service' "$unit"
}

remote_execstart() {
  local unit
  unit="$(unit_with_suffix "$1")"
  ssh_cmd "${sudo_prefix}systemctl cat '$unit' 2>/dev/null | sed -n 's/^[[:space:]]*ExecStart=//p' | head -n 1"
}

remote_execstart_args() {
  local unit="$1"
  local execstart
  execstart="$(remote_execstart "$unit" | tr -d '\r' || true)"
  if [[ -z "$execstart" ]]; then
    return 1
  fi
  local bin="${execstart%% *}"
  local rest="${execstart#"$bin"}"
  rest="${rest#" "}"
  printf '%s' "$rest"
}

status_cmd() {
  ssh_cmd "set -euo pipefail; for svc in '$arbitrage_service' '$arbitrage_equal_service' '$arbitrage_weighted_service'; do ${sudo_prefix}systemctl status \"\$svc\" --no-pager || true; echo; done"
}

logs_cmd() {
  ssh_cmd "set -euo pipefail; for svc in '$arbitrage_service' '$arbitrage_equal_service' '$arbitrage_weighted_service'; do ${sudo_prefix}journalctl -u \"\$svc\" -n $tail_lines --no-pager || true; echo; done"
}

follow_cmd() {
  # Follow all in one stream (journalctl supports multiple -u).
  ssh_cmd "${sudo_prefix}journalctl -u '$arbitrage_service' -u '$arbitrage_equal_service' -u '$arbitrage_weighted_service' -f --no-pager"
}

deploy_all() {
  local deploy_script="scripts/deploy_ssh.sh"
  if [[ ! -x "$deploy_script" ]]; then
    echo "Missing deploy script: $deploy_script" >&2
    exit 2
  fi

  local arbitrage_args_str="$arbitrage_deploy_args"
  if [[ -z "$arbitrage_args_str" ]]; then
    arbitrage_args_str="$(remote_execstart_args "$arbitrage_service" || true)"
  fi

  local arbitrage_equal_args_str="$arbitrage_equal_deploy_args"
  if [[ -z "$arbitrage_equal_args_str" ]]; then
    arbitrage_equal_args_str="$(remote_execstart_args "$arbitrage_equal_service" || true)"
  fi

  local arbitrage_weighted_args_str="$arbitrage_weighted_deploy_args"
  if [[ -z "$arbitrage_weighted_args_str" ]]; then
    arbitrage_weighted_args_str="$(remote_execstart_args "$arbitrage_weighted_service" || true)"
  fi

  if [[ -z "$arbitrage_args_str" || -z "$arbitrage_equal_args_str" || -z "$arbitrage_weighted_args_str" ]]; then
    local event_slug token_a token_b
    event_slug="$(python3 "$dotenv_py" --file "$env_file" get EVENT_SLUGS_FILE || true)"
    if [[ -z "$event_slug" ]]; then
      event_slug="$(python3 "$dotenv_py" --file "$env_file" get EVENT_SLUGS || true)"
      if [[ -z "$event_slug" ]]; then
        event_slug="$(python3 "$dotenv_py" --file "$env_file" get EVENT_SLUG || true)"
      fi
    fi
    token_a="$(python3 "$dotenv_py" --file "$env_file" get TOKEN_A || true)"
    token_b="$(python3 "$dotenv_py" --file "$env_file" get TOKEN_B || true)"
    if [[ -z "$event_slug" && ( -z "$token_a" || -z "$token_b" ) ]]; then
      echo "Deploy needs config: set ARBITRAGE_*_DEPLOY_ARGS, EVENT_SLUGS_FILE/EVENT_SLUGS/EVENT_SLUG, or TOKEN_A/TOKEN_B in $env_file" >&2
      exit 2
    fi
  fi

  echo "Deploying on $ssh_server:"
  echo "- arbitrage ($arbitrage_service)"
  echo "- arbitrage-equal ($arbitrage_equal_service)"
  echo "- arbitrage-weighted ($arbitrage_weighted_service)"

  deploy_one() {
    local cmd_name="$1"
    local svc_name="$2"
    local args_str="$3"
    local args=()
    if [[ -n "$args_str" ]]; then
      read -r -a args <<<"$args_str"
    fi
    if [[ ${#args[@]} -gt 0 ]]; then
      ENV_FILE="$env_file" SSH_SERVER="$ssh_server" SSH_PASSWORD="$ssh_password" DEPLOY_SERVICE_NAME="$svc_name" \
        "$deploy_script" "$cmd_name" -- "${args[@]}"
    else
      ENV_FILE="$env_file" SSH_SERVER="$ssh_server" SSH_PASSWORD="$ssh_password" DEPLOY_SERVICE_NAME="$svc_name" \
        "$deploy_script" "$cmd_name"
    fi
  }

  deploy_one arbitrage "$arbitrage_service" "$arbitrage_args_str"
  deploy_one arbitrage-equal "$arbitrage_equal_service" "$arbitrage_equal_args_str"
  deploy_one arbitrage-weighted "$arbitrage_weighted_service" "$arbitrage_weighted_args_str"
}

if $deploy; then
  deploy_all
  echo
fi

if ! $show_status_only; then
  if $reload; then
    echo "Reloading (reload-or-restart) on $ssh_server: $arbitrage_service + $arbitrage_equal_service + $arbitrage_weighted_service"
    ssh_cmd "set -euo pipefail; ${sudo_prefix}systemctl reload-or-restart '$arbitrage_service' '$arbitrage_equal_service' '$arbitrage_weighted_service'"
  else
    echo "Restarting on $ssh_server: $arbitrage_service + $arbitrage_equal_service + $arbitrage_weighted_service"
    ssh_cmd "set -euo pipefail; ${sudo_prefix}systemctl restart '$arbitrage_service' '$arbitrage_equal_service' '$arbitrage_weighted_service'"
  fi
fi

echo "Status:"
status_cmd

if $print_logs; then
  echo
  echo "Recent logs (tail=$tail_lines):"
  logs_cmd
fi

if $follow_logs; then
  echo
  echo "Following logs (Ctrl+C to stop):"
  follow_cmd
fi
