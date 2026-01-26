#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/push_source_ssh.sh [options]

Pushes the *source tree* of this repo to a remote host over SSH using rsync.
Configuration is read from a local .env file (default: ./.env):

  SSH_SERVER=user@host              (required)
  SSH_PASSWORD=...                  (or SSH_KEY_PATH=... for key auth)
  SSH_PORT=22                       (optional)
  SSH_USE_SUDO=1                    (optional; needed if SSH user isn't root and remote dir is under /opt)
  DEPLOY_REMOTE_DIR=/opt/poly-gocopy (optional; default if unset)

What it syncs:
  - The repo source (everything in the repo root)
  - Excludes: .git/, out/, Go caches, bin/, .env (keeps remote secrets intact)
  - Includes: .env.example

Options:
  --delete        Delete remote files that no longer exist locally (safe with the default excludes)
  --dry-run       Print what would change but don't upload
  --remote-dir D  Override remote target dir (default: DEPLOY_REMOTE_DIR or /opt/poly-gocopy)
  --env-file F    Override env file path (default: .env)
  -h, --help      Show help

Examples:
  scripts/push_source_ssh.sh --dry-run
  scripts/push_source_ssh.sh --delete
  scripts/push_source_ssh.sh --remote-dir=/opt/poly-gocopy --delete
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

delete=false
dry_run=false
remote_dir_override=""
env_file="${ENV_FILE:-.env}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete) delete=true ;;
    --dry-run) dry_run=true ;;
    --remote-dir)
      shift
      remote_dir_override="${1:-}"
      if [[ -z "$remote_dir_override" ]]; then
        echo "Missing value for --remote-dir" >&2
        exit 2
      fi
      ;;
    --remote-dir=*)
      remote_dir_override="${1#*=}"
      if [[ -z "$remote_dir_override" ]]; then
        echo "Missing value for --remote-dir" >&2
        exit 2
      fi
      ;;
    --env-file)
      shift
      env_file="${1:-}"
      if [[ -z "$env_file" ]]; then
        echo "Missing value for --env-file" >&2
        exit 2
      fi
      ;;
    --env-file=*)
      env_file="${1#*=}"
      if [[ -z "$env_file" ]]; then
        echo "Missing value for --env-file" >&2
        exit 2
      fi
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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
dotenv_py="$repo_root/scripts/dotenv.py"

if [[ ! -f "$dotenv_py" ]]; then
  echo "Missing helper: $dotenv_py" >&2
  exit 2
fi
if [[ ! -f "$repo_root/go.mod" ]]; then
  echo "This script must live inside the poly-gocopy repo; go.mod not found at: $repo_root/go.mod" >&2
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

if [[ -z "$ssh_server" ]]; then
  echo "Missing SSH_SERVER in $env_file" >&2
  exit 2
fi
if [[ "$ssh_server" != *"@"* ]]; then
  echo "SSH_SERVER must be in user@host form (got: $ssh_server)" >&2
  exit 2
fi

remote_user="${ssh_server%@*}"
if [[ -z "$remote_user" ]]; then
  echo "Failed to parse remote user from SSH_SERVER: $ssh_server" >&2
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

remote_dir="${remote_dir_override:-${deploy_remote_dir:-/opt/poly-gocopy}}"

# Basic sanity checks to avoid nasty quoting surprises.
if [[ ! "$remote_dir" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "--remote-dir must be an absolute path with safe characters, got: $remote_dir" >&2
  exit 2
fi

out_dir="$repo_root/out/push_source"
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

# Ensure remote dir exists and is writable.
echo "[push] Ensuring remote dir exists: $ssh_server:$remote_dir"
if [[ "$remote_user" == "root" ]]; then
  ssh_cmd "set -euo pipefail; mkdir -p '$remote_dir'"
else
  if ! $use_sudo; then
    echo "Remote user is not root ($remote_user) and SSH_USE_SUDO is not enabled." >&2
    echo "Either set SSH_USE_SUDO=1 in $env_file, or use root in SSH_SERVER, or choose a user-writable --remote-dir." >&2
    exit 2
  fi
  ssh_cmd "set -euo pipefail; sudo -n mkdir -p '$remote_dir'; sudo -n chown -R '$remote_user:$remote_user' '$remote_dir'"
fi

rsync_args=( -az --info=progress2 --human-readable )
if $delete; then
  rsync_args+=( --delete )
fi
if $dry_run; then
  rsync_args+=( --dry-run )
fi

rsync_filters=(
  --exclude='.git/'
  --exclude='out/'
  --exclude='.tmp/'
  --exclude='.gopath/'
  --exclude='bin/'
  --include='.env.example'
  --exclude='.env'
  --exclude='.env.*'
)

# For rsync -e we need a single string; this is safe because we validate input and avoid spaces in key paths.
ssh_e="ssh"
for o in "${ssh_opts[@]}"; do
  ssh_e+=" $o"
done

echo "[push] rsync repo → $ssh_server:$remote_dir"
if $dry_run; then
  echo "[push] (dry-run; no files will be uploaded)"
fi

if $use_key_auth; then
  rsync "${rsync_args[@]}" "${rsync_filters[@]}" -e "$ssh_e" "$repo_root/" "$ssh_server:$remote_dir/"
else
  SSH_PASSWORD="$ssh_password" DISPLAY=dummy SSH_ASKPASS="$askpass" SSH_ASKPASS_REQUIRE=force \
    setsid -w rsync "${rsync_args[@]}" "${rsync_filters[@]}" -e "$ssh_e" "$repo_root/" "$ssh_server:$remote_dir/" < /dev/null
fi

echo "[push] Done."
