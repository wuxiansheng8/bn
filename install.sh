#!/usr/bin/env bash
# =============================================================================
# 币安极速多IP监控与自动跟单系统 v3.1 — 一键安装脚本
#
#   用法：bash install.sh          （无需 chmod +x，脚本会自行赋权）
#   适配：Debian 12 / 13、Ubuntu 22.04 / 24.04
#
# 流程：sudo 提权 → 安装 git/curl/docker → 三问向导 → 生成 data/config.json
#       → docker compose build && up -d → 轮询 /api/health（最长 60 秒）
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# 通用输出（仅在终端或 FORCE_COLOR=1 时启用 ANSI 颜色）
# ---------------------------------------------------------------------------
if [ -t 1 ] || [ "${FORCE_COLOR:-0}" = "1" ]; then
  RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
  CYAN=$'\033[36m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
  RED=''; GREEN=''; YELLOW=''; CYAN=''; BOLD=''; NC=''
fi

info() { printf '%s[信息]%s %s\n' "$CYAN" "$NC" "$*"; }
ok()   { printf '%s[成功]%s %s\n' "$GREEN" "$NC" "$*"; }
warn() { printf '%s[警告]%s %s\n' "$YELLOW" "$NC" "$*"; }
err()  { printf '%s[错误]%s %s\n' "$RED" "$NC" "$*" >&2; }
die()  { err "$*"; exit 1; }

on_error() { err "安装中断（第 ${1} 行）。请根据上方输出排查后重试。"; }
trap 'on_error "$LINENO"' ERR

# ---------------------------------------------------------------------------
# 1. 切换到项目目录（支持从任意路径调用）
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SELF="$SCRIPT_DIR/$(basename -- "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR"

chmod +x "$SELF" 2>/dev/null || true
if [ -f "$SCRIPT_DIR/update.sh" ]; then
  chmod +x "$SCRIPT_DIR/update.sh" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 2. 非 root 自动 sudo 提权
# ---------------------------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    info "需要 root 权限，正在通过 sudo 重新执行脚本…"
    exec sudo -E bash "$SELF" "$@"
  fi
  die "当前不是 root 且系统未安装 sudo。请改用 root 执行：su -c 'bash $SELF'"
fi

[ -t 0 ] || die "安装向导需要交互输入，请直接在终端运行：bash install.sh"
[ -f "$SCRIPT_DIR/Dockerfile" ] || die "缺少 Dockerfile，请确认在项目根目录运行。"
[ -f "$SCRIPT_DIR/docker-compose.yml" ] || die "缺少 docker-compose.yml，请确认在项目根目录运行。"
[ -f "$SCRIPT_DIR/requirements.txt" ] || die "缺少 requirements.txt，请确认代码完整。"

# ---------------------------------------------------------------------------
# 3. 系统与基础依赖检查
# ---------------------------------------------------------------------------
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
fi
OS_ID="${ID:-unknown}"
OS_LIKE="${ID_LIKE:-}"
case " ${OS_ID} ${OS_LIKE} " in
  *debian*|*ubuntu*) ;;
  *)
    warn "当前系统（${PRETTY_NAME:-$OS_ID}）不在适配列表（Debian 12/13、Ubuntu 22.04/24.04）。"
    if [ "${SKIP_OS_CHECK:-0}" != "1" ]; then
      die "如确认继续，请运行：SKIP_OS_CHECK=1 bash install.sh"
    fi
    ;;
esac
command -v apt-get >/dev/null 2>&1 || die "未找到 apt-get，本脚本仅支持 Debian/Ubuntu。"

ARCH="$(uname -m)"
if [ "$ARCH" != "x86_64" ] && [ "$ARCH" != "aarch64" ]; then
  warn "当前架构 $ARCH 未经验证，可能无法获取预编译 Python 依赖。"
fi

APT_UPDATED=0
apt_update_once() {
  if [ "$APT_UPDATED" -eq 0 ]; then
    info "更新 apt 软件源索引…"
    apt-get update -y >/dev/null
    APT_UPDATED=1
  fi
}

apt_install() {
  apt_update_once
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@"
}

MISSING=()
command -v git     >/dev/null 2>&1 || MISSING+=(git)
command -v curl    >/dev/null 2>&1 || MISSING+=(curl)
command -v python3 >/dev/null 2>&1 || MISSING+=(python3)
command -v tar     >/dev/null 2>&1 || MISSING+=(tar)
if [ "${#MISSING[@]}" -gt 0 ]; then
  info "安装基础依赖：${MISSING[*]}"
  apt_install ca-certificates "${MISSING[@]}"
fi

# ---------------------------------------------------------------------------
# 4. Docker / Docker Compose
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  info "未检测到 Docker，开始安装（官方脚本，约 1-3 分钟）…"
  DOCKER_INSTALLER="$(mktemp /tmp/get-docker.XXXXXX.sh)"
  if ! curl -fsSL --connect-timeout 20 --retry 2 https://get.docker.com -o "$DOCKER_INSTALLER"; then
    rm -f "$DOCKER_INSTALLER"
    die "下载 Docker 安装脚本失败，请检查服务器网络或代理后重试。"
  fi
  if ! sh "$DOCKER_INSTALLER"; then
    warn "官方源安装失败，改用阿里云镜像源重试…"
    if ! sh "$DOCKER_INSTALLER" --mirror Aliyun; then
      rm -f "$DOCKER_INSTALLER"
      die "Docker 安装失败，请手动安装 Docker 后重新运行本脚本。"
    fi
  fi
  rm -f "$DOCKER_INSTALLER"
  ok "Docker 安装完成。"
else
  info "已检测到 Docker：$(docker --version)"
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl enable --now docker >/dev/null 2>&1 || warn "systemd 启动 Docker 失败，请稍后检查：systemctl status docker"
fi

if ! docker info >/dev/null 2>&1; then
  die "Docker 守护进程未运行，请先启动 Docker（systemctl start docker）后重试。"
fi

COMPOSE=()
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  info "未检测到 Docker Compose，尝试安装…"
  apt_install docker-compose-plugin >/dev/null 2>&1 || true
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
  else
    apt_install docker-compose >/dev/null 2>&1 || true
    if command -v docker-compose >/dev/null 2>&1; then
      COMPOSE=(docker-compose)
    fi
  fi
fi
if [ "${#COMPOSE[@]}" -eq 0 ]; then
  die "Docker Compose 安装失败，请手动安装后重试（Debian/Ubuntu 可 apt install docker-compose-plugin）。"
fi
compose() { "${COMPOSE[@]}" "$@"; }
info "使用 Compose：${COMPOSE[*]}"

# bcrypt 哈希依赖：宿主机没有 passlib 时先尝试 apt 安装，失败则稍后用临时容器兜底。
if ! python3 -c 'from passlib.hash import bcrypt; bcrypt.hash("probe")' >/dev/null 2>&1; then
  info "安装密码哈希依赖（python3-passlib / python3-bcrypt）…"
  apt_install python3-passlib python3-bcrypt >/dev/null 2>&1 || true
fi

EXISTING_PORT=""
if [ -f data/config.json ]; then
  EXISTING_PORT="$(python3 -c 'import json; print(json.load(open("data/config.json", encoding="utf-8")).get("port", ""))' 2>/dev/null || true)"
fi

port_in_use() {
  python3 - "$1" <<'PY'
import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.bind(("0.0.0.0", int(sys.argv[1])))
except OSError:
    sys.exit(0)          # 端口已被占用
finally:
    sock.close()
sys.exit(1)              # 端口可绑定
PY
}

# ---------------------------------------------------------------------------
# 5. 三问向导
# ---------------------------------------------------------------------------
printf '\n%s================ 币安跟单系统 v3.1 安装向导 ================%s\n\n' "$BOLD" "$NC"
info "只需回答 3 个问题；币安 API、交易员、代理、TG 全部装好后在网页《功能设置》里配置。"

# ① 网页端口
while :; do
  read -r -p "1) 网页访问端口 [8080]: " PORT || die "读取输入失败。"
  PORT="${PORT:-8080}"
  if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    warn "端口必须是 1-65535 之间的整数，请重新输入。"
    continue
  fi
  if port_in_use "$PORT"; then
    if [ -n "$EXISTING_PORT" ] && [ "$PORT" = "$EXISTING_PORT" ] \
       && [ "$(docker inspect -f '{{.State.Running}}' copybot 2>/dev/null || true)" = "true" ]; then
      warn "端口 $PORT 正由现有 copybot 容器占用，将继续安装并自动重建该容器。"
      break
    fi
    warn "端口 $PORT 已被其他程序占用，请换一个端口。"
    continue
  fi
  break
done

# ② 登录账号
while :; do
  read -r -p "2) 网页登录账号 [admin]: " ADMIN_USER || die "读取输入失败。"
  ADMIN_USER="${ADMIN_USER:-admin}"
  if [[ "$ADMIN_USER" =~ ^[A-Za-z0-9_.-]{1,32}$ ]]; then
    break
  fi
  warn "账号仅支持字母、数字、下划线、点和短横线，长度 1-32，请重新输入。"
done

# ③ 登录密码（二次确认，不能为空）
while :; do
  printf '3) 网页登录密码（输入不显示）: '
  IFS= read -r -s PASSWORD || { printf '\n'; die "读取密码失败。"; }
  printf '\n'
  if [ -z "$PASSWORD" ]; then
    warn "密码不能为空，请重新输入。"
    continue
  fi
  printf '   请再次输入密码: '
  IFS= read -r -s PASSWORD2 || { printf '\n'; die "读取密码失败。"; }
  printf '\n'
  if [ "$PASSWORD" != "$PASSWORD2" ]; then
    warn "两次输入的密码不一致，请重新输入。"
    continue
  fi
  if [ "${#PASSWORD}" -lt 8 ]; then
    warn "密码少于 8 位，安全性偏低（仍继续，建议装好后在网页修改为强密码）。"
  fi
  break
done

# ---------------------------------------------------------------------------
# 6. 生成 bcrypt 哈希（密码只经 stdin 传递，不出现在命令行 / 进程列表）
# ---------------------------------------------------------------------------
PASSWORD_HASH=""
if python3 -c 'from passlib.hash import bcrypt; bcrypt.hash("probe")' >/dev/null 2>&1; then
  if ! PASSWORD_HASH="$(printf '%s' "$PASSWORD" | python3 -c 'import sys; from passlib.hash import bcrypt; print(bcrypt.hash(sys.stdin.read()))' 2>/dev/null)"; then
    die "生成 bcrypt 哈希失败，请检查 python3-passlib / python3-bcrypt 安装。"
  fi
else
  warn "宿主机缺少 passlib，改用临时 Docker 容器生成 bcrypt 哈希（需联网拉取 python:3.11-slim）…"
  if ! PASSWORD_HASH="$(printf '%s' "$PASSWORD" | docker run --rm -i python:3.11-slim sh -c 'pip install --quiet "passlib[bcrypt]" >/dev/null 2>&1 && python -c "import sys; from passlib.hash import bcrypt; print(bcrypt.hash(sys.stdin.read()))"' 2>/dev/null)"; then
    die "生成 bcrypt 哈希失败，请检查网络后重试。"
  fi
fi

case "$PASSWORD_HASH" in
  \$2a\$*|\$2b\$*|\$2y\$*) ;;
  *) die "bcrypt 哈希格式异常，已中止（未写入任何配置）。" ;;
esac
unset PASSWORD2

# ---------------------------------------------------------------------------
# 7. 写入 data/config.json 与 .env
# ---------------------------------------------------------------------------
info "生成配置文件…"
mkdir -p data
chmod 700 data

export COPYBOT_PORT="$PORT"
export COPYBOT_ADMIN="$ADMIN_USER"
export COPYBOT_PASSWORD_HASH="$PASSWORD_HASH"
PYTHONIOENCODING=utf-8 python3 - <<'PY'
import json
import os
import pathlib
import shutil
import time

data_dir = pathlib.Path("data")
data_dir.mkdir(parents=True, exist_ok=True)
config_path = data_dir / "config.json"

defaults = {
    "port": 8080,
    "admin_username": "admin",
    "admin_password_hash": "",
    "poll_interval_sec": 2.5,
    "log_retention_days": 7,
    "live_trading_enabled": False,
    "tg_enabled": False,
    "tg_bot_token": "",
    "tg_chat_id": "",
    "tg_throttle_sec": 1800,
    "margin_cap_ratio": 0.9,
}

config = dict(defaults)
if config_path.exists():
    try:
        old = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        old = {}
    if isinstance(old, dict):
        config.update(old)
        backup = config_path.with_name(config_path.name + ".bak." + time.strftime("%Y%m%d-%H%M%S"))
        shutil.copy2(config_path, backup)
        print(f"[信息] 原配置已备份：{backup}")

config["port"] = int(os.environ["COPYBOT_PORT"])
config["admin_username"] = os.environ["COPYBOT_ADMIN"]
config["admin_password_hash"] = os.environ["COPYBOT_PASSWORD_HASH"]

config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
config_path.chmod(0o600)
print(f"[信息] 已写入配置：{config_path}（port={config['port']}，admin={config['admin_username']}）")
PY
unset COPYBOT_PASSWORD_HASH PASSWORD

if [ -f data/keyfile ]; then
  chmod 600 data/keyfile
fi
chmod 600 data/config.json

# docker compose 会自动读取项目根目录的 .env，保证手工执行 compose 命令时端口也一致。
printf 'COPYBOT_PORT=%s\n' "$PORT" > .env
chmod 600 .env
ok "配置已生成（密码仅保存 bcrypt 哈希，不落明文）。"

# ---------------------------------------------------------------------------
# 8. 构建并启动
# ---------------------------------------------------------------------------
info "构建镜像（首次安装需要数分钟，请耐心等待）…"
if ! compose build; then
  err "镜像构建失败。"
  die "请检查上方日志（常见原因：无法访问 PyPI、磁盘空间不足）后重试：bash install.sh"
fi

info "启动容器…"
if ! compose up -d; then
  err "容器启动失败。"
  compose logs --tail=80 copybot 2>/dev/null || true
  die "请根据上方容器日志排查后重试：bash install.sh"
fi

# ---------------------------------------------------------------------------
# 9. 健康检查（最长 60 秒）
# ---------------------------------------------------------------------------
http_ok() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsS --max-time 3 "$url" >/dev/null 2>&1
  else
    python3 - "$url" <<'PY' >/dev/null 2>&1
import sys
import urllib.request

urllib.request.urlopen(sys.argv[1], timeout=3).read()
PY
  fi
}

wait_health() {
  local port="$1" timeout="$2" interval="${3:-2}"
  local deadline=$((SECONDS + timeout))
  local url="http://127.0.0.1:${port}/api/health"
  while [ "$SECONDS" -lt "$deadline" ]; do
    if http_ok "$url"; then
      return 0
    fi
    sleep "$interval"
  done
  # 到点前最后再探测一次，避免刚好在边界时漏检
  http_ok "$url"
}

info "等待服务就绪（轮询 /api/health，最长 60 秒）…"
if ! wait_health "$PORT" 60 2; then
  err "服务在 60 秒内未通过健康检查。"
  compose ps 2>/dev/null || true
  compose logs --tail=120 copybot 2>/dev/null || true
  die "安装未完成。请根据上方容器日志排查后重新运行：bash install.sh"
fi
ok "服务健康检查通过。"

# ---------------------------------------------------------------------------
# 10. 完成提示
# ---------------------------------------------------------------------------
LAN_IP="$(hostname -I 2>/dev/null | awk '{print $1}')" || LAN_IP=""
if [ -z "$LAN_IP" ]; then
  LAN_IP="<服务器公网IP>"
fi

printf '\n%s================ 安装完成 ================%s\n' "$GREEN" "$NC"
printf '  访问地址 : %shttp://%s:%s%s\n' "$CYAN" "$LAN_IP" "$PORT" "$NC"
printf '  本机访问 : http://127.0.0.1:%s\n' "$PORT"
printf '  登录账号 : %s\n' "$ADMIN_USER"
printf '  登录密码 : 向导中设置的密码（仅存 bcrypt 哈希，无法找回；可重跑本脚本重置）\n'
printf '  配置文件 : %s/data/config.json\n' "$SCRIPT_DIR"
printf '  数据目录 : %s/data（含数据库与 keyfile，请定期备份）\n' "$SCRIPT_DIR"
printf '  查看日志 : %s logs -f copybot\n' "${COMPOSE[*]}"
printf '  一键升级 : bash %s/update.sh\n' "$SCRIPT_DIR"
printf '%s==========================================%s\n\n' "$GREEN" "$NC"

printf '%s提示：%s\n' "$YELLOW" "$NC"
printf '  · 打不开网页时：执行 ufw allow %s/tcp，并在云服务器安全组放行 TCP %s\n' "$PORT" "$PORT"
printf '  · 实盘总开关默认关闭；请先在网页配置 API / 交易员 / 代理，确认无误后再手动开启\n'
printf '  · 浏览器建议使用 Chrome / Edge 最新版\n'
if [ ! -d "$SCRIPT_DIR/.git" ]; then
  printf '  · 当前目录不是 git 仓库，update.sh 暂不可用；从 GitHub clone 部署即可启用一键升级\n'
fi
printf '\n'
