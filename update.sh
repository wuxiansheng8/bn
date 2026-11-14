#!/usr/bin/env bash
# =============================================================================
# 币安极速多IP监控与自动跟单系统 v3.1 — 一键升级脚本
#
#   用法：bash update.sh
#   流程：备份 data/ → git fetch → git reset --hard origin/main
#         → docker compose up -d --build → 轮询 /api/health（最长 120 秒）
#   说明：不用 git pull，避免服务器上的本地改动造成合并冲突、升级半途而废；
#         配置 / 数据库 / keyfile 全在 data/，reset 不会动未跟踪文件，升级不丢数据。
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

on_error() { err "升级中断（第 ${1} 行）。请根据上方输出排查，必要时按提示回滚。"; }
trap 'on_error "$LINENO"' ERR

# ---------------------------------------------------------------------------
# 1. 切换到项目目录（支持从任意路径调用）
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SELF="$SCRIPT_DIR/$(basename -- "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR"

chmod +x "$SELF" 2>/dev/null || true

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

[ -f "$SCRIPT_DIR/docker-compose.yml" ] || die "缺少 docker-compose.yml，请确认在项目根目录运行。"
[ -d "$SCRIPT_DIR/.git" ] || die "当前目录不是 git 仓库，无法执行代码升级。首次部署请先 git clone 本项目。"
command -v git >/dev/null 2>&1 || die "未安装 git，请先执行：apt-get install -y git"
command -v docker >/dev/null 2>&1 || die "未安装 Docker，请先执行 bash install.sh。"
if ! docker info >/dev/null 2>&1; then
  die "Docker 守护进程未运行，请先启动 Docker（systemctl start docker）后重试。"
fi

COMPOSE=()
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
fi
if [ "${#COMPOSE[@]}" -eq 0 ]; then
  die "未找到 Docker Compose（docker compose / docker-compose），请先执行 bash install.sh。"
fi
compose() { "${COMPOSE[@]}" "$@"; }

# ---------------------------------------------------------------------------
# 3. 解析当前端口（data/config.json 为准，其次 .env，最后回退 8080）
# ---------------------------------------------------------------------------
PORT=""
if [ -f data/config.json ]; then
  PORT="$(python3 -c 'import json; print(int(json.load(open("data/config.json", encoding="utf-8")).get("port", 8080)))' 2>/dev/null || true)"
fi
if [ -z "$PORT" ] && [ -f .env ]; then
  PORT="$(grep -E '^[[:space:]]*COPYBOT_PORT[[:space:]]*=' .env | tail -n 1 | cut -d= -f2- | tr -d '[:space:]"' || true)"
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
  PORT=8080
fi
export COPYBOT_PORT="$PORT"
info "当前网页端口：$PORT"

# ---------------------------------------------------------------------------
# 4. 升级前检查与 data/ 备份
# ---------------------------------------------------------------------------
REMOTE="$(git remote get-url origin 2>/dev/null || true)"
if [ -z "$REMOTE" ]; then
  die "未配置 origin 远程仓库，无法升级。请执行：git remote add origin <仓库地址>"
fi

BRANCH="${UPDATE_BRANCH:-main}"
PREV_COMMIT="$(git rev-parse HEAD 2>/dev/null || true)"
if [ -z "$PREV_COMMIT" ]; then
  die "无法读取当前 git 提交，请检查仓库是否完整。"
fi
PREV_SHORT="$(git rev-parse --short HEAD)"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  warn "检测到已跟踪文件有本地改动，git reset --hard 会丢弃它们（data/ 不受影响）。"
fi

BACKUP=""
if [ -d data ] && command -v tar >/dev/null 2>&1; then
  BACKUP="/tmp/copybot-data-$(date +%Y%m%d-%H%M%S).tar.gz"
  if tar -czf "$BACKUP" data 2>/dev/null; then
    ok "已备份 data/ → $BACKUP"
  else
    warn "data/ 备份失败，继续升级（git reset 不会删除未跟踪的 data/）。"
    BACKUP=""
  fi
fi

print_rollback() {
  printf '\n%s---------------- 回滚方法 ----------------%s\n' "$YELLOW" "$NC"
  printf '  1) 回到升级前版本：\n'
  printf '     git reset --hard %s\n' "$PREV_COMMIT"
  printf '     %s up -d --build\n' "${COMPOSE[*]}"
  if [ -n "$BACKUP" ]; then
    printf '  2) 如数据异常，恢复升级前备份：\n'
    printf '     tar -xzf %s -C %s\n' "$BACKUP" "$SCRIPT_DIR"
  fi
  printf '  3) 查看容器日志：\n'
  printf '     %s logs --tail=200 copybot\n' "${COMPOSE[*]}"
  printf '%s------------------------------------------%s\n\n' "$YELLOW" "$NC"
}

# ---------------------------------------------------------------------------
# 5. 拉取并重置到远程分支（reset --hard，不用 pull）
# ---------------------------------------------------------------------------
info "从 origin 拉取最新代码…"
if ! git fetch --prune origin; then
  err "git fetch 失败，请检查网络或仓库访问权限。"
  die "本次升级未改动任何代码，可稍后重试。"
fi

if ! git rev-parse --verify --quiet "origin/$BRANCH" >/dev/null; then
  die "远程分支 origin/$BRANCH 不存在。可用 UPDATE_BRANCH=<分支名> bash update.sh 指定分支。"
fi

info "重置到 origin/$BRANCH（丢弃服务器本地改动，data/ 不受影响）…"
if ! git reset --hard "origin/$BRANCH"; then
  err "git reset 失败。"
  print_rollback
  exit 1
fi

NEW_COMMIT="$(git rev-parse HEAD)"
NEW_SHORT="$(git rev-parse --short HEAD)"
if [ "$PREV_COMMIT" = "$NEW_COMMIT" ]; then
  info "代码已是最新（$NEW_SHORT），仍将重建镜像并重启容器。"
else
  ok "代码已更新：$PREV_SHORT → $NEW_SHORT"
fi

# data/ 本应被 .gitignore 排除；若被跟踪，reset 可能覆盖运行数据，用升级前备份恢复。
if git ls-files -- data | grep -q .; then
  warn "检测到 data/ 被 git 跟踪，正在用升级前备份恢复 data/（建议把 data/ 加入 .gitignore）。"
  if [ -n "$BACKUP" ]; then
    if ! tar -xzf "$BACKUP" -C "$SCRIPT_DIR"; then
      warn "恢复 data/ 失败，请手动从 $BACKUP 解压。"
    fi
  else
    warn "没有可用备份，data/ 可能已被仓库版本覆盖，请检查 data/config.json。"
  fi
fi

# 同步 .env，保证手工执行 docker compose 命令时端口也正确。
touch .env
if grep -q '^COPYBOT_PORT=' .env; then
  sed -i "s/^COPYBOT_PORT=.*/COPYBOT_PORT=$PORT/" .env
else
  printf 'COPYBOT_PORT=%s\n' "$PORT" >> .env
fi
chmod 600 .env

# ---------------------------------------------------------------------------
# 6. 重建并重启
# ---------------------------------------------------------------------------
info "重建并启动容器（${COMPOSE[*]} up -d --build）…"
if ! compose up -d --build; then
  err "镜像构建或容器启动失败。"
  compose ps 2>/dev/null || true
  compose logs --tail=120 copybot 2>/dev/null || true
  print_rollback
  exit 1
fi

# ---------------------------------------------------------------------------
# 7. 健康检查（最长 120 秒）
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
  local port="$1" timeout="$2" interval="${3:-3}"
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

info "等待服务就绪（轮询 /api/health，最长 120 秒）…"
if ! wait_health "$PORT" 120 3; then
  err "升级后服务在 120 秒内未通过健康检查。"
  compose ps 2>/dev/null || true
  compose logs --tail=200 copybot 2>/dev/null || true
  print_rollback
  exit 1
fi
ok "服务健康检查通过。"

# ---------------------------------------------------------------------------
# 8. 完成提示
# ---------------------------------------------------------------------------
LAN_IP="$(hostname -I 2>/dev/null | awk '{print $1}')" || LAN_IP=""
if [ -z "$LAN_IP" ]; then
  LAN_IP="<服务器公网IP>"
fi

printf '\n%s================ 升级完成 ================%s\n' "$GREEN" "$NC"
printf '  版本变化 : %s → %s\n' "$PREV_SHORT" "$NEW_SHORT"
printf '  当前版本 : %s\n' "$(git log -1 --pretty='%h %s')"
printf '  访问地址 : %shttp://%s:%s%s\n' "$CYAN" "$LAN_IP" "$PORT" "$NC"
printf '  数据备份 : %s\n' "${BACKUP:-无}"
printf '  查看日志 : %s logs -f copybot\n' "${COMPOSE[*]}"
printf '  回滚命令 : git reset --hard %s && %s up -d --build\n' "$PREV_COMMIT" "${COMPOSE[*]}"
printf '%s==========================================%s\n\n' "$GREEN" "$NC"
