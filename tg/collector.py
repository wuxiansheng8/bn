"""tg-collector —— Telegram 频道信号采集进程（v3.3）。

职责边界（铁律 12：解耦、不过度开发）：
  本进程只做四件事：登录、监听、解析、写 signals 表。
  不算仓位、不下单、不碰 ccxt —— 那些全在 copybot 主程序里。
  两个进程唯一的通信点是 data/copybot.db 的 signals 表。

用法：
  python collector.py login   [--phone +86...] [--qr]   一次性登录，生成 session
  python collector.py check   @频道                      只读体检：字段出现率/动作命中率/建议
  python collector.py run                                常驻监听（systemd 调这个）

依赖：telethon（其余全用标准库）。配置见同目录 config.json。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from telethon import TelegramClient, events
except ImportError:  # pragma: no cover
    print("缺少依赖：请在 tg 目录下执行  python -m venv venv && venv/bin/pip install telethon", file=sys.stderr)
    raise

TZ = timezone(timedelta(hours=8))
BASE = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("TG_CONFIG") or (BASE / "config.json"))
SESSION_DIR = BASE / "session"
SESSION_PATH = SESSION_DIR / "tg.session"
# 与主程序共用的数据库：默认是仓库根的 data/copybot.db，可用环境变量覆盖
DB_PATH = Path(os.environ.get("COPYBOT_DB") or (BASE.parent / "data" / "copybot.db"))

FIELD_RE = r"【{name}】\s*[：:]\s*([^\n]*)"
NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")
SNAPSHOT_MARK = "当前全部持仓"


def log(msg: str, level: str = "INFO", module: str = "tg") -> None:
    """控制台 + 写 system_logs（这样网页「日志」页也能看到 TG 侧的情况）。"""
    line = f"[{datetime.now(TZ).strftime('%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            "INSERT INTO system_logs (created_at, time_str, level, module, message) VALUES (?,?,?,?,?)",
            (int(time.time() * 1000), datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"), level, module, msg[:2000]))
        conn.commit()
        conn.close()
    except Exception:
        pass          # 日志写失败绝不能影响采集（铁律 9 的边界）


def api_credentials(cfg: dict) -> tuple[int, str]:
    """取 api_id/api_hash；缺失时给出可操作的报错，而不是 KeyError 堆栈。"""
    try:
        return int(cfg["api_id"]), str(cfg["api_hash"])
    except (KeyError, TypeError, ValueError):
        raise SystemExit("缺少 api_id/api_hash：请在同目录创建 secret.json，"
                         "内容形如 {\"api_id\": 12345, \"api_hash\": \"abcdef...\"}")


def load_config() -> dict:
    """读 config.json，并合并同目录 secret.json（api_id/api_hash 属于凭据，不进仓库）。"""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    secret = BASE / "secret.json"
    if secret.exists():
        with open(secret, encoding="utf-8") as f:
            cfg.update({k: v for k, v in json.load(f).items() if k in ("api_id", "api_hash")})
    return cfg


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------
def field_value(text: str, field_name: str | None) -> str | None:
    """取【字段】后面的值。冒号全角半角都认（频道里两种都有）。"""
    if not field_name:
        return None
    m = re.search(FIELD_RE.format(name=re.escape(field_name)), text)
    return m.group(1).strip() if m else None


def to_number(raw: str | None) -> float | None:
    """'6,516.58 SNDK' → 6516.58；解析不出返回 None。"""
    if not raw:
        return None
    m = NUM_RE.search(raw.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def detect_window(text: str, template: dict) -> tuple[str | None, str]:
    """三层动作识别：关键词 → 字段结构 → 放弃。

    返回 (action, 依据)。action 为 None 表示本条不应产生信号。
    """
    # ① 关键词：只在**标题区**匹配。
    #    不能扫全文 —— 字段名【开仓均价】里就含"开仓"，扫全文会把加仓/减仓/平仓全误判成 OPEN。
    header_lines = []
    for ln in text.splitlines():
        stripped = ln.strip()
        if re.match(r"^【[^】]+】\s*[：:]", stripped):
            continue                      # 字段行（【币种】：xxx）整行跳过
        header_lines.append(re.sub(r"^【[^】]+】", "", stripped))   # 只去掉行首前缀
    header = "\n".join(header_lines)
    for kw, act in sorted(template.get("actions", {}).items(), key=lambda kv: -len(kv[0])):
        if kw in header:
            return act, f"keyword:{kw}"
    # ② 字段结构
    f = template["fields"]
    prev = to_number(field_value(text, f.get("qty_prev")))
    now = to_number(field_value(text, f.get("qty_now")))
    if prev is not None and now is not None:
        if now > prev:
            return "INCREASE", "structure"
        if now < prev:
            return "DECREASE", "structure"
        return None, "leverage-only"          # 原=现，只改了杠杆
    if to_number(field_value(text, f.get("qty_close"))) is not None:
        return "CLOSE", "structure"
    if to_number(field_value(text, f.get("qty_open"))) is not None:
        return "OPEN", "structure"
    return None, "unrecognized"


def parse_message(text: str, cfg: dict) -> tuple[dict | None, str]:
    """把一条频道消息解析成信号；不能解析时返回 (None, 原因)。

    只认配置里登记的【前缀】；快照、公告、杠杆变更一律丢弃（返回原因便于 check 统计）。
    """
    if not text or not text.strip():
        return None, "empty"
    if SNAPSHOT_MARK in text:
        return None, "snapshot"
    m = re.search(r"【([^】]{2,24})】", text)
    if not m:
        return None, "no-prefix"
    prefix = m.group(1)
    channel = cfg.get("channels", {})
    for ch_name, ch in channel.items():
        portfolio_id = (ch.get("traders") or {}).get(prefix)
        if not portfolio_id:
            continue
        template = ch["template"]
        action, basis = detect_window(text, template)
        if not action:
            return None, basis
        f = template["fields"]
        symbol_raw = field_value(text, f.get("symbol"))
        symbol = (symbol_raw or "").split()[0].upper() if symbol_raw else ""
        side_raw = field_value(text, f.get("side")) or ""
        if "做多" in side_raw or "🟩" in side_raw:
            side = "LONG"
        elif "做空" in side_raw or "🟥" in side_raw:
            side = "SHORT"
        else:
            return None, "no-side"
        prev = to_number(field_value(text, f.get("qty_prev")))
        now = to_number(field_value(text, f.get("qty_now")))
        qty_map = {
            "OPEN": to_number(field_value(text, f.get("qty_open"))),
            "CLOSE": to_number(field_value(text, f.get("qty_close"))),
            "INCREASE": (now - prev) if (prev is not None and now is not None) else None,
            "DECREASE": (prev - now) if (prev is not None and now is not None) else None,
        }
        qty = qty_map.get(action)
        if not qty or qty <= 0:
            return None, f"no-qty({action})"
        price = to_number(field_value(text, f.get("price")))
        # pre_qty：事件发生前的交易员持仓。减仓要按 (减仓量 / 原仓位) 的比例来平我方仓位，
        # 没有它只能当"全平"，会把减仓做成清仓。OPEN 为 0；CLOSE 视为全平（pre=qty）。
        if action == "OPEN":
            pre_qty = 0.0
        elif action in ("INCREASE", "DECREASE"):
            pre_qty = prev or 0.0
        else:
            pre_qty = qty
        return {
            "portfolio_id": str(portfolio_id),
            "symbol": symbol,
            "side": side,
            "action": action,
            "qty": round(qty, 8),
            "pre_qty": round(pre_qty, 8),
            "price": price,
            "prefix": prefix,
            "basis": basis,
        }, "ok"
    return None, "unknown-prefix"


# ---------------------------------------------------------------------------
# 落库
# ---------------------------------------------------------------------------
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def save_signal(conn: sqlite3.Connection, msg_id: int, parsed: dict, event_ms: int, raw: str) -> bool:
    """写入 signals；UNIQUE(source, source_key) 保证重连/重启/重发都不会重复。"""
    cur = conn.execute(
        "INSERT OR IGNORE INTO signals"
        "(source, source_key, portfolio_id, symbol, side, action, qty, pre_qty, price, event_ms, raw)"
        " VALUES ('tg',?,?,?,?,?,?,?,?,?,?)",
        (str(msg_id), parsed["portfolio_id"], parsed["symbol"], parsed["side"],
         parsed["action"], parsed["qty"], parsed.get("pre_qty"), parsed["price"], event_ms, raw[:2000]))
    conn.commit()
    return cur.rowcount > 0


def handle_message(conn: sqlite3.Connection, cfg: dict, msg) -> None:
    text = msg.message or ""
    parsed, reason = parse_message(text, cfg)
    if not parsed:
        if reason not in ("snapshot", "empty", "no-prefix", "unknown-prefix", "leverage-only"):
            log(f"解析失败({reason}) #{msg.id}: {text[:80]}", "WARN")
        return
    event_ms = int(msg.date.timestamp() * 1000)
    if save_signal(conn, msg.id, parsed, event_ms, text):
        log(f"NEW #{msg.id} {parsed['action']} {parsed['symbol']} {parsed['side']} "
            f"qty={parsed['qty']:g} ({parsed['basis']})")


# ---------------------------------------------------------------------------
# 三个子命令
# ---------------------------------------------------------------------------
async def cmd_login(args) -> int:
    api_id, api_hash = int(args.api_id or 0), args.api_hash or ""
    if not api_id or not api_hash:
        api_id, api_hash = api_credentials(load_config())
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)
    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"已登录：{me.username or me.id}（无需重复登录）")
        await client.disconnect()
        return 0
    if args.qr:
        return await _login_qr(client)
    phone = args.phone
    if not phone:
        print("需要手机号：--phone +8613800000000", file=sys.stderr)
        return 2
    await client.send_code_request(phone)
    print(f"验证码已发送到 {phone}（若该号已在其它设备登录，码会出现在 Telegram 官方对话里）")
    if not sys.stdin.isatty():
        print("验证码登录需要交互终端；后台运行时请改用扫码模式 --qr", file=sys.stderr)
        await client.disconnect()
        return 2
    code = input("请输入验证码：").strip()
    try:
        await client.sign_in(phone, code)
    except Exception as exc:            # 需要 2FA
        if "SessionPasswordNeeded" in type(exc).__name__ or "password" in str(exc).lower():
            log("检测到两步验证，等待输入密码")
            import getpass
            pw = getpass.getpass("该账号开启了两步验证，请输入密码：")
            if not pw:
                log("未提供两步验证密码，登录中止", "ERROR")
                await client.disconnect()
                return 1
            await client.sign_in(password=pw)
        else:
            print(f"登录失败：{type(exc).__name__}: {exc}", file=sys.stderr)
            await client.disconnect()
            return 1
    me = await client.get_me()
    print(f"登录成功：{me.username or me.id}（session 已保存到 {SESSION_PATH}）")
    await client.disconnect()
    return 0


def _two_factor_password() -> str | None:
    """取两步验证密码：优先一次性文件（读完即删），其次环境变量，最后交互输入。"""
    pf = BASE / "pass.txt"
    if pf.exists():
        try:
            pw = pf.read_text(encoding="utf-8").strip()
            pf.unlink()
            return pw or None
        except Exception:
            pass
    env = os.environ.get("TG_2FA_PASSWORD")
    if env:
        return env.strip()
    if sys.stdin.isatty():
        import getpass
        return getpass.getpass("两步验证密码：").strip()
    return None


async def _login_qr(client) -> int:
    """扫码登录：本地起一个自动刷新的网页，手机扫屏幕即可（30 秒换一次码）。"""
    import http.server
    import socketserver
    import threading
    try:
        import qrcode                      # 仅扫码模式需要
    except ImportError:
        print("扫码模式需要 qrcode：venv/bin/pip install qrcode pillow\n"
              "或改用验证码登录：python collector.py login --phone +86...", file=sys.stderr)
        return 1

    qr = await client.qr_login()
    state = {"url": qr.url, "done": False}

    def render() -> bytes:
        import io
        buf = io.BytesIO()
        qrcode.make(state["url"]).save(buf, format="PNG")
        return buf.getvalue()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if state["done"]:                    # 登录成功后页面要给反馈，否则用户以为没反应（实测踩到）
                body = ("<!doctype html><meta charset=utf-8><title>登录成功</title>"
                        "<body style='text-align:center;background:#111;color:#3f3;font-family:sans-serif;padding-top:80px'>"
                        "<h2>✅ 登录成功</h2><p style='color:#aaa'>session 已保存，可以关闭此页面</p>").encode("utf-8")
                self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers(); self.wfile.write(body); return
            if self.path.startswith("/qr.png"):
                body, ctype = render(), "image/png"
            else:
                body = ("<!doctype html><meta charset=utf-8><title>TG 登录</title>"
                        "<body style='text-align:center;background:#111;color:#eee;font-family:sans-serif'>"
                        "<h3>用手机 Telegram 扫这个码</h3>"
                        "<img id=q src='/qr.png' width=340 height=340 style='background:#fff;padding:10px'>"
                        "<script>setInterval(()=>{q.src='/qr.png?t='+Date.now()},1500)</script>"
                        ).encode("utf-8")
                ctype = "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    port = 8099

    class _Reuse(socketserver.TCPServer):
        allow_reuse_address = True       # 否则重跑 login 会撞 TIME_WAIT（实测踩到）

    srv = _Reuse(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"用浏览器打开 http://<服务器IP>:{port}/ 扫码（每 25 秒自动换码）")
    while True:
        try:
            await qr.wait(timeout=25)
            break
        except asyncio.TimeoutError:
            await qr.recreate()
            state["url"] = qr.url
        except Exception as exc:
            # 账号开启两步验证时，扫码授权后会抛 SessionPasswordNeeded
            if "SessionPasswordNeeded" in type(exc).__name__:
                pw = _two_factor_password()
                if pw:
                    log("检测到两步验证，使用预置密码登录")
                if not pw:
                    print("该账号开启了两步验证，但没拿到密码（环境变量 TG_2FA_PASSWORD / 交互输入）", file=sys.stderr)
                    srv.shutdown(); await client.disconnect(); return 1
                await client.sign_in(password=pw)
                break
            raise
    state["done"] = True
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"登录成功：{me.username or me.id}（页面已显示成功，可关闭）")
        await asyncio.sleep(3)                   # 给浏览器留时间渲染成功页
    else:
        print("扫码未完成授权", file=sys.stderr)
    srv.shutdown()
    await client.disconnect()
    return 0


async def cmd_check(args) -> int:
    cfg = load_config()
    api_id, api_hash = api_credentials(cfg)
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        print("尚未登录，请先执行：python collector.py login", file=sys.stderr)
        return 2
    target = args.channel.lstrip("@")
    msgs = await client.get_messages(target, limit=args.limit)
    total = len(msgs)
    prefixes, reasons, actions, ok = {}, {}, {}, 0
    for m in msgs:
        text = m.message or ""
        pm = re.search(r"【([^】]{2,24})】", text)
        if pm:
            prefixes[pm.group(1)] = prefixes.get(pm.group(1), 0) + 1
        parsed, reason = parse_message(text, cfg)
        reasons[reason] = reasons.get(reason, 0) + 1
        if parsed:
            ok += 1
            actions[parsed["action"]] = actions.get(parsed["action"], 0) + 1
    print(f"\n频道：@{target}   样本：{total} 条\n" + "-" * 34)
    print("前缀分布：", " | ".join(f"{k} {v}" for k, v in sorted(prefixes.items(), key=lambda x: -x[1])))
    print("解析结果：", " | ".join(f"{k} {v}" for k, v in sorted(reasons.items(), key=lambda x: -x[1])))
    print("动作分布：", " | ".join(f"{k} {v}" for k, v in sorted(actions.items())) or "-")
    rate = (ok / total * 100) if total else 0
    snap = reasons.get("snapshot", 0) / total * 100 if total else 0
    print(f"可执行信号：{ok} 条（{rate:.0f}%），快照占比 {snap:.0f}%")
    if rate >= 10 and reasons.get("unrecognized", 0) <= max(2, total * 0.05):
        print("建议：可以直接接入")
    elif reasons.get("no-prefix", 0) + reasons.get("unknown-prefix", 0) > total * 0.5:
        print("建议：前缀未在 config.json 登记，先补 traders 映射")
    else:
        print("建议：格式差异较大，先补同义词/字段映射，或放弃该频道")
    await client.disconnect()
    return 0


async def cmd_run(args) -> int:
    cfg = load_config()
    api_id, api_hash = api_credentials(cfg)
    channels = list(cfg.get("channels", {}).keys())
    if not channels:
        print("config.json 里没有任何频道", file=sys.stderr)
        return 2
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        log("未登录，请先执行 login", "ERROR")
        return 2

    conn = db_conn()
    baselines = {}
    for ch in channels:
        msgs = await client.get_messages(ch, limit=1)
        baselines[ch] = msgs[0].id if msgs else 0
        log(f"基线 {ch} = #{baselines[ch]}（之前的历史不补）")

    channel_map = {c.lower(): c for c in channels}   # username 大小写不敏感，统一小写比较

    async def on_msg(event, edited=False):
        chat = (getattr(event.chat, "username", None) or "").lower()
        if chat not in channel_map:
            return
        if event.message.id <= baselines.get(channel_map[chat], 0):
            return                       # 基线之前的老消息
        handle_message(conn, cfg, event.message)

    client.add_event_handler(lambda e: on_msg(e), events.NewMessage(chats=channels))
    client.add_event_handler(lambda e: on_msg(e, edited=True), events.MessageEdited(chats=channels))
    log(f"监听中：{', '.join(channels)}")

    async def heartbeat():              # 每 10 分钟一行，便于确认进程还活着（不刷屏）
        while True:
            await asyncio.sleep(600)
            log("心跳：监听中")

    asyncio.create_task(heartbeat())
    retry = 0
    while True:
        try:
            await client.run_until_disconnected()
        except Exception as exc:          # 断线重连（Telethon 内部已重试，这里是兜底）
            retry += 1
            log(f"连接中断({retry})：{type(exc).__name__} {str(exc)[:120]}", "WARN")
            await asyncio.sleep(min(60, 5 * retry))
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    log("session 失效，需重新 login", "ERROR")
                    return 3
                log("已重连")
                retry = 0
            except Exception as exc2:
                log(f"重连失败：{type(exc2).__name__}", "ERROR")
        else:
            return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Telegram 频道信号采集（v3.3）")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_login = sub.add_parser("login", help="一次性登录，生成 session")
    p_login.add_argument("--phone", help="手机号，如 +8613800000000")
    p_login.add_argument("--qr", action="store_true", help="用扫码登录（起临时网页）")
    p_login.add_argument("--api-id", help="覆盖 config.json 里的 api_id")
    p_login.add_argument("--api-hash", help="覆盖 config.json 里的 api_hash")

    p_check = sub.add_parser("check", help="只读体检：字段/动作命中率")
    p_check.add_argument("channel", help="频道 username，如 shipan_jiankong")
    p_check.add_argument("--limit", type=int, default=200, help="取样条数，默认 200")

    sub.add_parser("run", help="常驻监听")

    args = ap.parse_args()
    if args.cmd == "login":
        return asyncio.run(cmd_login(args))
    if args.cmd == "check":
        return asyncio.run(cmd_check(args))
    return asyncio.run(cmd_run(args))


if __name__ == "__main__":
    sys.exit(main())
