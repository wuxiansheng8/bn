"""
main.py — FastAPI 入口
M1 骨架职责：鉴权、健康检查、静态前端托管、WebSocket 广播通道、路由契约桩。
铁律 3：采集/执行不在事件循环里跑（后续阶段接入后台线程）。
铁律 12：本文件只做路由装配，业务逻辑在各自模块。
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from passlib.context import CryptContext

try:
    from . import db, phone_alert
except ImportError:
    import db  # type: ignore
    import phone_alert  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = DATA_DIR / "config.json"
FRONTEND_DIR = BASE_DIR / "frontend"

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ---------------------------------------------------------------------------
# 配置读写
# ---------------------------------------------------------------------------
def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# 鉴权（简单 session token，M1 骨架级）
# ---------------------------------------------------------------------------
_active_tokens: set[str] = set()


def verify_password(plain: str, hashed: str) -> bool:
    if not hashed:
        return False
    try:
        return pwd_ctx.verify(plain, hashed)
    except Exception:
        return False


def hash_password(plain: str) -> str:
    return pwd_ctx.hash(plain)


def check_auth(token: str | None) -> bool:
    return token in _active_tokens


# ---------------------------------------------------------------------------
# WebSocket 连接管理（实时日志 / 状态推送）
# ---------------------------------------------------------------------------
class WSManager:
    def __init__(self) -> None:
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict[str, Any]) -> None:
        """向所有在线 WebSocket 客户端广播一条消息"""
        text = json.dumps(message, ensure_ascii=False)
        dead: list[WebSocket] = []
        for ws in self.active:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_mgr = WSManager()

# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------
app = FastAPI(title="Binance Copybot v3.1", docs_url="/api/docs")


@app.on_event("startup")
async def startup() -> None:
    db.init_db()
    db.insert_log("INFO", "main", "系统启动")
    db.start_log_cleanup_scheduler(lambda: load_config().get("log_retention_days", 7))
    logger.info("DB initialized, log cleanup scheduler started")


@app.on_event("shutdown")
async def shutdown() -> None:
    db.insert_log("INFO", "main", "系统关闭")


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------
@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "ts": int(time.time() * 1000)}


# ---------------------------------------------------------------------------
# 登录 / 登出 / 鉴权校验
# ---------------------------------------------------------------------------
@app.get("/api/setup-status")
async def setup_status() -> dict[str, Any]:
    """P2-4：密码哈希为空时前端显示「设置管理员密码」页。"""
    return {"need_setup": not load_config().get("admin_password_hash")}


@app.post("/api/setup")
def setup_admin(payload: dict[str, Any]) -> JSONResponse:
    """仅在尚未设置密码时可用；设置成功后立即失效，之后只能走登录。"""
    cfg = load_config()
    if cfg.get("admin_password_hash"):
        raise HTTPException(status.HTTP_409_CONFLICT, "管理员密码已设置，不能重复初始化")
    username = str(payload.get("username") or cfg.get("admin_username") or "admin").strip()
    password = str(payload.get("password") or "")
    if not username:
        raise HTTPException(400, "账号不能为空")
    if len(password) < 8:
        raise HTTPException(400, "密码至少 8 位")
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(400, "密码不能超过 72 字节")
    cfg["admin_username"] = username
    cfg["admin_password_hash"] = hash_password(password)
    save_config(cfg)
    db.insert_log("WARN", "main", f"首次设置管理员账号 {username}")
    token = f"{username}-{int(time.time()*1000)}"
    _active_tokens.add(token)
    return JSONResponse({"token": token})


@app.post("/api/login")
async def login(payload: dict[str, Any]) -> JSONResponse:
    username = payload.get("username", "")
    password = payload.get("password", "")
    cfg = load_config()
    if username != cfg.get("admin_username"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "账号或密码错误")
    if not verify_password(password, cfg.get("admin_password_hash", "")):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "账号或密码错误")
    token = f"{username}-{int(time.time()*1000)}"
    _active_tokens.add(token)
    db.insert_log("INFO", "main", f"管理员 {username} 登录")
    return JSONResponse({"token": token})


@app.post("/api/logout")
async def logout(payload: dict[str, Any]) -> JSONResponse:
    token = payload.get("token", "")
    _active_tokens.discard(token)
    return JSONResponse({"ok": True})


def require_auth(token: str | None) -> None:
    if not check_auth(token):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "未登录或登录已过期")


# ---------------------------------------------------------------------------
# 配置读写（功能设置页用，M1 只暴露基础结构）
# ---------------------------------------------------------------------------
@app.get("/api/config")
async def get_config(token: str | None = None) -> JSONResponse:
    require_auth(token)
    cfg = load_config()
    # 不回传敏感字段
    safe = {k: v for k, v in cfg.items()
            if k not in ("admin_password_hash", "tg_bot_token")}
    return JSONResponse(safe)


@app.put("/api/config")
async def update_config(payload: dict[str, Any], token: str | None = None) -> JSONResponse:
    require_auth(token)
    cfg = load_config()
    for k, v in payload.items():
        if k in cfg and k not in ("admin_password_hash",):
            cfg[k] = v
    save_config(cfg)
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# WebSocket 实时通道
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws_mgr.connect(ws)
    try:
        while True:
            # M1 骨架：接收客户端心跳，暂不下发业务数据
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)


# ---------------------------------------------------------------------------
# 静态前端托管（frontend/index.html）
# ---------------------------------------------------------------------------
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(FRONTEND_DIR / "index.html"))


app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")


# ===========================================================================
# 以下为 M2~M7 路由契约桩（前后端 API / WebSocket 契约定稿，M1 阶段先占位）
# 后续阶段由后端子智能体填充实现，前端子智能体按此契约对接。
# ===========================================================================

# --- 账户总览 (M4) ---
# GET  /api/accounts                  → [{id, remark, enabled, trader, equity, available, ...}]
# GET  /api/accounts/{id}/positions   → 当前持仓列表

# --- 交易员监控 (M4) ---
# GET  /api/traders                   → 交易员列表 + 四宫格数据
# GET  /api/traders/{pid}/orders      → 近期操作 (order-history)
# GET  /api/traders/{pid}/positions-hist → 历史仓位 (position-history)

# --- 仓位与订单 (M4/M5) ---
# GET  /api/copy-orders              → 我方历史跟单订单
# POST /api/position/{id}/close-half → 手动平半
# POST /api/position/{id}/close-all  → 手动全平
# POST /api/position/{id}/tp-sl      → 挂价格止盈止损
# POST /api/position/{id}/resume     → 解除接管·恢复跟单

# --- 功能设置 (M5) ---
# GET  /api/settings/api-accounts     → API 账户列表(掩码)
# POST /api/settings/api-accounts    → 新增/修改 API 账户
# DELETE /api/settings/api-accounts/{id}
# POST /api/settings/api-accounts/{id}/check  → 检测 API
# GET  /api/settings/traders          → 监控交易员列表
# POST /api/settings/traders          → 新增交易员(粘贴 URL/portfolioId)
# DELETE /api/settings/traders/{pid}
# GET  /api/settings/proxies          → 代理节点列表
# POST /api/settings/proxies          → 新增/批量导入代理
# DELETE /api/settings/proxies/{id}
# POST /api/settings/proxies/{id}/check → 检测单代理
# POST /api/settings/proxies/check-all   → 一键检测全部
# GET  /api/settings/poll-tg          → 轮询节拍 + TG 配置
# PUT  /api/settings/poll-tg          → 保存节拍 + TG
# POST /api/settings/tg/test           → 发送测试通知

# --- 日志 (M4) ---
# GET  /api/logs?level=&keyword=&api=&trader=&limit=&offset= → 倒序日志
# PUT  /api/logs/retention             → 设置自动清理天数

# --- 全局控制 (M5~M7) ---
# GET  /api/system/status              → 实盘总开关 + 在线状态
# PUT  /api/system/live-toggle          → 开关实盘
# POST /api/system/emergency-close-all  → 一键紧急全平所有账户
# GET  /api/system/price-stream-status   → 标记价格流状态

# WebSocket /ws 推送消息类型契约:
# {type:"log", ...}            实时日志插入
# {type:"trader_update", ...}  交易员状态刷新
# {type:"position_update",...} 持仓刷新
# {type:"account_update",...}  账户刷新
# {type:"order_update", ...}   跟单订单刷新

# M5 runtime services and REST API implementation.
import re
import threading
from urllib.parse import urlparse, parse_qs
import requests
try:
    from . import bapi, executor, notifier, price_stream
    from .proxy_pool import ProxyPool, _normalize_proxy
    from .collector import acquire_page_proxy, bootstrap_trader, init_collector, rows_of, start_collector, stop_collector
except ImportError:
    import bapi, executor, notifier, price_stream  # type: ignore
    from proxy_pool import ProxyPool, _normalize_proxy  # type: ignore
    from collector import acquire_page_proxy, bootstrap_trader, init_collector, rows_of, start_collector, stop_collector  # type: ignore
from cryptography.fernet import Fernet

KEYFILE = DATA_DIR / "keyfile"
db.init_db()
pool = ProxyPool()
ccxt_clients: dict[int, object] = {}


def _get_fernet() -> Fernet:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not KEYFILE.exists():
        KEYFILE.write_bytes(Fernet.generate_key())
        try: KEYFILE.chmod(0o600)
        except OSError: pass
    return Fernet(KEYFILE.read_bytes())


def encrypt(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    return _get_fernet().decrypt(ciphertext.encode()).decode()


def _mask(ciphertext: str) -> str:
    try: key = decrypt(ciphertext)
    except Exception: return "****"
    return f"{key[:4]}***{key[-4:]}" if len(key) > 8 else "*" * len(key)


def _account_json(row) -> dict:
    """账户 JSON：同时给出前端约定的 cached_* 字段与短名（equity/available）。"""
    return {"id": row["id"], "remark": row["remark"], "api_key": _mask(row["api_key_enc"]),
            "enabled": bool(row["is_enabled"]), "is_enabled": bool(row["is_enabled"]),
            "copy_ratio": row["copy_ratio"], "default_leverage": row["default_leverage"],
            "followed_trader_id": row["followed_trader_id"],
            "equity": row["cached_equity"], "available": row["cached_available"],
            "cached_equity": row["cached_equity"], "cached_available": row["cached_available"],
              "updated_at": row["updated_at"],
            "realized_pnl": row["realized_pnl"] or 0.0,
            "created_at": (row["created_at"] or 0) if "created_at" in row.keys() else 0,
            "pnl_since_ms": (row["pnl_since_ms"] or 0) if "pnl_since_ms" in row.keys() else 0,
            "followed_channel": (row["followed_channel"] or "") if "followed_channel" in row.keys() else "",
            "status_msg": row["status_msg"]}


def _auth(token):
    require_auth(token)


def _read_with_proxy(fetch, *args, **kwargs):
    """用户点出来的只读查询：借一个代理发一次，限流就刷新冷却后换一个再试一次。

    只读接口等 2 秒可以接受（不占主节拍），所以这里允许等待；主调度仍是非阻塞的。
    """
    last_exc: Exception | None = None
    for _ in range(2):
        proxy = acquire_page_proxy(2.5)
        try:
            return fetch(*args, proxies=(proxy or {}).get("proxies"), **kwargs)
        except bapi.BapiRateLimitError as exc:
            last_exc = exc
            if proxy:
                pool.release(proxy["url"], False, 0, bapi.ERR_IP_BUSY)
                proxy = None
        except Exception:
            if proxy:
                pool.release(proxy["url"], False, 0)
                proxy = None
            raise
        finally:
            if proxy:
                pool.release(proxy["url"], True)
    if last_exc:
        raise last_exc
    raise HTTPException(502, "查询失败")


def _rowdict(row):
    return dict(row) if row is not None else None


@app.on_event("startup")
async def m5_startup() -> None:
    global ccxt_clients
    db.init_db()
    _get_fernet()
    pool.reload_from_db()
    conn = db.get_conn()
    ccxt_clients = executor.init_ccxt_clients(conn)
    init_collector(pool, ws_mgr.broadcast, load_config)
    import asyncio
    # 包内导入必须用相对形式：以 backend.main:app 启动时（Docker CMD / install.sh）
    # 顶层没有 collector 模块，裸导入会 ModuleNotFoundError 直接启动失败。
    # 保留绝对形式回退，兼容 cd backend && uvicorn main:app 的本地启动方式。
    try:
        from .collector import set_event_loop
    except ImportError:  # pragma: no cover - 仅本地以顶层模块启动时才走这里
        from collector import set_event_loop
    loop = asyncio.get_running_loop()
    set_event_loop(loop)

    def _push_log(message: dict) -> None:
        """后台线程写日志时，线程安全地推给所有 WebSocket 客户端（P1-13）。"""
        if loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(ws_mgr.broadcast(message), loop)

    db.set_log_hook(_push_log)
    # Collector bootstrap performs synchronous REST calls; isolate startup from the event loop.
    threading.Thread(target=start_collector, daemon=True, name="collector-startup").start()
    executor.start_executor(ccxt_clients, _push_log, load_config)   # 传线程安全的同步包装
    price_stream.start_price_stream(ws_mgr.broadcast)


@app.on_event("shutdown")
async def m5_shutdown() -> None:
    stop_collector()
    executor.stop_executor()
    price_stream.stop_price_stream()


_balance_refreshed_at: dict[int, float] = {}
_pnl_refreshed_at: dict[int, float] = {}
PNL_REFRESH_SEC = 60.0     # 已实现盈亏/净入金：只在平仓时变，10 秒级没意义（v3.2 由 5 秒改为 60 秒）


def _refresh_balances(conn, rows, min_interval: float = 5.0) -> None:
    """打开账户总览时向币安拉一次真实余额与累计已实现盈亏（方案 B）。

    必须是同步函数，由 FastAPI 丢进线程池执行（铁律 3：禁止在事件循环里调同步 requests）。
    - 余额：`min_interval`（5 秒）节流；
    - 已实现盈亏 / 净入金：`PNL_REFRESH_SEC`（60 秒）节流，另外开/平仓后由 executor 立即刷新并 WS 推送。
    """
    now = time.time()
    changed = False
    for row in rows:
        aid = int(row["id"])
        if not row["is_enabled"]:
            continue
        client = ccxt_clients.get(aid)
        if not client:
            continue
        if now - _balance_refreshed_at.get(aid, 0) >= min_interval:
            try:
                equity, available = executor._account_equity(client)
                conn.execute("UPDATE api_accounts SET cached_equity=?, cached_available=?, updated_at=? WHERE id=?",
                             (equity, available, int(now), aid))
                _balance_refreshed_at[aid] = now
                changed = True
            except Exception as exc:
                db.insert_log("WARN", "api", f"刷新账户余额失败 {aid}: {exc}")
        if now - _pnl_refreshed_at.get(aid, 0) >= PNL_REFRESH_SEC:
            try:
                since = 0
                if "pnl_since_ms" in row.keys():
                    since = int(row["pnl_since_ms"] or 0) or int((row["created_at"] if "created_at" in row.keys() else 0) or 0)
                total = executor.realized_pnl_total(client, since)
                conn.execute("UPDATE api_accounts SET realized_pnl=? WHERE id=?", (total, aid))
                _pnl_refreshed_at[aid] = now
                changed = True
            except Exception as exc:
                db.insert_log("WARN", "api", f"刷新已实现盈亏失败 {aid}: {exc}")
    if changed:
        conn.commit()


@app.get("/api/accounts")
def accounts(token: str | None = None):
    _auth(token); conn=db.get_conn()
    rows=conn.execute("SELECT * FROM api_accounts ORDER BY id").fetchall()
    _refresh_balances(conn, rows)            # 方案 B：进页面就拉一次真实余额
    rows=conn.execute("SELECT * FROM api_accounts ORDER BY id").fetchall()
    return [_account_json(r) for r in rows]


@app.get("/api/accounts/{account_id}/positions")
def account_positions(account_id: int, token: str | None = None):
    """同步 def：FastAPI 自动放进线程池，不堵事件循环（铁律 3）。输出统一结构，不返回 ccxt 原始字段。"""
    _auth(token); client=ccxt_clients.get(account_id)
    if not client: raise HTTPException(404,"账户 CCXT 客户端不可用")
    try: raw=client.fetch_positions()
    except Exception as exc: db.insert_log("ERROR","api",f"查询账户持仓失败 {account_id}: {exc}"); raise HTTPException(502,str(exc))
    conn=db.get_conn()
    acc=conn.execute("SELECT followed_trader_id FROM api_accounts WHERE id=?",(account_id,)).fetchone()
    nick=""
    if acc and acc["followed_trader_id"]:
        t=conn.execute("SELECT nickname FROM traders WHERE portfolio_id=?",(acc["followed_trader_id"],)).fetchone(); nick=t["nickname"] if t else ""
    out=[]
    for p in raw:
        info=p.get("info",{}) or {}
        qty=abs(float(p.get("contracts") or info.get("positionAmt") or 0))
        if qty<=0: continue
        symbol=str(info.get("symbol") or p.get("symbol","")).replace("/","").replace(":USDT","")
        side=str(info.get("positionSide") or "").upper()
        if side not in ("LONG","SHORT"): side="LONG" if str(p.get("side","")).lower()=="long" else "SHORT"
        entry=float(p.get("entryPrice") or info.get("entryPrice") or 0); mark=float(p.get("markPrice") or info.get("markPrice") or 0) or None
        upnl=float(p.get("unrealizedPnl") or info.get("unRealizedProfit") or 0)
        notional=qty*(mark or entry)
        ov=conn.execute("SELECT is_overridden FROM manual_overrides WHERE api_account_id=? AND symbol=? AND position_side=?",(account_id,symbol,side)).fetchone()
        out.append({"account_id":account_id,"symbol":symbol,"position_side":side,"qty":qty,"avg_price":entry,"mark_price":mark,
                    "unrealized_pnl":upnl,"pnl_pct":(upnl/(qty*entry) if qty*entry else 0),"is_overridden":bool(ov and ov["is_overridden"]),"trader_nickname":nick})
    return out


def _trader_symbols(row) -> list[str]:
    """交易员当前持仓的币种（供标记价快照过滤）。"""
    try:
        data = json.loads(row.get("hidden_positions_json") or "{}")
    except Exception:
        return []
    if not isinstance(data, dict): return []
    return [str(s).upper() for s, sides in data.items()
            if isinstance(sides, dict) and any(isinstance(v, dict) and v.get("qty") for v in sides.values())]


@app.get("/api/traders")
async def traders(token: str | None = None):
    _auth(token)
    rows = [dict(r) for r in db.get_conn().execute("SELECT * FROM traders ORDER BY last_sync_ts DESC").fetchall()]
    prices = price_stream.snapshot()          # 首屏用标记价算浮盈亏，之后由 WS 实时更新
    for row in rows:
        row["mark_prices"] = {s: prices[s] for s in _trader_symbols(row) if s in prices}
    return rows


@app.get("/api/traders/{pid}/orders")
def trader_orders(pid: str, token: str | None = None):
    """近期操作：返回拆好的记录数组（前端直接 v-for，不下发币安外层 code/data 包装）。"""
    _auth(token)
    try: return rows_of(_read_with_proxy(bapi.fetch_order_history, pid))
    except Exception as exc: db.insert_log("ERROR","api",f"查询交易员订单失败 {pid}: {exc}"); raise HTTPException(502,str(exc))


@app.get("/api/traders/{pid}/positions-hist")
def trader_positions_hist(pid: str, token: str | None = None):
    """历史仓位：返回统一字段（positionSide/qty/openTime/closeTime/pnl…），见 bapi.normalize_position_history。"""
    _auth(token)
    try: return bapi.normalize_position_history(rows_of(_read_with_proxy(bapi.fetch_position_history, pid)))
    except Exception as exc: db.insert_log("ERROR","api",f"查询交易员历史仓位失败 {pid}: {exc}"); raise HTTPException(502,str(exc))


@app.get("/api/copy-orders")
async def copy_orders(limit: int=50, offset: int=0, token: str | None=None):
    _auth(token); limit=max(1,min(limit,500)); offset=max(0,offset); conn=db.get_conn()
    return [dict(r) for r in conn.execute("SELECT * FROM copy_orders ORDER BY created_at DESC LIMIT ? OFFSET ?",(limit,offset)).fetchall()]


def _pos_key(payload: dict[str,Any]):
    """手动操作主键 = (api_account_id, symbol, position_side)，直接操作交易所实际持仓（文档 5.4）。"""
    try:
        aid=int(payload["account_id"]); symbol=str(payload["symbol"]).upper(); side=str(payload["position_side"]).upper()
    except (KeyError, TypeError, ValueError): raise HTTPException(400,"需要 account_id / symbol / position_side")
    if side not in ("LONG","SHORT"): raise HTTPException(400,"position_side 必须是 LONG 或 SHORT")
    acc=db.get_conn().execute("SELECT followed_trader_id FROM api_accounts WHERE id=?",(aid,)).fetchone()
    return aid,symbol,side,(acc["followed_trader_id"] if acc and acc["followed_trader_id"] else "")


@app.post("/api/position/close-half")
def close_half(payload: dict[str,Any], token: str | None=None):
    _auth(token); aid,sym,side,pid=_pos_key(payload)
    try: return executor.close_position(aid,sym,side,.5,pid)
    except Exception as exc: db.insert_log("ERROR","api",f"手动平半失败 {aid} {sym}: {exc}"); raise HTTPException(502,str(exc))


@app.post("/api/position/close-all")
def close_all(payload: dict[str,Any], token: str | None=None):
    _auth(token); aid,sym,side,pid=_pos_key(payload)
    try: return executor.close_position(aid,sym,side,1,pid)
    except Exception as exc: db.insert_log("ERROR","api",f"手动全平失败 {aid} {sym}: {exc}"); raise HTTPException(502,str(exc))


@app.post("/api/position/tp-sl")
def position_tp_sl(payload: dict[str,Any], token: str | None=None):
    _auth(token); aid,sym,side,pid=_pos_key(payload)
    try: return executor.set_tp_sl(aid,sym,side,payload.get("tp_price"),payload.get("sl_price"),pid)
    except Exception as exc: db.insert_log("ERROR","api",f"设置止盈止损失败 {aid} {sym}: {exc}"); raise HTTPException(502,str(exc))


@app.post("/api/position/resume")
def position_resume(payload: dict[str,Any], token: str | None=None):
    _auth(token); aid,sym,side,_=_pos_key(payload); conn=db.get_conn()
    conn.execute("UPDATE manual_overrides SET is_overridden=0 WHERE api_account_id=? AND symbol=? AND position_side=?",(aid,sym,side)); conn.commit()
    return {"ok":True}


@app.get("/api/settings/api-accounts")
def settings_accounts(token: str | None=None): return accounts(token)


def _parse_since(value) -> int:
    """解析「盈亏统计起点」：空/0 = 全部历史；支持毫秒时间戳或 YYYY-MM-DD（按北京时间 00:00）。"""
    if value in (None, "", 0, "0"):
        return 0
    if isinstance(value, (int, float)):
        ms = int(value)
    else:
        text = str(value).strip()
        try:
            ms = int(datetime.strptime(text[:10], "%Y-%m-%d")
                     .replace(tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
        except Exception:
            raise HTTPException(400, "盈亏统计起点格式错误：应为 YYYY-MM-DD 或留空")
    if ms > int(time.time() * 1000):
        raise HTTPException(400, "盈亏统计起点不能晚于当前时间")
    if ms < executor.PNL_EARLIEST_MS:
        raise HTTPException(400, "盈亏统计起点不能早于 2024-01-01（币安接口限制，更早会被退回最近 7 天）")
    return ms


@app.post("/api/settings/api-accounts")
async def save_api_account(payload: dict[str,Any], token: str | None=None):
    _auth(token)
    editing=bool(payload.get("id"))
    # 新增时三项必填；编辑时 api_key/api_secret 留空表示不修改（P1-1）
    required=("remark",) if editing else ("remark","api_key","api_secret")
    if any(not payload.get(k) for k in required): raise HTTPException(400,"新增时 remark、api_key、api_secret 必填" if not editing else "remark 必填")
    trader=payload.get("followed_trader_id") or None
    conn=db.get_conn()
    # 2026-11-19 用户要求：允许同一交易员被多个启用账户同时跟随（各账户按自己净值与比例独立算量），
    # 不再返回 409。铁律 11 限制的是「一个账户绑定多个交易员」，不是「一个交易员只能有一个账户」。
    if trader:
        others=conn.execute("SELECT count(*) FROM api_accounts WHERE followed_trader_id=? AND is_enabled=1 AND id!=?",(trader,payload.get("id",-1))).fetchone()[0]
        if others: db.insert_log("INFO","api",f"交易员 {trader} 已有 {others} 个启用账户跟随，本次允许并存（多账户将各自下单）")
    common=(payload["remark"],float(payload.get("copy_ratio",1)),int(payload.get("default_leverage",10)),int(bool(payload.get("is_enabled",True))),trader)
    # v3.3：跟随目标二选一 —— 交易员（bapi）或 TG 频道。同时给两个直接拒绝，
    # 否则该账户会同时消费两个源，等于重复下单。
    channel=str(payload.get("followed_channel") or "").strip() or None
    if trader and channel:
        raise HTTPException(400,"跟随交易员与 TG 频道只能选一个")
    if channel and not conn.execute("SELECT 1 FROM tg_channels WHERE channel=?",(channel,)).fetchone():
        raise HTTPException(400,f"TG 频道 {channel} 不存在，请先在「TG 频道设置」里添加")
    # v3.2：盈亏统计起点（0 = 全部历史）。允许传 ms 时间戳或 YYYY-MM-DD 字符串，留空/0 即全部。
    since_ms = _parse_since(payload.get("pnl_since_ms"))
    if editing:
        if bool(payload.get("api_key")) != bool(payload.get("api_secret")):
            raise HTTPException(400,"修改密钥时 api_key 和 api_secret 必须同时填写")
        account_id=int(payload["id"])
        conn.execute("UPDATE api_accounts SET remark=?,copy_ratio=?,default_leverage=?,is_enabled=?,followed_trader_id=?,followed_channel=?,pnl_since_ms=?,updated_at=? WHERE id=?",(*common,channel,since_ms,int(time.time()),account_id))
        if payload.get("api_key") and payload.get("api_secret"):
            conn.execute("UPDATE api_accounts SET api_key_enc=?,api_secret_enc=? WHERE id=?",(encrypt(payload["api_key"]),encrypt(payload["api_secret"]),account_id))
    else:
        values=(common[0],encrypt(payload["api_key"]),encrypt(payload["api_secret"]),*common[1:])
        cur=conn.execute("INSERT INTO api_accounts(remark,api_key_enc,api_secret_enc,copy_ratio,default_leverage,is_enabled,followed_trader_id,followed_channel,pnl_since_ms,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(*values,channel,since_ms,int(time.time()*1000),int(time.time())))
        account_id=cur.lastrowid
    _pnl_refreshed_at.pop(account_id, None)      # 改了统计起点就立即按新口径重算，不等 60 秒
    conn.commit()
    try:
        executor_clients=executor.init_ccxt_clients(conn)
        ccxt_clients.update(executor_clients)
    except Exception as exc: db.insert_log("ERROR","api",f"刷新 CCXT 客户端失败: {exc}")
    return {"ok":True,"id":account_id}


@app.delete("/api/settings/api-accounts/{account_id}")
async def delete_api_account(account_id: int, token: str | None=None):
    _auth(token); conn=db.get_conn(); conn.execute("DELETE FROM api_accounts WHERE id=?",(account_id,)); conn.commit(); ccxt_clients.pop(account_id,None); return {"ok":True}


@app.post("/api/settings/api-accounts/{account_id}/check")
def check_api_account(account_id: int, token: str | None=None):
    """检测 API：连通/额度/提现权限/持仓模式。成功后把余额写回缓存，总览页立刻有数。"""
    _auth(token); client=ccxt_clients.get(account_id)
    if not client: raise HTTPException(404,"账户客户端不存在")
    result={"ok":True,"connected":False,"equity":None,"available":None,"withdraw_enabled":None,
            "futures_enabled":None,"dual_side_position":None,"errors":[]}
    try:
        balance=client.fetch_balance({"type":"future"}); result["connected"]=True
        info=balance.get("info",{}) or {}
        result["equity"]=balance.get("total",{}).get("USDT") or info.get("totalMarginBalance")
        result["available"]=balance.get("free",{}).get("USDT") or info.get("availableBalance")
    except Exception as exc:
        db.insert_log("ERROR","api",f"API 检测失败(余额) {account_id}: {exc}"); raise HTTPException(502,f"连通/签名失败：{exc}")
    # 检测通过就把余额写回缓存：不然总览页要等第一笔下单才有数字（文档 5.2）
    if result["connected"]:
        def _num(value):
            try: return float(value)
            except (TypeError, ValueError): return None
        equity,available=_num(result["equity"]),_num(result["available"])
        if equity is not None or available is not None:
            try:
                conn=db.get_conn()
                conn.execute("UPDATE api_accounts SET cached_equity=COALESCE(?,cached_equity), "
                             "cached_available=COALESCE(?,cached_available), updated_at=? WHERE id=?",
                             (equity,available,int(time.time()),account_id))
                conn.commit()
            except Exception as exc:
                db.insert_log("WARN","api",f"检测结果写入余额缓存失败 {account_id}: {exc}")
    # 方法名以 ccxt 实际暴露为准；缺失时给出明确提示而不是静默失败
    fn=getattr(client,"sapi_get_account_apirestrictions",None)
    if fn is None: result["errors"].append("当前 ccxt 版本没有 apiRestrictions 方法，无法检测提现权限")
    else:
        try:
            info=fn(); info=info.get("data",info) if isinstance(info,dict) else {}
            result["withdraw_enabled"]=bool(info.get("enableWithdrawals",False)); result["futures_enabled"]=bool(info.get("enableFutures",False))
        except Exception as exc: result["errors"].append(f"提现权限检测失败：{exc}")
    fn=getattr(client,"fapiprivate_get_positionside_dual",None)
    if fn is None: result["errors"].append("当前 ccxt 版本没有持仓模式查询方法")
    else:
        try:
            dual=fn(); result["dual_side_position"]=str(dual.get("dualSidePosition")).lower()=="true"
        except Exception as exc: result["errors"].append(f"持仓模式检测失败：{exc}")
    if result["dual_side_position"] is False: result["errors"].append("账户不是双向持仓(Hedge)模式，带 positionSide 的下单会被拒")
    if result["withdraw_enabled"]: result["errors"].append("提现权限已开启，请先在币安关闭")
    return result


@app.get("/api/settings/traders")
async def settings_traders(token: str | None=None): return await traders(token)


def _portfolio_id_from(raw: str) -> str:
    """从带单主页 URL 或纯 portfolioId 中提取 ID。

    兼容的链接形态（2026-11-14 实测）：
      https://www.binance.com/zh-CN/copy-trading/lead-details/5075281354358777856?timeRange=7D
      https://www.binance.com/zh-CN/copy-trading/portfolio/<id>
      https://www.binance.com/...?portfolioId=<id>
    2026-11-19 修正：粘贴来的链接常缺 "http" 前缀（甚至缺首字母），
    旧实现遇到不含 "http" 就原样返回，导致整段 URL 被当成 portfolioId 入库、
    后续连删除都 404。现改为：只要不是纯净 ID 就按 URL 解析。
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_-]+", raw):  # 纯 ID：字母数字/下划线/连字符
        return raw
    target = raw if "://" in raw else "http://" + raw.lstrip("/")
    try:
        parsed = urlparse(target)
    except Exception:
        parsed = None
    if parsed is not None:
        query = parse_qs(parsed.query)
        for key in ("portfolioId", "portfolio_id", "leadPortfolioId"):
            values = query.get(key)
            if values and values[0].strip():
                return values[0].strip()
        match = re.search(r"/(?:lead-details|leadDetails|portfolio)/([A-Za-z0-9_-]+)", parsed.path)
        if match:
            return match.group(1)
        for segment in reversed([s for s in parsed.path.split("/") if s]):
            if segment.isdigit():
                return segment
    digits = re.findall(r"\d{10,}", raw)  # 兜底：整段里最后一个长数字
    if digits:
        return digits[-1]
    return ""


@app.post("/api/settings/traders")
def add_trader(payload: dict[str,Any], token: str | None=None):
    """新增交易员：落库后立刻建一次底账，并把昵称/保证金/推算持仓数回显（文档 5.5）。

    同步 def：底账要连发 4~5 个请求（秒级），必须走线程池，不能堵事件循环。
    """
    _auth(token); raw=str(payload.get("portfolio_id") or payload.get("url") or "").strip()
    pid=_portfolio_id_from(raw)
    if not pid: raise HTTPException(400,"无法从 URL 解析 portfolioId")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}",pid):
        raise HTTPException(400,f"portfolioId 格式非法（粘贴的整段链接未能解析）：{pid[:80]}")
    conn=db.get_conn()
    conn.execute("INSERT INTO traders(portfolio_id,nickname,source_url,status) VALUES(?,?,?,'NORMAL') "
                 "ON CONFLICT(portfolio_id) DO UPDATE SET source_url=excluded.source_url, status='NORMAL', error_msg=''",
                 (pid, payload.get("nickname") or pid, raw))
    conn.commit()
    info=bootstrap_trader(pid)
    if info is None:
        db.insert_log("WARN","api",f"交易员 {pid} 已加入，但底账初始化失败")
        return {"ok":True,"portfolio_id":pid,"initialized":False,
                "message":"已加入，但底账初始化失败（代理或网络异常，详见日志）；可稍后点【重置状态】重试"}
    return {"ok":True,"portfolio_id":pid,"initialized":True,
            "nickname":info["nickname"] or pid,"margin_balance":info["margin_balance"],
            "orders":info.get("orders"),"positions":info["positions"]}


@app.delete("/api/settings/traders/{pid:path}")
async def remove_trader(pid: str, token: str | None=None):
    _auth(token); conn=db.get_conn()
    if conn.execute("SELECT 1 FROM api_accounts WHERE followed_trader_id=?",(pid,)).fetchone(): raise HTTPException(409,"仍有 API 账户绑定该交易员")
    conn.execute("DELETE FROM traders WHERE portfolio_id=?",(pid,)); conn.commit(); return {"ok":True}


@app.post("/api/settings/traders/{pid}/reset")
def reset_trader(pid: str, token: str | None=None):
    """铁律 9：重置交易员状态并重建一次底账（新加交易员没建成功时的重试入口）。

    同步 def：重建底账要连发多个请求（秒级），必须走线程池。
    """
    _auth(token); conn=db.get_conn()
    if not conn.execute("SELECT 1 FROM traders WHERE portfolio_id=?",(pid,)).fetchone(): raise HTTPException(404,"交易员不存在")
    conn.execute("UPDATE traders SET status='NORMAL', error_msg='' WHERE portfolio_id=?",(pid,)); conn.commit()
    db.insert_log("INFO","api",f"交易员 {pid} 状态已手动重置，开始重建底账")
    info=bootstrap_trader(pid)
    if info is None:
        return {"ok":True,"initialized":False,"message":"状态已重置，但底账重建失败（详见日志）"}
    return {"ok":True,"initialized":True,"nickname":info["nickname"] or pid,
            "margin_balance":info["margin_balance"],"orders":info.get("orders"),"positions":info["positions"]}


@app.get("/api/settings/proxies")
async def settings_proxies(token: str | None=None):
    _auth(token); return [dict(r) for r in db.get_conn().execute("SELECT * FROM proxy_nodes ORDER BY id").fetchall()]


@app.post("/api/settings/proxies")
async def add_proxies(payload: dict[str,Any], token: str | None=None):
    """批量添加代理：逐行校验，格式不对的直接回原因（文档 5.5 模块 3）。

    全角冒号、漏写 // 等由 _normalize_proxy 兜住；入库统一存规范化后的地址，
    这样同一个代理的几种写法不会重复占位。
    """
    _auth(token); items=payload.get("proxies") or [payload]
    if isinstance(items,str): items=[{"proxy_url":v.strip()} for v in items.splitlines() if v.strip()]
    conn=db.get_conn(); added=0; rejected=[]
    for line_no, item in enumerate(items, start=1):
        raw=str(item.get("proxy_url") or item.get("url") or "").strip()
        if not raw: continue
        try:
            normalized,_=_normalize_proxy(raw)
        except (ValueError, TypeError) as exc:
            rejected.append({"line":line_no,"proxy":raw,"error":str(exc)}); continue
        cur=conn.execute("INSERT OR IGNORE INTO proxy_nodes(proxy_url,display_name,status,updated_at) "
                         "VALUES(?,?,'UNTESTED',?)",
                         (normalized, item.get("display_name") or normalized, int(time.time())))
        added+=cur.rowcount
    conn.commit(); pool.reload_from_db()
    return {"ok":True,"added":added,"rejected":rejected}


@app.delete("/api/settings/proxies/{proxy_id}")
async def remove_proxy(proxy_id: int, token: str | None=None):
    _auth(token); conn=db.get_conn(); conn.execute("DELETE FROM proxy_nodes WHERE id=?",(proxy_id,)); conn.commit(); pool.reload_from_db(); return {"ok":True}


def _test_proxy_id():
    row=db.get_conn().execute("SELECT portfolio_id FROM traders ORDER BY rowid LIMIT 1").fetchone()
    return row["portfolio_id"] if row else None


IP_ECHO_URL="https://api.ipify.org?format=json"


def _probe_exit_ip(proxies) -> str:
    """通过该代理查一次出口 IP，仅用于表格展示；查不到就留空，绝不影响检测结论。

    目标域名和币安不同，不会占用币安的 2.05 秒同接口锁。
    """
    try:
        data=requests.get(IP_ECHO_URL,proxies=proxies,timeout=4).json()
        return str(data.get("ip") or "") if isinstance(data,dict) else ""
    except Exception:
        return ""


def _probe_proxy(url):
    pid=_test_proxy_id()
    if not pid: return {"ok":False,"error":"请先添加一个交易员，代理检测需要 portfolioId"}
    started=time.perf_counter()
    try:
        _,proxies=_normalize_proxy(url); response=bapi.fetch_order_history(pid,proxies,page_size=1)
        return {"ok":True,"latency_ms":round((time.perf_counter()-started)*1000,2),
                "exit_ip":_probe_exit_ip(proxies),"data_present":"data" in response}
    except Exception as exc: notifier.notify("proxy_error",f"代理检测失败 {url}: {exc}"); return {"ok":False,"error":str(exc)}


def _save_proxy_result(proxy_id: int, result: dict) -> None:
    """把检测结果（含失败原因与出口 IP）写回 proxy_nodes，前端表格直接显示。"""
    ok = bool(result.get("ok"))
    exit_ip = str(result.get("exit_ip") or "")
    db.get_conn().execute(
        "UPDATE proxy_nodes SET status=?, last_latency_ms=?, error_msg=?, "
        "exit_ip=COALESCE(NULLIF(?, ''), exit_ip), updated_at=? WHERE id=?",
        ("OK" if ok else "FAIL", result.get("latency_ms"),
         "" if ok else str(result.get("error") or "")[:300], exit_ip, int(time.time()), proxy_id))
    db.get_conn().commit()


@app.post("/api/settings/proxies/{proxy_id}/check")
async def test_proxy(proxy_id: int, token: str | None=None):
    _auth(token); row=db.get_conn().execute("SELECT proxy_url FROM proxy_nodes WHERE id=?",(proxy_id,)).fetchone()
    if not row: raise HTTPException(404,"代理不存在")
    result=_probe_proxy(row["proxy_url"]); _save_proxy_result(proxy_id,result); return result


@app.post("/api/settings/proxies/check-all")
async def test_all_proxies(token: str | None=None):
    _auth(token)
    def task():
        for row in db.get_conn().execute("SELECT id,proxy_url FROM proxy_nodes").fetchall():
            _save_proxy_result(row["id"], _probe_proxy(row["proxy_url"]))
    threading.Thread(target=task,daemon=True,name="proxy-check-all").start(); return {"ok":True,"started":True}


@app.get("/api/settings/poll-tg")
async def get_poll_tg(token: str | None=None):
    _auth(token); cfg=load_config(); return {k:cfg.get(k) for k in ("poll_interval_sec","tg_bot_token","tg_chat_id","tg_enabled","tg_throttle_sec")}


@app.put("/api/settings/poll-tg")
async def put_poll_tg(payload: dict[str,Any], token: str | None=None):
    _auth(token); cfg=load_config()
    for key in ("poll_interval_sec","tg_bot_token","tg_chat_id","tg_enabled","tg_throttle_sec"):
        if key in payload: cfg[key]=payload[key]
    save_config(cfg); return {"ok":True}


@app.post("/api/settings/tg/test")
async def test_tg(token: str | None=None):
    _auth(token); notifier.notify("trade_open","这是一条测试通知",force=True); return {"ok":True}


# ── ⑤ 电话提醒（FlashDuty）：每个 API 账户一个可选推送地址 ──
@app.get("/api/settings/phone-alerts")
def list_phone_alerts(token: str | None=None):
    """账户列表 + 各自是否已配置推送地址（地址掩码，不回传明文）。"""
    _auth(token)
    rows = db.get_conn().execute(
        "SELECT id,remark,is_enabled,phone_key,phone_last_status,phone_last_at FROM api_accounts ORDER BY id"
    ).fetchall()
    return [{
        "id": r["id"], "remark": r["remark"], "is_enabled": r["is_enabled"],
        "has_key": bool(phone_alert.extract_key(r["phone_key"] or "")),
        "masked": phone_alert.masked(r["phone_key"] or ""),
        "last_status": r["phone_last_status"] or "",
        "last_at": r["phone_last_at"] or 0,
    } for r in rows]


@app.put("/api/settings/phone-alerts/{account_id}")
async def save_phone_alert(account_id: int, payload: dict[str,Any], token: str | None=None):
    """保存/清空某账户的推送地址。传空字符串 = 清空（该账户不再提醒）。"""
    _auth(token)
    url = str(payload.get("url") or "").strip()
    if url and not phone_alert.extract_key(url):
        raise HTTPException(400, "推送地址缺少 integration_key")
    conn = db.get_conn()
    if not conn.execute("SELECT 1 FROM api_accounts WHERE id=?", (account_id,)).fetchone():
        raise HTTPException(404, "账户不存在")
    conn.execute("UPDATE api_accounts SET phone_key=?, phone_last_status='', phone_last_at=0 WHERE id=?",
                 (url, account_id))
    conn.commit()
    db.insert_log("INFO", "phone", f"账户 {account_id} 电话提醒地址{'已清空' if not url else '已保存'}")
    return {"ok": True, "has_key": bool(url)}


@app.post("/api/settings/phone-alerts/{account_id}/test")
def test_phone_alert(account_id: int, token: str | None=None):
    """发送测试提醒（Critical，走与真实告警完全相同的渠道）。"""
    _auth(token)
    row = db.get_conn().execute("SELECT * FROM api_accounts WHERE id=?", (account_id,)).fetchone()
    if not row:
        raise HTTPException(404, "账户不存在")
    if not phone_alert.extract_key(row["phone_key"] or ""):
        raise HTTPException(400, "该账户还没有配置推送地址")
    return phone_alert.send_test(row)


@app.get("/api/logs")
async def get_logs(level: str | None=None, keyword: str | None=None, limit: int=100, offset: int=0, token: str | None=None):
    _auth(token); where=[]; vals=[]
    if level: where.append("level=?"); vals.append(level.upper())
    if keyword: where.append("message LIKE ?"); vals.append(f"%{keyword}%")
    clause=" WHERE "+" AND ".join(where) if where else ""; conn=db.get_conn()
    total=conn.execute(f"SELECT COUNT(*) FROM system_logs{clause}",vals).fetchone()[0]
    rows=conn.execute(f"SELECT * FROM system_logs{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",(*vals,max(1,min(limit,500)),max(0,offset))).fetchall()
    return {"total":total,"items":[dict(r) for r in rows]}


@app.get("/api/system/status")
async def system_status(token: str | None=None):
    _auth(token); return {"live_trading_enabled":bool(load_config().get("live_trading_enabled",False)),"system_online":True,"ws_clients":len(ws_mgr.active)}


@app.put("/api/system/live-toggle")
async def live_toggle(payload: dict[str,Any], token: str | None=None):
    _auth(token); cfg=load_config(); cfg["live_trading_enabled"]=bool(payload.get("live_trading_enabled",payload.get("enabled",False))); save_config(cfg); db.insert_log("WARN","api",f"实盘开关设置为 {cfg['live_trading_enabled']}"); return {"live_trading_enabled":cfg["live_trading_enabled"]}


@app.post("/api/system/emergency-close-all")
def emergency_close(token: str | None=None):
    _auth(token); return executor.emergency_close_all(ccxt_clients,ws_mgr.broadcast,load_config)


@app.get("/api/system/price-stream-status")
async def price_stream_status(token: str | None=None):
    _auth(token); return {"connected":bool(price_stream._thread and price_stream._thread.is_alive()),"symbols":len(price_stream._prices)}


# ---------------------------------------------------------------------------
# v3.3 TG 频道（⑥ 菜单）：频道/映射 CRUD 直接读写 DB；状态/登录/体检代理到 tgcollector
# 管理接口只绑 127.0.0.1:8098（采集容器），浏览器不直连，权限与主程序一致。
# ---------------------------------------------------------------------------
TG_ADMIN = os.environ.get("TG_ADMIN_URL") or "http://127.0.0.1:8098"
TG_SECRET_PATH = DATA_DIR / "tg_secret.json"          # 共享卷：两个容器都挂载 ./data
TG_SECRET_LEGACY = BASE_DIR / "tg" / "secret.json"    # 旧位置，仅用于读取兼容


def _tg_admin(method: str, path: str, timeout: float = 10, **kwargs):
    try:
        return requests.request(method, f"{TG_ADMIN}{path}", timeout=timeout, **kwargs)
    except Exception as exc:
        raise HTTPException(503, f"tgcollector 不可达：{type(exc).__name__}")


@app.get("/api/tg/status")
def tg_status(token: str | None = None):
    _auth(token)
    try:
        r = _tg_admin("GET", "/status", timeout=3)
        data = r.json()
        data["online"] = True
        return data
    except HTTPException:
        return {"online": False, "authorized": False, "listening": False, "channels": [],
                "today_signals": 0, "last_error": "", "login_busy": False, "qr_at": 0}


@app.post("/api/tg/login/start")
def tg_login_start(token: str | None = None, payload: dict[str, Any] | None = None):
    _auth(token)
    # 两步验证密码只在本次请求里透传给采集器，不落库、不写日志
    pw = str((payload or {}).get("password") or "")
    r = _tg_admin("POST", "/login/start", json={"password": pw} if pw else None)
    return r.json()


@app.get("/api/tg/qr")
def tg_qr(token: str | None = None):
    _auth(token)
    r = _tg_admin("GET", "/login/qr", timeout=5)
    if r.status_code != 200:
        raise HTTPException(404, "二维码尚未生成")
    from fastapi import Response
    # SVG：采集器侧用标准库生成，不依赖 pillow
    return Response(content=r.content, media_type="image/svg+xml",
                    headers={"Cache-Control": "no-store"})


@app.post("/api/tg/check")
def tg_check(payload: dict[str, Any], token: str | None = None):
    _auth(token)
    channel = str((payload or {}).get("channel") or "")
    r = _tg_admin("POST", f"/check?channel={channel}", timeout=120)
    try:
        return r.json()
    except Exception:
        raise HTTPException(502, f"体检失败：{r.status_code}")


def _read_tg_secret() -> dict:
    """读凭据：优先新位置 data/tg_secret.json，其次旧的 tg/secret.json。"""
    for path in (TG_SECRET_PATH, TG_SECRET_LEGACY):
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {}


@app.get("/api/tg/secret")
def tg_secret_get(token: str | None = None):
    _auth(token)
    data = _read_tg_secret()
    if not data:
        return {"api_id": "", "api_hash_set": False}
    return {"api_id": str(data.get("api_id") or ""), "api_hash_set": bool(data.get("api_hash"))}


@app.post("/api/tg/secret")
def tg_secret_save(payload: dict[str, Any], token: str | None = None):
    _auth(token)
    api_id = str((payload or {}).get("api_id") or "").strip()
    api_hash = str((payload or {}).get("api_hash") or "").strip()
    if not api_id.isdigit():
        raise HTTPException(400, "api_id 必须是数字")
    if not api_hash:                      # 留空 = 不修改已有 hash
        api_hash = str(_read_tg_secret().get("api_hash") or "")
    if len(api_hash) < 20:
        raise HTTPException(400, "api_hash 长度不足（首次必须填）")
    TG_SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
    TG_SECRET_PATH.write_text(json.dumps({"api_id": int(api_id), "api_hash": api_hash}), encoding="utf-8")
    try:
        TG_SECRET_PATH.chmod(0o600)
    except Exception:
        pass
    db.insert_log("INFO", "tg", "TG api 凭据已更新（secret.json）")
    return {"ok": True}


@app.get("/api/tg/channels")
def tg_channels(token: str | None = None):
    _auth(token)
    conn = db.get_conn()
    rows = conn.execute("SELECT channel, remark, enabled FROM tg_channels ORDER BY created_at, channel").fetchall()
    out = []
    for r in rows:
        links = conn.execute(
            "SELECT id, prefix, portfolio_id, enabled FROM tg_links WHERE channel=? ORDER BY id",
            (r["channel"],)).fetchall()
        out.append({"channel": r["channel"], "remark": r["remark"] or "", "enabled": int(r["enabled"]),
                    "links": [dict(l) for l in links]})
    return out


def _norm_channel(raw: str) -> str:
    """把用户输入整理成频道 username：支持 @xxx、t.me/xxx、https://t.me/xxx（含 ?query）。"""
    s = (raw or "").strip()
    s = re.sub(r"^(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/", "", s, flags=re.I)
    s = s.split("?")[0].split("#")[0].strip().strip("/")
    return s.lstrip("@").strip().lower()


@app.post("/api/tg/channels")
def tg_channel_add(payload: dict[str, Any], token: str | None = None):
    _auth(token)
    ch = _norm_channel((payload or {}).get("channel"))
    if not ch:
        raise HTTPException(400, "频道名不能为空")
    if not re.fullmatch(r"[a-z0-9_]{3,64}", ch):
        raise HTTPException(400, "频道名不合法：请填 @username 或 https://t.me/username")
    # 先让采集器确认频道真实存在：填错 username 当场报错，而不是等日志里出现“加入失败”
    try:
        r = _tg_admin("POST", f"/verify?channel={ch}", timeout=20)   # 采集器该接口是 POST
    except HTTPException:
        raise HTTPException(409, "采集器未在线：请先完成登录，再添加频道")
    try:
        info = r.json()
    except Exception:
        raise HTTPException(502, f"采集器返回异常（HTTP {r.status_code}），请稍后重试")
    if not info.get("ok"):
        raise HTTPException(400, info.get("error") or "频道校验失败")
    conn = db.get_conn()
    conn.execute("INSERT OR IGNORE INTO tg_channels (channel, remark, enabled, created_at) VALUES (?,?,1,?)",
                 (ch, str(payload.get("remark") or ""), int(time.time())))
    conn.commit()
    db.insert_log("INFO", "tg", f"添加 TG 频道 @{ch}（{info.get('title','')}）")
    return {"ok": True}


@app.delete("/api/tg/channels/{channel}")
def tg_channel_del(channel: str, token: str | None = None):
    _auth(token)
    conn = db.get_conn()
    conn.execute("DELETE FROM tg_links WHERE channel=?", (channel,))
    conn.execute("DELETE FROM tg_channels WHERE channel=?", (channel,))
    conn.commit()
    db.insert_log("INFO", "tg", f"删除 TG 频道 @{channel}（含映射）")
    return {"ok": True}


@app.post("/api/tg/links")
def tg_link_save(payload: dict[str, Any], token: str | None = None):
    _auth(token)
    ch = str((payload or {}).get("channel") or "").strip()
    prefix = str(payload.get("prefix") or "").strip()
    pid = str(payload.get("portfolio_id") or "").strip()
    if not (ch and prefix and pid):
        raise HTTPException(400, "频道、前缀、交易员都必须填写")
    conn = db.get_conn()
    if not conn.execute("SELECT 1 FROM traders WHERE portfolio_id=?", (pid,)).fetchone():
        raise HTTPException(400, "该交易员不在监控列表里，请先在「监控交易员」中添加")
    conn.execute("INSERT INTO tg_links (channel, prefix, portfolio_id, enabled) VALUES (?,?,?,1) "
                 "ON CONFLICT(channel, prefix) DO UPDATE SET portfolio_id=excluded.portfolio_id, enabled=1",
                 (ch, prefix, pid))
    conn.commit()
    return {"ok": True}


@app.delete("/api/tg/links/{link_id}")
def tg_link_del(link_id: int, token: str | None = None):
    _auth(token)
    conn = db.get_conn()
    conn.execute("DELETE FROM tg_links WHERE id=?", (link_id,))
    conn.commit()
    return {"ok": True}


@app.get("/api/tg/recent")
def tg_recent(token: str | None = None):
    _auth(token)
    conn = db.get_conn()
    rows = conn.execute(
        "SELECT id, source_key, channel, symbol, side, action, qty, price, event_ms, consumed_at "
        "FROM signals WHERE source='tg' ORDER BY id DESC LIMIT 20").fetchall()
    return [dict(r) for r in rows]

