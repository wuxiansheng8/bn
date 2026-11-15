"""Binance Futures mark-price stream (market endpoint, dynamic per-symbol subscription).

币安 2026-04 起把 WebSocket 拆成 /public、/market、/private 三个入口：
旧地址 wss://fstream.binance.com/ws/... 只推送 public 流（depth/bookTicker），
markPrice 属于 market 流，必须走 /market，否则连上了也收不到任何数据。

为了省流量（全市场 !markPrice@arr 约 430GB/月），这里只订阅"我们实际持有的币种"：
  - 每 10 秒从库里算一次目标集合（我们的净持仓 + 交易员当前持仓）；
  - 与已订阅集合对比，用 SUBSCRIBE / UNSUBSCRIBE 帧动态增删，不用重连；
  - 开仓后最多 10 秒（通常更快）就会开始推送该币种标记价。
"""
from __future__ import annotations
import asyncio
import json
import logging
import threading
import time

import websockets
try:
    from . import db
except ImportError:
    import db  # type: ignore

logger = logging.getLogger(__name__)
URL = "wss://fstream.binance.com/market/ws"
RECV_TIMEOUT = 5.0        # 已订阅币种 5 秒没数据 → 判定断线并重连
IDLE_TIMEOUT = 30.0       # 没有订阅任何币种时，只是保活，不因"无数据"重连
RECONCILE_SEC = 10.0      # 每 10 秒同步一次订阅集合

_prices: dict[str, float] = {}
_running = threading.Event()
_thread: threading.Thread | None = None
_broadcast_fn = None
_last_sync = 0.0


def _compute_symbols() -> set[str]:
    """目标订阅集合：我们自己的净持仓 + 交易员当前持仓（均为大写交易对）。"""
    symbols: set[str] = set()
    try:
        conn = db.get_conn()
        for row in conn.execute(
            "select symbol, sum(case when action='OPEN' then my_qty else -my_qty end) as net "
            "from copy_orders where status='SUCCESS' group by symbol"
        ):
            if (row["net"] or 0) > 1e-9:
                symbols.add(str(row["symbol"]).upper())
        for row in conn.execute("select hidden_positions_json from traders"):
            try:
                data = json.loads(row["hidden_positions_json"] or "{}")
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            for symbol, sides in data.items():
                if not isinstance(sides, dict):
                    continue
                if any(isinstance(v, dict) and v.get("qty") for v in sides.values()):
                    symbols.add(str(symbol).upper())
    except Exception as exc:
        logger.warning("计算订阅币种失败: %s", exc)
    return symbols


def _subscribe_params(symbols) -> list[str]:
    return [f"{s.lower()}@markPrice@1s" for s in sorted(symbols)]


async def _apply_subscriptions(ws, current: set[str], wanted: set[str]) -> None:
    add = wanted - current
    remove = current - wanted
    if add:
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": _subscribe_params(add), "id": 1}))
    if remove:
        await ws.send(json.dumps({"method": "UNSUBSCRIBE", "params": _subscribe_params(remove), "id": 2}))


async def _consume():
    global _last_sync
    reconnect_count = 0
    while _running.is_set():
        try:
            async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                subscribed: set[str] = set()
                _last_sync = 0.0          # 重连后立刻重新订阅
                if reconnect_count == 0:
                    db.insert_log("INFO", "price_stream", "标记价格流已连接")
                else:
                    db.insert_log("INFO", "price_stream", f"标记价格流已恢复（第 {reconnect_count} 次重连成功）")
                while _running.is_set():
                    if time.monotonic() - _last_sync >= RECONCILE_SEC:
                        _last_sync = time.monotonic()
                        wanted = _compute_symbols()
                        if wanted != subscribed:
                            await _apply_subscriptions(ws, subscribed, wanted)
                            subscribed = wanted
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=RECV_TIMEOUT if subscribed else IDLE_TIMEOUT)
                    except asyncio.TimeoutError:
                        if not subscribed:
                            continue          # 没有持仓，保活而已
                        reconnect_count += 1
                        if _running.is_set():
                            db.insert_log("WARN", "price_stream",
                                          f"标记价格流 {int(RECV_TIMEOUT)} 秒无数据，第 {reconnect_count} 次重连")
                        break                     # 跳出内层 → 重连
                    payload = json.loads(raw)
                    if isinstance(payload, dict): payload = [payload]
                    updates = []
                    for row in payload:
                        if not isinstance(row, dict) or not row.get("s") or row.get("p") is None: continue
                        symbol = str(row["s"]); price = float(row["p"]); _prices[symbol] = price
                        updates.append({"symbol": symbol, "mark_price": price})
                    if updates and _broadcast_fn:
                        try:
                            result = _broadcast_fn({"type": "price_update", "prices": updates})
                            if asyncio.iscoroutine(result): await result
                        except Exception as exc: db.insert_log("WARN", "price_stream", f"价格广播失败: {exc}")
        except asyncio.TimeoutError:
            reconnect_count += 1
            if _running.is_set():
                db.insert_log("WARN", "price_stream", f"标记价格流等待超时，第 {reconnect_count} 次重连")
            continue
        except Exception as exc:
            if _running.is_set():
                reconnect_count += 1
                logger.warning("价格流断开: %s", exc)
                db.insert_log("WARN", "price_stream", f"标记价格流断开，第 {reconnect_count} 次重连: {exc}")
                await asyncio.sleep(2)


def start_price_stream(ws_broadcast_fn) -> None:
    global _thread, _broadcast_fn
    _broadcast_fn = ws_broadcast_fn
    if _thread and _thread.is_alive(): return
    _running.set()
    def run(): asyncio.run(_consume())
    _thread = threading.Thread(target=run, daemon=True, name="price-stream")
    _thread.start()


def stop_price_stream() -> None:
    _running.clear()
    if _thread and _thread.is_alive(): _thread.join(timeout=3)


def snapshot() -> dict[str, float]:
    """当前标记价快照（symbol -> price），供接口首屏渲染浮盈亏。"""
    return dict(_prices)


def get_mark_price(symbol: str) -> float | None:
    return _prices.get(symbol.replace("/", "").replace(":USDT", "").upper()) or _prices.get(symbol.upper())


def subscribed_symbols() -> list[str]:
    """当前想要订阅的币种（供接口/排查用）。"""
    return sorted(_compute_symbols())


def cached_symbol_count() -> int:
    return len(_prices)
