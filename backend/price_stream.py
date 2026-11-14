"""Direct Binance Futures mark-price stream with reconnect."""
from __future__ import annotations
import asyncio
import json
import logging
import threading

import websockets
try:
    from . import db
except ImportError:
    import db  # type: ignore

logger = logging.getLogger(__name__)
URL = "wss://fstream.binance.com/ws/!markPrice@arr@1s"
_prices: dict[str, float] = {}
_running = threading.Event()
_thread: threading.Thread | None = None
_broadcast_fn = None


async def _consume():
    while _running.is_set():
        try:
            async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                db.insert_log("INFO", "price_stream", "标记价格流已连接")
                while _running.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    payload = json.loads(raw)
                    if isinstance(payload, dict): payload = [payload]
                    updates = []
                    for row in payload:
                        if not isinstance(row, dict) or not row.get("s") or row.get("p") is None: continue
                        symbol = str(row["s"]); price = float(row["p"]); _prices[symbol] = price
                        updates.append({"symbol": symbol, "mark_price": price})
                    if updates and _broadcast_fn:
                        try:
                            result = _broadcast_fn({"type":"price_update", "prices":updates})
                            if asyncio.iscoroutine(result): await result
                        except Exception as exc: db.insert_log("WARN", "price_stream", f"价格广播失败: {exc}")
        except asyncio.TimeoutError:
            continue
        except Exception as exc:
            if _running.is_set():
                logger.warning("价格流断开: %s", exc)
                db.insert_log("WARN", "price_stream", f"价格流断开，2秒后重连: {exc}")
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


def get_mark_price(symbol: str) -> float | None:
    return _prices.get(symbol.replace("/", "").replace(":USDT", "").upper()) or _prices.get(symbol.upper())
