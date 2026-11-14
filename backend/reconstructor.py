"""M3 推算持仓回放。锚点只用于屏蔽锚点之前的旧订单，不依赖隐藏持仓接口清零。"""
from __future__ import annotations

import copy
import json
import logging
import threading
from typing import Any

try:
    from . import db
except ImportError:
    import db  # type: ignore

logger = logging.getLogger(__name__)
_lock = threading.RLock()
_positions: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
_closed_anchors: dict[str, dict[tuple[str, str], int]] = {}


def _time_ms(value: Any) -> int:
    try:
        number = int(float(value or 0))
        return number * 1000 if 0 < number < 10_000_000_000 else number
    except (TypeError, ValueError, OverflowError):
        return 0


def _record_key(record: dict) -> tuple[str, str]:
    symbol = str(record.get("symbol") or record.get("asset") or "")
    side = str(record.get("positionSide") or record.get("position_side") or record.get("side") or "").upper()
    if side not in ("LONG", "SHORT"):
        side = "LONG"
    return symbol, side


def _record_close_time(record: dict) -> int:
    for key in ("closeTime", "closedTime", "close_time", "updateTime", "endTime", "orderTime"):
        if record.get(key) not in (None, ""):
            return _time_ms(record[key])
    return 0


def set_closed_anchors(portfolio_id: str, closed_records: list[dict]) -> None:
    """设置 position-history All Closed 锚点，并清理锚点时间之前的旧仓位（M-02）。"""
    anchors: dict[tuple[str, str], int] = {}
    for record in closed_records:
        if not isinstance(record, dict):
            continue
        key, timestamp = _record_key(record), _record_close_time(record)
        if key[0] and timestamp:
            anchors[key] = max(timestamp, anchors.get(key, 0))
    with _lock:
        _closed_anchors[portfolio_id] = anchors
        positions = _positions.setdefault(portfolio_id, {})
        for key, anchor in anchors.items():
            current = positions.get(key)
            if current and current.get("last_order_time", 0) <= anchor:
                positions.pop(key, None)


def _apply_locked(portfolio_id: str, order: dict) -> bool:
    try:
        symbol = str(order["symbol"])
        side = str(order["position_side"]).upper()
        qty = float(order["executed_qty"])
        price = float(order.get("avg_price", 0) or 0)
        timestamp = _time_ms(order.get("order_time", 0))
        is_open = bool(order["is_open"])
        if side not in ("LONG", "SHORT") or qty < 0:
            raise ValueError("无效 position_side 或数量")
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    key = (symbol, side)
    anchor = _closed_anchors.get(portfolio_id, {}).get(key, 0)
    if timestamp and anchor and timestamp <= anchor:
        return False
    positions = _positions.setdefault(portfolio_id, {})
    current = positions.get(key)
    if is_open:
        old_qty = float(current.get("qty", 0)) if current else 0.0
        old_price = float(current.get("avg_price", 0)) if current else 0.0
        total = old_qty + qty
        avg = ((old_qty * old_price + qty * price) / total) if total > 0 else 0.0
        orders = list(current.get("open_orders", [])) if current else []
        orders.append(order.get("order_key", ""))
        positions[key] = {"qty": total, "avg_price": avg, "open_orders": orders,
                          "last_order_time": max(timestamp, int(current.get("last_order_time", 0)) if current else 0)}
        return True
    # 铁律 10：空仓收到平仓信号不可推断全平，也不创建负仓。
    if not current or float(current.get("qty", 0)) <= 0:
        return False
    old_qty = float(current["qty"])
    close_ratio = qty / old_qty if old_qty > 0 else 1.0
    remaining = max(0.0, old_qty - qty)
    if close_ratio >= 0.98 or remaining <= max(1e-12, old_qty * 1e-12):
        positions.pop(key, None)
    else:
        current["qty"] = remaining
        current["last_order_time"] = max(timestamp, int(current.get("last_order_time", 0)))
    return True


def initialize_from_history(portfolio_id: str, orders: list[dict]) -> None:
    """按时间升序回放底账，先应用 position-history 的 All Closed 锚点过滤旧开仓。"""
    ordered = sorted((o for o in orders if isinstance(o, dict)),
                     key=lambda o: _time_ms(o.get("order_time", o.get("orderTime", 0))))
    replay = []
    for raw in ordered:
        if "executed_qty" in raw:
            replay.append(raw)
            continue
        try:
            side = str(raw["side"]).upper()
            pos_side = str(raw["positionSide"]).upper()
            replay.append({"symbol": raw["symbol"], "position_side": pos_side,
                           "executed_qty": float(raw["executedQty"]), "avg_price": float(raw["avgPrice"]),
                           "order_time": _time_ms(raw["orderTime"]), "order_key": str(raw.get("orderId", "")),
                           "is_open": (side, pos_side) in (("BUY", "LONG"), ("SELL", "SHORT"))})
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            db.insert_log("ERROR", "reconstructor", f"历史订单字段解析失败 {portfolio_id}: {exc}")
    with _lock:
        _positions[portfolio_id] = {}
        for event in replay:
            _apply_locked(portfolio_id, event)
    persist_positions(portfolio_id)


def apply_order_event(portfolio_id: str, order: dict) -> dict:
    """应用一笔标准订单事件并返回快照；空仓收到平仓信号记 ERROR。"""
    empty_close = False
    with _lock:
        if not bool(order.get("is_open")):
            key = (str(order.get("symbol", "")), str(order.get("position_side", "")).upper())
            current = _positions.get(portfolio_id, {}).get(key)
            empty_close = not current or float(current.get("qty", 0)) <= 0
        _apply_locked(portfolio_id, order)
        snapshot = _snapshot(_positions.get(portfolio_id, {}))
    if empty_close:
        db.insert_log("ERROR", "reconstructor", f"推算持仓为空，跳过平仓信号 {portfolio_id} {order.get('symbol')} {order.get('position_side')}")
    return snapshot


def _snapshot(positions: dict[tuple[str, str], dict]) -> dict:
    result: dict[str, dict] = {}
    for (symbol, side), position in positions.items():
        if float(position.get("qty", 0)) <= 0:
            continue
        result.setdefault(symbol, {})[side] = {"qty": float(position["qty"]),
                                                "avg_price": float(position.get("avg_price", 0))}
    return result


def get_inferred_positions(portfolio_id: str) -> dict:
    """返回 {symbol: {LONG/SHORT: {qty, avg_price}}} 的深拷贝。"""
    with _lock:
        return copy.deepcopy(_snapshot(_positions.get(portfolio_id, {})))


def is_all_closed(portfolio_id: str, symbol: str, position_side: str) -> bool:
    """推算指定方向持仓是否已经清零。"""
    with _lock:
        position = _positions.get(portfolio_id, {}).get((symbol, position_side.upper()))
        return not position or float(position.get("qty", 0)) <= 0


def persist_positions(portfolio_id: str) -> None:
    """锁外写入推算快照；不调用持仓接口，也不因 positionShow 隐藏而清仓。"""
    try:
        with _lock:
            payload = json.dumps(_snapshot(_positions.get(portfolio_id, {})), ensure_ascii=False, separators=(",", ":"))
        conn = db.get_conn()
        conn.execute("UPDATE traders SET hidden_positions_json=? WHERE portfolio_id=?", (payload, portfolio_id))
        conn.commit()
    except Exception as exc:
        logger.exception("持仓推算写入失败")
        db.insert_log("ERROR", "reconstructor", f"推算持仓持久化失败 {portfolio_id}: {exc}")
