"""CCXT execution and copy-trading orchestration. Live trading is opt-in."""
from __future__ import annotations

import logging
import math
import queue
import threading
import time
from datetime import datetime
from pathlib import Path

try:
    from . import db, reconstructor, notifier, phone_alert
    from .collector import signal_queue
except ImportError:
    import db  # type: ignore
    import reconstructor  # type: ignore
    import notifier  # type: ignore
    import phone_alert  # type: ignore
    from collector import signal_queue  # type: ignore

logger = logging.getLogger(__name__)
KEYFILE = Path(__file__).parent.parent / "data" / "keyfile"
_running = threading.Event()
_thread = None
_clients = {}
_config_getter = None
# 币安条件单类型（改价/归零时要撤销的就是这两类）
CONDITIONAL_ORDER_TYPES = ("TAKE_PROFIT_MARKET", "STOP_MARKET")
# 下单参数口径（币安 USDⓈ-M 官方文档 · New Order）：
#   reduceOnly —— "Cannot be sent in Hedge Mode"
#   positionSide —— "It must be sent in Hedge Mode"
# 本系统固定带 positionSide（双向持仓），因此所有平仓单一律「只传 positionSide + 数量」，
# 绝不下发 reduceOnly；closePosition=true 的条件单同理（数量还必须为空）。
# 双向模式下 SELL+LONG / BUY+SHORT 本身就是减仓语义，不需要 reduceOnly 兜底。


def _fernet():
    from cryptography.fernet import Fernet
    KEYFILE.parent.mkdir(parents=True, exist_ok=True)
    if not KEYFILE.exists():
        KEYFILE.write_bytes(Fernet.generate_key())
        try: KEYFILE.chmod(0o600)
        except OSError: pass
    return Fernet(KEYFILE.read_bytes())


def decrypt(ciphertext: str) -> str:
    return _fernet().decrypt(ciphertext.encode()).decode()


def init_ccxt_clients(conn) -> dict[int, object]:
    try: import ccxt
    except Exception as exc:
        db.insert_log("ERROR", "executor", f"CCXT 初始化失败: {exc}"); return {}
    clients = {}
    for row in conn.execute("SELECT id,api_key_enc,api_secret_enc FROM api_accounts WHERE is_enabled=1"):
        try:
            # fetchMarkets 只加载 USDT 本位合约：现货与合约的 market id 同名（都是 ZECUSDT），
            # 混在一起会让 _symbol 解析到现货市场（2026-11-15 手动平仓静默失效的根因）。
            clients[int(row["id"])] = ccxt.binance({"apiKey": decrypt(row["api_key_enc"]), "secret": decrypt(row["api_secret_enc"]), "enableRateLimit": True, "options": {"defaultType": "future", "fetchMarkets": ["linear"]}})
        except Exception as exc: db.insert_log("ERROR", "executor", f"账户 {row['id']} CCXT 初始化失败: {exc}")
    return clients


def calc_open_qty(trader_qty, trader_margin, my_equity, copy_ratio, mark_price, step_size) -> float:
    if trader_margin <= 0 or step_size <= 0 or min(trader_qty, my_equity, copy_ratio) <= 0: return 0.0
    # 2026-11-18 用户要求去掉 0.9 保证金缓冲：仓位放大 11.1%，风险率与交易员 1:1
    raw = trader_qty * (my_equity / trader_margin) * copy_ratio
    return math.floor((raw + 1e-12) / step_size) * step_size


def _account_equity(client):
    balance = client.fetch_balance({"type": "future"}); info = balance.get("info", {})
    return (float(balance.get("total", {}).get("USDT") or info.get("totalMarginBalance") or 0),
            float(balance.get("free", {}).get("USDT") or info.get("availableBalance") or 0))


def _positions(client):
    return [p for p in client.fetch_positions() if abs(float(p.get("contracts") or p.get("info", {}).get("positionAmt") or 0)) > 0]


def _symbol(client, raw):
    """把「ZECUSDT」这类原始符号解析成 ccxt 的统一符号。

    2026-11-15 修复：币安现货与合约的 market id 同名（都是 ZECUSDT），
    旧实现会先命中现货市场 ZEC/USDT，导致：
      ① _find_position 与持仓（ZEC/USDT:USDT）匹配不上 → 手动平仓静默返回 closed=0；
      ② 算量用的是现货的 stepSize/precision，而非合约的。
    现在只认合约市场（contract=True 或带 :USDT 后缀），现货一律跳过；
    实在找不到就返回合成符号，让下单明确报错，而不是悄悄走到现货。
    """
    markets = getattr(client, "markets", {}) or {}
    if not markets:
        try: markets = client.load_markets()
        except Exception: pass
    if raw in markets and (markets[raw].get("contract") is True or str(raw).endswith(":USDT")): return raw
    candidate = raw.replace("/", "").replace(":USDT", "")
    for key, market in markets.items():
        if market.get("id") != candidate and key.replace("/", "").replace(":USDT", "") != candidate:
            continue
        if market.get("contract") is True or str(key).endswith(":USDT"):
            return key
    return candidate[:-4] + "/USDT:USDT" if candidate.endswith("USDT") else raw


def _insert_copy(account, signal, action, qty, price, status, reason="", scale=None, partial=False):
    conn = db.get_conn(); trader = conn.execute("SELECT nickname FROM traders WHERE portfolio_id=?", (signal.get("portfolio_id"),)).fetchone()
    conn.execute("INSERT INTO copy_orders(api_account_id,api_remark,portfolio_id,trader_nickname,symbol,action,position_side,trader_qty,trader_price,my_qty,my_price,scale,status,error_msg,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (account["id"], account["remark"], signal.get("portfolio_id"), trader["nickname"] if trader else "", signal.get("symbol"), action, signal.get("position_side"), signal.get("executed_qty"), signal.get("avg_price"), qty, price, scale, status, reason, int(time.time()))); conn.commit()
    # P1-13：每条跟单记录同步写一条 TRADE 级别日志，「交易与跟单」筛选才有数据
    db.insert_log("TRADE", "executor", f"[{account['remark']}] {action} {signal.get('symbol')} {signal.get('position_side')} 数量={qty} 价格={price} → {status}{(' ' + reason) if reason else ''}")
    # 电话/短信提醒（按账户独立，只对 SUCCESS/FAILED 发；失败不影响主流程）
    try:
        phone_alert.notify_copy(account, signal, action, qty, price, status, reason,
                                trader=(trader["nickname"] if trader else ""), partial=partial)
    except Exception as exc:  # noqa: BLE001
        db.insert_log("WARN", "executor", f"电话提醒调用异常: {exc}")


def _override(conn, aid, pid, symbol, side, **fields):
    row = conn.execute("SELECT id FROM manual_overrides WHERE api_account_id=? AND symbol=? AND position_side=?", (aid,symbol,side)).fetchone()
    if row:
        sets=["portfolio_id=?","is_overridden=1","created_at=?"]; vals=[pid,int(time.time())]
        for k,v in fields.items(): sets.append(f"{k}=?"); vals.append(v)
        vals.append(row["id"]); conn.execute(f"UPDATE manual_overrides SET {','.join(sets)} WHERE id=?", vals)
    else:
        cols=["api_account_id","portfolio_id","symbol","position_side","is_overridden","created_at",*fields.keys()]; vals=[aid,pid,symbol,side,1,int(time.time()),*fields.values()]
        conn.execute(f"INSERT INTO manual_overrides({','.join(cols)}) VALUES({','.join('?' for _ in vals)})", vals)
    conn.commit()


MARGIN_GATE_RATIO = 0.97   # 单账户保证金使用率上限（2026-11-18 用户定为 97%，原 90%）


def check_margin_gate(account_id: int, estimated_margin: float, conn) -> bool:
    row=conn.execute("SELECT cached_equity FROM api_accounts WHERE id=?",(account_id,)).fetchone(); equity=float(row["cached_equity"] or 0) if row else 0
    client=_clients.get(account_id); used=0.0
    if client:
        try:
            for p in _positions(client):
                info=p.get("info",{}); initial=float(info.get("initialMargin") or 0)
                used += initial or abs(float(p.get("contracts") or 0))*float(p.get("markPrice") or p.get("entryPrice") or 0)/max(float(p.get("leverage") or 1),1)
        except Exception as exc: db.insert_log("ERROR","executor",f"账户 {account_id} 查询保证金失败: {exc}"); return False
    return equity > 0 and used + estimated_margin <= equity * MARGIN_GATE_RATIO


def _find_position(client, symbol, position_side):
    """取我方该币种方向的实际持仓；无持仓返回 None。

    2026-11-15 修复：不再直接比较 ccxt 的 symbol 字符串
    （"ZECUSDT" / "ZEC/USDT" / "ZEC/USDT:USDT" 混用会匹配不到），
    改为按「币种 id」比较，并兼容 info.positionSide 与 unified side 两种来源。
    """
    want = str(symbol).upper().replace("/", "").replace(":USDT", "")
    side = str(position_side).lower()
    for p in _positions(client):
        info = p.get("info") or {}
        raw_id = str(info.get("symbol") or "").upper() or str(p.get("symbol") or "").upper().replace("/", "").replace(":USDT", "")
        if raw_id != want:
            continue
        p_side = str(info.get("positionSide") or p.get("side") or "").lower()
        if p_side == side:
            return p
    return None


def _apply_leverage(client, symbol: str, leverage: int) -> int:
    """设置杠杆；该币种上限低于所填值时自动降级（文档 5.5），返回实际生效值。

    必须在保证金总闸门之前调用，否则闸门会按偏小的占用误放行。
    """
    try:
        client.set_leverage(leverage, symbol)
        return leverage
    except Exception as exc:
        limits = ((client.market(symbol) or {}).get("limits") or {}).get("leverage") or {}
        max_leverage = int(limits.get("max") or 0)
        if max_leverage <= 0 or max_leverage >= leverage:
            raise
        client.set_leverage(max_leverage, symbol)
        db.insert_log("WARN", "executor",
                      f"{symbol} 杠杆 {leverage}x 超过该币种上限，已自动降级为 {max_leverage}x: {exc}")
        return max_leverage


def _algo_request(client, method: str, path: str, params: dict):
    """调用币安「算法单」私有接口。

    2025-12-09 起币安把 STOP_MARKET / TAKE_PROFIT_MARKET 等条件单迁到 Algo 服务，
    旧接口 `/fapi/v1/order` 对这些类型直接返回 -4120 STOP_ORDER_SWITCH_ALGO；
    ccxt 4.4.27 还没有 algoOrder 封装，因此用 fetch2 发签名请求
    （fetch2 的 path 不带 /fapi/v1 前缀，由 ccxt 自动拼上）。
    """
    return client.fetch2(path, "fapiPrivate", method, params)


def _exchange_id(client, symbol: str) -> str:
    """统一符号 → 币安交易对 id（ZEC/USDT:USDT → ZECUSDT），算法单接口要的是 id。"""
    market = client.market(symbol) or {}
    return str(market.get("id") or symbol)


def _cancel_conditional_orders(client, symbol: str, position_side: str) -> int:
    """撤掉该币种方向全部未触发的算法条件单（改价 / 仓位归零时用），返回撤销数量。"""
    try:
        orders = _algo_request(client, "GET", "openAlgoOrders", {"symbol": _exchange_id(client, symbol)}) or []
    except Exception as exc:
        db.insert_log("WARN", "executor", f"查询算法条件单失败 {symbol}: {exc}")
        return 0
    cancelled = 0
    for order in orders:
        # 币安 openAlgoOrders 返回的字段名是 orderType（不是 type），漏读会导致一条都撤不掉，
        # 改价时撞 -4130「同方向已有 closePosition 单」
        order_type = str(order.get("orderType") or order.get("type") or "").upper()
        if order_type not in CONDITIONAL_ORDER_TYPES:
            continue
        order_side = str(order.get("positionSide", "")).upper()
        if order_side and order_side != position_side.upper():
            continue
        algo_id = order.get("algoId")
        if algo_id in (None, ""):
            continue
        try:
            _algo_request(client, "DELETE", "algoOrder", {"algoId": algo_id})
            cancelled += 1
        except Exception as exc:
            db.insert_log("ERROR", "executor", f"撤算法条件单失败 {symbol} algoId={algo_id}: {exc}")
    return cancelled


PNL_INCOME_TYPES = ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE")
# 币安 income 接口对"太早的 startTime"会**静默忽略并退回最近 7 天**（2026-11-22 实测：
# startTime=2019-01-01 → 只回 7 天；startTime=2024-01-01 → 回全部可用历史）。
# 因此起点不得早于该下限，否则页面又会算成滚动的 7 天窗口。
PNL_EARLIEST_MS = 1704067200000        # 2024-01-01 00:00:00 UTC


def realized_pnl_total(client, since_ms: int = 0) -> float:
    """汇总「净已实现盈亏」与「净入金」，供页面显示。

    2026-11-15：realized_pnl 此前只读不写，页面上永远是 0。
    只算 REALIZED_PNL 会低估成本（永续合约的手续费与资金费都是真实支出），
    因此把 COMMISSION / FUNDING_FEE 一并计入，得到"跟币安 App 对得上"的净额。

    2026-11-22（v3.2 修正）：币安 GET /fapi/v1/income 在**不传 startTime 时只返回最近 7 天**（官方文档），
    旧实现没传 → 页面上的"已实现"其实是滚动 7 天窗口：
    实测琦账户显示 -251.43，而全量真实值是 -182.06（差价为窗口外那段的 +69.37）。
    现固定传 startTime（默认 2019-01-01 即"全部历史"），边界只在业务层过滤；
    若返回正好 1000 条（上限）则再补一页，避免截断。
    净入金只统计 TRANSFER 类（充提），与盈亏严格分开。
    """
    # 兜底：没传起点时取 90 天前；早于下限的一律钳到下限（否则币安退回 7 天）
    now_ms = int(time.time() * 1000)
    start = int(since_ms) if since_ms else now_ms - 90 * 86400 * 1000
    start = max(start, PNL_EARLIEST_MS)
    getter = getattr(client, "fapiPrivateGetIncome", None)
    if not callable(getter):
        def getter(params):  # type: ignore[misc]
            return client.fetch2("income", "fapiPrivate", "GET", params)
    rows = list(getter({"asset": "USDT", "limit": 1000, "startTime": start}) or [])
    if len(rows) >= 1000:                      # 可能被截断：补一页
        try:
            last = int(rows[-1].get("time") or 0)
            if last:
                rows.extend(getter({"asset": "USDT", "limit": 1000, "startTime": last + 1}) or [])
        except Exception:
            pass
    total = 0.0
    for row in rows:
        if str(row.get("incomeType") or "") not in PNL_INCOME_TYPES:
            continue
        try:
            total += float(row.get("income") or 0)
        except (TypeError, ValueError, AttributeError):
            continue
    return total


_ws_push = None


def _refresh_equity_cache(account_id: int) -> None:
    """平仓后刷新账户余额与已实现盈亏缓存（方案 A），避免页面停在旧数值。

    v3.2：开/平仓后除了余额，也把「净已实现盈亏 + 净入金」重算一次并推给前端，
    否则已实现要等 60 秒节流才更新（用户 2026-11-22 反馈"刚平仓没变化"）。
    """
    client = _clients.get(account_id)
    if not client: return
    try:
        equity, available = _account_equity(client)
        conn = db.get_conn()
        conn.execute("UPDATE api_accounts SET cached_equity=?, cached_available=?, updated_at=? WHERE id=?",
                     (equity, available, int(time.time()), account_id))
        conn.commit()
        payload = {"type": "account_update", "id": account_id,
                   "cached_equity": equity, "cached_available": available}
        try:
            row = conn.execute("SELECT pnl_since_ms, created_at FROM api_accounts WHERE id=?", (account_id,)).fetchone()
            since = 0
            if row:
                since = int(row["pnl_since_ms"] or 0) or int(row["created_at"] or 0)
            total = realized_pnl_total(client, since)
            conn.execute("UPDATE api_accounts SET realized_pnl=? WHERE id=?", (total, account_id))
            conn.commit()
            payload["realized_pnl"] = total
        except Exception as exc:
            db.insert_log("WARN", "executor", f"刷新已实现盈亏失败 {account_id}: {exc}")
        if _ws_push:                       # 交易后立即推给前端，净值/可用/已实现不用等对账
            _ws_push(payload)
    except Exception as exc:
        db.insert_log("WARN", "executor", f"刷新账户余额缓存失败 {account_id}: {exc}")

def _process_signal(signal: dict, ccxt_clients: dict, config_getter, source: str = "bapi") -> None:
    try:
        required=("portfolio_id","symbol","position_side","executed_qty","avg_price","is_open")
        missing=[k for k in required if k not in signal]
        if missing: raise ValueError(f"信号缺字段: {','.join(missing)}")
        conn=db.get_conn()
        # v3.3：按账户的信号源路由。bapi 采到的信号只喂 signal_source='bapi' 的账户，
        # TG 采到的只喂 'tg'，从根上避免同一笔被两个源各下一次单。
        accounts=conn.execute(
            "SELECT * FROM api_accounts WHERE followed_trader_id=? AND is_enabled=1 "
            "AND COALESCE(signal_source,'bapi')=?",
            (signal["portfolio_id"], source)).fetchall()
        trow=conn.execute("SELECT margin_balance FROM traders WHERE portfolio_id=?",(signal["portfolio_id"],)).fetchone()
        for account in accounts:
            aid=int(account["id"]); client=ccxt_clients.get(aid)
            override=conn.execute("SELECT 1 FROM manual_overrides WHERE api_account_id=? AND symbol=? AND position_side=? AND is_overridden=1",(aid,signal["symbol"],signal["position_side"])).fetchone()
            if override:
                db.insert_log("INFO","executor",f"账户 {aid} {signal['symbol']} 处于手动接管，跳过")
                if reconstructor.is_all_closed(signal["portfolio_id"],signal["symbol"],signal["position_side"]):
                    conn.execute("UPDATE manual_overrides SET is_overridden=0 WHERE api_account_id=? AND symbol=? AND position_side=?",(aid,signal["symbol"],signal["position_side"])); conn.commit()
                continue
            if not client: _insert_copy(account,signal,"OPEN" if signal["is_open"] else "CLOSE",0,0,"FAILED","CCXT 客户端不可用"); continue
            try:
                client.load_markets(); symbol=_symbol(client,signal["symbol"]); market=client.market(symbol)
                precision=market.get("precision",{}).get("amount",3); step=10**(-int(precision)) if isinstance(precision,int) else float(precision or .001)
                price=float(signal["avg_price"] or 0)
                live=bool((config_getter() or {}).get("live_trading_enabled",False))
                if signal["is_open"]:
                    equity,available=_account_equity(client); conn.execute("UPDATE api_accounts SET cached_equity=?,cached_available=?,updated_at=? WHERE id=?",(equity,available,int(time.time()),aid)); conn.commit()
                    margin=float(trow["margin_balance"] or 0) if trow else 0
                    qty=calc_open_qty(float(signal["executed_qty"]),margin,equity,float(account["copy_ratio"]),price,step)
                    if qty*price<5:
                        db.insert_log("INFO","executor",f"账户 {aid} 名义价值 {qty*price:.4f} USDT 小于 5，跳过"); _insert_copy(account,signal,"OPEN",qty,price,"SKIPPED","NOTIONAL_BELOW_5"); continue
                    leverage=int(account["default_leverage"] or 1)
                    # 文档 5.5 + 铁律 11：先定杠杆（含降级），再按实际杠杆估算占用，最后过闸门
                    if live:
                        leverage=_apply_leverage(client,symbol,leverage)
                    estimated=qty*price/max(leverage,1)
                    if not check_margin_gate(aid,estimated,conn):
                        why=f"保证金总闸门拒绝：超过账户净值 {MARGIN_GATE_RATIO:.0%}"; db.insert_log("WARN","executor",f"账户 {aid} {why}"); notifier.notify("trade_fail",why); _insert_copy(account,signal,"OPEN",qty,0,"FAILED",why); continue
                    if not live:
                        db.insert_log("INFO","executor",f"[模拟] 开仓账户 {aid} {symbol} qty={qty}"); _insert_copy(account,signal,"OPEN",qty,price,"SKIPPED","SIMULATED"); continue
                    side="buy" if signal["position_side"]=="LONG" else "sell"
                    # newOrderRespType=RESULT：市价单默认只回 ACK（average 为空），
                    # 那样入库的"我方均价"会退化成交员价，滑点恒为 0（2026-11-15 用户发现）
                    order=client.create_order(symbol,"market",side,qty,None,{"positionSide":signal["position_side"],"newOrderRespType":"RESULT"}); _insert_copy(account,signal,"OPEN",qty,float(order.get("average") or price),"SUCCESS"); notifier.notify("trade_open",f"跟单开仓 {symbol} 数量 {qty}")
                else:
                    # 铁律 4：只用采集侧写入事件的平仓前持仓，禁止回读推算持仓倒推。
                    trader_close=float(signal["executed_qty"]); pre=signal.get("pre_close_qty")
                    if pre is None or float(pre)<=0:
                        why="信号缺少平仓前持仓量，拒绝按比例平仓"; db.insert_log("ERROR","executor",f"{signal['portfolio_id']} {signal['symbol']} {why}"); notifier.notify("parse_error",why); _insert_copy(account,signal,"CLOSE",0,0,"FAILED",why); continue
                    ratio=min(1.0, trader_close/float(pre))
                    match=_find_position(client,symbol,signal["position_side"])
                    actual=abs(float(match.get("contracts") or match.get("info",{}).get("positionAmt") or 0)) if match else 0
                    qty=actual if ratio>=.98 else actual*ratio; qty=float(client.amount_to_precision(symbol,qty)) if qty else 0
                    if qty<=0: _insert_copy(account,signal,"CLOSE",0,price,"SKIPPED","NO_POSITION"); continue
                    if not live:
                        db.insert_log("INFO","executor",f"[模拟] 平仓账户 {aid} {symbol} qty={qty}"); _insert_copy(account,signal,"CLOSE",qty,price,"SKIPPED","SIMULATED"); continue
                    side="sell" if signal["position_side"]=="LONG" else "buy"; order=client.create_order(symbol,"market",side,qty,None,{"positionSide":signal["position_side"],"newOrderRespType":"RESULT"}); _insert_copy(account,signal,"CLOSE",qty,float(order.get("average") or price),"SUCCESS",partial=(ratio<.98)); notifier.notify("trade_close",f"跟单平仓 {symbol} 数量 {qty}"); _refresh_equity_cache(aid)
                    if ratio>=.98:
                        # 我方该币种方向已全平 → 撤掉残留的价格条件单（文档 5.4）
                        _cancel_conditional_orders(client,symbol,signal["position_side"])
                if reconstructor.is_all_closed(signal["portfolio_id"],signal["symbol"],signal["position_side"]):
                    conn.execute("UPDATE manual_overrides SET is_overridden=0 WHERE api_account_id=? AND symbol=? AND position_side=?",(aid,signal["symbol"],signal["position_side"])); conn.commit()
            except Exception as exc:
                logger.exception("CCXT 执行失败"); db.insert_log("ERROR","executor",f"账户 {aid} 下单失败: {exc}"); notifier.notify("trade_fail",f"账户 {aid} 下单失败: {exc}")
                try: _insert_copy(account,signal,"OPEN" if signal["is_open"] else "CLOSE",0,0,"FAILED",str(exc))
                except Exception as dbexc: db.insert_log("ERROR","executor",f"FAILED 记录写入失败: {dbexc}")
    except Exception as exc:
        logger.exception("信号解析失败"); db.insert_log("ERROR","executor",f"信号解析异常: {exc}"); notifier.notify("parse_error",str(exc))


# ---------------------------------------------------------------------------
# v3.3：TG 信号消费。tg-collector 把解析结果写进 signals 表，这里轮询取走并复用
# 同一套 _process_signal（算量/闸门/下单/记账全都不用重写）。
# ---------------------------------------------------------------------------
TG_POLL_SEC = 2.0


def _tg_row_to_signal(row) -> dict:
    """signals 行 → _process_signal 认识的信号结构。"""
    action = str(row["action"])
    is_open = action in ("OPEN", "INCREASE")
    return {
        "portfolio_id": str(row["portfolio_id"]),
        "symbol": str(row["symbol"]),
        "position_side": str(row["side"]),
        "executed_qty": float(row["qty"] or 0),
        "avg_price": float(row["price"] or 0),
        "is_open": is_open,
        # 平仓/减仓要按 (本次减少量 / 事件前持仓) 的比例平我方仓位，铁律 4 同口径
        "pre_close_qty": None if is_open else float(row["pre_qty"] or 0),
        "source": "tg",
    }


def _tg_consumer():
    while _running.is_set():
        try:
            conn = db.get_conn()
            rows = conn.execute(
                "SELECT * FROM signals WHERE source='tg' AND consumed_at=0 ORDER BY id LIMIT 50"
            ).fetchall()
            for row in rows:
                # 先认领再下单：若下单途中进程崩溃，重启后不会重复开仓（宁可漏一笔，
                # 也不能重复下单）。标记成功后才真正执行。
                claimed = conn.execute(
                    "UPDATE signals SET consumed_at=? WHERE id=? AND consumed_at=0",
                    (int(time.time()), row["id"])).rowcount
                conn.commit()
                if not claimed:
                    continue
                sig = _tg_row_to_signal(row)
                try:
                    if sig["executed_qty"] <= 0 or sig["avg_price"] <= 0:
                        db.insert_log("WARN", "tg", f"信号 #{row['source_key']} 数量或价格异常，跳过")
                    else:
                        _process_signal(sig, _clients, _config_getter, source="tg")
                except Exception as exc:
                    logger.exception("TG 信号处理失败")
                    db.insert_log("ERROR", "tg", f"信号 #{row['source_key']} 处理失败（已标记消费，不重试）: {exc}")
        except Exception as exc:
            db.insert_log("ERROR", "tg", f"消费 TG 信号异常: {exc}")
        time.sleep(TG_POLL_SEC)


def start_executor(ccxt_clients: dict, ws_broadcast_fn, config_getter) -> None:
    global _thread,_clients,_config_getter,_ws_push
    _clients,_config_getter=ccxt_clients,config_getter; _ws_push=ws_broadcast_fn
    if _thread and _thread.is_alive(): return
    _running.set()
    def consume():
        while _running.is_set():
            try: event=signal_queue.get(timeout=.5)
            except queue.Empty: continue
            try: _process_signal(event,_clients,_config_getter)
            finally: signal_queue.task_done()
    _thread=threading.Thread(target=consume,daemon=True,name="copy-executor"); _thread.start()
    threading.Thread(target=_tg_consumer,daemon=True,name="tg-consumer").start()
    db.insert_log("INFO","executor","执行器已启动（实盘默认关闭；TG 信号消费已开启）")


def stop_executor() -> None:
    _running.clear()
    if _thread and _thread.is_alive(): _thread.join(timeout=5)
    db.insert_log("INFO","executor","执行器已停止")


def close_position(account_id: int, symbol: str, position_side: str, fraction: float=1.0, portfolio_id="") -> dict:
    """手动平半/全平：直接操作交易所上的实际持仓，成交后才打接管标记（文档 5.4）。"""
    client=_clients.get(account_id)
    if not client: raise RuntimeError("账户 CCXT 客户端不可用")
    sym=_symbol(client,symbol); side=position_side.upper()
    pos=_find_position(client,sym,side)
    qty=abs(float(pos.get("contracts") or pos.get("info",{}).get("positionAmt") or 0))*min(max(fraction,0),1) if pos else 0
    qty=float(client.amount_to_precision(sym,qty)) if qty else 0
    if not (_config_getter() or {}).get("live_trading_enabled",False):
        db.insert_log("INFO","executor",f"[模拟] 手动平仓账户 {account_id} {symbol} qty={qty}（未下单，不打接管标记）"); return {"ok":True,"closed":qty,"simulated":True}
    if qty<=0:
        db.insert_log("WARN","executor",f"手动平仓未找到持仓：{symbol} {side}（账户 {account_id}，fraction={fraction}），未下单")
        return {"ok":True,"closed":0,"simulated":False,"warning":"未找到该币种方向的持仓，未下单"}
    order=client.create_order(sym,"market","sell" if side=="LONG" else "buy",qty,None,{"positionSide":side,"newOrderRespType":"RESULT"})
    _override(db.get_conn(),account_id,portfolio_id,symbol,side)
    _refresh_equity_cache(account_id)
    if fraction>=1:
        # 全平成交后撤掉残留的价格条件单（文档 5.4）
        _cancel_conditional_orders(client,sym,side)
    return {"ok":True,"closed":qty,"order":order}


def set_tp_sl(account_id: int,symbol: str,position_side: str,tp_price=None,sl_price=None,portfolio_id="") -> dict:
    """挂价格止盈/止损：先撤旧单再挂新单，单号写回 manual_overrides（文档 5.4）。

    下单口径（2025-12-09 起）：币安已把条件单迁到 Algo 服务，
    必须走 `POST /fapi/v1/algoOrder`，否则返回 -4120；
    字段名为 `triggerPrice`（不再是 stopPrice），`closePosition="true"` 表示触发后市价全平，
    此时不得再传 quantity。
    """
    client=_clients.get(account_id)
    if not client: raise RuntimeError("账户 CCXT 客户端不可用")
    if tp_price is None and sl_price is None: raise ValueError("止盈价和止损价至少填一个")
    if not (_config_getter() or {}).get("live_trading_enabled",False):
        db.insert_log("INFO","executor",f"[模拟] 设置止盈止损 {symbol}（未下单，不打接管标记）"); return {"ok":True,"simulated":True}
    sym=_symbol(client,symbol); side=position_side.upper()
    if _find_position(client,sym,side) is None: raise ValueError("当前无实际持仓")
    exchange_id=_exchange_id(client,sym)
    _cancel_conditional_orders(client,sym,side)          # 改价 = 先撤旧单
    result={}; order_ids={}
    for order_type,price,id_key in (("TAKE_PROFIT_MARKET",tp_price,"tp_order_id"),
                                    ("STOP_MARKET",sl_price,"sl_order_id")):
        if price is None: continue
        order=_algo_request(client,"POST","algoOrder",{
            "algoType":"CONDITIONAL",                 # 文档：目前仅支持 CONDITIONAL
            "symbol":exchange_id,                     # 算法单接口要币安 id（ZECUSDT）
            "side":"SELL" if side=="LONG" else "BUY",
            "positionSide":side,                      # 双向持仓必须传
            "type":order_type,
            "triggerPrice":float(price),              # 字段名是 triggerPrice，不是 stopPrice
            "closePosition":"true",                   # 触发后市价全平；此时不得传 quantity
        })
        result[order_type]=order; order_ids[id_key]=str(order.get("algoId") or "")
    _override(db.get_conn(),account_id,portfolio_id,symbol,side,
              tp_price=tp_price,sl_price=sl_price,**order_ids)
    db.insert_log("TRADE","executor",f"[账户{account_id}] 设置止盈止损 {symbol} {side} tp={tp_price} sl={sl_price}")
    return {"ok":True,"orders":result}


def emergency_close_all(ccxt_clients: dict,ws_broadcast_fn,config_getter) -> dict:
    import json
    path=Path(__file__).parent.parent/"data"/"config.json"; cfg=config_getter() or {}; cfg["live_trading_enabled"]=False
    try: path.write_text(json.dumps(cfg,ensure_ascii=False,indent=2),encoding="utf-8")
    except Exception as exc: db.insert_log("ERROR","executor",f"关闭实盘开关写配置失败: {exc}")
    closed=failed=0; remaining=[]
    for aid,client in ccxt_clients.items():
        try:
            # 条件单已迁到 Algo 服务，普通订单列表看不到，必须走 openAlgoOrders
            for pos in _positions(client):
                _cancel_conditional_orders(client, pos.get("symbol"), str(pos.get("side") or "").upper())
        except Exception as exc: db.insert_log("ERROR","executor",f"撤条件单失败 {aid}: {exc}")
    for aid,client in ccxt_clients.items():
        try: positions=_positions(client)
        except Exception as exc: failed+=1; remaining.append({"account_id":aid,"error":str(exc)}); continue
        for pos in positions:
            sym=pos.get("symbol"); side=str(pos.get("side") or "LONG").upper(); qty=abs(float(pos.get("contracts") or pos.get("info",{}).get("positionAmt") or 0))
            try:
                client.create_order(sym,"market","sell" if side=="LONG" else "buy",float(client.amount_to_precision(sym,qty)),None,{"positionSide":side,"newOrderRespType":"RESULT"}); closed+=1
                _override(db.get_conn(),int(aid),"",sym,side); time.sleep(.1)
            except Exception as exc:
                failed+=1; remaining.append({"account_id":aid,"symbol":sym,"error":str(exc)}); db.insert_log("ERROR","executor",f"紧急平仓失败 {aid} {sym}: {exc}"); notifier.notify("emergency_close",str(exc),force=True)
    for aid,client in ccxt_clients.items():
        try:
            for pos in _positions(client): remaining.append({"account_id":aid,"symbol":pos.get("symbol"),"side":pos.get("side"),"contracts":pos.get("contracts")})
        except Exception as exc: remaining.append({"account_id":aid,"error":str(exc)})
    if remaining: db.insert_log("ERROR","executor",f"紧急平仓残留: {remaining}"); notifier.notify("emergency_close",f"紧急平仓残留: {remaining}",force=True)
    return {"closed":closed,"failed":failed,"remaining":remaining}
