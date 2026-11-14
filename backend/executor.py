"""CCXT execution and copy-trading orchestration. Live trading is opt-in."""
from __future__ import annotations

import logging
import math
import queue
import threading
import time
from pathlib import Path

try:
    from . import db, reconstructor, notifier
    from .collector import signal_queue
except ImportError:
    import db  # type: ignore
    import reconstructor  # type: ignore
    import notifier  # type: ignore
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
    raw = trader_qty * (my_equity / trader_margin) * copy_ratio * 0.90
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


def _insert_copy(account, signal, action, qty, price, status, reason="", scale=None):
    conn = db.get_conn(); trader = conn.execute("SELECT nickname FROM traders WHERE portfolio_id=?", (signal.get("portfolio_id"),)).fetchone()
    conn.execute("INSERT INTO copy_orders(api_account_id,api_remark,portfolio_id,trader_nickname,symbol,action,position_side,trader_qty,trader_price,my_qty,my_price,scale,status,error_msg,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (account["id"], account["remark"], signal.get("portfolio_id"), trader["nickname"] if trader else "", signal.get("symbol"), action, signal.get("position_side"), signal.get("executed_qty"), signal.get("avg_price"), qty, price, scale, status, reason, int(time.time()))); conn.commit()
    # P1-13：每条跟单记录同步写一条 TRADE 级别日志，「交易与跟单」筛选才有数据
    db.insert_log("TRADE", "executor", f"[{account['remark']}] {action} {signal.get('symbol')} {signal.get('position_side')} 数量={qty} 价格={price} → {status}{(' ' + reason) if reason else ''}")


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


def check_margin_gate(account_id: int, estimated_margin: float, conn) -> bool:
    row=conn.execute("SELECT cached_equity FROM api_accounts WHERE id=?",(account_id,)).fetchone(); equity=float(row["cached_equity"] or 0) if row else 0
    client=_clients.get(account_id); used=0.0
    if client:
        try:
            for p in _positions(client):
                info=p.get("info",{}); initial=float(info.get("initialMargin") or 0)
                used += initial or abs(float(p.get("contracts") or 0))*float(p.get("markPrice") or p.get("entryPrice") or 0)/max(float(p.get("leverage") or 1),1)
        except Exception as exc: db.insert_log("ERROR","executor",f"账户 {account_id} 查询保证金失败: {exc}"); return False
    return equity > 0 and used + estimated_margin <= equity * .9


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


def _process_signal(signal: dict, ccxt_clients: dict, config_getter) -> None:
    try:
        required=("portfolio_id","symbol","position_side","executed_qty","avg_price","is_open")
        missing=[k for k in required if k not in signal]
        if missing: raise ValueError(f"信号缺字段: {','.join(missing)}")
        conn=db.get_conn(); accounts=conn.execute("SELECT * FROM api_accounts WHERE followed_trader_id=? AND is_enabled=1",(signal["portfolio_id"],)).fetchall()
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
                        why="保证金总闸门拒绝：超过账户净值 90%"; db.insert_log("WARN","executor",f"账户 {aid} {why}"); notifier.notify("trade_fail",why); _insert_copy(account,signal,"OPEN",qty,price,"FAILED",why); continue
                    if not live:
                        db.insert_log("INFO","executor",f"[模拟] 开仓账户 {aid} {symbol} qty={qty}"); _insert_copy(account,signal,"OPEN",qty,price,"SKIPPED","SIMULATED"); continue
                    side="buy" if signal["position_side"]=="LONG" else "sell"
                    order=client.create_order(symbol,"market",side,qty,None,{"positionSide":signal["position_side"]}); _insert_copy(account,signal,"OPEN",qty,float(order.get("average") or price),"SUCCESS"); notifier.notify("trade_open",f"跟单开仓 {symbol} 数量 {qty}")
                else:
                    # 铁律 4：只用采集侧写入事件的平仓前持仓，禁止回读推算持仓倒推。
                    trader_close=float(signal["executed_qty"]); pre=signal.get("pre_close_qty")
                    if pre is None or float(pre)<=0:
                        why="信号缺少平仓前持仓量，拒绝按比例平仓"; db.insert_log("ERROR","executor",f"{signal['portfolio_id']} {signal['symbol']} {why}"); notifier.notify("parse_error",why); _insert_copy(account,signal,"CLOSE",0,price,"FAILED",why); continue
                    ratio=min(1.0, trader_close/float(pre))
                    match=_find_position(client,symbol,signal["position_side"])
                    actual=abs(float(match.get("contracts") or match.get("info",{}).get("positionAmt") or 0)) if match else 0
                    qty=actual if ratio>=.98 else actual*ratio; qty=float(client.amount_to_precision(symbol,qty)) if qty else 0
                    if qty<=0: _insert_copy(account,signal,"CLOSE",0,price,"SKIPPED","NO_POSITION"); continue
                    if not live:
                        db.insert_log("INFO","executor",f"[模拟] 平仓账户 {aid} {symbol} qty={qty}"); _insert_copy(account,signal,"CLOSE",qty,price,"SKIPPED","SIMULATED"); continue
                    side="sell" if signal["position_side"]=="LONG" else "buy"; order=client.create_order(symbol,"market",side,qty,None,{"positionSide":signal["position_side"]}); _insert_copy(account,signal,"CLOSE",qty,float(order.get("average") or price),"SUCCESS"); notifier.notify("trade_close",f"跟单平仓 {symbol} 数量 {qty}")
                    if ratio>=.98:
                        # 我方该币种方向已全平 → 撤掉残留的价格条件单（文档 5.4）
                        _cancel_conditional_orders(client,symbol,signal["position_side"])
                if reconstructor.is_all_closed(signal["portfolio_id"],signal["symbol"],signal["position_side"]):
                    conn.execute("UPDATE manual_overrides SET is_overridden=0 WHERE api_account_id=? AND symbol=? AND position_side=?",(aid,signal["symbol"],signal["position_side"])); conn.commit()
            except Exception as exc:
                logger.exception("CCXT 执行失败"); db.insert_log("ERROR","executor",f"账户 {aid} 下单失败: {exc}"); notifier.notify("trade_fail",f"账户 {aid} 下单失败: {exc}")
                try: _insert_copy(account,signal,"OPEN" if signal["is_open"] else "CLOSE",0,float(signal.get("avg_price") or 0),"FAILED",str(exc))
                except Exception as dbexc: db.insert_log("ERROR","executor",f"FAILED 记录写入失败: {dbexc}")
    except Exception as exc:
        logger.exception("信号解析失败"); db.insert_log("ERROR","executor",f"信号解析异常: {exc}"); notifier.notify("parse_error",str(exc))


def start_executor(ccxt_clients: dict, ws_broadcast_fn, config_getter) -> None:
    global _thread,_clients,_config_getter
    _clients,_config_getter=ccxt_clients,config_getter
    if _thread and _thread.is_alive(): return
    _running.set()
    def consume():
        while _running.is_set():
            try: event=signal_queue.get(timeout=.5)
            except queue.Empty: continue
            try: _process_signal(event,_clients,_config_getter)
            finally: signal_queue.task_done()
    _thread=threading.Thread(target=consume,daemon=True,name="copy-executor"); _thread.start(); db.insert_log("INFO","executor","执行器已启动（实盘默认关闭）")


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
    order=client.create_order(sym,"market","sell" if side=="LONG" else "buy",qty,None,{"positionSide":side})
    _override(db.get_conn(),account_id,portfolio_id,symbol,side)
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
                client.create_order(sym,"market","sell" if side=="LONG" else "buy",float(client.amount_to_precision(sym,qty)),None,{"positionSide":side}); closed+=1
                _override(db.get_conn(),int(aid),"",sym,side); time.sleep(.1)
            except Exception as exc:
                failed+=1; remaining.append({"account_id":aid,"symbol":sym,"error":str(exc)}); db.insert_log("ERROR","executor",f"紧急平仓失败 {aid} {sym}: {exc}"); notifier.notify("emergency_close",str(exc),force=True)
    for aid,client in ccxt_clients.items():
        try:
            for pos in _positions(client): remaining.append({"account_id":aid,"symbol":pos.get("symbol"),"side":pos.get("side"),"contracts":pos.get("contracts")})
        except Exception as exc: remaining.append({"account_id":aid,"error":str(exc)})
    if remaining: db.insert_log("ERROR","executor",f"紧急平仓残留: {remaining}"); notifier.notify("emergency_close",f"紧急平仓残留: {remaining}",force=True)
    return {"closed":closed,"failed":failed,"remaining":remaining}
