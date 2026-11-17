"""M3 只读监控引擎：代理线程采集、持久去重与标准信号分发。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

try:
    from . import bapi, db, notifier, reconstructor
    from .proxy_pool import COOLDOWN_IP_BUSY_SEC
except ImportError:
    import bapi  # type: ignore
    import db  # type: ignore
    import notifier  # type: ignore
    import reconstructor  # type: ignore
    from proxy_pool import COOLDOWN_IP_BUSY_SEC  # type: ignore


logger = logging.getLogger(__name__)
MAX_BACKFILL, HOT_PAGE_SIZE, BACKFILL_PAGE_SIZE, FILL_PAGE_SIZE = 200, 10, 200, 200
OBSERVE_INTERVAL_SEC = 15
ERROR_PROBE_INTERVAL_SEC = 15   # 铁律 9：置 ERROR 后不停采，低频探测，成功自动恢复
MARGIN_REFRESH_SEC = 60         # P1-6：后台定时刷新交易员保证金，下单只读库里的缓存值
# 翻页/补页专用等待参数：只在后台工作线程里等，绝不阻塞主调度节拍（铁律 8 只约束主定时器）。
BOOTSTRAP_FIRST_WAIT_SEC = 2.5  # 开机第一枪：多交易员排队时要熬过一个 2.05s 冷却，等不到就整轮跳过
BOOTSTRAP_PAGE_WAIT_SEC = 3.5   # 底账第 2/3 页：最多等满一个 2.05s 冷却 + 余量
FILL_PAGE_WAIT_SEC = 2.2        # 热通道补页：必须熬过一个 2.05s 冷却，否则漏单（见下方注释）
WORKER_RETRY_WAIT_SEC = 1.0     # 限流后换 IP 重试
PAGE_WAIT_POLL_SEC = 0.05

# 一个代理都没配时的一次性直连（仅用于「底账初始化」这种只读动作，绝不用于热通道轮询）：
# 底账读的是币安公开数据，海外机器直连即可；国内机器会自然失败并留下日志。
DIRECT_PROXY = {"url": "", "proxies": None}
_last_margin_refresh: dict[str, float] = {}
_rr_offset = 0                  # P1-7：每拍轮转起点，代理少于交易员时不让前面的交易员独占
_pool = _broadcast_fn = _config_getter = None
_executor: ThreadPoolExecutor | None = None
_scheduler: threading.Thread | None = None
_stop_event = threading.Event()
_state_lock = threading.RLock()
_seen: dict[str, set[str]] = {}
_fail_counts: dict[str, int] = {}
_last_observe: dict[str, float] = {}
_last_failure_kind: dict[str, str] = {}
signal_queue: queue.Queue[dict[str, Any]] = queue.Queue()
_loop = None
_last_error_probe: dict[str, float] = {}


def init_collector(pool, broadcast_fn, config_getter) -> None:
    """注入代理池、异步广播器和动态配置读取器。"""
    global _pool, _broadcast_fn, _config_getter
    _pool, _broadcast_fn, _config_getter = pool, broadcast_fn, config_getter


def rows_of(payload: dict) -> list[dict]:
    """从币安回包里取出记录列表（order-history / position-history 共用）。

    公开函数：main 的「近期操作 / 历史仓位」弹窗接口直接用它拆包，
    避免把 {"code":..., "data":{"list":[...]}} 整块丢给前端。
    """
    data = payload.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for key in ("list", "orders", "rows", "items", "positionHistory", "positionList"):
            if isinstance(data.get(key), list):
                return [r for r in data[key] if isinstance(r, dict)]
    raise bapi.BapiParseError("回包 data 缺少订单/记录列表")


def _detail(payload: dict) -> dict:
    data = payload.get("data", payload)
    if isinstance(data, list):
        data = data[0] if data and isinstance(data[0], dict) else {}
    if not isinstance(data, dict):
        raise bapi.BapiParseError("lead-detail 缺少 data 对象")
    return data


def _order_key(order: dict) -> str:
    if order.get("orderId") not in (None, ""):
        return str(order["orderId"])
    fields = tuple(order.get(k) for k in ("orderTime", "symbol", "side", "positionSide", "executedQty", "avgPrice"))
    return hashlib.sha256(json.dumps(fields, ensure_ascii=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _standardize(pid: str, raw: dict) -> dict:
    try:
        side, pos_side = str(raw["side"]).upper(), str(raw["positionSide"]).upper()
        symbol, qty, price = str(raw["symbol"]), float(raw["executedQty"]), float(raw["avgPrice"])
        order_time = int(float(raw["orderTime"]))
        if side not in ("BUY", "SELL") or pos_side not in ("LONG", "SHORT") or qty < 0:
            raise ValueError("无效 side/positionSide/数量")
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise bapi.BapiParseError(f"订单缺少关键字段或字段无效: {exc}") from exc
    is_open = (side, pos_side) in (("BUY", "LONG"), ("SELL", "SHORT"))
    return {"portfolio_id": pid, "symbol": symbol, "side": side, "position_side": pos_side,
            "executed_qty": qty, "avg_price": price, "order_time": order_time,
            "order_key": _order_key(raw), "is_open": is_open}


def _persist_seen(pid: str, e: dict) -> None:
    try:
        conn = db.get_conn()
        conn.execute("INSERT OR IGNORE INTO orders_seen (trader_portfolio_id,order_key,order_time,symbol,side,position_side,qty,price) VALUES (?,?,?,?,?,?,?,?)",
                     (pid, e["order_key"], e["order_time"], e["symbol"], e["side"], e["position_side"], str(e["executed_qty"]), str(e["avg_price"])))
        conn.commit()
    except Exception as exc:
        logger.exception("写入订单去重记录失败")
        db.insert_log("ERROR", "collector", f"orders_seen 写入失败 {pid}: {exc}")


def _trader_status(pid: str, status: str, message: str = "") -> None:
    try:
        conn = db.get_conn()
        conn.execute("UPDATE traders SET status=?, error_msg=?, last_sync_ts=? WHERE portfolio_id=?", (status, message, int(time.time()), pid))
        conn.commit()
    except Exception as exc:
        logger.exception("更新交易员状态失败")
        db.insert_log("ERROR", "collector", f"交易员状态写入失败 {pid}: {exc}")


def set_event_loop(loop) -> None:
    """由 FastAPI 启动钩子在事件循环线程内调用，供后台线程安全广播。"""
    global _loop
    _loop = loop


def _broadcast(message: dict) -> None:
    if _broadcast_fn is None or _loop is None:
        return
    try:
        future = asyncio.run_coroutine_threadsafe(_broadcast_fn(message), _loop)
        future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
    except Exception as exc:
        db.insert_log("WARN", "collector", f"WebSocket 广播提交失败: {exc}")


def _pool_size() -> int:
    try:
        return len(_pool.get_stats()) if _pool else 0
    except Exception as exc:
        db.insert_log("ERROR", "collector", f"读取代理池状态失败: {exc}")
        return 0


def _cursor(payload: dict, rows: list[dict]) -> str | None:
    data = payload.get("data")
    if isinstance(data, dict):
        value = data.get("indexValue") or data.get("nextIndexValue")
        if value is not None:
            return str(value)
    value = rows[-1].get("indexValue") if rows else None
    return str(value) if value is not None else None


def _acquire():
    try:
        return _pool.acquire() if _pool else None
    except Exception as exc:
        db.insert_log("ERROR", "collector", f"获取代理失败: {exc}")
        return None


def acquire_page_proxy(timeout: float = BOOTSTRAP_PAGE_WAIT_SEC) -> dict | None:
    """翻页/补页/只读查询专用：有空闲代理立刻返回（0ms 轮换），全在 2.05s 冷却内则等到冷却结束。

    只能在工作线程（ThreadPoolExecutor）里调用；主调度必须继续用非阻塞 acquire()，
    否则 sleep 会打乱全局节拍（铁律 8）。等待可被 _stop_event 打断。
    池里没有任何节点时立刻返回 None（调用方自行决定直连还是报错）。
    """
    if _pool is None or not _pool.get_stats():
        return None
    deadline = time.monotonic() + timeout
    while not _stop_event.is_set():
        proxy = _acquire()
        if proxy is not None:
            return proxy
        if time.monotonic() >= deadline:
            return None
        _stop_event.wait(PAGE_WAIT_POLL_SEC)
    return None


def _kind_of(exc: Exception) -> str:
    if isinstance(exc, bapi.BapiRateLimitError):
        return "rate"
    if isinstance(exc, bapi.BapiNetworkError):
        return "network"
    return "parse"


def _release(proxy: dict, success: bool, http_code: int = 200, biz_code: str = "") -> None:
    try:
        _pool.release(proxy.get("url", ""), success=success, http_code=http_code, biz_code=biz_code)
    except Exception as exc:
        db.insert_log("ERROR", "collector", f"归还代理失败: {exc}")


def _release_on(kind: str, proxy: dict | None, exc_text: str = "") -> None:
    """代理归还的唯一出口（避免双释放，也避免失败结论被后续 release(True) 洗白）。

    kind: ok / rate / network / parse —— 解析失败是币安改了回包，不是 IP 的锅，按成功归还。
    """
    if not proxy or not proxy.get("url"):
        return
    if kind == "rate":
        code = next((c for c in bapi.HTTP_BLOCK_CODES if str(c) in exc_text), 0)
        biz = bapi.ERR_IP_BUSY if bapi.ERR_IP_BUSY in exc_text else ""
        _release(proxy, False, code, biz)
    elif kind == "network":
        _release(proxy, False, 0)
    else:
        _release(proxy, True)


def _failure(pid: str, exc: Exception, kind: str) -> None:
    """只做失败记录与计数，绝不碰代理（代理归还在调用方的 finally 里，单点负责）。"""
    _last_failure_kind[pid] = kind
    if kind == "rate":
        db.insert_log("WARN", "collector", f"交易员 {pid} 限流，切换代理: {exc}")
    elif kind == "network":
        db.insert_log("WARN", "collector", f"交易员 {pid} 网络异常: {exc}")
    else:
        db.insert_log("ERROR", "collector", f"交易员 {pid} 回包解析失败: {exc}")
        notifier.notify("parse_error", f"交易员 {pid} 回包解析失败: {exc}")  # 铁律 9：解析异常 ERROR + 告警
    with _state_lock:
        _fail_counts[pid] = _fail_counts.get(pid, 0) + 1
        failures = _fail_counts[pid]
    if failures >= 3:
        _trader_status(pid, "ERROR", str(exc))
        db.insert_log("ERROR", "collector", f"交易员 {pid} 连续失败 {failures} 次，状态置为 ERROR")
        notifier.notify("collect_error", f"交易员 {pid} 连续抓取失败 {failures} 次，已置 ERROR（仍每 15 秒探测）：{exc}")


def _collect_once(pid: str, proxy: dict) -> bool:
    try:
        payload = bapi.fetch_order_history(pid, proxy.get("proxies"), page_size=HOT_PAGE_SIZE)
        rows = rows_of(payload)
        # 铁律 9：主页拿到就立刻归还（proxy=None），补页要另借 IP；
        # 这样补页时本节点不再是 busy，单代理也能在等满 2.05s 后复用。
        _release(proxy, True)
        proxy = None
        with _state_lock:
            known = set(_seen.setdefault(pid, set()))
            first_keys = [_order_key(r) for r in rows]
            fill = len(rows) == HOT_PAGE_SIZE and all(k not in known for k in first_keys)
        known.update(first_keys)
        cursor = _cursor(payload, rows)
        accumulated = list(rows)
        # 铁律 1/2/6：整页皆新才补页。补页必须等满一个 2.05s 冷却（不能只等一小会儿就放弃）：
        # 放弃后下一拍热通道拿到的还是同样那 10 条（全都已知 → fill 变 false → 不再补页），
        # 被挤出身位的那几条就永远补不回来了。宁可本拍多等 2 秒，也不能漏单。
        fill_proxy = None
        try:
            for _ in range(2):
                if not fill or not cursor:
                    break
                fill_proxy = acquire_page_proxy(FILL_PAGE_WAIT_SEC)
                if not fill_proxy:
                    db.insert_log("WARN", "collector", f"交易员 {pid} 补页无可用代理，保留主页 {len(rows)} 条（下一拍续取）")
                    break
                more_payload = bapi.fetch_order_history(pid, fill_proxy.get("proxies"), page_size=FILL_PAGE_SIZE, index_value=cursor)
                more = rows_of(more_payload)
                _release(fill_proxy, True)
                fill_proxy = None
                if not more:
                    break
                accumulated.extend(more)
                cursor = _cursor(more_payload, more)
                more_keys = [_order_key(r) for r in more]
                fill = len(more) == FILL_PAGE_SIZE and all(k not in known for k in more_keys)
                known.update(more_keys)
        except Exception as exc:
            _release_on(_kind_of(exc), fill_proxy, str(exc))
            fill_proxy = None
            db.insert_log("WARN", "collector", f"交易员 {pid} 补页失败，保留已解析的 {len(accumulated)} 条: {exc}")
        events = sorted((_standardize(pid, r) for r in accumulated), key=lambda e: e["order_time"])
        accepted = []
        # 铁律 4 + 铁律 7：每笔新单按时间顺序处理：
        # 先读当前推算持仓写入 pre_close_qty，立刻 apply 更新推算，再放入队列。
        # 同秒拆单（如连续两笔平仓）时，第 2 笔拿到的是第 1 笔 apply 后的推算量，不会重复按初始仓位计算。
        # 锁内只做去重集合操作（铁律 7），apply 和 persist 在锁外执行。
        with _state_lock:
            seen = _seen.setdefault(pid, set())
            new_events = []
            for event in events:
                if event["order_key"] in seen:
                    continue
                seen.add(event["order_key"])
                new_events.append(event)
        for event in new_events:
            # 锁外：读推算持仓、apply、入队（铁律 7：锁内不做网络/推算 IO）
            if not event["is_open"]:
                inferred = reconstructor.get_inferred_positions(pid)
                event["pre_close_qty"] = float(
                    inferred.get(event["symbol"], {}).get(event["position_side"], {}).get("qty") or 0
                )
            try:
                snapshot = reconstructor.apply_order_event(pid, event)
                reconstructor.persist_positions(pid)
                _broadcast({"type": "position_update", "portfolio_id": pid, "positions": snapshot})
            except Exception as exc:
                logger.exception("应用订单事件失败")
                db.insert_log("ERROR", "collector", f"推算订单事件失败 {pid}: {exc}")
            accepted.append(event)
            signal_queue.put(event)
        for event in accepted:
            _persist_seen(pid, event)
        now = int(time.time())
        try:
            conn = db.get_conn()
            latest = max((e["order_time"] for e in accepted), default=0)
            conn.execute("UPDATE traders SET last_sync_ts=?, status='ACTIVE', error_msg='', latest_order_time=MAX(COALESCE(latest_order_time,0),?) WHERE portfolio_id=?", (now, latest, pid))
            conn.commit()
        except Exception as exc:
            db.insert_log("ERROR", "collector", f"同步时间写入失败 {pid}: {exc}")
        with _state_lock:
            _fail_counts[pid] = 0
        _last_failure_kind.pop(pid, None)
        # 代理已在主页解析后归还（proxy 已置 None），这里不再重复归还。
        # P1-6：发现新的开仓单后，后台补刷一次保证金（不挡下单，只让下一单更新鲜）
        if any(e["is_open"] for e in accepted) and _executor is not None:
            try:
                _executor.submit(_refresh_margin, pid)
            except Exception as exc:
                db.insert_log("WARN", "collector", f"提交开仓后保证金刷新失败 {pid}: {exc}")
        _broadcast({"type": "trader_update", "portfolio_id": pid, "last_sync_ts": now})
        return True
    except bapi.BapiRateLimitError as exc:
        _release_on("rate", proxy, str(exc)); _failure(pid, exc, "rate")
    except bapi.BapiNetworkError as exc:
        _release_on("network", proxy); _failure(pid, exc, "network")
    except bapi.BapiParseError as exc:
        _release_on("parse", proxy); _failure(pid, exc, "parse")
    except Exception as exc:
        logger.exception("采集出现未预期异常")
        _release_on("parse", proxy); _failure(pid, exc, "parse")
    return False


def _worker(pid: str, proxy: dict) -> None:
    if _collect_once(pid, proxy) or _last_failure_kind.get(pid) != "rate" or _stop_event.is_set():
        return
    replacement = acquire_page_proxy(WORKER_RETRY_WAIT_SEC)
    if replacement:
        _collect_once(pid, replacement)


def _targets() -> tuple[list[str], list[str]]:
    try:
        conn = db.get_conn()
        rows = conn.execute("SELECT portfolio_id,status FROM traders WHERE status!='REMOVED'").fetchall()
        followed = {r[0] for r in conn.execute("SELECT DISTINCT followed_trader_id FROM api_accounts WHERE is_enabled=1 AND followed_trader_id IS NOT NULL").fetchall()}
        # 铁律 9：ERROR 不再永久停采，单独进入低频探测队列。
        active = [r["portfolio_id"] for r in rows if r["portfolio_id"] in followed and r["status"] != "ERROR"]
        observe = [r["portfolio_id"] for r in rows if r["portfolio_id"] not in followed and r["status"] != "ERROR"]
        errored = [r["portfolio_id"] for r in rows if r["status"] == "ERROR"]
        return active, observe, errored
    except Exception as exc:
        logger.exception("加载采集目标失败")
        db.insert_log("ERROR", "collector", f"加载交易员列表失败: {exc}")
        return [], [], []


def _refresh_margin(pid: str) -> None:
    """刷新交易员保证金到 traders.margin_balance。失败保留旧值，绝不写 0（否则开仓量会算成 0）。"""
    proxy = _acquire()
    if not proxy:
        return
    try:
        detail = _detail(bapi.fetch_lead_detail(pid, proxy.get("proxies")))
        raw = detail.get("marginBalance", detail.get("margin_balance"))
        margin = float(raw)
        if margin <= 0:
            raise bapi.BapiParseError(f"marginBalance 非正数: {raw}")
        conn = db.get_conn()
        conn.execute("UPDATE traders SET margin_balance=? WHERE portfolio_id=?", (margin, pid))
        conn.commit()
        _release(proxy, True)
    except bapi.BapiRateLimitError as exc:
        _release(proxy, False, 0, bapi.ERR_IP_BUSY if bapi.ERR_IP_BUSY in str(exc) else "")
        db.insert_log("WARN", "collector", f"交易员 {pid} 保证金刷新限流，保留旧值: {exc}")
    except bapi.BapiNetworkError as exc:
        _release(proxy, False, 0)
        db.insert_log("WARN", "collector", f"交易员 {pid} 保证金刷新网络异常，保留旧值: {exc}")
    except (bapi.BapiParseError, TypeError, ValueError) as exc:
        _release(proxy, True)
        db.insert_log("ERROR", "collector", f"交易员 {pid} 保证金刷新解析失败，保留旧值: {exc}")
    except Exception as exc:
        _release(proxy, False, 0)
        logger.exception("保证金刷新未预期异常")
        db.insert_log("ERROR", "collector", f"交易员 {pid} 保证金刷新异常，保留旧值: {exc}")


def _dispatch(pid: str) -> None:
    proxy = _acquire()
    if not proxy:
        return
    try:
        _executor.submit(_worker, pid, proxy)
    except Exception as exc:
        _release(proxy, False, 0)
        db.insert_log("ERROR", "collector", f"提交采集任务失败 {pid}: {exc}")


def _schedule_loop() -> None:
    while not _stop_event.is_set():
        started = time.monotonic()
        try:
            inflight = _pool.get_inflight_count()
            if inflight > _pool_size() * 2:
                db.insert_log("WARN", "collector", f"采集背压跳过本拍，在途={inflight}")
            else:
                active, observe, errored = _targets()
                global _rr_offset
                if active:
                    start = _rr_offset % len(active)
                    active = active[start:] + active[:start]
                    _rr_offset += 1
                for pid in active:
                    _dispatch(pid)
                now = time.monotonic()
                # 观察队列（仅观察、未绑定账户）也要刷新保证金，否则卡片上的保证金/杠杆永远为空。
                for pid in active + observe:
                    if now - _last_margin_refresh.get(pid, 0.0) >= MARGIN_REFRESH_SEC:
                        _last_margin_refresh[pid] = now
                        try:
                            _executor.submit(_refresh_margin, pid)
                        except Exception as exc:
                            db.insert_log("ERROR", "collector", f"提交保证金刷新失败 {pid}: {exc}")
                for pid in observe:
                    if now - _last_observe.get(pid, 0.0) >= OBSERVE_INTERVAL_SEC:
                        _last_observe[pid] = now
                        _dispatch(pid)
                for pid in errored:
                    if now - _last_error_probe.get(pid, 0.0) >= ERROR_PROBE_INTERVAL_SEC:
                        _last_error_probe[pid] = now
                        _dispatch(pid)
        except Exception as exc:
            logger.exception("采集调度循环异常")
            db.insert_log("ERROR", "collector", f"调度循环异常: {exc}")
        try:
            cfg = _config_getter() if _config_getter else {}
            interval = max(0.05, float(cfg.get("poll_interval_sec", 1.0)))
        except Exception as exc:
            db.insert_log("ERROR", "collector", f"读取轮询配置失败，使用 1 秒: {exc}")
            interval = 1.0
        _stop_event.wait(max(0.0, interval - (time.monotonic() - started)))


def _bootstrap_one(row: dict) -> None:
    """开机底账：detail + position-history + order-history 第 1 页共用同一个代理。

    这三个是不同接口，实测互不占 2.05 秒锁，所以第一次取到的 IP 可以连发三枪；
    第 2、3 页再用 acquire_page_proxy() 换 IP 或等满冷却（池子为空时走直连，翻页自己等 2.05 秒）。

    铁律 9：本函数不得向上抛异常——单个交易员初始化失败不能拖死整个采集引擎。
    """
    pid = row["portfolio_id"]
    proxy = acquire_page_proxy(BOOTSTRAP_FIRST_WAIT_SEC)
    # 池子里一个节点都没有时允许直连跑这一次底账（东京机器直连币安可读）；
    # 池子里有节点但都不可用时不降级——那说明代理本身出问题了，应该让人看到。
    direct = proxy is None and not _pool_size()
    if direct:
        proxy = DIRECT_PROXY
        db.insert_log("WARN", "collector",
                      f"初始化 {pid} 时池中没有任何代理节点，本次底账改用服务器直连（仅此一次，热通道仍需代理）")
    elif not proxy:
        db.insert_log("WARN", "collector", f"初始化 {pid} 无可用代理，本轮跳过（调度器仍会启动）")
        return

    orders: list[dict] = []
    cursor: str | None = None
    page1_pending = True
    page1_short = False
    failure: tuple[str, Exception] | None = None

    try:
        detail = _detail(bapi.fetch_lead_detail(pid, proxy.get("proxies")))
        nickname = detail.get("nickname") or detail.get("nickName")
        margin = detail.get("marginBalance", detail.get("margin_balance"))
        show = detail.get("positionShow", detail.get("position_show", False))
        visible = int(str(show).lower() in ("true", "1", "yes"))
        conn = db.get_conn()
        conn.execute("UPDATE traders SET nickname=COALESCE(?,nickname),margin_balance=COALESCE(?,margin_balance),position_show=? WHERE portfolio_id=?", (nickname, margin, visible, pid))
        conn.commit()
        history = bapi.fetch_position_history(pid, proxy.get("proxies"))
        try:
            pos_rows = rows_of(history)
        except bapi.BapiParseError as exc:
            pos_rows = []
            db.insert_log("ERROR", "collector", f"交易员 {pid} position-history 结构无法识别: {exc}")
        anchors = [r for r in pos_rows if str(r.get("status", "")) == bapi.POS_STATUS_ALL_CLOSED]
        reconstructor.set_closed_anchors(pid, anchors)
        # 第 1 页复用当前代理：省掉一次 IP 轮换，单 IP 也能立刻拿到第 1 页。
        try:
            response = bapi.fetch_order_history(pid, proxy.get("proxies"), page_size=BACKFILL_PAGE_SIZE)
            page = rows_of(response)
            orders.extend(page)
            cursor = _cursor(response, page)
            page1_pending = False
            page1_short = len(page) < BACKFILL_PAGE_SIZE
        except bapi.BapiRateLimitError as exc:
            # 该 IP 刚被别的交易员用于 order-history；留给下面换 IP 重试。
            page1_short = False
            db.insert_log("WARN", "collector", f"交易员 {pid} 底账第 1 页限流，稍后换代理重试: {exc}")
    except bapi.BapiRateLimitError as exc:
        failure = ("rate", exc)
    except bapi.BapiNetworkError as exc:
        failure = ("network", exc)
    except bapi.BapiParseError as exc:
        failure = ("parse", exc)
    except Exception as exc:
        logger.exception("交易员初始化(detail/pos-hist)失败")
        failure = ("parse", exc)
    finally:
        # 单点归还：成功失败都只还这一次，失败不会被后续 release(True) 洗白。
        _release_on(failure[0] if failure else "ok", proxy, str(failure[1]) if failure else "")

    if failure:
        _failure(pid, failure[1], failure[0])
        return

    # 第 2、3 页（以及任意一页的限流重试）：每页独立借还，拿不到就等冷却。
    # 注意：币安的 2.05 秒锁是「按 IP + 接口」计时的，而代理池只知道「这个 IP 上次被借用的时刻」。
    # 第 1 页前面还有 detail / position-history 两个请求，等轮到这里时池子以为 IP 已冷却，
    # 但 order-history 这个接口其实才过了一两秒 → 会回 11012005。所以限流必须「冷却后重试同一页」，
    # 不能直接放弃（放弃就会少 50 条底账）。
    pages_fetched = 0
    for _attempt in range(6):
        if not page1_pending and (page1_short or not cursor or len(orders) >= MAX_BACKFILL):
            break
        if direct:
            # 直连只有服务器自己的一个 IP，翻页必须自己守满 2.05 秒（第 1 页刚在 phase A 发过）
            if _stop_event.wait(COOLDOWN_IP_BUSY_SEC):
                break
            page_proxy = DIRECT_PROXY
        else:
            page_proxy = acquire_page_proxy(BOOTSTRAP_PAGE_WAIT_SEC)
        if not page_proxy:
            db.insert_log("WARN", "collector", f"交易员 {pid} 底账翻页无可用代理，保留已拿到的 {len(orders)} 条")
            break
        page_failure: tuple[str, Exception] | None = None
        response = None
        page: list[dict] = []
        try:
            response = bapi.fetch_order_history(pid, page_proxy.get("proxies"), page_size=BACKFILL_PAGE_SIZE,
                                                index_value=None if page1_pending else cursor)
            page = rows_of(response)
        except bapi.BapiRateLimitError as exc:
            page_failure = ("rate", exc)
        except bapi.BapiNetworkError as exc:
            page_failure = ("network", exc)
        except Exception as exc:
            logger.exception("底账翻页失败")
            page_failure = ("parse", exc)
        finally:
            _release_on(page_failure[0] if page_failure else "ok", page_proxy,
                        str(page_failure[1]) if page_failure else "")
        if page_failure:
            kind, exc = page_failure
            if kind == "rate":
                # 释放时已把该 IP 的 last_used 刷成「现在」，下一轮 acquire 会等满 2.05 秒
                db.insert_log("WARN", "collector", f"交易员 {pid} 底账第 {pages_fetched + 1} 页限流，冷却后重试: {exc}")
                continue
            log_level = "WARN" if kind == "network" else "ERROR"
            db.insert_log(log_level, "collector", f"交易员 {pid} 底账翻页{kind}失败，保留已拿到的 {len(orders)} 条: {exc}")
            break
        orders.extend(page)
        page1_pending = False
        pages_fetched += 1
        cursor = _cursor(response, page)
        if not page or len(page) < BACKFILL_PAGE_SIZE:
            break

    try:
        # 翻页在 indexValue 边界上可能把同一行重复返回，回放前先按去重键合并，
        # 否则那一笔会在推算里被计算两次（orders_seen 靠唯一索引挡得住，内存回放挡不住）。
        unique: list[dict] = []
        seen_keys: set[str] = set()
        for raw in orders:
            key = _order_key(raw)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique.append(raw)
        orders = sorted(unique[:MAX_BACKFILL], key=lambda r: int(float(r.get("orderTime", 0))))
        reconstructor.initialize_from_history(pid, orders)
        for raw in orders:
            try:
                _persist_seen(pid, _standardize(pid, raw))
            except Exception as exc:
                db.insert_log("ERROR", "collector", f"交易员 {pid} 底账订单写入去重失败，跳过该条: {exc}")
        restored = db.get_conn().execute("SELECT order_key FROM orders_seen WHERE trader_portfolio_id=?", (pid,)).fetchall()
        with _state_lock:
            _seen[pid] = {str(r["order_key"]) for r in restored}
        db.insert_log("INFO", "collector", f"交易员 {pid} 底账完成，共 {len(orders)} 条")
        # 底账建成即视为「监控中」，避免刚加完交易员卡片还显示抓取异常
        db.get_conn().execute("UPDATE traders SET status='ACTIVE', error_msg='' WHERE portfolio_id=?", (pid,))
        db.get_conn().commit()
    except Exception as exc:
        logger.exception("底账落库失败")
        db.insert_log("ERROR", "collector", f"交易员 {pid} 底账落库失败（不影响热通道继续采集）: {exc}")
        return None
    inferred = reconstructor.get_inferred_positions(pid)
    return {"nickname": nickname, "margin_balance": margin, "orders": len(orders),
            "positions": sum(len(sides) for sides in inferred.values())}


def bootstrap_trader(pid: str) -> dict | None:
    """对单个交易员跑一次开机底账（运行中新加交易员时由 main 调用）。

    返回 {nickname, margin_balance, positions}；失败返回 None（失败原因已落日志）。
    """
    return _bootstrap_one({"portfolio_id": pid})


def start_collector() -> None:
    """同步建最多 200 条底账并恢复去重，再启动热通道定时器。"""
    global _executor, _scheduler
    if _pool is None:
        raise RuntimeError("init_collector 必须先调用")
    if _scheduler and _scheduler.is_alive():
        return
    # 事件循环由 main 的启动钩子通过 set_event_loop 注入；后台线程里取不到运行中的循环。
    _stop_event.clear()
    try:
        rows = db.get_conn().execute("SELECT portfolio_id,status FROM traders WHERE status!='REMOVED'").fetchall()
    except Exception as exc:
        db.insert_log("ERROR", "collector", f"读取初始化交易员失败: {exc}")
        rows = []
    for row in rows:
        pid = row["portfolio_id"]
        try:
            restored = db.get_conn().execute("SELECT order_key FROM orders_seen WHERE trader_portfolio_id=?", (pid,)).fetchall()
            with _state_lock:
                _seen[pid] = {str(item["order_key"]) for item in restored}
        except Exception as exc:
            db.insert_log("ERROR", "collector", f"恢复订单去重集合失败 {pid}: {exc}")
        # 铁律 9：一个交易员初始化失败（含未预期异常）只跳过他自己，
        # 绝不拖死启动线程——否则调度器永远不会启动，整个系统静默停摆。
        try:
            _bootstrap_one(dict(row))
        except Exception as exc:
            logger.exception("交易员初始化未预期异常")
            db.insert_log("ERROR", "collector", f"交易员 {pid} 初始化异常已跳过（不影响其他交易员）: {exc}")
    _executor = ThreadPoolExecutor(max_workers=max(1, _pool_size() * 2), thread_name_prefix="collector")
    _scheduler = threading.Thread(target=_schedule_loop, daemon=True, name="collector-scheduler")
    _scheduler.start()
    db.insert_log("INFO", "collector", "监控引擎已启动")


def stop_collector() -> None:
    """停止调度器与线程池。"""
    global _executor
    _stop_event.set()
    if _scheduler and _scheduler.is_alive():
        _scheduler.join(timeout=5)
    if _executor:
        _executor.shutdown(wait=False, cancel_futures=True)
        _executor = None
    db.insert_log("INFO", "collector", "监控引擎已停止")
