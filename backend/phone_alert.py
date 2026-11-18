"""FlashDuty 电话/短信提醒：按 API 账户独立推送（每账户一个 integration_key）。

设计要点（2026-11-16 与用户确认）：
- 只发 Critical；不发 Ok，告警由 FlashDuty 侧「超时自动关闭」清理。
- 开仓 / 减仓 / 平仓 / 下单失败 都发，每笔唯一 alert_key（避免被去重吞掉）。
- 同账户同类型 60 秒冷却，防止同一笔因重试响两次。
- 发送失败不重试，只记 WARN 日志，绝不影响交易主流程。
"""
from __future__ import annotations

import threading
import time
from urllib.parse import parse_qs, urlparse

import requests

try:  # 兼容包内导入与顶层导入两种启动方式
    from . import db
except ImportError:  # pragma: no cover
    import db  # type: ignore

PUSH_URL = "https://api.flashcat.cloud/event/push/alert/standard"
COOLDOWN_SEC = 60
TIMEOUT_SEC = 8
# 2026-11-19：Critical 成功后延迟 10 秒自动补发 Ok 关闭该故障。
# 原因：FlashDuty 分派策略「重复通知」关闭时，同一未关闭故障的后续事件不再通知，
# 而我们的故障从不关闭（只发 Critical、不发 Ok），导致第一条之后全部静音。
# 10 秒是为确保语音/短信已真正拨出（窗口太短可能在推送前就关闭）。
CLOSE_DELAY_SEC = 10.0

_last_sent: dict[tuple[int, str], float] = {}
_lock = threading.Lock()


def extract_key(raw: str) -> str:
    """从完整推送地址或裸 integration_key 中取出 key。"""
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.startswith("http"):
        try:
            query = parse_qs(urlparse(raw).query)
        except Exception:
            return ""
        return (query.get("integration_key") or [""])[0].strip()
    return raw


def masked(raw: str) -> str:
    """返回给前端显示的掩码地址。"""
    key = extract_key(raw)
    if not key:
        return ""
    return f"{PUSH_URL.rsplit('/', 1)[0]}/...?integration_key={key[:6]}***{key[-4:]}"


def _post(key: str, payload: dict) -> tuple[bool, str, dict]:
    try:
        resp = requests.post(f"{PUSH_URL}?integration_key={key}", json=payload,
                             timeout=TIMEOUT_SEC, headers={"Content-Type": "application/json"})
        try:
            body = resp.json() or {}
        except Exception:
            body = {}
        err = (body.get("error") or {}).get("code") or ""
        ok = resp.status_code == 200 and not err
        return ok, ("" if ok else (err or f"HTTP {resp.status_code}")), body
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}", {}


def _record(account_id: int, ok: bool, err: str, now: float) -> None:
    try:
        conn = db.get_conn()
        conn.execute("UPDATE api_accounts SET phone_last_status=?, phone_last_at=? WHERE id=?",
                     ("ok" if ok else f"fail:{err}", int(now), account_id))
        conn.commit()
    except Exception:  # noqa: BLE001
        pass


def _close_alert(key: str, alert_key: str, title: str) -> None:
    """补发 Ok 关闭本条故障：让下一条交易提醒能以"新故障"触发通知。"""
    try:
        time.sleep(CLOSE_DELAY_SEC)
        ok, err, _ = _post(key, {
            "event_status": "Ok",
            "alert_key": alert_key,
            "title_rule": title[:512],
            "description": "单笔交易提醒已自动关闭，无需处理",
        })
        if not ok:
            db.insert_log("WARN", "phone", f"自动关闭告警失败 {alert_key}: {err}")
    except Exception as exc:  # noqa: BLE001
        db.insert_log("WARN", "phone", f"自动关闭告警异常 {alert_key}: {exc}")


def _spawn_close(key: str, alert_key: str, title: str) -> None:
    """后台线程关闭，不阻塞交易主流程（send 本身已在执行器线程里同步调用）。"""
    try:
        threading.Thread(target=_close_alert, args=(key, alert_key, title), daemon=True).start()
    except Exception:  # noqa: BLE001
        pass


def send(account, kind: str, title: str, description: str, labels: dict | None = None,
         use_cooldown: bool = True) -> dict:
    """推送一条 Critical 事件。account 为 api_accounts 行（含 phone_key）。"""
    key = extract_key(account["phone_key"] if "phone_key" in account.keys() else "")
    if not key:
        return {"ok": False, "skipped": "未配置推送地址"}
    account_id = int(account["id"])
    now = time.time()
    if use_cooldown:
        with _lock:
            if now - _last_sent.get((account_id, kind), 0) < COOLDOWN_SEC:
                return {"ok": False, "skipped": "冷却中"}
            _last_sent[(account_id, kind)] = now

    alert_key = f"bn-{account_id}-{kind}-{int(now * 1000)}"
    payload = {
        "event_status": "Critical",
        "title_rule": title[:512],
        "alert_key": alert_key,
        "description": (description or "")[:2048],
        "labels": {k: str(v) for k, v in (labels or {}).items()},
    }
    ok, err, body = _post(key, payload)
    _record(account_id, ok, err, now)
    db.insert_log("INFO" if ok else "WARN", "phone",
                  f"[{account['remark']}] {title} → {'成功' if ok else '失败 ' + err}")
    if ok:
        _spawn_close(key, alert_key, title)
    return {"ok": ok, "error": err, "request_id": body.get("request_id", "")}


def notify_copy(account, signal: dict, action: str, qty: float, price: float,
                status: str, reason: str = "", trader: str = "", partial: bool = False) -> None:
    """跟单结果通知：开仓 / 减仓 / 平仓 / 失败。"""
    if status not in ("SUCCESS", "FAILED"):
        return
    symbol = signal.get("symbol") or ""
    side_cn = "多" if signal.get("position_side") == "LONG" else "空"
    clock = time.strftime("%H:%M:%S")
    labels = {"account": account["remark"], "symbol": symbol,
              "side": signal.get("position_side") or "", "status": status}

    if status == "FAILED":
        kind = "fail"
        title = f"[失败] {account['remark']} · {symbol} {side_cn}"
        desc = f"{reason or '下单失败'} · {clock}"
    elif action == "OPEN":
        kind = "open"
        title = f"[开仓] {account['remark']} · {symbol} {side_cn}"
        desc = f"数量 {qty:g} 均价 {price:g} · 跟单 {trader or '-'} · {clock}"
    else:
        kind = "reduce" if partial else "close"
        word = "减仓" if partial else "平仓"
        title = f"[{word}] {account['remark']} · {symbol} {side_cn}"
        desc = f"数量 {qty:g} 均价 {price:g} · 跟单 {trader or '-'} · {clock}"

    try:
        send(account, kind, title, desc, labels)
    except Exception as exc:  # noqa: BLE001
        db.insert_log("WARN", "phone", f"电话提醒异常 {account.get('remark')}: {exc}")


def send_test(account) -> dict:
    """测试推送（绕过冷却，强制发送）。"""
    title = f"[测试] {account['remark']} · 电话提醒测试"
    desc = f"这是一条测试消息，收到说明电话/短信通道正常 · {time.strftime('%H:%M:%S')}"
    return send(account, "test", title, desc, {"account": account["remark"], "test": "1"},
                use_cooldown=False)
