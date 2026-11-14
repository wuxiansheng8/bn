"""Telegram notifications with per-event throttling."""
from __future__ import annotations
import json
import logging
import threading
import time
from pathlib import Path
import requests
try:
    from . import db
except ImportError:
    import db  # type: ignore

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).parent.parent / "data" / "config.json"
_last_sent: dict[str, float] = {}
_lock = threading.Lock()
DEFAULT_THROTTLE_SEC = 1800
# 用户 2026-11-14 裁决：这几类每笔都要通知，不节流；其余（代理/抓取/解析异常等）按配置节流
NO_THROTTLE_EVENTS = {"trade_open", "trade_close", "tp_sl_trigger", "emergency_close"}


def notify(event_type: str, message: str, force: bool = False) -> None:
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        db.insert_log("WARN", "notifier", f"读取通知配置失败: {exc}")
        return
    try:
        throttle = float(cfg.get("tg_throttle_sec", DEFAULT_THROTTLE_SEC))
    except (TypeError, ValueError):
        throttle = DEFAULT_THROTTLE_SEC
    now = time.monotonic()
    with _lock:
        skip_throttle = force or event_type in NO_THROTTLE_EVENTS
        if not skip_throttle and now - _last_sent.get(event_type, -throttle - 1) < throttle:
            return
        _last_sent[event_type] = now
    if not cfg.get("tg_enabled", False):
        db.insert_log("INFO", "notifier", f"TG 已关闭 [{event_type}]: {message}")
        return
    token, chat_id = cfg.get("tg_bot_token", ""), cfg.get("tg_chat_id", "")
    if not token or not chat_id:
        db.insert_log("WARN", "notifier", "TG 已启用但 bot token/chat id 未配置")
        return
    try:
        response = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                 json={"chat_id": chat_id, "text": f"[{event_type}] {message}"}, timeout=8)
        response.raise_for_status()
        db.insert_log("INFO", "notifier", f"TG 通知已发送 [{event_type}]")
    except Exception as exc:
        logger.warning("Telegram 通知失败: %s", exc)
        db.insert_log("WARN", "notifier", f"Telegram 通知失败 [{event_type}]: {exc}")
