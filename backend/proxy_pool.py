"""
proxy_pool.py - 代理节点轮转、冷却与状态管理。
本模块只管理代理池，不负责调度线程的创建；调用方应在后台线程池中发起同步请求。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

try:
    from . import bapi, db
except ImportError:
    import bapi  # type: ignore
    import db  # type: ignore

# 可调策略集中定义，具体冷却与背压阈值遵循开发文档铁律 2、3、8、9。
COOLDOWN_IP_BUSY_SEC = 2.05
COOLDOWN_HTTP_BLOCK_SEC = 60
COOLDOWN_NETWORK_FAIL_SEC = 10
COOLDOWN_CONSECUTIVE_FAIL_SEC = 60
MAX_INFLIGHT_RATIO = 2
CONSECUTIVE_FAIL_THRESHOLD = 3

# 探测接口要求 portfolio_id。优先使用业务配置，否则使用已知公开数据标识。
TEST_PORTFOLIO_ID = ""
DEFAULT_TEST_PORTFOLIO_ID = ""

logger = logging.getLogger(__name__)


# 手机/中文输入法常把半角符号打成全角，粘进来就成了非法地址；这里先统一成半角（2026-11-14 实测踩坑）。
_FULLWIDTH_MAP = str.maketrans({"：": ":", "＠": "@", "／": "/", "．": ".", "％": "%"})
_SCHEMES = ("socks5", "socks5h", "http", "https")


def _normalize_proxy(raw_url: str) -> tuple[str, dict[str, str]]:
    """规范化代理地址，并生成 requests 用的 proxies 字典。

    兼容三种输入：
      1. scheme://[user:pass@]host:port     （标准 URL）
      2. scheme:user:pass@host:port         （漏了 //，代理商文档常见写法）
      3. host:port:user:pass                （无账号时可写 host:port）
    全角冒号/＠/／会先转成半角。解析失败抛出 ValueError，由调用方转成可读提示。
    """
    value = (raw_url or "").strip().translate(_FULLWIDTH_MAP)
    if not value:
        raise ValueError("代理地址为空")
    if "://" not in value:
        head, sep, rest = value.partition(":")
        if sep and head.lower() in _SCHEMES and "@" in rest:
            value = f"{head}://{rest}"
        else:
            parts = value.split(":")
            if len(parts) not in (2, 4):
                raise ValueError("代理格式应为 scheme://[user:pass@]host:port、scheme:user:pass@host:port 或 host:port:user:pass")
            if len(parts) == 2:
                host, port = parts
                value = f"http://{host}:{port}"
            else:
                host, port, username, password = parts
                value = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in _SCHEMES:
        raise ValueError(f"不支持的代理协议: {parsed.scheme or '(空)'}，只支持 socks5 / socks5h / http / https")
    try:
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValueError("代理端口不是有效数字")
    if not host:
        raise ValueError("代理地址缺少有效主机")
    if not port or not (0 < port < 65536):
        raise ValueError("代理地址缺少有效端口（1-65535）")
    # URL 标准化并保留认证信息；SOCKS 一律用 socks5h（远端 DNS 解析）。
    scheme = parsed.scheme.lower()
    if scheme in ("socks5", "socks5h"):
        scheme = "socks5h"
    normalized = urlunsplit((scheme, parsed.netloc, "", "", ""))
    return normalized, {"http": normalized, "https": normalized}


class ProxyPool:
    """线程安全的代理池；锁只保护内存状态，不覆盖数据库或网络 IO。"""

    def __init__(self) -> None:
        """初始化内存状态并载入数据库节点（铁律 7：加载 IO 在锁外）。"""
        self._lock = threading.Lock()
        self._nodes: dict[str, dict[str, Any]] = {}
        self._cursor = 0
        self.inflight_count = 0
        self.reload_from_db()

    def reload_from_db(self) -> None:
        """从 proxy_nodes 刷新节点元数据，保留运行期冷却与在途状态。"""
        try:
            rows = db.get_conn().execute(
                "SELECT proxy_url, display_name, exit_ip, last_latency_ms, status, fail_count "
                "FROM proxy_nodes ORDER BY id"
            ).fetchall()
        except Exception as exc:
            logger.exception("加载代理节点失败")
            db.insert_log("ERROR", "proxy_pool", f"加载代理节点失败: {exc}")
            return
        now = time.monotonic()
        with self._lock:
            old = self._nodes
            fresh: dict[str, dict[str, Any]] = {}
            for row in rows:
                url = row["proxy_url"]
                try:
                    normalized, proxies = _normalize_proxy(url)
                except (TypeError, ValueError) as exc:
                    db.insert_log("ERROR", "proxy_pool", f"忽略无效代理 {url}: {exc}")
                    continue
                prior = old.get(url, {})
                fresh[url] = {
                    "url": url, "normalized_url": normalized, "proxies": proxies,
                    "display_name": row["display_name"] or url,
                    "exit_ip": row["exit_ip"] or "", "latency": row["last_latency_ms"],
                    "status": row["status"] or "UNTESTED",
                    "fail_count": int(row["fail_count"] or 0), "busy": prior.get("busy", False),
                    "last_used_ts": prior.get("last_used_ts", 0.0),
                    "cooldown_until": max(now, prior.get("cooldown_until", 0.0))
                    if prior.get("cooldown_until", 0.0) > now else prior.get("cooldown_until", 0.0),
                }
            self._nodes = fresh
            self.inflight_count = sum(1 for node in fresh.values() if node["busy"])
            self._cursor = 0
        db.insert_log("INFO", "proxy_pool", f"代理池已加载 {len(fresh)} 个节点")

    def acquire(self) -> dict[str, Any] | None:
        """非阻塞地取得可用节点；无节点或背压时立即跳过（铁律 3、8）。"""
        now = time.monotonic()
        with self._lock:
            nodes = list(self._nodes.values())
            if not nodes or self.inflight_count >= len(nodes) * MAX_INFLIGHT_RATIO:
                return None
            count = len(nodes)
            for offset in range(count):
                index = (self._cursor + offset) % count
                node = nodes[index]
                if node["busy"] or node["cooldown_until"] > now:
                    continue
                if now - node["last_used_ts"] < COOLDOWN_IP_BUSY_SEC:
                    continue
                node["busy"] = True
                # acquire 紧邻调用方发射请求，记录当下真实调度时刻，不写未来占位时间。
                node["last_used_ts"] = now
                self.inflight_count += 1
                self._cursor = (index + 1) % count
                return {"url": node["url"], "proxies": dict(node["proxies"]),
                        "display_name": node["display_name"]}
        return None

    def release(self, proxy_url: str, success: bool, http_code: int = 200,
                biz_code: str = "") -> None:
        """归还节点并分类更新冷却、失败计数及持久状态（铁律 2、9）。"""
        now = time.monotonic()
        code = str(biz_code or "")
        logs: list[tuple[str, str]] = []
        with self._lock:
            node = self._nodes.get(proxy_url)
            if node is None:
                # URL 可能由调用方规范化后返回，尝试按规范化地址匹配。
                node = next((item for item in self._nodes.values()
                             if item["normalized_url"] == proxy_url), None)
            if node is not None:
                node["busy"] = False
                self.inflight_count = max(0, self.inflight_count - 1)
                if code == bapi.ERR_IP_BUSY:
                    node["last_used_ts"] = now
                    logs.append(("WARN", f"代理 {node['display_name']} 命中 11012005，刷新请求时间并切换节点"))
                elif http_code in bapi.HTTP_BLOCK_CODES:
                    node["fail_count"] += 1
                    node["cooldown_until"] = max(node["cooldown_until"], now + COOLDOWN_HTTP_BLOCK_SEC)
                    node["status"] = "FAIL"
                    logs.append(("WARN", f"代理 {node['display_name']} HTTP {http_code}，冷却 {COOLDOWN_HTTP_BLOCK_SEC}s，连续失败 {node['fail_count']} 次"))
                elif not success and http_code == 0:
                    node["fail_count"] += 1
                    node["cooldown_until"] = max(node["cooldown_until"], now + COOLDOWN_NETWORK_FAIL_SEC)
                    node["status"] = "FAIL"
                    logs.append(("WARN", f"代理 {node['display_name']} 网络失败，冷却 {COOLDOWN_NETWORK_FAIL_SEC}s，连续失败 {node['fail_count']} 次"))
                elif success:
                    recovered = node["fail_count"] > 0 or node["status"] != "OK"
                    node["fail_count"] = 0
                    node["status"] = "OK"
                    node["cooldown_until"] = 0.0
                    if recovered:
                        logs.append(("INFO", f"代理 {node['display_name']} 请求恢复，失败计数已清零"))
                else:
                    # 解析异常不归类为网络故障，但记录状态和错误，避免静默失败。
                    node["fail_count"] += 1
                    node["status"] = "FAIL"
                    logs.append(("ERROR", f"代理 {node['display_name']} 请求异常，HTTP {http_code}，连续失败 {node['fail_count']} 次"))
                if node["fail_count"] >= CONSECUTIVE_FAIL_THRESHOLD:
                    node["cooldown_until"] = max(node["cooldown_until"], now + COOLDOWN_CONSECUTIVE_FAIL_SEC)
                    logs.append(("ERROR", f"代理 {node['display_name']} 连续失败 {node['fail_count']} 次，升级冷却 {COOLDOWN_CONSECUTIVE_FAIL_SEC}s"))
                url = node["url"]
                status = node["status"]
                fail_count = node["fail_count"]
        if node is None:
            db.insert_log("WARN", "proxy_pool", f"归还未知代理: {proxy_url}")
            return
        for level, message in logs:
            db.insert_log(level, "proxy_pool", message)
        if logs:
            self._persist_node(url, status, fail_count)

    def _persist_node(self, proxy_url: str, status: str, fail_count: int,
                      latency_ms: int | None = None, exit_ip: str | None = None) -> None:
        """将节点健康状态写入 proxy_nodes；数据库操作始终在状态锁外。"""
        try:
            conn = db.get_conn()
            if latency_ms is None and exit_ip is None:
                conn.execute("UPDATE proxy_nodes SET status=?, fail_count=?, updated_at=? WHERE proxy_url=?",
                             (status, fail_count, int(time.time()), proxy_url))
            else:
                conn.execute("UPDATE proxy_nodes SET status=?, fail_count=?, updated_at=?, "
                             "last_latency_ms=COALESCE(?, last_latency_ms), exit_ip=COALESCE(?, exit_ip) "
                             "WHERE proxy_url=?",
                             (status, fail_count, int(time.time()), latency_ms, exit_ip, proxy_url))
            conn.commit()
        except Exception as exc:
            logger.exception("写入代理状态失败")
            db.insert_log("ERROR", "proxy_pool", f"写入代理状态失败 {proxy_url}: {exc}")

    def get_stats(self) -> list[dict[str, Any]]:
        """返回节点状态快照，供系统页展示失败次数、在途和剩余冷却。"""
        now = time.monotonic()
        with self._lock:
            return [{"url": n["url"], "display_name": n["display_name"],
                     "exit_ip": n["exit_ip"], "latency": n["latency"],
                     "status": n["status"], "fail_count": n["fail_count"],
                     "busy": n["busy"],
                     "cooldown_remaining": max(0.0, round(n["cooldown_until"] - now, 3))}
                    for n in self._nodes.values()]

    def get_inflight_count(self) -> int:
        """返回当前在途任务数，供上层调度器执行背压判断（铁律 3）。"""
        with self._lock:
            return self.inflight_count

    def test_proxy(self, proxy_url: str) -> dict[str, Any]:
        """通过订单历史接口测试代理并测量延迟；调用者须提供可用测试 ID。"""
        started = time.perf_counter()
        try:
            normalized, proxies = _normalize_proxy(proxy_url)
            portfolio_id = TEST_PORTFOLIO_ID or DEFAULT_TEST_PORTFOLIO_ID
            if not portfolio_id:
                raise ValueError("未配置 TEST_PORTFOLIO_ID，无法调用 order-history 探测接口")
            result = bapi.fetch_order_history(portfolio_id, proxies=proxies, page_size=1)
            latency = round((time.perf_counter() - started) * 1000, 2)
            data = result.get("data")
            if data is None:
                raise bapi.BapiParseError("order-history 探测结果缺少 data 字段")
            exit_ip = ""
            self._persist_node(proxy_url, "OK", 0, int(latency), exit_ip or None)
            with self._lock:
                node = self._nodes.get(proxy_url)
                if node:
                    node["latency"] = int(latency)
                    node["status"] = "OK"
                    node["fail_count"] = 0
            db.insert_log("INFO", "proxy_pool", f"代理测试成功 {normalized}，延迟 {latency}ms")
            return {"ok": True, "latency_ms": latency, "exit_ip": exit_ip, "error": ""}
        except bapi.BapiRateLimitError as exc:
            is_ip_busy = bapi.ERR_IP_BUSY in str(exc)
            db.insert_log("WARN", "proxy_pool", f"代理测试限流 {proxy_url}: {exc}")
            return {"ok": False, "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "exit_ip": "", "error": str(exc), "biz_code": bapi.ERR_IP_BUSY if is_ip_busy else ""}
        except bapi.BapiNetworkError as exc:
            db.insert_log("WARN", "proxy_pool", f"代理测试网络异常 {proxy_url}: {exc}")
            return {"ok": False, "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "exit_ip": "", "error": str(exc)}
        except (bapi.BapiParseError, KeyError, ValueError, TypeError) as exc:
            logger.exception("代理测试解析或配置异常")
            db.insert_log("ERROR", "proxy_pool", f"代理测试解析/配置异常 {proxy_url}: {exc}；需触发 TG 告警")
            return {"ok": False, "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "exit_ip": "", "error": str(exc)}
        except Exception as exc:
            logger.exception("代理测试未预期异常")
            db.insert_log("ERROR", "proxy_pool", f"代理测试未预期异常 {proxy_url}: {exc}")
            return {"ok": False, "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "exit_ip": "", "error": str(exc)}


__all__ = ["ProxyPool", "COOLDOWN_IP_BUSY_SEC", "COOLDOWN_HTTP_BLOCK_SEC",
           "COOLDOWN_NETWORK_FAIL_SEC", "COOLDOWN_CONSECUTIVE_FAIL_SEC",
           "MAX_INFLIGHT_RATIO", "CONSECUTIVE_FAIL_THRESHOLD"]
