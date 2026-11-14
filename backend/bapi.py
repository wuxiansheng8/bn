"""
bapi.py — 币安 BAPI 接口唯一出口
铁律 12：URL、字段名、错误码只在此处定义，其余模块一律引用本模块常量
"""
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 基础地址
# ---------------------------------------------------------------------------
BAPI_BASE = "https://www.binance.com"
FSTREAM_WS = "wss://fstream.binance.com/ws/!markPrice@arr@1s"
REST_MARKPRICE_FALLBACK = "https://fapi.binance.com/fapi/v1/premiumIndex"

# ---------------------------------------------------------------------------
# 公共请求头（第六章）
# ---------------------------------------------------------------------------
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "clienttype": "web",
    "content-type": "application/json",
    "lang": "zh-CN",
}

# ---------------------------------------------------------------------------
# 路径与字段定义（集中在此）
# ---------------------------------------------------------------------------
ENDPOINTS = {
    "lead_detail": "/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail",
    "order_history": "/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/order-history",
    "position_history": "/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history",
    "lead_positions": "/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions",
    "api_restrictions": "/sapi/v1/account/apiRestrictions",
}

# order-history 回包字段
ORDER_FIELDS = ("symbol", "side", "positionSide", "executedQty", "avgPrice",
                "totalPnl", "orderTime", "orderId")

# position-history status 枚举
POS_STATUS_ALL_CLOSED = "All Closed"

# ---------------------------------------------------------------------------
# 业务错误码（第六章末）
# ---------------------------------------------------------------------------
ERR_IP_BUSY = "11012005"          # 单 IP 间隔 <2s，换 IP（铁律 2）

# HTTP 状态码含义（铁律 2 / 第八章 8.2.5）
HTTP_BLOCK_CODES = (403, 418, 429)   # 该 IP 冷却 60s


class BapiError(Exception):
    """币安接口异常基类"""


class BapiParseError(BapiError):
    """解析类异常：JSON 解析失败 / 缺关键字段 —— ERROR + TG 告警（铁律 9）"""


class BapiRateLimitError(BapiError):
    """限流类：11012005 / 403/418/429 —— 按铁律 2 处理"""


class BapiNetworkError(BapiError):
    """网络类：超时 / 连接失败 —— 该 IP 短冷却 + 换 IP（铁律 9）"""


# ---------------------------------------------------------------------------
# 采集请求（监控侧，免登录、走代理池）
# ---------------------------------------------------------------------------
def fetch_order_history(portfolio_id: str, proxies: dict | None = None,
                        page_size: int = 10, index_value: str | None = None,
                        timeout: float = 8.0) -> dict[str, Any]:
    """
    拉取交易员最新操作记录（热通道不传 indexValue；补页/回溯传 indexValue）。
    铁律 1：热通道只传 pageSize + portfolioId。
    返回原始 JSON dict；异常按铁律 9 分层抛出。
    """
    url = BAPI_BASE + ENDPOINTS["order_history"]
    body: dict[str, Any] = {"pageSize": page_size, "portfolioId": portfolio_id}
    if index_value is not None:
        body["indexValue"] = index_value
    try:
        resp = requests.post(url, json=body, headers=COMMON_HEADERS,
                             proxies=proxies, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        raise BapiNetworkError(f"order-history 网络异常: {exc}") from exc

    code = resp.status_code
    if code in HTTP_BLOCK_CODES:
        raise BapiRateLimitError(f"order-history HTTP {code}")
    if code != 200:
        raise BapiNetworkError(f"order-history HTTP {code}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise BapiParseError(f"order-history JSON 解析失败: {exc}") from exc

    # 业务码检查
    biz_code = str(data.get("code", ""))
    if biz_code == ERR_IP_BUSY:
        raise BapiRateLimitError("11012005 IP 忙")
    if biz_code and biz_code != "000000" and biz_code != "0":
        raise BapiParseError(f"order-history 业务码异常: {biz_code} {data.get('message','')}")

    if "data" not in data:
        raise BapiParseError("order-history 缺 data 字段")
    return data


def fetch_lead_detail(portfolio_id: str, proxies: dict | None = None,
                      timeout: float = 8.0) -> dict[str, Any]:
    """交易员详情：nickname / marginBalance / status / positionShow"""
    url = BAPI_BASE + ENDPOINTS["lead_detail"]
    params = {"portfolioId": portfolio_id}
    try:
        resp = requests.get(url, params=params, headers=COMMON_HEADERS,
                            proxies=proxies, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        raise BapiNetworkError(f"detail 网络异常: {exc}") from exc
    if resp.status_code in HTTP_BLOCK_CODES:
        raise BapiRateLimitError(f"detail HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise BapiNetworkError(f"detail HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise BapiParseError(f"detail JSON 解析失败: {exc}") from exc
    return data


def fetch_position_history(portfolio_id: str, proxies: dict | None = None,
                            timeout: float = 8.0) -> dict[str, Any]:
    """历史已平仓记录：status == All Closed 的 closed 时间为清零锚点"""
    url = BAPI_BASE + ENDPOINTS["position_history"]
    body = {"pageSize": 20, "portfolioId": portfolio_id}
    try:
        resp = requests.post(url, json=body, headers=COMMON_HEADERS,
                             proxies=proxies, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        raise BapiNetworkError(f"position-history 网络异常: {exc}") from exc
    if resp.status_code in HTTP_BLOCK_CODES:
        raise BapiRateLimitError(f"position-history HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise BapiNetworkError(f"position-history HTTP {resp.status_code}")
    try:
        return resp.json()
    except ValueError as exc:
        raise BapiParseError(f"position-history JSON 解析失败: {exc}") from exc


def fetch_lead_positions(portfolio_id: str, proxies: dict | None = None,
                         timeout: float = 8.0) -> dict[str, Any]:
    """
    当前持仓（仅校验）。铁律 M-02：仅当 positionShow=true 时调用；
    隐藏时返回空，严禁据此清零推算。
    """
    url = BAPI_BASE + ENDPOINTS["lead_positions"]
    params = {"portfolioId": portfolio_id}
    try:
        resp = requests.get(url, params=params, headers=COMMON_HEADERS,
                            proxies=proxies, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        raise BapiNetworkError(f"positions 网络异常: {exc}") from exc
    if resp.status_code in HTTP_BLOCK_CODES:
        raise BapiRateLimitError(f"positions HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise BapiNetworkError(f"positions HTTP {resp.status_code}")
    try:
        return resp.json()
    except ValueError as exc:
        raise BapiParseError(f"positions JSON 解析失败: {exc}") from exc


# ---------------------------------------------------------------------------
# 回包整形（字段名知识集中在 bapi，前端只负责渲染）
# ---------------------------------------------------------------------------
def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _millis(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def normalize_position_history(rows: list[dict]) -> list[dict]:
    """把 position-history 的原始行整理成前端直接渲染的结构。

    币安原始字段（2026-11-14 实测）：symbol / side("Short") / opened / closed /
    avgCost / avgClosePrice / closedVolume / closingPnl / status / leverage / roi。
    统一输出：symbol / positionSide / qty / openTime / closeTime / pnl / 均价 / 状态。
    """
    result: list[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        side = str(row.get("side") or "").upper()
        result.append({
            "symbol": str(row.get("symbol") or ""),
            "positionSide": side if side in ("LONG", "SHORT") else "",
            "qty": _number(row.get("closedVolume") or row.get("qty")),
            "openTime": _millis(row.get("opened")),
            "closeTime": _millis(row.get("closed") or row.get("updateTime")),
            "pnl": _number(row.get("closingPnl")),
            "avgOpenPrice": _number(row.get("avgCost")),
            "avgClosePrice": _number(row.get("avgClosePrice")),
            "leverage": str(row.get("leverage") or ""),
            "status": str(row.get("status") or ""),
        })
    return result
