"""
db.py — SQLite 初始化、建表、WAL 配置、日志定时清理
铁律 12：此模块只做数据库 DDL 与维护，不含业务逻辑
"""
import sqlite3
import threading
import time
import logging
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "copybot.db"

_local = threading.local()
logger = logging.getLogger(__name__)


def get_conn() -> sqlite3.Connection:
    """获取线程本地 SQLite 连接（WAL 模式）"""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


def init_db() -> None:
    """建表（幂等）并创建所有索引"""
    conn = get_conn()
    cur = conn.cursor()

    # 1. API 账户表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_accounts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            remark           TEXT NOT NULL,
            api_key_enc      TEXT NOT NULL,
            api_secret_enc   TEXT NOT NULL,
            copy_ratio       REAL NOT NULL DEFAULT 1.0,
            default_leverage INTEGER NOT NULL DEFAULT 10,
            is_enabled       INTEGER NOT NULL DEFAULT 1,
            followed_trader_id TEXT,
            cached_equity    REAL,
            cached_available REAL,
            realized_pnl     REAL DEFAULT 0.0,
            status_msg       TEXT DEFAULT '',
            updated_at       INTEGER DEFAULT 0,
            created_at       INTEGER DEFAULT 0,
            pnl_since_ms     INTEGER DEFAULT 0,
            signal_source    TEXT DEFAULT 'bapi'
        )
    """)

    # 2. 交易员表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traders (
            portfolio_id          TEXT PRIMARY KEY,
            nickname              TEXT,
            source_url            TEXT,
            margin_balance        REAL,
            position_show         INTEGER DEFAULT 0,
            status                TEXT DEFAULT 'NORMAL',
            error_msg             TEXT DEFAULT '',
            hidden_positions_json TEXT DEFAULT '{}',
            recent_orders_json    TEXT DEFAULT '[]',
            history_positions_json TEXT DEFAULT '[]',
            latest_order_time     INTEGER DEFAULT 0,
            last_sync_ts          INTEGER DEFAULT 0
        )
    """)

    # 3. 去重表（铁律 10）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders_seen (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_portfolio_id TEXT NOT NULL,
            order_key           TEXT NOT NULL,
            order_time          INTEGER,
            symbol              TEXT,
            side                TEXT,
            position_side       TEXT,
            qty                 TEXT,
            price               TEXT,
            UNIQUE (trader_portfolio_id, order_key)
        )
    """)

    # 4. 代理节点表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS proxy_nodes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            proxy_url       TEXT NOT NULL UNIQUE,
            display_name    TEXT,
            exit_ip         TEXT,
            last_latency_ms INTEGER,
            status          TEXT DEFAULT 'UNTESTED',
            fail_count      INTEGER DEFAULT 0,
            error_msg       TEXT DEFAULT '',
            updated_at      INTEGER DEFAULT 0
        )
    """)
    # 兼容旧库：补 error_msg 列（存最近一次检测/采集的失败原因，前端表格要显示）
    proxy_cols = {row[1] for row in cur.execute("PRAGMA table_info(proxy_nodes)")}
    if "error_msg" not in proxy_cols:
        cur.execute("ALTER TABLE proxy_nodes ADD COLUMN error_msg TEXT DEFAULT ''")

    # 兼容旧库：api_accounts 补电话提醒三列（2026-11-16）
    account_cols = {row[1] for row in cur.execute("PRAGMA table_info(api_accounts)")}
    for col, ddl in (
        ("phone_key", "ALTER TABLE api_accounts ADD COLUMN phone_key TEXT DEFAULT ''"),
        ("phone_last_status", "ALTER TABLE api_accounts ADD COLUMN phone_last_status TEXT DEFAULT ''"),
        ("phone_last_at", "ALTER TABLE api_accounts ADD COLUMN phone_last_at INTEGER DEFAULT 0"),
        # v3.2：盈亏统计口径（created_at=接入日期，pnl_since_ms=统计起点）
        ("created_at", "ALTER TABLE api_accounts ADD COLUMN created_at INTEGER DEFAULT 0"),
        ("pnl_since_ms", "ALTER TABLE api_accounts ADD COLUMN pnl_since_ms INTEGER DEFAULT 0"),
        # v3.3：信号源二选一（bapi 默认 / tg），由 tg-collector 提供 tg 信号
        ("signal_source", "ALTER TABLE api_accounts ADD COLUMN signal_source TEXT DEFAULT 'bapi'"),
    ):
        if col not in account_cols:
            cur.execute(ddl)


    # 5. 手动接管表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS manual_overrides (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            api_account_id  INTEGER NOT NULL,
            portfolio_id    TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            position_side   TEXT NOT NULL,
            is_overridden   INTEGER NOT NULL DEFAULT 0,
            tp_price        REAL,
            sl_price        REAL,
            tp_order_id     TEXT,
            sl_order_id     TEXT,
            trigger_close_ratio REAL,
            created_at      INTEGER DEFAULT 0,
            UNIQUE (api_account_id, symbol, position_side)
        )
    """)

    # 6. 跟单订单记录表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS copy_orders (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            api_account_id   INTEGER,
            api_remark       TEXT,
            portfolio_id     TEXT,
            trader_nickname  TEXT,
            symbol           TEXT,
            action           TEXT,
            position_side    TEXT,
            trader_qty       REAL,
            trader_price     REAL,
            my_qty           REAL,
            my_price         REAL,
            scale            REAL,
            realized_pnl     REAL,
            status           TEXT DEFAULT 'SUCCESS',
            error_msg        TEXT DEFAULT '',
            created_at       INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_api
        ON copy_orders (api_account_id, created_at DESC)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_trader
        ON copy_orders (portfolio_id, created_at DESC)
    """)

    # 7. 系统日志表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            time_str   TEXT NOT NULL,
            level      TEXT NOT NULL,
            module     TEXT,
            message    TEXT
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_logs_created
        ON system_logs (created_at DESC)
    """)

    # 8. 信号表（v3.3）：tg-collector 写入，执行器读取。两进程唯一通信点。
    #    UNIQUE(source, source_key) 提供幂等：重连/重启/编辑重推都不会重复下单。
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source       TEXT NOT NULL,
            source_key   TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            side         TEXT NOT NULL,
            action       TEXT NOT NULL,
            qty          REAL,
            pre_qty      REAL,          -- 事件发生前交易员的持仓量（减仓要按比例平，见 11.4）
            price        REAL,
            event_ms     INTEGER,
            raw          TEXT,
            consumed_at  INTEGER DEFAULT 0,
            UNIQUE(source, source_key)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_consume
        ON signals (consumed_at, id)
    """)
    # 兼容旧库：signals 补 pre_qty（v3.3 实现减仓比例时新增）
    sig_cols = {row[1] for row in cur.execute("PRAGMA table_info(signals)")}
    if "pre_qty" not in sig_cols:
        cur.execute("ALTER TABLE signals ADD COLUMN pre_qty REAL")

    # 9. v3.2 回填：老账户 created_at = 该账户第一笔跟单时间（copy_orders.created_at 是秒）。
    #    必须放在所有建表之后：本句引用 copy_orders，全新安装时它在上面的迁移段之后才被创建。
    #    无跟单记录的账户回填 0（= 全部历史），不能用 updated_at，否则统计起点会被挪到迁移时刻。
    cur.execute("""
        UPDATE api_accounts SET created_at = COALESCE(
            (SELECT MIN(c.created_at) * 1000 FROM copy_orders c WHERE c.api_account_id = api_accounts.id),
            0)
        WHERE created_at IS NULL OR created_at = 0
    """)

    conn.commit()
    logger.info("DB initialized at %s", DB_PATH)


_log_hook = None


def set_log_hook(fn) -> None:
    """注册新日志回调（main 用它把日志经 WebSocket 推给前端）。回调异常不得影响写日志。"""
    global _log_hook
    _log_hook = fn


def insert_log(level: str, module: str, message: str) -> None:
    """写一条系统日志（同时写 SQLite + Python logging）"""
    import datetime
    now_ms = int(time.time() * 1000)
    dt = datetime.datetime.fromtimestamp(now_ms / 1000,
                                          tz=datetime.timezone(datetime.timedelta(hours=8)))
    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO system_logs (created_at, time_str, level, module, message) VALUES (?,?,?,?,?)",
            (now_ms, time_str, level, module, message),
        )
        conn.commit()
    except Exception as exc:
        logger.error("insert_log failed: %s", exc)
        return
    hook = _log_hook
    if hook is not None:
        try:
            hook({"type": "log", "id": now_ms, "time_str": time_str, "level": level,
                  "module": module, "message": message})
        except Exception as exc:
            logger.error("log hook failed: %s", exc)


def cleanup_old_logs(retention_days: int) -> int:
    """删除 retention_days 天前的日志，返回删除行数"""
    cutoff_ms = int((time.time() - retention_days * 86400) * 1000)
    conn = get_conn()
    cur = conn.execute("DELETE FROM system_logs WHERE created_at < ?", (cutoff_ms,))
    conn.commit()
    return cur.rowcount


def start_log_cleanup_scheduler(retention_days_getter) -> None:
    """每小时在后台线程执行一次日志清理（retention_days_getter 是返回天数的可调用对象）"""
    def _loop():
        while True:
            time.sleep(3600)
            try:
                days = retention_days_getter()
                deleted = cleanup_old_logs(days)
                if deleted:
                    insert_log("INFO", "db", f"日志清理：删除 {deleted} 条超过 {days} 天的记录")
            except Exception as exc:
                logger.error("log cleanup error: %s", exc)

    t = threading.Thread(target=_loop, daemon=True, name="log-cleanup")
    t.start()
