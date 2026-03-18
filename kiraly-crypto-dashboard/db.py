import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DB_PATH = Path("data/app.db")

DEFAULT_EXCHANGES = [
    ("Bitvavo", "exchange", "https://bitvavo.com", "https://bitvavo.com/invite?a=87389B1D4B"),
    ("Binance", "exchange", "https://www.binance.com", ""),
    ("Bybit", "exchange", "https://www.bybit.com", "https://partner.bybit.eu/b/studiocrypto"),
    ("Kraken", "exchange", "https://www.kraken.com", "https://invite.kraken.com/JDNW/oa95occ3"),
    ("Finst", "broker", "https://finst.com", ""),
    ("Coinbase", "exchange", "https://coinbase.com", "https://coinbase-consumer.sjv.io/c/5355518/552039/9251"),
    ("Coinmerce", "broker", "https://coinmerce.io", ""),
    ("BLOX", "broker", "https://weareblox.com", ""),
    ("eToro", "broker", "https://etoro.com", ""),
    ("Amdax", "broker", "https://amdax.com", ""),
    ("OKX", "exchange", "https://www.okx.com", "https://my.okx.com/join/STUDIOCRYPTO"),
]

DEFAULT_FEES_BY_NAME = {
    "Bitvavo": {
        "trading_fee_pct": 0.25,
        "maker_fee_pct": 0.25,
        "taker_fee_pct": 0.25,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.10,
        "source_url": "https://bitvavo.com/en/fees",
    },
    "Finst": {
        "trading_fee_pct": 0.0,
        "maker_fee_pct": 0.0,
        "taker_fee_pct": 0.0,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.50,
        "source_url": "",
    },
    "Binance": {
        "trading_fee_pct": 0.10,
        "maker_fee_pct": 0.10,
        "taker_fee_pct": 0.10,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.15,
        "source_url": "https://www.binance.com/en/fee/trading",
    },
    "Bybit": {
        "trading_fee_pct": 0.10,
        "maker_fee_pct": 0.10,
        "taker_fee_pct": 0.10,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.20,
        "source_url": "https://www.bybit.com/en/help-center/article/Bybit-Spot-Fees-Explained",
    },
    "Kraken": {
        "trading_fee_pct": 0.26,
        "maker_fee_pct": 0.26,
        "taker_fee_pct": 0.26,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.20,
        "source_url": "https://www.kraken.com/features/fee-schedule",
    },
    "Coinbase": {
        "trading_fee_pct": 0.60,
        "maker_fee_pct": 0.60,
        "taker_fee_pct": 0.60,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.80,
        "source_url": "https://help.coinbase.com/en/coinbase/trading-and-funding/advanced-trade/advanced-trade-fees",
    },
    "Coinmerce": {
        "trading_fee_pct": 0.0,
        "maker_fee_pct": 0.0,
        "taker_fee_pct": 0.0,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 1.00,
        "source_url": "",
    },
    "BLOX": {
        "trading_fee_pct": 0.0,
        "maker_fee_pct": 0.0,
        "taker_fee_pct": 0.0,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 1.00,
        "source_url": "",
    },
    "eToro": {
        "trading_fee_pct": 0.0,
        "maker_fee_pct": 0.0,
        "taker_fee_pct": 0.0,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 1.50,
        "source_url": "",
    },
    "Amdax": {
        "trading_fee_pct": 0.0,
        "maker_fee_pct": 0.0,
        "taker_fee_pct": 0.0,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 1.00,
        "source_url": "",
    },
    "OKX": {
        "trading_fee_pct": 0.10,
        "maker_fee_pct": 0.08,
        "taker_fee_pct": 0.10,
        "deposit_ideal_fee_eur": 0.0,
        "withdraw_eur_fee_eur": 0.0,
        "spread_estimate_pct": 0.15,
        "source_url": "https://www.okx.com/help/fee-details",
    },
}


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def init_db() -> None:
    con = connect()
    try:
        cur = con.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exchanges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL,
                website TEXT,
                affiliate_url TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fees (
                exchange_id INTEGER PRIMARY KEY,
                trading_fee_pct REAL NOT NULL DEFAULT 0,
                maker_fee_pct REAL NOT NULL DEFAULT 0,
                taker_fee_pct REAL NOT NULL DEFAULT 0,
                deposit_ideal_fee_eur REAL NOT NULL DEFAULT 0,
                withdraw_eur_fee_eur REAL NOT NULL DEFAULT 0,
                spread_estimate_pct REAL NOT NULL DEFAULT 0,
                source_url TEXT,
                updated_at TEXT,
                FOREIGN KEY(exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                exchange_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                ts TEXT NOT NULL,
                FOREIGN KEY(exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE
            )
            """
        )

        if _needs_fk_cascade_migration(con):
            _migrate_fk_to_cascade(con)
            print("Migration applied")
        else:
            print("No migration needed")

        _migrate_additive_columns(con)
        _ensure_quote_indexes(con)
        _ensure_seed_steps_table(con)
        _apply_incremental_seed_steps(con)
        _backfill_default_metadata(con)

        con.commit()
    finally:
        con.close()


def _table_column_names(con: sqlite3.Connection, table_name: str) -> set[str]:
    cur = con.cursor()
    rows = cur.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(
    con: sqlite3.Connection,
    table_name: str,
    column_name: str,
    definition: str,
) -> None:
    if column_name in _table_column_names(con, table_name):
        return
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _migrate_additive_columns(con: sqlite3.Connection) -> None:
    _add_column_if_missing(con, "exchanges", "affiliate_url", "TEXT")
    _add_column_if_missing(con, "fees", "maker_fee_pct", "REAL NOT NULL DEFAULT 0")
    _add_column_if_missing(con, "fees", "taker_fee_pct", "REAL NOT NULL DEFAULT 0")
    _backfill_fee_columns(con)


def _backfill_fee_columns(con: sqlite3.Connection) -> None:
    fee_columns = _table_column_names(con, "fees")
    if "maker_fee_pct" not in fee_columns or "taker_fee_pct" not in fee_columns:
        return

    cur = con.cursor()
    cur.execute(
        """
        UPDATE fees
        SET maker_fee_pct = trading_fee_pct,
            taker_fee_pct = trading_fee_pct
        WHERE maker_fee_pct = 0
          AND taker_fee_pct = 0
          AND trading_fee_pct != 0
        """
    )
    cur.execute(
        """
        UPDATE fees
        SET trading_fee_pct = taker_fee_pct
        WHERE trading_fee_pct != taker_fee_pct
        """
    )


def _ensure_quote_indexes(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quotes_exchange_symbol_ts
        ON quotes(exchange_id, symbol, ts DESC)
        """
    )


def _ensure_seed_steps_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS seed_steps (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def _default_affiliate_urls_by_name() -> dict[str, str]:
    return {
        str(name): str(affiliate_url)
        for name, _, _, affiliate_url in DEFAULT_EXCHANGES
        if str(affiliate_url or "").strip()
    }


def _needs_fk_cascade_migration(con: sqlite3.Connection) -> bool:
    cur = con.cursor()
    fees_fk = cur.execute("PRAGMA foreign_key_list('fees')").fetchall()
    quotes_fk = cur.execute("PRAGMA foreign_key_list('quotes')").fetchall()

    if not fees_fk or not quotes_fk:
        return False

    fees_on_delete = str(fees_fk[0][6]).upper()
    quotes_on_delete = str(quotes_fk[0][6]).upper()
    return fees_on_delete != "CASCADE" or quotes_on_delete != "CASCADE"


def _existing_column_or_default(
    columns: set[str],
    preferred: str,
    *,
    fallback: str | None = None,
    default_sql: str = "0",
) -> str:
    if preferred in columns:
        return preferred
    if fallback and fallback in columns:
        return fallback
    return default_sql


def _migrate_fk_to_cascade(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    try:
        con.execute("PRAGMA foreign_keys = OFF")
        con.execute("BEGIN")

        cur.execute("ALTER TABLE fees RENAME TO fees_old")
        cur.execute("ALTER TABLE quotes RENAME TO quotes_old")

        old_fee_columns = _table_column_names(con, "fees_old")

        cur.execute(
            """
            CREATE TABLE fees (
                exchange_id INTEGER PRIMARY KEY,
                trading_fee_pct REAL NOT NULL DEFAULT 0,
                maker_fee_pct REAL NOT NULL DEFAULT 0,
                taker_fee_pct REAL NOT NULL DEFAULT 0,
                deposit_ideal_fee_eur REAL NOT NULL DEFAULT 0,
                withdraw_eur_fee_eur REAL NOT NULL DEFAULT 0,
                spread_estimate_pct REAL NOT NULL DEFAULT 0,
                source_url TEXT,
                updated_at TEXT,
                FOREIGN KEY(exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE quotes (
                exchange_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                ts TEXT NOT NULL,
                FOREIGN KEY(exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE
            )
            """
        )

        trading_fee_expr = _existing_column_or_default(
            old_fee_columns,
            "taker_fee_pct",
            fallback="trading_fee_pct",
        )
        maker_fee_expr = _existing_column_or_default(
            old_fee_columns,
            "maker_fee_pct",
            fallback="trading_fee_pct",
        )
        taker_fee_expr = _existing_column_or_default(
            old_fee_columns,
            "taker_fee_pct",
            fallback="trading_fee_pct",
        )
        deposit_fee_expr = _existing_column_or_default(
            old_fee_columns,
            "deposit_ideal_fee_eur",
        )
        withdraw_fee_expr = _existing_column_or_default(
            old_fee_columns,
            "withdraw_eur_fee_eur",
        )
        spread_expr = _existing_column_or_default(
            old_fee_columns,
            "spread_estimate_pct",
        )
        source_expr = _existing_column_or_default(
            old_fee_columns,
            "source_url",
            default_sql="''",
        )
        updated_at_expr = _existing_column_or_default(
            old_fee_columns,
            "updated_at",
            default_sql="NULL",
        )

        cur.execute(
            f"""
            INSERT INTO fees(
                exchange_id, trading_fee_pct, maker_fee_pct, taker_fee_pct,
                deposit_ideal_fee_eur, withdraw_eur_fee_eur,
                spread_estimate_pct, source_url, updated_at
            )
            SELECT
                exchange_id, {trading_fee_expr}, {maker_fee_expr}, {taker_fee_expr},
                {deposit_fee_expr}, {withdraw_fee_expr},
                {spread_expr}, {source_expr}, {updated_at_expr}
            FROM fees_old
            """
        )

        cur.execute(
            """
            INSERT INTO quotes(exchange_id, symbol, bid, ask, ts)
            SELECT exchange_id, symbol, bid, ask, ts
            FROM quotes_old
            """
        )

        cur.execute("DROP TABLE fees_old")
        cur.execute("DROP TABLE quotes_old")
        _ensure_quote_indexes(con)

        con.commit()
    except sqlite3.Error:
        con.rollback()
        raise
    finally:
        con.execute("PRAGMA foreign_keys = ON")


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _apply_incremental_seed_steps(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    row = cur.execute("SELECT COUNT(*) AS cnt FROM exchanges").fetchone()
    exchange_count = int(row["cnt"]) if row else 0
    if exchange_count == 0:
        return

    step_name = "seed_okx_exchange_v1"
    step_exists = cur.execute(
        "SELECT 1 FROM seed_steps WHERE name = ? LIMIT 1",
        (step_name,),
    ).fetchone()
    if step_exists:
        return

    seed_exchanges_and_fees(
        con,
        exchanges=[entry for entry in DEFAULT_EXCHANGES if str(entry[0]) == "OKX"],
        fees_by_name={"OKX": DEFAULT_FEES_BY_NAME["OKX"]},
    )
    cur.execute(
        "INSERT OR IGNORE INTO seed_steps(name, applied_at) VALUES (?, ?)",
        (step_name, _now_utc_iso()),
    )


def _backfill_default_metadata(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    now = _now_utc_iso()

    for name, affiliate_url in _default_affiliate_urls_by_name().items():
        cur.execute(
            """
            UPDATE exchanges
            SET affiliate_url = ?
            WHERE name = ?
              AND COALESCE(TRIM(affiliate_url), '') = ''
            """,
            (affiliate_url, name),
        )

    for name, fee_defaults in DEFAULT_FEES_BY_NAME.items():
        source_url = str(fee_defaults.get("source_url", "") or "").strip()
        if not source_url:
            continue

        cur.execute(
            """
            UPDATE fees
            SET source_url = ?,
                updated_at = CASE
                    WHEN COALESCE(TRIM(updated_at), '') = '' THEN ?
                    ELSE updated_at
                END
            WHERE exchange_id = (SELECT id FROM exchanges WHERE name = ?)
              AND COALESCE(TRIM(source_url), '') = ''
            """,
            (source_url, now, name),
        )


def get_exchange_by_name(con: sqlite3.Connection, name: str) -> sqlite3.Row | None:
    cur = con.cursor()
    cur.execute(
        "SELECT id, name, type, website, affiliate_url FROM exchanges WHERE name = ?",
        (name,),
    )
    return cur.fetchone()


def list_exchanges(con: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = con.cursor()
    cur.execute(
        "SELECT id, name, type, website, affiliate_url FROM exchanges ORDER BY name"
    )
    return cur.fetchall()


def create_exchange(
    con: sqlite3.Connection,
    name: str,
    exchange_type: str,
    website: str,
    affiliate_url: str = "",
) -> int:
    clean_name = (name or "").strip()
    clean_type = (exchange_type or "").strip()
    clean_website = (website or "").strip()
    clean_affiliate_url = (affiliate_url or "").strip()

    if not clean_name:
        raise ValueError("Exchange name is required.")
    if clean_type not in {"exchange", "broker"}:
        raise ValueError("Exchange type must be 'exchange' or 'broker'.")

    cur = con.cursor()
    cur.execute(
        "INSERT INTO exchanges(name, type, website, affiliate_url) VALUES (?, ?, ?, ?)",
        (clean_name, clean_type, clean_website, clean_affiliate_url),
    )
    exchange_id = int(cur.lastrowid)
    ensure_fee_row(con, exchange_id)
    con.commit()
    return exchange_id


def update_exchange(
    con: sqlite3.Connection,
    exchange_id: int,
    name: str,
    exchange_type: str,
    website: str,
    affiliate_url: str = "",
) -> bool:
    clean_name = (name or "").strip()
    clean_type = (exchange_type or "").strip()
    clean_website = (website or "").strip()
    clean_affiliate_url = (affiliate_url or "").strip()

    if not clean_name:
        raise ValueError("Exchange name is required.")
    if clean_type not in {"exchange", "broker"}:
        raise ValueError("Exchange type must be 'exchange' or 'broker'.")

    cur = con.cursor()
    cur.execute(
        """
        UPDATE exchanges
        SET name = ?, type = ?, website = ?, affiliate_url = ?
        WHERE id = ?
        """,
        (clean_name, clean_type, clean_website, clean_affiliate_url, int(exchange_id)),
    )
    con.commit()
    return cur.rowcount > 0


def delete_exchange(con: sqlite3.Connection, exchange_id: int) -> bool:
    cur = con.cursor()
    cur.execute("DELETE FROM exchanges WHERE id = ?", (int(exchange_id),))
    con.commit()
    return cur.rowcount > 0


def delete_exchange_cascade(con: sqlite3.Connection, exchange_id: int) -> bool:
    ex_id = int(exchange_id)
    cur = con.cursor()
    try:
        con.execute("BEGIN")
        cur.execute("DELETE FROM quotes WHERE exchange_id = ?", (ex_id,))
        cur.execute("DELETE FROM fees WHERE exchange_id = ?", (ex_id,))
        cur.execute("DELETE FROM exchanges WHERE id = ?", (ex_id,))
        deleted = cur.rowcount > 0
        con.commit()
        return deleted
    except sqlite3.Error:
        con.rollback()
        raise


def ensure_fee_row(con: sqlite3.Connection, exchange_id: int) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO fees(
            exchange_id, trading_fee_pct, maker_fee_pct, taker_fee_pct,
            deposit_ideal_fee_eur, withdraw_eur_fee_eur,
            spread_estimate_pct, source_url, updated_at
        )
        VALUES (?, 0, 0, 0, 0, 0, 0, '', ?)
        """,
        (int(exchange_id), _now_utc_iso()),
    )


def get_fee_row(con: sqlite3.Connection, exchange_id: int) -> sqlite3.Row | None:
    cur = con.cursor()
    cur.execute(
        """
        SELECT trading_fee_pct, maker_fee_pct, taker_fee_pct,
               deposit_ideal_fee_eur, withdraw_eur_fee_eur,
               spread_estimate_pct, source_url, updated_at
        FROM fees
        WHERE exchange_id = ?
        """,
        (int(exchange_id),),
    )
    return cur.fetchone()


def upsert_fee_row(
    con: sqlite3.Connection,
    exchange_id: int,
    trading_fee_pct: float,
    deposit_ideal_fee_eur: float,
    withdraw_eur_fee_eur: float,
    spread_estimate_pct: float,
    source_url: str,
    updated_at: str | None = None,
    maker_fee_pct: float | None = None,
    taker_fee_pct: float | None = None,
) -> None:
    ts = updated_at or _now_utc_iso()
    clean_taker_fee_pct = float(
        taker_fee_pct if taker_fee_pct is not None else trading_fee_pct
    )
    clean_maker_fee_pct = float(
        maker_fee_pct if maker_fee_pct is not None else clean_taker_fee_pct
    )
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO fees(
            exchange_id, trading_fee_pct, maker_fee_pct, taker_fee_pct,
            deposit_ideal_fee_eur, withdraw_eur_fee_eur,
            spread_estimate_pct, source_url, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(exchange_id),
            clean_taker_fee_pct,
            clean_maker_fee_pct,
            clean_taker_fee_pct,
            float(deposit_ideal_fee_eur),
            float(withdraw_eur_fee_eur),
            float(spread_estimate_pct),
            (source_url or "").strip(),
            ts,
        ),
    )
    con.commit()


def insert_quote(
    con: sqlite3.Connection,
    exchange_id: int,
    symbol: str,
    bid: float,
    ask: float,
    ts: str | None = None,
) -> None:
    quote_ts = ts or _now_utc_iso()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO quotes(exchange_id, symbol, bid, ask, ts) VALUES (?, ?, ?, ?, ?)",
        (int(exchange_id), symbol, float(bid), float(ask), quote_ts),
    )
    con.commit()


def get_latest_quote(
    con: sqlite3.Connection,
    exchange_id: int,
    symbol: str,
) -> sqlite3.Row | None:
    cur = con.cursor()
    cur.execute(
        """
        SELECT bid, ask, ts
        FROM quotes
        WHERE exchange_id = ? AND symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (int(exchange_id), symbol),
    )
    return cur.fetchone()


def list_exchange_fee_rows(con: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = con.cursor()
    cur.execute(
        """
        SELECT e.id, e.name, e.type, e.website, e.affiliate_url,
               f.trading_fee_pct, f.maker_fee_pct, f.taker_fee_pct,
               f.deposit_ideal_fee_eur, f.withdraw_eur_fee_eur,
               f.spread_estimate_pct, f.source_url, f.updated_at
        FROM exchanges e
        JOIN fees f ON f.exchange_id = e.id
        ORDER BY e.name
        """
    )
    return cur.fetchall()


def _normalize_exchange_seed_entry(entry: tuple[str, ...]) -> tuple[str, str, str, str]:
    if len(entry) == 3:
        name, exchange_type, website = entry
        affiliate_url = ""
    elif len(entry) == 4:
        name, exchange_type, website, affiliate_url = entry
    else:
        raise ValueError("Exchange seed entry must contain 3 or 4 values.")

    return (
        (name or "").strip(),
        (exchange_type or "").strip(),
        (website or "").strip(),
        (affiliate_url or "").strip(),
    )


def seed_exchanges_and_fees(
    con: sqlite3.Connection,
    exchanges: list[tuple[str, ...]],
    fees_by_name: dict[str, dict[str, Any]],
) -> None:
    now = _now_utc_iso()
    cur = con.cursor()

    for entry in exchanges:
        name, exchange_type, website, affiliate_url = _normalize_exchange_seed_entry(entry)
        cur.execute(
            """
            INSERT OR IGNORE INTO exchanges(name, type, website, affiliate_url)
            VALUES (?, ?, ?, ?)
            """,
            (name, exchange_type, website, affiliate_url),
        )
        cur.execute(
            """
            UPDATE exchanges
            SET website = CASE
                    WHEN COALESCE(TRIM(website), '') = '' THEN ?
                    ELSE website
                END,
                affiliate_url = CASE
                    WHEN COALESCE(TRIM(affiliate_url), '') = '' THEN ?
                    ELSE affiliate_url
                END
            WHERE name = ?
            """,
            (website, affiliate_url, name),
        )

    cur.execute("SELECT id, name FROM exchanges")
    rows = cur.fetchall()

    for row in rows:
        ex_id = int(row["id"])
        name = str(row["name"])
        fee = fees_by_name.get(
            name,
            {
                "trading_fee_pct": 0.0,
                "maker_fee_pct": 0.0,
                "taker_fee_pct": 0.0,
                "deposit_ideal_fee_eur": 0.0,
                "withdraw_eur_fee_eur": 0.0,
                "spread_estimate_pct": 0.0,
                "source_url": "",
            },
        )
        taker_fee_pct = float(fee.get("taker_fee_pct", fee.get("trading_fee_pct", 0.0)))
        maker_fee_pct = float(fee.get("maker_fee_pct", taker_fee_pct))
        cur.execute(
            """
            INSERT OR IGNORE INTO fees(
                exchange_id, trading_fee_pct, maker_fee_pct, taker_fee_pct,
                deposit_ideal_fee_eur, withdraw_eur_fee_eur,
                spread_estimate_pct, source_url, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ex_id,
                taker_fee_pct,
                maker_fee_pct,
                taker_fee_pct,
                float(fee.get("deposit_ideal_fee_eur", 0.0)),
                float(fee.get("withdraw_eur_fee_eur", 0.0)),
                float(fee.get("spread_estimate_pct", 0.0)),
                str(fee.get("source_url", "") or ""),
                now,
            ),
        )

    con.commit()


def ensure_seed_data(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    row = cur.execute("SELECT COUNT(*) AS cnt FROM exchanges").fetchone()
    exchange_count = int(row["cnt"]) if row else 0

    if exchange_count > 0:
        return

    seed_exchanges_and_fees(
        con,
        exchanges=DEFAULT_EXCHANGES,
        fees_by_name=DEFAULT_FEES_BY_NAME,
    )
