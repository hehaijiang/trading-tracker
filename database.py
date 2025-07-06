import sqlite3, pandas as pd, pathlib, textwrap

DB = "trades.db"

SCHEMA = textwrap.dedent("""
CREATE TABLE IF NOT EXISTS trades AS
SELECT
    rowid            AS trade_id,
    Symbol           AS symbol,
    CASE WHEN Quantity > 0 THEN 'LONG' ELSE 'SHORT' END AS side,
    ABS(Quantity)    AS qty,
    EntryDateTime    AS entry_time,
    ExitDateTime     AS exit_time,
    EntryPrice       AS entry_price,
    ExitPrice        AS exit_price,
    Commission       AS commission,
    (ExitPrice - EntryPrice) * ABS(Quantity) AS pnl,
    NULL AS tag_notes,
    NULL AS free_text
FROM raw_trades;
""")

def ensure_schema():
    if not pathlib.Path(DB).exists():
        return
    with sqlite3.connect(DB) as conn:
        conn.executescript(SCHEMA)

def load_trades() -> pd.DataFrame:
    ensure_schema()
    with sqlite3.connect(DB) as conn:
        return pd.read_sql("SELECT * FROM trades", conn,
                           parse_dates=["entry_time", "exit_time"])
