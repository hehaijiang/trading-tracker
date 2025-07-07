# ingest.py
import pathlib, sqlite3, pandas as pd, datetime as dt

RAW_DIR = pathlib.Path("data")
RAW_DIR.mkdir(exist_ok=True)
DB = "trades.db"

def process_csv(csv_path: pathlib.Path):
    # ---- read only the columns we care about -----------------------------
    cols_to_keep = [
        "symbol",            # instrument
        "qty",               # position size
        "buyPrice",          # entry
        "sellPrice",         # exit
        "boughtTimestamp",   # entry time
        "soldTimestamp",     # exit time
        "pnl"                # P/L (string like "$40.00")
    ]
    df = pd.read_csv(csv_path, usecols=cols_to_keep)

    # ---- rename to the canonical column set used by database.py ----------
    df = df.rename(
        columns={
            "symbol":           "Symbol",
            "qty":              "Quantity",
            "buyPrice":         "EntryPrice",
            "sellPrice":        "ExitPrice",
            "boughtTimestamp":  "EntryTime",
            "soldTimestamp":    "ExitTime",
        }
    )

    # ---- type & value cleanup --------------------------------------------
    df["Quantity"] = df["Quantity"].astype(int).abs()
    df["Side"]     = "LONG"                     # buy→sell pair
    df["Commission"] = 0.0                      # Tradovate export omits this

    df["PnL"] = (
        df["pnl"]
        .str.replace("[\$,]", "", regex=True)   # "$40.00" → "40.00"
        .astype(float)
    )
    df.drop(columns=["pnl"], inplace=True)

    df["EntryTime"] = pd.to_datetime(df["EntryTime"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["ExitTime"]  = pd.to_datetime(df["ExitTime"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    # ---- write to raw_trades ---------------------------------------------
    with sqlite3.connect(DB) as conn:
        df.to_sql("raw_trades", conn, if_exists="append", index=False)

    csv_path.rename(csv_path.with_suffix(".done"))
    print(f"Imported {csv_path.name}")


def main():
    for csv in RAW_DIR.glob("*.csv"):
        process_csv(csv)

if __name__ == "__main__":
    main()
