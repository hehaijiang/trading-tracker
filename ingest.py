# ingest.py
import pathlib, sqlite3, pandas as pd, datetime as dt

RAW_DIR = pathlib.Path("data")
RAW_DIR.mkdir(exist_ok=True)
DB = "trades.db"

def process_csv(csv_path: pathlib.Path):
    df = pd.read_csv(csv_path)
    df["imported_at"] = dt.datetime.utcnow()
    with sqlite3.connect(DB) as conn:
        df.to_sql("raw_trades", conn, if_exists="append", index=False)
    csv_path.rename(csv_path.with_suffix(".done"))
    print(f"Imported {csv_path.name}")

def main():
    for csv in RAW_DIR.glob("*.csv"):
        process_csv(csv)

if __name__ == "__main__":
    main()
