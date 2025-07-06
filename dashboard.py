import streamlit as st, pandas as pd
from database import load_trades, ensure_schema
import sqlite3

DB = "trades.db"

st.set_page_config(page_title="Futures Performance Tracker", layout="wide")

ensure_schema()
df = load_trades()

# ---- sidebar filters ----
with st.sidebar:
    st.header("Filters")
    symbols = st.multiselect("Symbol", sorted(df["symbol"].unique()), [])
    if symbols:
        df = df[df["symbol"].isin(symbols)]

st.title("Overview")

# ---- topline metrics ----
k1, k2, k3 = st.columns(3)
k1.metric("Net PnL", f"${df['pnl'].sum():,.0f}")
k2.metric("Win rate", f"{(df['pnl'] > 0).mean():.1%}")
k3.metric("Avg R", f"{df['pnl'].mean() / df['pnl'].abs().mean():.2f}")

# ---- editable trade log ----
st.subheader("Trade Log (click a cell to edit)")
edited = st.data_editor(df, num_rows="dynamic", key="trade_table")

if st.button("Save edits"):
    with sqlite3.connect(DB) as conn:
        edited.to_sql("trades", conn, if_exists="replace", index=False)
    st.success("Saved!")

