import json, sqlite3, pandas as pd, os
from openai import OpenAI
from database import load_trades

TAG_SET = ["EarlyExit","OverSized","FOMO","NoPlan",
           "ImpulseExit","MissedStop","GoodTrade"]

system_msg = ("You are a trading coach. "
              "Label each trade with up to 3 tags from: "
              + ", ".join(TAG_SET) + ".")

client = OpenAI()

def llm_tag(row):
    prompt = [
        {"role":"system", "content": system_msg},
        {"role":"user", "content": json.dumps(row.to_dict(), default=str)}
    ]
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=prompt,
        temperature=0
    )
    tags = json.loads(resp.choices[0].message.content)
    return ",".join([t for t in tags if t in TAG_SET])

def main():
    df = load_trades()
    untagged = df[df["tag_notes"].isna()].copy()
    if untagged.empty:
        print("No untagged trades.")
        return
    untagged["tag_notes"] = untagged.apply(llm_tag, axis=1)
    print(untagged[["trade_id","tag_notes"]])

    # write back
    with sqlite3.connect("trades.db") as conn:
        df.update(untagged)
        df.to_sql("trades", conn, if_exists="replace", index=False)

if __name__ == "__main__":
    main()
