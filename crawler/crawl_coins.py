import requests
import json
import time
import os
from datetime import datetime

# ================= CONFIG =================
BASE_OUTPUT_DIR = "data/raw/coins"

CURRENCY = "usd"
PER_PAGE = 250
SLEEP_SECONDS = 60

PRIORITY_COINS = [
    "bitcoin", "ethereum", "tether", "binancecoin", "solana",
    "ripple", "usd-coin", "dogecoin", "cardano"
]

# ================= HELPERS =================
def get_all_coin_ids():
    url = "https://api.coingecko.com/api/v3/coins/list"
    return [c["id"] for c in requests.get(url, timeout=30).json()]


def fetch_market_data(ids):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": CURRENCY,
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "per_page": len(ids),
        "page": 1,
        "sparkline": "false"
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    timestamp = datetime.now().isoformat()
    return [
        {
            "symbol": c["symbol"],
            "name": c["name"],
            "price": c["current_price"],
            "market_cap": c["market_cap"],
            "volume": c["total_volume"],
            "snapshot_time": timestamp
        }
        for c in resp.json()
    ]


def save_jsonl(data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H%M%S")

    out_dir = os.path.join(BASE_OUTPUT_DIR, f"date={date_str}")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, f"snapshot_{time_str}.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for r in data:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Saved {len(data)} records → {out_path}")


# ================= MAIN =================
def main():
    all_ids = get_all_coin_ids()

    priority = [c for c in PRIORITY_COINS if c in all_ids]
    others = [c for c in all_ids if c not in priority]

    all_data = []

    print("Fetching priority coins...")
    all_data.extend(fetch_market_data(priority))

    for i in range(0, len(others), PER_PAGE):
        batch = others[i:i + PER_PAGE]
        print(f"Fetching batch {i // PER_PAGE + 1}")
        all_data.extend(fetch_market_data(batch))
        time.sleep(SLEEP_SECONDS)

    save_jsonl(all_data)


if __name__ == "__main__":
    main()
