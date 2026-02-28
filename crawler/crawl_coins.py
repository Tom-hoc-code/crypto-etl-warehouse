import requests
import json
import time
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= CONFIG =================
BASE_OUTPUT_DIR = "data/raw/coins"

CURRENCY = "usd"
PER_PAGE = 250
SLEEP_SECONDS = 10   # giảm xuống 10s thay vì 60

PRIORITY_COINS = [
    "bitcoin","ethereum","tether","binancecoin","solana","ripple","usd-coin","dogecoin",
    "cardano","avalanche-2","shiba-inu","tron","chainlink","polkadot","polygon",
    "bitcoin-cash","litecoin","uniswap","cosmos","stellar","ethereum-classic",
    "near","immutable-x","aptos","vechain","filecoin","internet-computer","hedera",
    "render-token","celestia","injective-protocol","arweave","maker","algorand",
    "elrond","sui","the-graph","quant-network","aave","flow","decentraland",
    "sandbox","tezos","theta-network","axie-infinity","kaspa","neo","zcash","lido-dao",
    "pancakeswap-token","ens","gmx","fantom","convex-finance","optimism","arbitrum",
    "celsius-network","kyber-network-crystal","thorchain","ren","basic-attention-token",
    "0x","chiliz","nexo","ontology","dash","gnosis","kava","gala","ftx-token"
]

# ================= SESSION (cực quan trọng) =================
def create_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    return session


session = create_session()

# ================= HELPERS =================
def get_all_coin_ids():
    url = "https://api.coingecko.com/api/v3/coins/list"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return [c["id"] for c in resp.json()]


def fetch_market_data(ids):
    if not ids:
        return []

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": CURRENCY,
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "per_page": len(ids),
        "page": 1,
        "sparkline": "false"
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()

        timestamp = datetime.now().isoformat()

        return [
            {
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "price": coin.get("current_price"),
                "fully_diluted_valuation": coin.get("fully_diluted_valuation"),
                "total_volume": coin.get("total_volume"),
                "high_24h": coin.get("high_24h"),
                "low_24h": coin.get("low_24h"),
                "circulating_supply": coin.get("circulating_supply"),
                "total_supply": coin.get("total_supply"),
                "max_supply": coin.get("max_supply"),
                "ath": coin.get("ath"),                    
                "time_key": timestamp,
            
            }
            for coin in resp.json()
        ]

    except Exception as e:
        print(f"⚠ Error fetching batch: {e}")
        return []


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
    print("Getting all coin IDs...")
    all_ids = get_all_coin_ids()

    priority = [c for c in PRIORITY_COINS if c in all_ids]
    others = [c for c in all_ids if c not in priority]

    all_data = []

    print("Fetching priority coins...")
    all_data.extend(fetch_market_data(priority))

    for i in range(0, len(others), PER_PAGE):
        batch = others[i:i + PER_PAGE]
        batch_no = i // PER_PAGE + 1

        print(f"Fetching batch {batch_no}")
        data = fetch_market_data(batch)
        all_data.extend(data)

        time.sleep(SLEEP_SECONDS)

    save_jsonl(all_data)


if __name__ == "__main__":
    main()