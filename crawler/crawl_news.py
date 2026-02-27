import os
import json
import requests
import feedparser
import trafilatura
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse

# ================= CONFIG =================
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss",
    "https://bitcoinist.com/category/ethereum/feed/",
    "https://bitcoinist.com/category/bitcoin/feed/",
    "https://cryptobriefing.com/feed/",
    "https://zycrypto.com/feed/",
    "https://cointelegraph.com/rss",
    "https://crypto.news/feed/",
    "https://blockchain.news/rss",
    "https://coingeek.com/feed/",
    "https://cryptodaily.co.uk/feed",
    "https://coincentral.com/news/feed/",
    "https://www.livebitcoinnews.com/feed/",
    "https://crypto-economy.com/feed/",
    "https://coinphoton.com/rss/",
    "https://vn.investing.com/news/cryptocurrency-news/feed/",
    "https://blogtienao.com/rss/",
    "https://cryptopotato.com/feed/",
    "https://www.ccn.com/feed/",
    "https://bitcoinnews.com/feed/",
    "https://www.theblock.co/rss",
    "https://cryptoslate.com/feed",
    "https://decrypt.co/feed",
    "https://www.newsbtc.com/feed/",
    "https://coinjournal.net/feed/",
    "https://www.cryptoglobe.com/latest/feed/",
    "https://dailyhodl.com/feed/",
    "https://cryptonews.com/news/feed/",
    "https://beincrypto.com/feed/",
    "https://bitcoinke.io/feed/",
    "https://blockonomi.com/feed/",
    "https://www.coinbureau.com/feed/",
    "https://ambcrypto.com/feed/",
    "https://bitcoinmagazine.com/.rss/full/"
]

MAX_ENTRIES_PER_FEED = 10
MAX_THREADS = 10

BASE_OUTPUT_DIR = "data/raw/news"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ================= HELPERS =================
def get_published(entry):
    for key in ["published", "pubDate", "updated"]:
        if key in entry:
            try:
                dt = parse(entry[key])
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except:
                return None
    return None


def collect_article_links():
    articles = []
    for feed_url in RSS_FEEDS:
        try:
            resp = requests.get(feed_url, headers=HEADERS, timeout=15)
            feed = feedparser.parse(resp.content)

            for e in feed.entries[:MAX_ENTRIES_PER_FEED]:
                articles.append({
                    "title": e.get("title"),
                    "url": e.get("link"),
                    "published": get_published(e),
                    "source": feed_url
                })
        except Exception as ex:
            print(f"Feed error {feed_url}: {ex}")

    print(f"Collected {len(articles)} articles")
    return articles


def extract_article(entry):
    try:
        html = requests.get(entry["url"], headers=HEADERS, timeout=15).text
        text = trafilatura.extract(html)
    except:
        text = None

    return {
        "domain": urlparse(entry["url"]).netloc,
        "title": entry["title"],
        "url": entry["url"],
        "content": text,
        "published": entry["published"].isoformat() if entry["published"] else None,
        "source": entry["source"]
    }


def filter_yesterday(data):
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return [
        a for a in data
        if a["published"] and parse(a["published"]).date() == yesterday
    ]


def save_jsonl(records):
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_dir = os.path.join(BASE_OUTPUT_DIR, f"date={date_str}")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "news.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Saved {len(records)} articles → {out_path}")


# ================= MAIN =================
def main():
    links = collect_article_links()

    results = []
    with ThreadPoolExecutor(MAX_THREADS) as ex:
        futures = [ex.submit(extract_article, a) for a in links]
        for f in as_completed(futures):
            results.append(f.result())

    yesterday_news = filter_yesterday(results)
    save_jsonl(yesterday_news)


if __name__ == "__main__":
    main()
