import json
import boto3
import urllib.request
import os
from datetime import datetime, timezone, timedelta

HN_SEARCH = "https://hn.algolia.com/api/v1/search_by_date"

BUCKET_NAME = os.environ["BRONZE_BUCKET_NAME"]

s3 = boto3.client("s3")

def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())

def yesterday_range() -> tuple[int, int]:
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    return int(yesterday.timestamp()), int(today.timestamp())


def fetch_items_by_type(item_type: str, date_str: str) -> list[dict]:
    items = []
    page = 0

    while True:
        url = (
            f"{HN_SEARCH}"
            f"?tags={item_type}"
            f"&numericFilters=created_at_i>{int((datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)).timestamp())},"
            f"created_at_i<{int((datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp())}"
            f"&hitsPerPage=1000&page={page}"
        )
        data = fetch_json(url)
        hits = data.get("hits", [])

        if not hits:
            break

        items.extend(hits)

        if page >= data.get("nbPages", 1) - 1:
            break

        page += 1

    return items


def save_to_s3(data: list[dict], item_type: str, date_str: str) -> None:
    key = f"bronze/hacker-news/{date_str}/{item_type}.json"
    body = json.dumps(data, ensure_ascii=False, indent=2)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[OK] Saved {len(data)} items → s3://{BUCKET_NAME}/{key}")


def lambda_handler(_event, _context):
    yesterday_str = (
            datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    print(f"Starting HN bronze collection for: {yesterday_str}")

    item_types = ["story", "comment", "ask_hn", "job", "poll"]

    results = {}
    for item_type in item_types:
        try:
            items = fetch_items_by_type(item_type, yesterday_str)
            save_to_s3(items, item_type, yesterday_str)
            results[item_type] = len(items)
        except Exception as e:
            print(f"[ERROR] Failed for {item_type}: {e}")
            results[item_type] = f"ERROR: {e}"

    print(f"Collection complete: {results}")
    return {
        "statusCode": 200,
        "date": yesterday_str,
        "results": results
    }
