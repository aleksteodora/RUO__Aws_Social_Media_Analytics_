import os
import json
import uuid
import logging
from datetime import datetime, timezone
from html.parser import HTMLParser

import boto3
import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME        = os.environ["BUCKET_NAME"]
BRONZE_HN_PREFIX   = os.environ["BRONZE_HN_PREFIX"]
BRONZE_TWITTER_KEY = os.environ["BRONZE_TWITTER_KEY"]
SILVER_PREFIX      = os.environ["SILVER_PREFIX"]

HN_TYPES = {
    "story":   None,
    "comment": "comment_text",
    "ask_hn":  "story_text",
    "job":     "story_text",
    "poll":    "story_text",
}

class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed).strip()


def clean_html(raw: str) -> str:
    """Remove HTML tags and unescape HTML entities."""
    if not raw:
        return ""
    s = _HTMLStripper()
    s.feed(raw)
    return s.get_data()


def normalise_ts(value) -> str:
    """
    Accept either:
      - Unix epoch integer / string  (HN created_at_i)
      - ISO-8601 string              (HN created_at, Twitter)
    Returns UTC ISO-8601 string: 'YYYY-MM-DDTHH:MM:SSZ'
    """
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        value = str(value).strip()
        if value.isdigit():
            return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, OSError, OverflowError, AttributeError):
        return None


def build_hn_users(usernames: list) -> list:
    users = []
    for username in usernames:
        users.append({
            "user_id":        str(uuid.uuid4()),
            "username":       username,
            "platform":       "HackerNews",
            "karma_score":    None,
            "user_followers": None,
            "user_verified":  None,
            "created_at":     None,
        })
    logger.info(f"Built {len(users)} HN user records (karma_score=None: Firebase API requires auth)")
    return users


def read_hn_bronze(s3_client, date_prefix: str) -> tuple[list, list]:
    """
    Returns (posts_rows, hn_usernames)
    date_prefix: e.g. 'bronze/hacker-news/2026-05-27'
    """
    posts_rows = []
    hn_usernames = set()

    for post_type, text_field in HN_TYPES.items():
        key = f"{date_prefix}/{post_type}.json"
        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            items = json.loads(obj["Body"].read())
        except s3_client.exceptions.NoSuchKey:
            logger.warning(f"Missing bronze file: {key}")
            continue
        except Exception as e:
            logger.error(f"Error reading {key}: {e}")
            continue

        if not isinstance(items, list):
            items = [items]

        for item in items:
            author = item.get("author") or item.get("by")
            if not author:
                continue

            raw_text = item.get(text_field) if text_field else None
            content  = clean_html(raw_text) if raw_text else None

            if post_type == "story" and not content:
                content = clean_html(item.get("title", ""))

            posts_rows.append({
                "post_id":         str(item.get("objectID") or item.get("story_id")),
                "author_username": author,
                "content_text":    content,
                "created_at":      normalise_ts(item.get("created_at") or item.get("created_at_i")),
                "post_type":       post_type,
                "platform":        "HackerNews",
                "score":           item.get("points"),
            })
            hn_usernames.add(author)

    return posts_rows, list(hn_usernames)


def read_twitter_bronze(s3_client) -> tuple[list, list]:
    """Returns (posts_rows, users_rows)"""
    logger.info("Reading Twitter CSV from S3...")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=BRONZE_TWITTER_KEY)

    df = pd.read_csv(
        obj["Body"],
        encoding="utf-8",
        dtype=str,
        low_memory=False,
    )

    logger.info(f"Twitter CSV loaded: {len(df)} rows")

    posts_rows = []
    users_rows = []
    seen_users: set[str] = set()

    for _, row in df.iterrows():
        username = str(row.get("user_name", "") or "").strip()
        if not username:
            continue

        is_retweet = str(row.get("is_retweet", "")).strip().lower() in ("true", "1", "yes")
        post_type  = "retweet" if is_retweet else "tweet"

        raw_ts     = row.get("date")
        created_at = normalise_ts(raw_ts) if raw_ts and not pd.isna(raw_ts) else None

        text_val   = row.get("text")
        content    = str(text_val) if text_val and not pd.isna(text_val) else None

        posts_rows.append({
            "post_id":         str(uuid.uuid4()),
            "author_username": username,
            "content_text":    content,
            "created_at":      created_at,
            "post_type":       post_type,
            "platform":        "X",
            "score":           None,
        })

        if username not in seen_users:
            seen_users.add(username)

            followers_raw = row.get("user_followers")
            try:
                followers = int(float(followers_raw)) if followers_raw and not pd.isna(followers_raw) else None
            except (ValueError, TypeError):
                followers = None

            verified_raw = str(row.get("user_verified", "")).strip().lower()
            verified = True if verified_raw == "true" else (False if verified_raw == "false" else None)

            user_created_raw = row.get("user_created")
            user_created = normalise_ts(user_created_raw) if user_created_raw and not pd.isna(user_created_raw) else None

            users_rows.append({
                "user_id":        str(uuid.uuid4()),
                "username":       username,
                "platform":       "X",
                "karma_score":    None,
                "user_followers": followers,
                "user_verified":  verified,
                "created_at":     user_created,
            })

    return posts_rows, users_rows


def deduplicate_posts(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["post_id", "platform"])

def deduplicate_users(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["username", "platform"])


def write_posts(df: pd.DataFrame):
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["year"]  = df["created_at"].dt.year.astype("Int64").astype(str)
    df["month"] = df["created_at"].dt.month.apply(lambda x: f"{x:02d}" if pd.notna(x) else None)
    df["day"]   = df["created_at"].dt.day.apply(lambda x: f"{x:02d}" if pd.notna(x) else None)

    path = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/posts/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        partition_cols=["year", "month", "day"],
        mode="overwrite_partitions",
    )
    logger.info(f"Written posts parquet to {path}")


def write_users(df: pd.DataFrame):
    path = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/users/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        partition_cols=["platform"],
        mode="overwrite_partitions",
    )
    logger.info(f"Written users parquet to {path}")


def process(event, _context):
    logger.info(f"Silver processor started. Event: {json.dumps(event)}")

    s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=f"{BRONZE_HN_PREFIX}/",
        Delimiter="/",
    )
    date_prefixes = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            date_prefixes.append(prefix["Prefix"].rstrip("/"))

    logger.info(f"Found HN date prefixes: {date_prefixes}")

    all_posts    = []
    all_hn_users = []

    for date_prefix in date_prefixes:
        logger.info(f"Processing HN bronze: {date_prefix}")
        posts, usernames = read_hn_bronze(s3_client, date_prefix)
        all_posts.extend(posts)
        hn_users = build_hn_users(usernames)
        all_hn_users.extend(hn_users)

    twitter_posts, twitter_users = read_twitter_bronze(s3_client)
    all_posts.extend(twitter_posts)

    posts_df = pd.DataFrame(all_posts)
    users_df = pd.DataFrame(all_hn_users + twitter_users)

    logger.info(f"Total posts before dedup: {len(posts_df)}")
    logger.info(f"Total users before dedup: {len(users_df)}")

    posts_df = deduplicate_posts(posts_df)
    users_df = deduplicate_users(users_df)

    logger.info(f"Total posts after dedup: {len(posts_df)}")
    logger.info(f"Total users after dedup: {len(users_df)}")

    write_posts(posts_df)
    write_users(users_df)

    logger.info("Silver processor completed successfully.")
    return {"status": "ok", "posts": len(posts_df), "users": len(users_df)}