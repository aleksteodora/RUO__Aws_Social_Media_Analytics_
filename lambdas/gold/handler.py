import os
import json
import logging
from datetime import datetime, timezone

import pandas as pd
import awswrangler as wr
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME     = os.environ["BUCKET_NAME"]
SILVER_PREFIX   = os.environ["SILVER_PREFIX"]
GOLD_PREFIX     = os.environ["GOLD_PREFIX"]
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")


def notify_discord(message: str):
    """Send error notification to Discord webhook. Fails silently."""
    if not DISCORD_WEBHOOK:
        logger.warning("DISCORD_WEBHOOK_URL not set — skipping notification")
        return
    try:
        payload = {"content": f":red_circle: **Gold Lambda Error**\n```\n{message}\n```"}
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info("Discord notification sent")
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")


def read_posts() -> pd.DataFrame:
    path = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/posts/"
    logger.info(f"Reading silver posts from {path}")
    df = wr.s3.read_parquet(path=path, dataset=True)
    logger.info(f"Loaded {len(df)} post rows from silver")
    return df


def read_users() -> pd.DataFrame:
    path = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/users/"
    logger.info(f"Reading silver users from {path}")
    df = wr.s3.read_parquet(path=path, dataset=True)
    logger.info(f"Loaded {len(df)} user rows from silver")
    return df


def today_str() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def write_gold(df: pd.DataFrame, metric_name: str, date_str: str, partition_cols: list = None):
    """Write a single gold metric parquet to S3."""
    if partition_cols is None:
        partition_cols = ["date"]
    path = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}/{metric_name}/"
    df["date"] = date_str
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
    )
    logger.info(f"Written gold/{metric_name} ({len(df)} rows) to {path}")


def metric_daily_posts(posts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily count of posts by type (story, ask_hn, comment, job, poll) for HackerNews only.
    gold schema: post_type, count
    """
    hn_posts = posts_df[posts_df["platform"] == "HackerNews"].copy()
    df = (
        hn_posts
        .groupby("post_type", dropna=False)
        .size()
        .reset_index(name="count")
    )
    return df


def metric_daily_users(users_df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Daily count of total and new users by platform (HackerNews and X).
    New users = those whose created_at falls on the given date.
    gold schema: platform, total_users, new_users
    Partitioning: platform + date (the only metric common to both platforms)
    """
    users_df = users_df.copy()
    users_df["created_at"] = pd.to_datetime(users_df["created_at"], utc=True, errors="coerce")
    users_df["created_date"] = users_df["created_at"].dt.strftime("%Y-%m-%d")

    total = (
        users_df
        .groupby("platform")
        .size()
        .reset_index(name="total_users")
    )

    new = (
        users_df[users_df["created_date"] == date_str]
        .groupby("platform")
        .size()
        .reset_index(name="new_users")
    )

    df = total.merge(new, on="platform", how="left")
    df["new_users"] = df["new_users"].fillna(0).astype(int)
    return df


def metric_top_followers_x(users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 10 X users by user_followers.
    gold schema: username, user_followers, rank
    """
    x_users = users_df[users_df["platform"] == "X"].copy()
    x_users["user_followers"] = pd.to_numeric(x_users["user_followers"], errors="coerce")
    df = (
        x_users
        .dropna(subset=["user_followers"])
        .sort_values("user_followers", ascending=False)
        .head(10)
        [["username", "user_followers"]]
        .reset_index(drop=True)
    )
    df["rank"] = range(1, len(df) + 1)
    return df


def metric_top_karma_hn(users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 10 HN users by karma_score — highest and lowest.
    gold schema: username, karma_score, rank_type ('highest'/'lowest'), rank
    NOTE: will be empty until a colleague adds Firebase API in Silver.
    """
    hn_users = users_df[users_df["platform"] == "HackerNews"].copy()
    hn_users["karma_score"] = pd.to_numeric(hn_users["karma_score"], errors="coerce")
    hn_with_karma = hn_users.dropna(subset=["karma_score"])

    if hn_with_karma.empty:
        logger.warning("karma_score is null for all HN users — top_karma_hn will be empty")
        return pd.DataFrame(columns=["username", "karma_score", "rank_type", "rank"])

    highest = (
        hn_with_karma
        .sort_values("karma_score", ascending=False)
        .head(10)[["username", "karma_score"]]
        .reset_index(drop=True)
    )
    highest["rank_type"] = "highest"
    highest["rank"] = range(1, len(highest) + 1)

    lowest = (
        hn_with_karma
        .sort_values("karma_score", ascending=True)
        .head(10)[["username", "karma_score"]]
        .reset_index(drop=True)
    )
    lowest["rank_type"] = "lowest"
    lowest["rank"] = range(1, len(lowest) + 1)

    return pd.concat([highest, lowest], ignore_index=True)


def metric_top_jobs_hn(posts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 10 HN job posts by score.
    gold schema: post_id, content_text, score, rank
    """
    jobs = posts_df[
        (posts_df["platform"] == "HackerNews") &
        (posts_df["post_type"] == "job")
    ].copy()
    jobs["score"] = pd.to_numeric(jobs["score"], errors="coerce")
    df = (
        jobs
        .dropna(subset=["score"])
        .sort_values("score", ascending=False)
        .head(10)
        [["post_id", "content_text", "score"]]
        .reset_index(drop=True)
    )
    df["rank"] = range(1, len(df) + 1)
    return df


def metric_top_stories_hn(posts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 10 HN story posts by score.
    gold schema: post_id, content_text, score, rank
    """
    stories = posts_df[
        (posts_df["platform"] == "HackerNews") &
        (posts_df["post_type"] == "story")
    ].copy()
    stories["score"] = pd.to_numeric(stories["score"], errors="coerce")
    df = (
        stories
        .dropna(subset=["score"])
        .sort_values("score", ascending=False)
        .head(10)
        [["post_id", "content_text", "score"]]
        .reset_index(drop=True)
    )
    df["rank"] = range(1, len(df) + 1)
    return df


def metric_data_quality(posts_df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Data Quality Score: percentage of non-null values per column per table.
    gold schema: table_name, column_name, non_null_pct, total_rows
    """
    rows = []
    for table_name, df in [("posts", posts_df), ("users", users_df)]:
        total = len(df)
        for col in df.columns:
            non_null = df[col].notna().sum()
            pct = round(non_null / total * 100, 2) if total > 0 else 0.0
            rows.append({
                "table_name":   table_name,
                "column_name":  col,
                "non_null_pct": pct,
                "total_rows":   total,
            })
    return pd.DataFrame(rows)


def process(event, _context):
    logger.info(f"Gold processor started. Event: {json.dumps(event)}")

    date_str = today_str()
    logger.info(f"Processing gold metrics for date: {date_str}")

    try:
        posts_df = read_posts()
        users_df = read_users()

        daily_users = metric_daily_users(users_df, date_str)
        write_gold(daily_users, "daily_users_metric", date_str, partition_cols=["platform", "date"])

        other_metrics = {
            "daily_posts_metric": metric_daily_posts(posts_df),
            "top_followers_x":    metric_top_followers_x(users_df),
            "top_karma_hn":       metric_top_karma_hn(users_df),
            "top_jobs_hn":        metric_top_jobs_hn(posts_df),
            "top_stories_hn":     metric_top_stories_hn(posts_df),
            "data_quality_kpi":   metric_data_quality(posts_df, users_df),
        }

        for metric_name, df in other_metrics.items():
            if df.empty:
                logger.warning(f"Metric {metric_name} is empty — still writing empty parquet")
            write_gold(df, metric_name, date_str)

        summary = {"daily_users_metric": len(daily_users)}
        summary.update({k: len(v) for k, v in other_metrics.items()})

        logger.info(f"Gold processor completed. Metrics written: {summary}")
        return {"status": "ok", "date": date_str, "metrics": summary}

    except Exception as e:
        error_msg = f"Gold Lambda failed on {date_str}: {type(e).__name__}: {e}"
        logger.error(error_msg, exc_info=True)
        notify_discord(error_msg)
        raise