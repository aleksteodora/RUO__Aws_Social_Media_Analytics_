# Social Media Analytics Platform

AWS-based platform for collecting, processing, storing and analyzing data from social media platforms using Medallion architecture (Bronze → Silver → Gold).

## Architecture

```
EventBridge (daily 00:00 UTC)
  → HN Bronze Lambda     → S3 bronze/hacker-news/YYYY-MM-DD/
  → EventBridge S3 rule  → Silver Lambda → S3 silver/posts/ + silver/users/
  → EventBridge S3 rule  → Gold Lambda   → S3 gold/
  → EventBridge S3 rule  → PostgreSQL Lambda → EC2 (PostgreSQL + Apache Superset)
```

## Data Sources

- **Hacker News** — stories, comments, ask_hn, jobs, polls collected daily via Algolia HN Search API
- **Twitter/X** — Sentiment140 dataset (~179k tweets, 2020)

## S3 Structure

```
s3://social-media-data-lake/
├── bronze/
│   ├── hacker-news/YYYY-MM-DD/{story,comment,ask_hn,job,poll}.json
│   └── twitter/sentiment140.csv
├── silver/
│   ├── posts/year=YYYY/month=MM/day=DD/data.parquet
│   └── users/platform={HackerNews,X}/data.parquet
└── gold/
    └── <metrics>/
```

## Silver Schema

**posts** — partitioned by `year/month/day`

| Column | Type | Notes |
|---|---|---|
| post_id | String | objectID (HN) or UUID (Twitter) |
| author_username | String | FK → users.username |
| content_text | String | HTML cleaned |
| created_at | Timestamp | UTC ISO-8601 |
| post_type | String | story, comment, ask_hn, job, poll, tweet, retweet |
| platform | String | HackerNews or X |
| score | Integer | HN points, null for Twitter |

**users** — partitioned by `platform`

| Column | Type | Notes |
|---|---|---|
| user_id | String | UUID generated |
| username | String | |
| karma_score | Integer | null (Firebase API requires auth) |
| user_followers | Integer | Twitter only |
| user_verified | Boolean | Twitter only |
| created_at | Timestamp | UTC ISO-8601, Twitter only |

## Stacks

| Stack | Description |
|---|---|
| BronzeStack | S3 bucket, HN collector Lambda, daily EventBridge trigger |
| SilverStack | Normalization Lambda, EventBridge S3 trigger |
| GoldStack | Metrics/KPI Lambda, EventBridge S3 trigger |
| VisualizationStack | EC2 (PostgreSQL + Superset), VPC, PostgreSQL loader Lambda |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Deploy

```bash
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_REGION=us-east-1

cdk deploy BronzeStack
cdk deploy SilverStack
```

## Region

`us-east-1`
