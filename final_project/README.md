# Tweet Sentiment Analysis Pipeline

## Overview

Build a complete end-to-end data pipeline that ingests tweets, applies ML-based sentiment analysis, and delivers analytics through an automated dashboard.

**Core Technologies**: Spark Declarative Pipelines, Delta Lake, MLflow, Unity Catalog
**Architecture**: Medallion (Bronze → Silver → Gold → Application)

---

## Pipeline Architecture

![Tweet Pipeline Architecture](tweet%20pipeline%20architecture.jpeg)

---

## Data Source

**Location**: `s3://dsas-datasets/tweets/`
**Format**: JSON files (~50,000 tweets)
**Schema**:
```json
{
  "date": "Mon Apr 06 22:19:49 PDT 2009",
  "user": "username123",
  "text": "@someuser This is a sample tweet!",
  "sentiment": "4"
}
```

**Note**: Data is pre-provisioned. CloudFiles Auto Loader handles incremental ingestion. New data may be added daily - your automated job will process it.

---

### Pipeline Flow
```
S3 Bucket (Raw Tweets)
    ↓
Bronze Layer → Raw JSON ingestion
    ↓
Silver Layer → Text cleaning & mention extraction
    ↓
Gold Layer → ML sentiment predictions
    ↓
Application Layer → Aggregated metrics
    ↓
Dashboard + MLflow Tracking
```

### Bronze Layer: Raw Ingestion

**Purpose**: Ingest raw tweets from S3 using CloudFiles Auto Loader
**Input**: JSON files from `s3://dsas-datasets/tweets/`
**Output**: Delta table `tweets_bronze`

**Functionality**:
- Incremental ingestion with CloudFiles Auto Loader
- Schema enforcement for JSON fields (date, user, text, sentiment)
- Metadata tracking (source_file path, processing_time timestamp)
- Uses Spark Declarative Pipelines API (`pyspark.pipelines`)

---

### Silver Layer: Text Preprocessing

**Purpose**: Extract @mentions and clean tweet text for ML analysis
**Input**: `tweets_bronze` Delta table (streaming)
**Output**: Delta table `tweets_silver`

**Functionality**:
1. Extracts all @mentions from tweet text using regex pattern `@[\w]+`
2. Removes @mentions from text (create `cleaned_text` column)
3. Explodes mentions into individual rows (one row per mention per tweet)
4. Parses Twitter date format to proper timestamp
5. Normalizes mentions to lowercase
6. Preserves tweets without mentions (use appropriate explode strategy)

---

### Gold Layer: ML Inference

**Purpose**: Apply sentiment model to predict tweet sentiment
**Input**: `tweets_silver` Delta table (streaming)
**Output**: Delta table `tweets_gold`

**Functionality**:
1. Loads sentiment model from Unity Catalog (`workspace.default.tweet_sentiment_model`)
2. Creates Spark UDF for distributed ML inference
3. Applys model to `cleaned_text` column
4. Maps model output labels (LABEL_0, LABEL_1, LABEL_2) to sentiment strings (negative, neutral, positive)
5. Extracts confidence scores and scale to 0-100 range
6. Creates binary sentiment indicators for metrics (sentiment_id, predicted_sentiment_id)

---

### Application Layer: Analytics Aggregations

**Purpose**: Pre-compute analytics for dashboard consumption
**Input**: `tweets_gold` Delta table
**Output**: Materialized view `gold_tweet_aggregations`

**Functionality**:
1. Aggregates by `mention` (lowercased username)
2. Calculates metrics per mention:
   - Count of positive mentions
   - Count of negative mentions
   - Total mentions (positive + negative, excluding neutral)
   - Min and max timestamp (for tracking mention timeline)
3. Filters out NULL mentions
4. Sorts by total mentions descending

---

## Data Schemas

### Bronze Schema
| Column | Type | Source |
|--------|------|--------|
| date | string | JSON field |
| user | string | JSON field |
| text | string | JSON field |
| sentiment | string | JSON field |
| source_file | string | _metadata.file_path |
| processing_time | timestamp | current_timestamp() |

### Silver Schema
| Column | Type | Transformation |
|--------|------|----------------|
| timestamp | timestamp | Parse date string to timestamp |
| mention | string | Extract from text using regex |
| cleaned_text | string | Remove @mentions from original text |
| text | string | Original text (unchanged) |
| sentiment | string | Original sentiment label |

### Gold Schema
| Column | Type | Source/Transformation |
|--------|------|----------------------|
| ... | ... | All silver columns passed through |
| predicted_score | double | Model confidence * 100 |
| predicted_sentiment | string | Map LABEL_0/1/2 → negative/neutral/positive |
| sentiment_id | int | Binary ground truth (0 or 1) |
| predicted_sentiment_id | int | Binary prediction (0 or 1) |

### Application Schema
| Column | Type | Aggregation |
|--------|------|-------------|
| mention | string | GROUP BY (lowercased username) |
| positive | int | COUNT WHERE predicted_sentiment = 'positive' |
| negative | int | COUNT WHERE predicted_sentiment = 'negative' |
| total | int | positive + negative (exclude neutral) |
| min_timestamp | timestamp | MIN(timestamp) |
| max_timestamp | timestamp | MAX(timestamp) |

---

## Dashboard 

1. **Total Tweets Counter** - Count of all tweets from bronze layer
2. **Tweets with Mentions Counter** - Count from silver WHERE mention IS NOT NULL
3. **Tweets without Mentions Counter** - Count from silver WHERE mention IS NULL
4. **Top 10 Users by Tweet Count** - Bar chart of most active users
5. **Top 10 Most Positively Mentioned** - Bar chart (green), from application layer
6. **Top 10 Most Negatively Mentioned** - Bar chart (red), from application layer

---

## Databricks Job 

Configured an automated Databricks Job with:

**Run Pipeline**
- Type: DLT pipeline
- Notebook/Pipeline: Tweet sentiment pipeline
- Schedule: Daily at 2:00 AM UTC

**Refresh Dashboard** 
- Type: Refresh dashboard task
- Dashboard: Your tweet analytics dashboard
- Runs after Task 1 completes successfully

**Notifications**: Configure email alerts on failure

---

## Model Performance Analysis

Created an MLflow experiment that tracks:
- **Parameters**: Model name, version, data path
- **Metrics**: Classification report (precision, recall, F1 for each class)
- **Artifacts**: Confusion matrix visualization

**Analysis**:
1. Loaded `tweets_gold` with ground truth (`sentiment_id`) and predictions (`predicted_sentiment_id`)
2. Calculated classification metrics using sklearn.metrics
3. Generated confusion matrix
4. Logged all to MLflow experiment
5. Register experiment in Unity Catalog

---
