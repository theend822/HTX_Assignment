# Data Architecture Design

## Objectives

To build a **near–real-time data pipeline** on **GCP** that:

* Ingests high-volume streaming events (10,000/sec) from video cameras (**Dataset A**)
* Deduplicates events
* Joins with a static geographical lookup (**Dataset B**)
* Visualize joined results via dashboards


## Clarification Questions

**1. Latency Requirements**
- Q: What is acceptable end-to-end latency? (e.g., sub-second, <5 seconds, <30 seconds)
- Assumption: <60 seconds acceptable for dashboard updates

**2. Duplicate handling**  
- Q: How do we define duplications? What to do if we find duplicates while processing the data?
- Assumption: Each detection_id should only exist once

**3. Data Retention**
- Q: How long should raw events be retained? (for replay, auditing, compliance)
- Assumption: 30 days hot storage, 1 year cold storage

**3. Dashboard Query Patterns**
- Q: What aggregation levels? (real-time per location, hourly rollups, daily summaries)
- Assumption: Real-time + hourly aggregations

**4. Failure Handling**
- Q: Can we lose events during failures?
- Assumption: Exactly-once processing required (deduplication critical)

**5. Data Quality**
- Q: What % of duplicates expected? Any other data quality issues?
- Assumption: <5% duplicates from retries, all events have valid schema


## Proposed Data Architecture (Assuming GCP + Spark)

### High-Level Architecture

```
Video Cameras (Edge)
    |
    | Pub/Sub Topic: raw-detections
    |
    v
Dataproc Cluster (Spark Structured Streaming)
    |
    |--> Deduplication (watermark + dropDuplicates)
    |--> Join with Location Data (broadcast join)
    |--> Write to BigQuery (foreachBatch)
    |
    v
BigQuery Tables
    |
    |--> Layer 1: raw_detection_events (7-day retention)
    |--> Layer 2: detection_events (deduplicated + joined)
    |--> Layer 3: detection_events_enriched (materialized view)
    |
    v
Looker Studio Dashboard
```


### Components and Responsibilities

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Ingestion | Cloud Pub/Sub | Serverless, handles high throughput, built-in retry, decouples producers/consumers |
| Processing | Dataproc (Spark Structured Streaming) | Consistent with assignment PySpark stack, broadcast join, watermark deduplication |
| Storage | BigQuery | Serverless analytics DB, fast queries, materialized views, partitioning/clustering |
| Reference Data | Cloud Storage (Parquet) | Cost-effective, versioned, read by Spark as broadcast variable |
| Visualization | Looker Studio | Free, native BigQuery integration, real-time dashboard refresh |
| Monitoring | Cloud Monitoring + Logging | Native GCP integration, custom metrics, unified observability |
| Checkpointing | Cloud Storage | Exactly-once Spark checkpoints, failure recovery |

### Detailed Components

#### 1. Ingestion Layer

**Pub/Sub Topic: `raw-detections`**
- Purpose: Decouple producers (cameras) from consumers, provide buffering
- Features:
  - 7-day message retention for replay capability
  - Dead letter queue for failed messages
- Why Pub/Sub?
  - Handles 10k events/sec easily (scales to millions)
  - At-least-once delivery guarantee
  - Built-in retry mechanism (causes duplicates we need to handle)
  - Serverless, no infrastructure management

**Location Reference Data**
- Storage: Cloud Storage (Parquet format)
- Features:
  - Versioned storage for rollback capability
  - Cost-effective for static data
- Why Cloud Storage?
  - Low cost
  - Easy integration with Spark (read as broadcast variable)

#### 2. Processing Layer

**Dataproc + Spark Structured Streaming**

**Processing Pipeline:**
1. **Ingestion:** Read streaming events from Pub/Sub subscription
2. **Deduplication:** Watermark-based deduplication using `detection_oid`
3. **Enrichment:** Broadcast join with location reference data
4. **Output:** Write to BigQuery using micro-batches (10-second intervals)

**Key Features:**
- **Watermark-based deduplication:**
  - Handles late-arriving events (up to 2 minutes delay)
  - Exactly-once semantics for deduplication

- **Broadcast join pattern:**
  - Location table (<1GB) broadcasted to all workers
  - Avoids shuffle overhead (same pattern as assignment)
  - Fast in-memory lookup

- **Checkpoint-based fault tolerance:**
  - State stored in Cloud Storage
  - Exactly-once processing guarantee
  - Automatic recovery from failures


#### 3. Storage Layer

**BigQuery - Three-Layer Architecture**

**Layer 1: Raw Streaming Events (`raw_detection_events`)**
- Purpose: Landing zone for all streaming events from Pub/Sub
- Features:
  - Captures raw events with minimal transformation
  - Includes deduplication metadata (is_duplicate flag)
  - Partitioned by ingestion timestamp
  - Retention: 7 days (for replay and debugging)
- Why keep raw layer?
  - Enables replay if processing logic changes
  - Audit trail for compliance
  - Debugging data quality issues

**Layer 2: Processed Events (`detection_events`)**
- Purpose: Deduplicated, enriched events with location names joined
- Features:
  - Partitioned by date (timestamp_detected)
  - Clustered by geographical_location_oid
  - Contains joined geographical_location name
  - Duplicate events flagged
  - Retention: 30 days hot, 1 year cold storage
- Why partitioning/clustering?
  - Improves query performance
  - Reduces query costs

**Layer 3: Materialized View (`detection_events_enriched`)**
- Purpose: Pre-joined, deduplicated view optimized for dashboard queries
- Features:
  - Auto-refresh every 5 minutes
  - Partitioned by date for cost optimization
  - Pre-computed joins (event + location data)
  - Only includes valid, unique events
- Why materialized view?
  - Reduces dashboard query latency (pre-computed joins)
  - Lower query costs (reads materialized data, not raw events)
  - Automatic refresh on base table changes
  - Consistent snapshot for concurrent dashboard users


**Why BigQuery?**
- Serverless, scales automatically (no cluster management)
- Streaming inserts support real-time ingestion
- Materialized views for pre-aggregated queries
- Cost-effective for analytics workloads (pay per query)
- Native BI tool integration (Looker Studio)
- Handles 26B events/month easily

#### 4. Visualization Layer

**Looker Studio**

**Data Source:** BigQuery materialized view `detection_events_enriched`

**Dashboard Features:**
- Real-time detection events with location names (refreshes every 1 minute)
- Time-series trends for detection volumes
- Geographic breakdown by location
- Data quality metrics (duplicate rate, event lag, processing latency)

**Why Looker Studio?**
- Free, native GCP integration (zero additional cost)
- Direct BigQuery connection (no ETL required)
- Auto-refresh capabilities (1-minute intervals)
- Shareable dashboards with access control
- Flexible ad-hoc analysis (PM can drill down by location, item, time)


## Monitoring & Alerting

**Dataproc/Spark Metrics (Cloud Monitoring)**
- Processing lag (alert if > 2 minutes)
- Duplicate rate (custom metric from Spark job)
- Checkpoint age (alert if not updating)
- Cluster resource utilization (CPU, memory)

**BigQuery Metrics**
- Streaming insert errors
- Query performance
- Slot utilization

**Pub/Sub Metrics**
- Oldest unacked message age
- Dead letter queue depth


## Alternative Approach: Apache Flink

While Spark Structured Streaming is the primary proposed solution (aligned with PySpark in the test), **Apache Flink** on **GCP Dataflow** offers a powerful alternative for **true real-time, event-by-event processing**.

### Comparison: Spark vs Flink

| Criteria                   | **Spark Structured Streaming (Dataproc)** | **Apache Flink (Dataflow Runner)**      |
| -------------------------- | ----------------------------------------- | --------------------------------------- |
| **Processing Model**       | Micro-batch (near-real-time)              | True record-by-record streaming         |
| **Latency**                | Seconds to tens of seconds                | Milliseconds to low seconds             |
| **Deduplication**          | `dropDuplicates()` with watermark         | Stateful deduplication with TTL         |
| **Stream–Static Join**     | Broadcast join (manual reloads)           | Temporal join (native support)          |
| **State Management**       | Simplified via microbatch                 | Advanced, fine-grained state            |
| **Operational Complexity** | Lower (PySpark friendly)                  | Higher (Beam/Flink skills required)     |
| **Integration with Test**  | Direct fit                                | Different API (Beam/Java)               |
| **Best Use Case**          | Batch + near-real-time analytics          | Continuous, ultra-low-latency streaming |

