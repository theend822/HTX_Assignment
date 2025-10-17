"""
Top Detected Items by Geographical Location Pipeline with Data Skew Handling.
Executable pipeline for data with known geographical skew.

This pipeline includes the skew handling logic directly instead of using
a separate module, as suggested in the assignment requirements.
"""
import sys
import os
import random

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from pipelines.config.config_top_detected_by_geo import CONFIG as BASE_CONFIG

# Create skew-specific config by adding skew parameters to base config
CONFIG_SKEWED = BASE_CONFIG.copy()
CONFIG_SKEWED.update({
    'pipeline_name': 'top_detected_by_geo_skewed',
    'app_name': 'HTX_TopDetected_Skewed',
    'output': {
        **BASE_CONFIG['output'],
        'output_path': 'output/top_detected_skewed_results.parquet'
    },
    'processing': {
        **BASE_CONFIG['processing'],
        # NOTE: In production, skewed_location_id should be determined by:
        # 1. Data profiling to identify which location(s) have disproportionate data
        # 2. Spark UI analysis showing partition skew
        # 3. Dynamic detection rather than hardcoded values
        # For this demo, assuming location ID 1 has skew (this is a bad assumption!)
        'skewed_location_id': 1,  
        'salt_factor': 10  # Number of salt buckets to distribute skewed data
    }
})
from lib.io.data_io import read_data, write_data, get_schema
from lib.spark_session import create_spark_session


def process_top_items_with_known_skew(detection_rdd, location_lookup, top_x,
                                      skewed_location_id=1, salt_factor=10):
    """
    Modified version of process_top_items_by_location that handles known skew
    in a specific geographical location using salting technique.

    This replaces the original process_top_items_by_location function when
    you know location_id 1 contains most of the data.
    """
    # Split data into skewed and non-skewed partitions
    skewed_data = detection_rdd.filter(
        lambda row: row.geographical_location_oid == skewed_location_id
    )
    non_skewed_data = detection_rdd.filter(
        lambda row: row.geographical_location_oid != skewed_location_id
    )

    # Process non-skewed locations normally (unchanged logic)
    non_skewed_counts = non_skewed_data \
        .map(lambda row: ((row.geographical_location_oid, row.item_name), 1)) \
        .reduceByKey(lambda a, b: a + b)

    non_skewed_items = non_skewed_counts \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey()

    # Process skewed location with salting to distribute load
    salted_skewed_data = skewed_data.map(lambda row: (
        (f"{row.geographical_location_oid}_{random.randint(0, salt_factor-1)}",
         row.item_name), 1
    ))

    # Count items in salted buckets
    salted_counts = salted_skewed_data.reduceByKey(lambda a, b: a + b)

    # Aggregate back to original location (remove salt)
    skewed_aggregated = salted_counts \
        .map(lambda x: ((int(x[0][0].split('_')[0]), x[0][1]), x[1])) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey()

    # Combine skewed and non-skewed results
    all_location_items = non_skewed_items.union(skewed_aggregated)

    # Apply ranking (same as original)
    def assign_ranks(location_data):
        location_oid, items = location_data
        sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))

        ranked_items = []
        for rank, (item_name, count) in enumerate(sorted_items[:top_x], 1):
            ranked_items.append((location_oid, rank, item_name))

        return ranked_items

    return all_location_items.flatMap(assign_ranks)


def deduplicate_detections(rdd):
    """Remove duplicate detection_oid entries, keeping only unique detection events."""
    return rdd.map(lambda row: (row.detection_oid, row)) \
              .reduceByKey(lambda a, b: a) \
              .map(lambda x: x[1])


def create_location_lookup(location_rdd):
    """Create a broadcast-friendly lookup dictionary for geographical locations."""
    return location_rdd.map(
        lambda row: (row.geographical_location_oid, row.geographical_location)
    ).collectAsMap()


def run_skewed_pipeline():
    """
    Run the top detected items pipeline with skew handling.
    """
    print(f"Starting pipeline: {CONFIG_SKEWED['pipeline_name']}")
    print(f"Top X: {CONFIG_SKEWED['processing']['top_x']}")
    print(f"Skewed Location ID: {CONFIG_SKEWED['processing']['skewed_location_id']}")
    print(f"Salt Factor: {CONFIG_SKEWED['processing']['salt_factor']}")

    # Create Spark session
    spark = create_spark_session(CONFIG_SKEWED)

    try:
        # Step 1: READ DATA
        print("\n=== READING DATA ===")
        print(f"Reading detection data from: {CONFIG_SKEWED['inputs']['detection_data']['file_path']}")
        detection_rdd = read_data(spark, CONFIG_SKEWED['inputs']['detection_data'])

        print(f"Reading location data from: {CONFIG_SKEWED['inputs']['location_data']['file_path']}")
        location_rdd = read_data(spark, CONFIG_SKEWED['inputs']['location_data'])

        print(f"Detection data count: {detection_rdd.count():,}")
        print(f"Location data count: {location_rdd.count():,}")

        # Step 2: PROCESS DATA (with skew handling)
        print("\n=== PROCESSING DATA WITH SKEW HANDLING ===")

        # Deduplicate detection events
        unique_detections = deduplicate_detections(detection_rdd)
        print(f"After deduplication: {unique_detections.count():,}")

        # Create location lookup
        location_lookup = create_location_lookup(location_rdd)
        broadcast_lookup = spark.sparkContext.broadcast(location_lookup)

        # Process with skew handling
        result_rdd = process_top_items_with_known_skew(
            unique_detections,
            broadcast_lookup.value,
            CONFIG_SKEWED['processing']['top_x'],
            skewed_location_id=CONFIG_SKEWED['processing']['skewed_location_id'],
            salt_factor=CONFIG_SKEWED['processing']['salt_factor']
        )

        result_count = result_rdd.count()
        print(f"Processed results count: {result_count:,}")

        # Step 3: WRITE DATA
        print("\n=== WRITING DATA ===")
        output_schema = get_schema(CONFIG_SKEWED['output']['schema'])
        write_data(result_rdd, output_schema, spark, CONFIG_SKEWED['output'])

        print(f"Results written to: {CONFIG_SKEWED['output']['output_path']}")
        print(f"Format: {CONFIG_SKEWED['output']['format']}")

        # Show sample results
        print("\n=== SAMPLE RESULTS ===")
        sample_results = result_rdd.take(10)
        print("Location | Rank | Item")
        print("-" * 25)
        for geo_oid, rank, item in sample_results:
            print(f"{geo_oid:8} | {rank:4} | {item}")

        print(f"\n✅ Skewed pipeline {CONFIG['pipeline_name']} completed successfully!")

        return result_rdd

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_skewed_pipeline()