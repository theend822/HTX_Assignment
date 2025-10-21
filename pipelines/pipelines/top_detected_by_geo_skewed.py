"""
Top Detected Items Pipeline with Automatic Data Skew Handling.
This pipeline automatically detects and handles data skew without hardcoding skewed keys.
"""
import sys
import os
import random

# Add project root to path for imports when running pipeline directly
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from pipelines.config.config_top_detected_by_geo import CONFIG as BASE_CONFIG
from lib.operators.TopDetectedOperator import TopDetectedOperator
from lib.io.data_io import read_data, write_data, get_schema
from lib.spark_session import create_spark_session


# Skew-specific config
CONFIG = BASE_CONFIG.copy()
CONFIG.update({
    'pipeline_name': 'top_detected_by_geo_skewed',
    'app_name': 'HTX_TopDetected_Skewed',
    'output': {
        **BASE_CONFIG['output'],
        'output_path': 'output/top_detected_skewed_results.parquet'
    },
    'processing': {
        **BASE_CONFIG['processing'],
        'skew_threshold': 3.0,  # Salt if partition > 3x median size
        'salt_factor': 10,      # Number of salt buckets
        'sample_fraction': 0.1  # Sample data to detect skew
    }
})


def detect_skewed_keys(data_rdd, groupby_columns, sample_fraction=0.1, skew_threshold=3.0):
    """
    Automatically detect skewed keys by profiling data distribution.

    Args:
        data_rdd: Input RDD
        groupby_columns: List of columns to group by
        sample_fraction: Fraction of data to sample for profiling
        skew_threshold: Salt if key has > threshold * median_count records

    Returns:
        Set of skewed keys (tuples)
    """
    # Sample data for profiling
    sampled = data_rdd.sample(False, sample_fraction)

    # Create composite key from groupby columns
    def create_key(row):
        return tuple(getattr(row, col) for col in groupby_columns)

    # Count records per key
    key_counts = sampled.map(lambda row: (create_key(row), 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .collect()

    if not key_counts:
        return set()

    # Calculate median count
    counts = sorted([count for key, count in key_counts])
    median_count = counts[len(counts) // 2]

    # Identify skewed keys
    skewed_keys = {
        key for key, count in key_counts
        if count > skew_threshold * median_count
    }

    return skewed_keys


def calculate_top_items_with_skew_handling(data_rdd, config):
    """
    Calculate top X items with automatic skew detection and handling.

    Process:
    1. Detect skewed keys automatically
    2. Salt only the skewed keys
    3. Process non-skewed keys normally
    4. Combine results
    """
    groupby_columns = config['processing']['groupby_columns']
    rank_column = config['processing']['rank_column']
    top_x = config['processing']['top_x']
    salt_factor = config['processing']['salt_factor']

    # Step 1: Detect skewed keys
    skewed_keys = detect_skewed_keys(
        data_rdd,
        groupby_columns,
        sample_fraction=config['processing']['sample_fraction'],
        skew_threshold=config['processing']['skew_threshold']
    )

    if not skewed_keys:
        # No skew detected, use standard operator
        operator = TopDetectedOperator(config)
        return operator.calculate(data_rdd)

    # Step 2: Split data into skewed and non-skewed
    def create_key(row):
        return tuple(getattr(row, col) for col in groupby_columns)

    def is_skewed(row):
        return create_key(row) in skewed_keys

    skewed_data = data_rdd.filter(is_skewed)
    non_skewed_data = data_rdd.filter(lambda row: not is_skewed(row))

    # Step 3: Process non-skewed data normally
    if non_skewed_data.isEmpty():
        non_skewed_counts = non_skewed_data.sparkContext.parallelize([])
    else:
        operator_non_skewed = TopDetectedOperator(config)
        non_skewed_counts = operator_non_skewed._calculate_top_items(
            non_skewed_data.map(lambda row: (row, True))  # All flagged as unique
        )

    # Step 4: Process skewed data with salting
    def salt_row(row):
        """Add random salt to skewed keys"""
        group_key = create_key(row)
        salt = random.randint(0, salt_factor - 1)
        salted_key = (*group_key, salt)  # Append salt to key
        return (salted_key, getattr(row, rank_column))

    # Add salt, count, then remove salt and re-aggregate
    salted_counts = skewed_data \
        .map(lambda row: (salt_row(row), 1)) \
        .reduceByKey(lambda a, b: a + b)

    # Remove salt and aggregate back
    def desalt_key(salted_key_item_count):
        (salted_key, item), count = salted_key_item_count
        original_key = salted_key[:-1]  # Remove salt (last element)
        return ((original_key, item), count)

    skewed_aggregated = salted_counts \
        .map(desalt_key) \
        .reduceByKey(lambda a, b: a + b)

    # Reorganize and rank
    skewed_grouped = skewed_aggregated \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey()

    operator_skewed = TopDetectedOperator(config)
    skewed_results = skewed_grouped.flatMap(operator_skewed._rank_and_select_top)

    # Step 5: Combine skewed and non-skewed results
    return non_skewed_counts.union(skewed_results)


def run_pipeline():
    """
    Run the top detected items pipeline with automatic skew handling.
    Orchestration tools like Airflow will handle failure monitoring.
    """
    # Create Spark session
    spark = create_spark_session(CONFIG)

    # Step 1: READ DATA
    detection_rdd = read_data(spark, CONFIG['inputs']['detection_data'])
    location_rdd = read_data(spark, CONFIG['inputs']['location_data'])

    # Step 2: DETECT AND FLAG DUPLICATES
    operator = TopDetectedOperator(CONFIG)
    if CONFIG['processing']['dedup_columns']:
        flagged_data = operator._detect_and_flag(detection_rdd)
    else:
        flagged_data = detection_rdd.map(lambda row: (row, True))

    # Filter only unique rows
    unique_rows = flagged_data.filter(lambda x: x[1]).map(lambda x: x[0])

    # Step 3: CALCULATE TOP X WITH SKEW HANDLING
    result_rdd = calculate_top_items_with_skew_handling(unique_rows, CONFIG)

    # Step 4: JOIN WITH LOCATION NAMES
    location_lookup = location_rdd.map(
        lambda row: (row.geographical_location_oid, row.geographical_location)
    ).collectAsMap()

    # result_rdd format: (geographical_location_oid, item_rank, item_name)
    # Transform to: (geographical_location_oid, geographical_location, item_rank, item_name)
    result_with_location = result_rdd.map(
        lambda row: (
            row[0],  # geographical_location_oid
            location_lookup.get(row[0], 'Unknown'),  # geographical_location_name
            row[1],  # item_rank
            row[2]   # item_name
        )
    )

    # Step 5: WRITE DATA
    output_schema = get_schema(CONFIG['schemas'])['output']
    write_data(spark, CONFIG['output'], result_with_location, output_schema)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
