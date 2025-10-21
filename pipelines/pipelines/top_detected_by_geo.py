"""
Top Detected Items by Geographical Location Pipeline.
Executable pipeline following: read -> process -> write pattern.
"""
import sys
import os

# Add project root to path for imports when running pipeline directly
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from pipelines.config.config_top_detected_by_geo import CONFIG
from lib.operators.TopDetectedOperator import TopDetectedOperator
from lib.io.data_io import read_data, write_data, get_schema
from lib.spark_session import create_spark_session


def run_pipeline():
    """
    Run the top detected items pipeline.
    Orchestration tools like Airflow will handle failure monitoring.
    """
    # Create Spark session
    spark = create_spark_session(CONFIG)

    # Step 1: READ DATA
    detection_rdd = read_data(spark, CONFIG['inputs']['detection_data'])
    location_rdd = read_data(spark, CONFIG['inputs']['location_data'])

    # Step 2: PROCESS DATA
    operator = TopDetectedOperator(CONFIG)
    result_rdd = operator.calculate(detection_rdd)

    # Step 3: JOIN WITH LOCATION NAMES
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

    # Step 4: WRITE DATA
    output_schema = get_schema(CONFIG['schemas'])['output']
    write_data(spark, CONFIG['output'], result_with_location, output_schema)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
