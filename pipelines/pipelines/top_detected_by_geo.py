"""
Top Detected Items by Geographical Location Pipeline.
Executable pipeline following: read -> process -> write pattern.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from pipelines.config.config_top_detected_by_geo import CONFIG
from lib.operators.TopDetectedOperator import TopDetectedOperator
from lib.io.data_io import read_data, write_data, get_schema
from lib.spark_session import create_spark_session


def run_pipeline():
    """
    Run the top detected items pipeline.
    """
    print(f"Starting pipeline: {CONFIG['pipeline_name']}")
    print(f"Top X: {CONFIG['processing']['top_x']}")

    # Create Spark session
    spark = create_spark_session(CONFIG)

    try:
        # Step 1: READ DATA
        print("\n=== READING DATA ===")
        print(f"Reading detection data from: {CONFIG['inputs']['detection_data']['file_path']}")
        detection_rdd = read_data(spark, CONFIG['inputs']['detection_data'])

        print(f"Reading location data from: {CONFIG['inputs']['location_data']['file_path']}")
        location_rdd = read_data(spark, CONFIG['inputs']['location_data'])

        print(f"Detection data count: {detection_rdd.count():,}")
        print(f"Location data count: {location_rdd.count():,}")

        # Step 2: PROCESS DATA
        print("\n=== PROCESSING DATA ===")
        operator = TopDetectedOperator(CONFIG)
        result_rdd = operator.process(detection_rdd, location_rdd)

        result_count = result_rdd.count()
        print(f"Processed results count: {result_count:,}")

        # Step 3: WRITE DATA
        print("\n=== WRITING DATA ===")
        output_schema = get_schema(CONFIG['output']['schema'])
        write_data(result_rdd, output_schema, spark, CONFIG['output'])

        print(f"Results written to: {CONFIG['output']['output_path']}")
        print(f"Format: {CONFIG['output']['format']}")

        # Show sample results
        print("\n=== SAMPLE RESULTS ===")
        sample_results = result_rdd.take(10)
        print("Location | Rank | Item")
        print("-" * 25)
        for geo_oid, rank, item in sample_results:
            print(f"{geo_oid:8} | {rank:4} | {item}")

        print(f"\n✅ Pipeline {CONFIG['pipeline_name']} completed successfully!")

        return result_rdd

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()