"""
Configuration for top detected items by geographical location pipeline.
"""

CONFIG = {
    'pipeline_name': 'top_detected_by_geo',
    'app_name': 'HTX_TopDetected_Calculator',

    # Spark configuration
    'spark_config': {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true'
    },

    # Input configuration
    'inputs': {
        'detection_data': {
            'file_path': 'data/detection_data.parquet',
            'format': 'parquet'
        },
        'location_data': {
            'file_path': 'data/location_data.parquet',
            'format': 'parquet'
        }
    },

    # Output configuration
    'output': {
        'output_path': 'output/top_detected_results.parquet',
        'format': 'parquet',
        'mode': 'overwrite',
        'schema': 'top_detected_output'
    },

    # Processing parameters
    'processing': {
        'top_x': 10,
        'dedup_column': 'detection_oid',
        'grouping_column': 'geographical_location_oid',
        'item_column': 'item_name'
    }
}