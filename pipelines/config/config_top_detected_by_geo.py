"""
Configuration for top detected items by geographical location pipeline.
"""

CONFIG = {
    'pipeline_name': 'top_detected_by_geo',
    'app_name': 'HTX_TopDetected',

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
        'mode': 'overwrite'
    },

    # Processing parameters
    'processing': {
        'top_x': 10,
        'dedup_columns': ['detection_oid'],
        'groupby_columns': ['geographical_location_oid'],
        'rank_column': 'item_name',
        'count_column': 'detection_oid'
    },

    # Schema definitions
    'schemas': {
        'detection_data': {
            'geographical_location_oid': 'LongType',
            'video_camera_oid': 'LongType',
            'detection_oid': 'LongType',
            'item_name': 'StringType',
            'timestamp_detected': 'LongType'
        },
        'location_data': {
            'geographical_location_oid': 'LongType',
            'geographical_location': 'StringType'
        },
        'output': {
            'geographical_location_oid': 'LongType',
            'geographical_location': 'StringType',
            'item_rank': 'IntegerType',
            'item_name': 'StringType'
        }
    }
}
