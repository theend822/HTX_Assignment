"""
Spark session creation utilities.
Used across all Spark pipelines.
"""
from pyspark.sql import SparkSession
from typing import Dict, Any


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create and configure Spark session from config."""
    app_name = config.get('app_name', 'DataPipeline')
    spark_config = config.get('spark_config', {})

    builder = SparkSession.builder.appName(app_name)

    # Apply spark configurations
    for key, value in spark_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()