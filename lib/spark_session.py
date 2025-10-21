"""
Spark session creation utilities.
Used across all Spark pipelines.
"""
from pyspark.sql import SparkSession
from typing import Dict, Any
from datetime import datetime
import getpass


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create and configure Spark session from config."""
    # if app_name not provided, generate an unique one based on timestamp and username
    if 'app_name' not in config:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        username = getpass.getuser()
        app_name = f'spark_pipeline_{timestamp}_{username}' 
    else:
        app_name = config['app_name']

    spark_config = config.get('spark_config', {})

    builder = SparkSession.builder.appName(app_name) # type: ignore[attr-defined]

    # Apply spark configurations
    for key, value in spark_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()