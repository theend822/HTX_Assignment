"""
General data I/O functions for reading and writing data in pipelines.
These functions are reusable across different pipeline tasks.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Dict, Any


def read_data(spark: SparkSession, config: Dict[str, Any]):
    """
    General function to read data based on configuration.

    Args:
        spark: SparkSession instance
        config: Configuration dict with read parameters

    Returns:
        RDD containing the data
    """
    file_path = config['file_path']
    file_format = config.get('format', 'parquet')

    if file_format.lower() == 'parquet':
        df = spark.read.parquet(file_path)
    elif file_format.lower() == 'csv':
        df = spark.read.option("header", "true").csv(file_path)
    elif file_format.lower() == 'json':
        df = spark.read.json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return df.rdd


def write_data(rdd, schema: StructType, spark: SparkSession, config: Dict[str, Any]):
    """
    General function to write data based on configuration.

    Args:
        rdd: RDD to write
        schema: Schema for the output DataFrame
        spark: SparkSession instance
        config: Configuration dict with write parameters
    """
    output_path = config['output_path']
    output_format = config.get('format', 'parquet')
    write_mode = config.get('mode', 'overwrite')

    df = spark.createDataFrame(rdd, schema)
    writer = df.write.mode(write_mode)

    if output_format.lower() == 'parquet':
        writer.parquet(output_path)
    elif output_format.lower() == 'csv':
        writer.option("header", "true").csv(output_path)
    elif output_format.lower() == 'json':
        writer.json(output_path)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def get_schema(schema_name: str) -> StructType:
    """Get predefined schemas by name."""
    from pyspark.sql.types import StructField, LongType, StringType, IntegerType

    schemas = {
        'top_detected_output': StructType([
            StructField("geographical_location_oid", LongType(), False),
            StructField("item_rank", IntegerType(), False),
            StructField("item_name", StringType(), False)
        ]),
        'detection_data': StructType([
            StructField("geographical_location_oid", LongType(), False),
            StructField("video_camera_oid", LongType(), False),
            StructField("detection_oid", LongType(), False),
            StructField("item_name", StringType(), False),
            StructField("timestamp_detected", LongType(), False)
        ]),
        'location_data': StructType([
            StructField("geographical_location_oid", LongType(), False),
            StructField("geographical_location", StringType(), False)
        ])
    }

    if schema_name not in schemas:
        raise ValueError(f"Unknown schema: {schema_name}")

    return schemas[schema_name]
