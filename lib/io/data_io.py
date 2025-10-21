"""
General data I/O functions for reading and writing data in pipelines.
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


def write_data(spark: SparkSession, config: Dict[str, Any], rdd,
               schema: StructType):
    """
    General function to write data based on configuration.

    Args:
        spark: SparkSession instance
        config: Configuration dict with write parameters
        rdd: RDD to write
        schema: Schema for the output DataFrame
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


def get_schema(config: Dict[str, Dict[str, str]]) -> Dict[str, StructType]:
    """
    Create schemas based on configuration.

    Args:
        config: Nested dict where outer key is dataset name,
                inner key-value pairs are column name and column type
                Example: {
                    'dataset_name': {
                        'column1': 'LongType',
                        'column2': 'StringType'
                    }
                }

    Returns:
        Dict mapping dataset names to StructType schemas
    """
    from pyspark.sql.types import (
        StructField, LongType, StringType, IntegerType,
        FloatType, DoubleType, BooleanType, TimestampType
    )

    # Map string type names to PySpark types
    type_mapping = {
        'LongType': LongType(),
        'StringType': StringType(),
        'IntegerType': IntegerType(),
        'FloatType': FloatType(),
        'DoubleType': DoubleType(),
        'BooleanType': BooleanType(),
        'TimestampType': TimestampType()
    }

    schemas = {}
    for dataset_name, columns in config.items():
        fields = []
        for col_name, col_type in columns.items():
            if col_type not in type_mapping:
                raise ValueError(f"Unsupported column type: {col_type}")
            fields.append(StructField(col_name, type_mapping[col_type], False))
        schemas[dataset_name] = StructType(fields)

    return schemas
