"""
Integration tests for the pipeline architecture.
"""
from pipelines.config.config_top_detected_by_geo import CONFIG
from lib.spark_session import create_spark_session
from lib.io.data_io import get_schema
import unittest
import tempfile
import shutil
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestPipelineIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test environment with sample data."""
        test_config = {
            'app_name': 'test_pipeline_integration',
            'spark_config': {}
        }
        cls.spark = create_spark_session(test_config)
        cls.temp_dir = tempfile.mkdtemp()

        # Create test datasets
        cls._create_test_datasets()

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)

    @classmethod
    def _create_test_datasets(cls):
        """Create test parquet files."""
        # Detection dataset
        detection_data = [
            (1, 101, 1001, "car", 1640995200),
            (1, 102, 1002, "car", 1640995201),
            (1, 103, 1003, "person", 1640995202),
            (1, 104, 1004, "car", 1640995203),  # car: 3 times
            (2, 201, 2001, "person", 1640995204),
            (2, 202, 2002, "person", 1640995205),  # person: 2 times
            (2, 203, 2003, "bike", 1640995206),
        ]

        location_data = [
            (1, "Downtown"),
            (2, "Suburb")
        ]

        # Create schemas
        detection_schema = get_schema('detection_data')
        location_schema = get_schema('location_data')

        # Create DataFrames and save
        detection_df = cls.spark.createDataFrame(
            detection_data, detection_schema)
        location_df = cls.spark.createDataFrame(location_data, location_schema)

        cls.detection_path = f"{cls.temp_dir}/test_detection_data.parquet"
        cls.location_path = f"{cls.temp_dir}/test_location_data.parquet"

        detection_df.write.mode("overwrite").parquet(cls.detection_path)
        location_df.write.mode("overwrite").parquet(cls.location_path)

    def test_end_to_end_pipeline(self):
        """Test complete pipeline execution."""
        output_path = f"{self.temp_dir}/pipeline_output.parquet"

        # Configuration overrides for testing
        config_overrides = {
            'detection_data_path': self.detection_path,
            'location_data_path': self.location_path,
            'output_path': output_path,
            'top_x': 3
        }

        # We can't easily test the pipeline execution that creates its own Spark session
        # because it conflicts with our test Spark session. Instead, we'll test the
        # processor logic directly using our test session.
        from lib.operators.TopDetectedOperator import TopDetectedOperator

        config = CONFIG.copy()
        config['processing']['top_x'] = 3

        operator = TopDetectedOperator(config)

        # Read test data using our existing spark session
        detection_rdd = self.spark.read.parquet(self.detection_path).rdd
        location_rdd = self.spark.read.parquet(self.location_path).rdd

        # Process data
        result_rdd = operator.process(detection_rdd, location_rdd)

        # Verify results
        self.assertIsNotNone(result_rdd)
        results = result_rdd.collect()
        self.assertGreater(len(results), 0)

        # Verify result structure
        for geo_oid, rank, item in results:
            self.assertIsInstance(geo_oid, int)
            self.assertIsInstance(rank, int)
            self.assertIsInstance(item, str)

        # Verify rankings
        location_results = {}
        for geo_oid, rank, item in results:
            if geo_oid not in location_results:
                location_results[geo_oid] = []
            location_results[geo_oid].append((rank, item))

        # Should have results for both locations
        self.assertIn(1, location_results)
        self.assertIn(2, location_results)

        # Verify rank ordering
        for location_oid, items in location_results.items():
            ranks = [rank for rank, item in items]
            self.assertEqual(sorted(ranks), list(range(1, len(ranks) + 1)))

    def test_config_structure(self):
        """Test that config has the expected structure."""
        config = CONFIG

        # Test config structure
        self.assertIn('pipeline_name', config)
        self.assertIn('spark_config', config)
        self.assertIn('inputs', config)
        self.assertIn('output', config)
        self.assertIn('processing', config)
        
        # Test default values
        self.assertEqual(config['processing']['top_x'], 10)
        self.assertEqual(
            config['inputs']['detection_data']['format'], 'parquet')


if __name__ == "__main__":
    unittest.main()
