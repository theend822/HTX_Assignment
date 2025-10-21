"""
Integration tests for the pipeline architecture.
"""
import unittest
import tempfile
import shutil
import sys
import os

# Add project root to path BEFORE importing project modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.config.config_top_detected_by_geo import CONFIG  # noqa: E402
from lib.spark_session import create_spark_session  # noqa: E402
from lib.io.data_io import get_schema  # noqa: E402


class TestPipelineIntegration(unittest.TestCase):
    """
    Integration test: Proves the entire PySpark job can run on local Spark.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment with sample data."""
        test_config = {
            'app_name': 'test_pipeline_integration_top_detected_by_geo',
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

        # Create schemas using new nested dict format
        schemas = get_schema(CONFIG['schemas'])
        detection_schema = schemas['detection_data']
        location_schema = schemas['location_data']

        # Create DataFrames and save
        detection_df = cls.spark.createDataFrame(
            detection_data, detection_schema)
        location_df = cls.spark.createDataFrame(location_data, location_schema)

        cls.detection_path = f"{cls.temp_dir}/test_detection_data.parquet"
        cls.location_path = f"{cls.temp_dir}/test_location_data.parquet"

        detection_df.write.mode("overwrite").parquet(cls.detection_path)
        location_df.write.mode("overwrite").parquet(cls.location_path)

    def test_pipeline_execution(self):
        """
        Integration test: Prove the entire PySpark job can run on local Spark.
        Uses dummy datasets, runs pipeline end-to-end, verifies no errors.
        """
        from pipelines.pipelines.top_detected_by_geo import run_pipeline
        import copy

        output_path = f"{self.temp_dir}/pipeline_output.parquet"

        # Override config to use test data and output paths
        test_config = copy.deepcopy(CONFIG)
        test_config['inputs']['detection_data']['file_path'] = \
            self.detection_path
        test_config['inputs']['location_data']['file_path'] = \
            self.location_path
        test_config['output']['output_path'] = output_path

        # Temporarily replace CONFIG
        import pipelines.pipelines.top_detected_by_geo as pipeline_module
        original_config = pipeline_module.CONFIG
        pipeline_module.CONFIG = test_config

        try:
            # Run the entire pipeline - should complete without errors
            run_pipeline()

            # Verify output file was created
            self.assertTrue(os.path.exists(output_path))

            # Verify output can be read and has data
            output_df = self.spark.read.parquet(output_path)
            self.assertGreater(output_df.count(), 0)

        finally:
            # Restore original config
            pipeline_module.CONFIG = original_config

    def test_config_structure(self):
        """Test that config has the expected structure."""
        config = CONFIG

        # Test config structure
        self.assertIn('pipeline_name', config)
        self.assertIn('inputs', config)
        self.assertIn('output', config)
        self.assertIn('processing', config)
        self.assertIn('schemas', config)

        # Test processing parameters
        self.assertEqual(config['processing']['top_x'], 10)
        self.assertIn('dedup_columns', config['processing'])
        self.assertIn('groupby_columns', config['processing'])
        self.assertIn('rank_column', config['processing'])

        # Test schemas structure
        self.assertIn('detection_data', config['schemas'])
        self.assertIn('location_data', config['schemas'])
        self.assertIn('output', config['schemas'])

        # Test input formats
        self.assertEqual(
            config['inputs']['detection_data']['format'], 'parquet')


if __name__ == "__main__":
    unittest.main()
