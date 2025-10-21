"""
Unit tests for TopDetectedOperator.
"""
from pipelines.config.config_top_detected_by_geo import CONFIG
from lib.operators.TopDetectedOperator import TopDetectedOperator
from lib.spark_session import create_spark_session
from collections import namedtuple
import unittest
import tempfile
import shutil
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestTopDetectedOperator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and test data."""
        test_config = {
            'app_name': 'test_top_detected_operator',
            'spark_config': {}
        }
        cls.spark = create_spark_session(test_config)
        cls.temp_dir = tempfile.mkdtemp()

        # Create named tuples for test data
        cls.DetectionRow = namedtuple(
            'DetectionRow',
            ['geographical_location_oid', 'video_camera_oid',
             'detection_oid', 'item_name', 'timestamp_detected']
        )

        cls.LocationRow = namedtuple(
            'LocationRow',
            ['geographical_location_oid', 'geographical_location']
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)

    def test_standard_operator(self):
        """Test standard operator calculation."""
        config = CONFIG.copy()
        config['processing']['top_x'] = 2

        operator = TopDetectedOperator(config)

        # Create test data
        test_detections = [
            self.DetectionRow(1, 101, 1001, "car", 1234567890),
            self.DetectionRow(1, 102, 1002, "car", 1234567891),
            self.DetectionRow(1, 103, 1003, "bike", 1234567892),
            # car appears 3 times
            self.DetectionRow(1, 104, 1004, "car", 1234567893),
            self.DetectionRow(2, 201, 1005, "person", 1234567894),
            # person appears 2 times
            self.DetectionRow(2, 202, 1006, "person", 1234567895),
            self.DetectionRow(2, 203, 1007, "dog", 1234567896)
        ]

        detection_rdd = self.spark.sparkContext.parallelize(test_detections)

        # Process data
        result_rdd = operator.calculate(detection_rdd)
        results = result_rdd.collect()

        # Sort results for consistent testing
        results = sorted(results, key=lambda x: (x[0], x[1]))

        # Verify results
        self.assertEqual(len(results), 4)  # 2 items per location

        # Location 1 results
        location_1_results = [r for r in results if r[0] == 1]
        self.assertEqual(len(location_1_results), 2)
        self.assertEqual(location_1_results[0], (1, 1, "car"))    # rank 1
        self.assertEqual(location_1_results[1], (1, 2, "bike"))   # rank 2

        # Location 2 results
        location_2_results = [r for r in results if r[0] == 2]
        self.assertEqual(len(location_2_results), 2)
        self.assertEqual(location_2_results[0], (2, 1, "person"))  # rank 1
        self.assertEqual(location_2_results[1], (2, 2, "dog"))    # rank 2


    def test_deduplication(self):
        """Test that duplicate detection_oid are properly flagged."""
        config = CONFIG.copy()
        operator = TopDetectedOperator(config)

        # Create data with duplicates
        test_data = [
            self.DetectionRow(1, 101, 1001, "car", 1234567890),
            # duplicate detection_oid
            self.DetectionRow(1, 101, 1001, "car", 1234567891),
            self.DetectionRow(1, 102, 1002, "bike", 1234567892),
        ]

        rdd = self.spark.sparkContext.parallelize(test_data)
        flagged_rdd = operator._detect_and_flag(rdd)
        result = flagged_rdd.collect()

        # Should have 3 rows (all preserved), with flags
        self.assertEqual(len(result), 3)

        # Check flags: first occurrence of 1001 is True, second is False, 1002 is True
        unique_count = sum(1 for row, is_unique in result if is_unique)
        duplicate_count = sum(1 for row, is_unique in result if not is_unique)

        self.assertEqual(unique_count, 2)  # 1001 (first), 1002
        self.assertEqual(duplicate_count, 1)  # 1001 (second)

    def test_empty_data_handling(self):
        """Test handling of empty datasets."""
        config = CONFIG.copy()
        operator = TopDetectedOperator(config)

        empty_rdd = self.spark.sparkContext.parallelize([])
        flagged_rdd = operator._detect_and_flag(empty_rdd)
        self.assertEqual(flagged_rdd.count(), 0)


if __name__ == "__main__":
    unittest.main()
