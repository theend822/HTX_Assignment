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
        """Test standard processing without skew handling."""
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

        test_locations = [
            self.LocationRow(1, "Downtown"),
            self.LocationRow(2, "Suburb")
        ]

        detection_rdd = self.spark.sparkContext.parallelize(test_detections)
        location_rdd = self.spark.sparkContext.parallelize(test_locations)

        # Process data
        result_rdd = operator.process(detection_rdd, location_rdd)
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
        """Test that duplicate detection_oid are properly handled."""
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
        deduplicated_rdd = operator._deduplicate_detections(rdd)
        result = deduplicated_rdd.collect()

        # Should have 2 unique detection_oid values
        self.assertEqual(len(result), 2)
        detection_oids = [row.detection_oid for row in result]
        self.assertEqual(set(detection_oids), {1001, 1002})

    def test_location_lookup(self):
        """Test location lookup creation."""
        config = CONFIG.copy()
        operator = TopDetectedOperator(config)

        test_data = [
            self.LocationRow(1, "Downtown"),
            self.LocationRow(2, "Suburb"),
            self.LocationRow(3, "Industrial")
        ]

        rdd = self.spark.sparkContext.parallelize(test_data)
        lookup = operator._create_location_lookup(rdd)

        expected = {1: "Downtown", 2: "Suburb", 3: "Industrial"}
        self.assertEqual(lookup, expected)

    def test_empty_data_handling(self):
        """Test handling of empty datasets."""
        config = CONFIG.copy()
        operator = TopDetectedOperator(config)

        empty_rdd = self.spark.sparkContext.parallelize([])
        deduplicated = operator._deduplicate_detections(empty_rdd)
        self.assertEqual(deduplicated.count(), 0)

        empty_lookup = operator._create_location_lookup(empty_rdd)
        self.assertEqual(empty_lookup, {})


if __name__ == "__main__":
    unittest.main()
