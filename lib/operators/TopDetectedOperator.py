"""
Top Detected Items by Geographical Location Operator.
This class handles the specific business logic for calculating top X detected items per location.
"""
from pyspark import RDD
from typing import Dict, Any


class TopDetectedOperator:
    """
    Operator for calculating top X detected items by geographical location.
    Handles deduplication, aggregation, and ranking.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize operator with configuration."""
        self.config = config
        self.processing_config = config.get('processing', {})

        self.top_x = self.processing_config.get('top_x', 10)
        self.dedup_column = self.processing_config.get(
            'dedup_column', 'detection_oid')
        self.grouping_column = self.processing_config.get(
            'grouping_column', 'geographical_location_oid')
        self.item_column = self.processing_config.get(
            'item_column', 'item_name')

    def process(self, detection_rdd: RDD, location_rdd: RDD) -> RDD:
        """
        Main processing method: read -> process -> ready for write.

        Args:
            detection_rdd: Detection data RDD
            location_rdd: Location data RDD

        Returns:
            RDD with (geographical_location_oid, item_rank, item_name)
        """
        # Step 1: Deduplicate detection events
        unique_detections = self._deduplicate_detections(detection_rdd)

        # Step 2: Create location lookup for reference
        location_lookup = self._create_location_lookup(location_rdd)

        # Step 3: Process data
        return self._process_standard(unique_detections, location_lookup)

    def _deduplicate_detections(self, rdd: RDD) -> RDD:
        """Remove duplicate detection events by dedup_column."""
        return rdd.map(lambda row: (getattr(row, self.dedup_column), row)) \
                  .reduceByKey(lambda a, b: a) \
                  .map(lambda x: x[1])

    def _create_location_lookup(self, location_rdd: RDD) -> dict:
        """Create lookup dictionary for geographical locations."""
        return location_rdd.map(
            lambda row: (row.geographical_location_oid,
                         row.geographical_location)
        ).collectAsMap()

    def _process_standard(self, detection_rdd: RDD, location_lookup: dict) -> RDD:
        """Standard processing without skew handling."""
        # Group by location and item, count occurrences
        location_item_counts = detection_rdd \
            .map(lambda row: ((getattr(row, self.grouping_column),
                              getattr(row, self.item_column)), 1)) \
            .reduceByKey(lambda a, b: a + b)

        # Group by location to get all items per location
        location_items = location_item_counts \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .groupByKey()

        # Assign ranks within each location
        return location_items.flatMap(self._assign_ranks)


    def _assign_ranks(self, location_data):
        """Assign ranks to items within a location."""
        location_oid, items = location_data
        # Sort by count (descending) then by item_name (ascending) for tie-breaking
        sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))

        # Take top X items and assign ranks
        ranked_items = []
        for rank, (item_name, count) in enumerate(sorted_items[:self.top_x], 1):
            ranked_items.append((location_oid, rank, item_name))

        return ranked_items

