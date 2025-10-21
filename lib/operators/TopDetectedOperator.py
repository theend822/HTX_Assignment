"""
Top X Items Operator
"""
from pyspark import RDD
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class TopDetectedOperator:
    """
    General-purpose operator for calculating top X items with grouping and
    aggregation.

    This operator performs the following steps:
    1. Detect and flag duplicate records based on specified deduplication
       columns
       - This is a dedicated data quality check
       - Duplicates are flagged with is_unique=False (first occurrence gets
         is_unique=True)
       - All original data is preserved, no deletion occurs
       - Duplicate findings are logged
    2. Group by specified columns (similar to SQL: SELECT group_cols,
       COUNT(count_col) GROUP BY group_cols WHERE is_unique=True)
    3. Rank items within each group and return top X
    4. Only unique records (is_unique=True) are counted in the ranking

    Config structure:
        processing:
            top_x: Number of top items to return (default: 10)
            groupby_columns: List of column names to group by for
                aggregation
            dedup_columns: List of column names to check for duplicates;
                by default, same as groupby_columns
            count_column: Column name to count occurrences
            rank_column: Column name containing the item to be ranked
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize operator with configuration."""
        self.config = config
        self.processing_config = config.get('processing', {})

        self.top_x = self.processing_config.get('top_x', 10)
        self.groupby_columns = self.processing_config.get(
            'groupby_columns', [])
        self.dedup_columns = self.processing_config.get(
            'dedup_columns', 'groupby_columns')
        self.count_column = self.processing_config.get('count_column')
        self.rank_column = self.processing_config.get('rank_column')

        # Validate required config
        if not self.groupby_columns:
            raise ValueError("groupby_columns must be specified in config")
        if not self.rank_column:
            raise ValueError("rank_column must be specified in config")

    def calculate(self, data_rdd: RDD) -> RDD:
        """
        Main calculation method: read -> detect & flag -> aggregate ->
        rank & select top.

        Args:
            data_rdd: Input data RDD

        Returns:
            RDD with (*groupby_columns, item_rank, rank_column_value)
        """
        # Step 1: Detect and flag duplicates
        if self.dedup_columns:
            flagged_data = self._detect_and_flag(data_rdd)
        else:
            # If no dedup columns, add flag=True to all rows
            flagged_data = data_rdd.map(lambda row: (row, True))

        # Step 2: Calculate top X items
        return self._calculate_top_items(flagged_data)

    def _detect_and_flag(self, rdd: RDD) -> RDD:
        """
        Detect and flag duplicates based on dedup_columns.

        This is a dedicated data quality check that:
        1. Detects duplicates at the granularity of dedup_columns
        2. Logs findings when duplicates are found
        3. Adds an 'is_unique' flag to each row (True for unique/first
           occurrence, False for duplicates)
        4. Preserves all original data without deletion

        Args:
            rdd: Input RDD

        Returns:
            RDD with tuples (row, is_unique_flag)
        """
        # Create composite key from dedup_columns
        def create_dedup_key(row):
            return tuple(getattr(row, col) for col in self.dedup_columns)

        # Map to (dedup_key, row) and track duplicates
        keyed_rdd = rdd.map(lambda row: (create_dedup_key(row), row))

        # Group by key to detect duplicates
        grouped = keyed_rdd.groupByKey()

        # Flag duplicates and log them
        def flag_duplicates(key_group):
            key, rows = key_group
            rows_list = list(rows)

            # If duplicates found, log it
            if len(rows_list) > 1:
                logger.warning(
                    f"Duplicate detected: {len(rows_list)} records found "
                    f"for key {key}. Flagging first occurrence as unique, "
                    f"marking {len(rows_list) - 1} as duplicate(s)."
                )

            # Return all rows with flags: first one is unique (True), rest
            # are duplicates (False)
            flagged_rows = []
            for idx, row in enumerate(rows_list):
                is_unique = (idx == 0)  # Only first occurrence is unique
                flagged_rows.append((row, is_unique))

            return flagged_rows

        return grouped.flatMap(flag_duplicates)

    def _calculate_top_items(self, data_rdd: RDD) -> RDD:
        """
        Calculate top X items by grouping and ranking.
        Only counts rows flagged as unique (is_unique=True).

        Equivalent to SQL:
        SELECT groupby_columns, rank_column, COUNT(count_column)
        FROM data
        WHERE is_unique = True
        GROUP BY groupby_columns, rank_column
        ORDER BY COUNT(count_column) DESC
        LIMIT top_x

        Args:
            data_rdd: RDD of tuples (row, is_unique_flag) where
                is_unique_flag is boolean

        Returns:
            RDD with (*groupby_columns, item_rank, rank_column_value)
        """
        # Filter only unique rows: data_rdd contains tuples (row, flag)
        # x[1] is the boolean flag, x[0] is the row
        unique_rows = data_rdd.filter(lambda x: x[1]).map(lambda x: x[0])

        # Create composite key from groupby_columns and rank_column
        def create_group_key(row):
            group_vals = tuple(
                getattr(row, col) for col in self.groupby_columns)
            rank_val = getattr(row, self.rank_column)
            return (group_vals, rank_val)

        # Group and count occurrences - reduceByKey sums up counts for
        # same (group, item) combinations
        group_item_counts = unique_rows \
            .map(lambda row: (create_group_key(row), 1)) \
            .reduceByKey(lambda a, b: a + b)  # type: ignore[arg-type]

        # Reorganize: (group_vals, (rank_val, count))
        grouped_items = group_item_counts \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .groupByKey()

        # Rank and select top X within each group
        return grouped_items.flatMap(self._rank_and_select_top)

    def _rank_and_select_top(self, group_data):
        """
        Rank items within a group and select top X.

        Args:
            group_data: Tuple of (group_values, items_iterator) where items
                are (item_value, count)

        Returns:
            List of tuples: (*group_values, rank, item_value) for top X
                items only
        """
        group_vals, items = group_data

        # Sort by count (descending) then by item value (ascending) for
        # tie-breaking
        sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))

        # Take top X items and assign ranks (1-based ranking)
        ranked_items = []
        for rank, (item_value, count) in enumerate(
                sorted_items[:self.top_x], 1):
            # Unpack group values and append rank and item
            ranked_items.append((*group_vals, rank, item_value))

        return ranked_items
