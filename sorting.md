==============================================
SPARK SORTING STRATEGIES
==============================================

1. SORT MERGE JOIN
-----------------
Overview:
- Default join strategy for large-large table joins
- Both datasets are sorted by join keys, then merged
- Requires shuffle operations for both sides

Characteristics:
+ Works well for large-large joins
+ Predictable performance
+ Memory efficient for very large datasets
- High shuffle cost
- Slower for small-large joins


2. BROADCAST HASH JOIN
---------------------
Overview:
- Small dataset is broadcast to all executors
- Large dataset is processed locally with broadcasted hash table
- No shuffle required for large dataset

Characteristics:
+ Fastest for small-large joins
+ Eliminates shuffle for large dataset
+ Excellent for reference table joins
- Limited by broadcast size threshold (default 10MB)
- Driver memory pressure for large broadcasts


3. SHUFFLE HASH JOIN
-------------------
Overview:
- Data is shuffled by join keys
- Hash table built on smaller side per partition
- No sorting required

Characteristics:
+ Faster than sort merge for medium datasets
+ No sorting overhead
- Still requires shuffle operations
- Memory intensive for hash table construction


4. CARTESIAN JOIN
----------------
Overview:
- Cross product of all rows
- No join condition optimization
- Should be avoided in most cases

Characteristics:
- Extremely expensive
- Only for specific analytical needs
- Not applicable to our use case


==============================================
RECOMMENDED STRATEGY FOR OUR USE CASE: BROADCAST HASH JOIN
==============================================

Rationale:
1. Dataset B (locations) is small (10K rows ~1-5MB)
2. Dataset A (detections) is large (1M rows ~100-500MB)
3. Reference table join pattern (locations rarely change)
4. Performance critical for real-time processing requirements


OUR RDD IMPLEMENTATION APPROACH
================================

In our actual implementation, we use RDD-based map-side join instead of DataFrame operations:

```python
# Step 1: Collect small location data to driver
location_lookup = location_rdd.map(
    lambda row: (row.geographical_location_oid, row.geographical_location)
).collectAsMap()

# Step 2: Map-side join (equivalent to broadcast hash join)
result_with_location = result_rdd.map(
    lambda row: (
        row[0],  # geographical_location_oid
        location_lookup.get(row[0], 'Unknown'),  # Lookup in dictionary
        row[1],  # item_rank
        row[2]   # item_name
    )
)
```

Why This Works (Broadcast Pattern):
------------------------------------
1. **collectAsMap()** creates a Python dict on the driver
2. The dict is automatically **broadcast** when used in the lambda function
3. Each executor gets a copy of the dictionary in memory
4. Lookup happens **locally** on each partition (no shuffle!)
5. Equivalent to DataFrame broadcast join, but with explicit control
