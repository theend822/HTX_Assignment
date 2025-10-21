# HTX Assignment Documentation

Assignment goal: Create Spark RDD based transformation code to find top detected items by geo location

## 1. Folder Structure & Design Philosophy

```
HTX_Assignment/
├── lib/                    # Reusable components
│   ├── io/
│   │   └── data_io.py     # Read/write operations
│   ├── operators/
│   │   └── TopDetectedOperator.py  # Business logic
│   └── spark_session.py   # Spark session creation
├── pipelines/
│   ├── config/
│   │   └── config_top_detected_by_geo.py  # Pipeline config
│   └── pipelines/
│       ├── top_detected_by_geo.py         # Standard pipeline
│       └── top_detected_by_geo_skewed.py  # Skew-aware pipeline
└── tests/
    ├── functionality_test_TopDetectedOperator.py
    └── integration_test_top_detected_by_geo.py
```

**Design Pattern: READ DATA → PROCESS DATA → WRITE DATA**

- `lib/io/`: general READ and WRITE operations
- `lib/operators/`: commonly used PROCESS operations
- `pipelines/`: Individual pipeline created for specific use case
- `tests/`: general TEST capability

**Why this separation?**
- Reusability: lib components work across different pipelines
- Maintainability: Business logic separate from infrastructure
- Testability: Each layer can be tested independently

## 2. Design Ideas

### lib/ - Reusable Components

**spark_session.py**
- Purpuse: Creates Spark session from config
- Features: Generates unique app_name if not provided (timestamp + username)

**data_io.py**
- Purposes:
    - `read_data()`: Generic function to read parquet/csv/json
    - `write_data()`: Generic function to write parquet/csv/json
    - `get_schema()`: Config-driven schema generation
- Features: 
    - All functions accept config dict

**TopDetectedOperator.py**
- Purposes: Operator for all ranking calculations
- Features: 
    - Config-driven operator (no hardcoded column names)
    - Uses RDD operations only (map, filter, reduceByKey, groupByKey, flatMap)
    - Implements data quality check: flags duplicates instead of deleting

### pipelines/ - Specific Use Cases

**config_top_detected_by_geo.py**
- Single source of truth for pipeline configuration
- Contains: input paths, output paths, processing params, schemas

**top_detected_by_geo.py**
- Standard pipeline: read → calculate → join → write
- Uses broadcast variable (collectAsMap) for location lookup
- Avoids explicit .join() for performance (bonus requirement)
- Returns spark session instead of stopping it

**top_detected_by_geo_skewed.py**
- Extends standard pipeline with automatic skew detection
- Samples data to identify skewed keys (no hardcoded assumptions)
- Applies salting only to skewed partitions
- Non-skewed data processed normally

### tests/ - Quality Assurance

**integration_test_top_detected_by_geo.py**
- End-to-end pipeline test
- Creates dummy parquet files in temp directory
- Verifies pipeline runs without errors
- Tests config structure

**functionality_test_TopDetectedOperator.py**
- Unit tests for operator functionality
- Tests: standard calculation, deduplication, empty data handling
- Uses namedtuple to simulate RDD rows


## 7. To-Do
- Considering integrating data quality check in the process to meet industry best practices. It should, in general, follow `input data --> dq check --> specific data operation --> temp output data --> dq check --> output data`

## 8. Lessons Learnt

