# HTX Data Processing - PySpark Top Detected Items Calculator

A PySpark application for calculating the top X detected items by video cameras across different geographical locations in the Town of Utopia.

## Core Design Principle

This project follows a **simple, fundamental data engineering principle**:

### **READ → DO SOMETHING → WRITE**

Every data processing task can be broken down into these three essential steps:

1. **READ**: Load data from sources (files, databases, APIs)
2. **DO SOMETHING**: Transform, aggregate, analyze, or process the data
3. **WRITE**: Save results to destinations (files, databases, dashboards)

This principle drives the entire architecture and folder structure.

## Folder Structure & Design Philosophy

```
HTX_Assignment/
├── lib/                              # GENERAL/REUSABLE CODE
│   ├── io/                          # READ & WRITE operations (reusable)
│   │   └── data_io.py               # General I/O functions
│   └── operators/                   # DO SOMETHING operations (business logic)
│       └── top_detected_operator.py # Top detected items calculation logic
├── pipelines/                       # SPECIFIC IMPLEMENTATIONS
│   ├── config/                      # Configuration for specific pipelines
│   │   └── pipeline_configs.py     # Pipeline-specific configurations
│   └── pipelines/                   # Executable pipeline scripts
│       ├── top_detected_by_geo.py       # Standard top detected items pipeline
│       └── top_detected_by_geo_skewed.py # Skew-aware pipeline
├── tests/                           # Testing all components
│   ├── test_top_detected_operator.py   # Operator unit tests
│   └── test_pipeline_integration.py    # End-to-end pipeline tests
├── results/                         # Output storage
└── venv/                           # Virtual environment
```

### Design Philosophy Explained

**`lib/` = General, Reusable Components**
- **`io/`**: Universal READ/WRITE functions that work with any data format
- **`operators/`**: Business logic for "DO SOMETHING" - these are reusable data transformation operations

**`pipelines/` = Specific Implementations**
- **`config/`**: Configuration specific to particular use cases
- **`pipelines/`**: Complete executable scripts that combine READ → DO SOMETHING → WRITE

**Why This Separation?**
- **Reusability**: `lib/` components can be used across different projects and use cases
- **Modularity**: Each pipeline is self-contained and independently executable
- **Maintainability**: Business logic is separated from infrastructure concerns
- **Scalability**: Easy to add new operators or pipelines without touching existing code

## Key Features

- ✅ **Clean Architecture**: Clear separation between reusable components and specific implementations
- ✅ **RDD-Only Processing**: Core logic uses only Spark RDD operations (no DataFrames in business logic)
- ✅ **Join-Free Implementation**: Avoids explicit `.join()` calls using broadcast lookups
- ✅ **Duplicate Handling**: Properly deduplicates detection events by `detection_oid`
- ✅ **Configuration-Driven**: Uses dictionaries instead of long parameter lists
- ✅ **Multiple Output Formats**: Supports Parquet, CSV, and JSON output
- ✅ **Data Skew Handling**: Separate pipeline for handling known geographical data skew
- ✅ **Comprehensive Testing**: Unit tests, integration tests, and performance validation
- ✅ **Code Quality**: Clean, well-documented code following best practices

## Installation

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (for Spark)

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd HTX_Assignment
```

2. **Create virtual environment and install dependencies**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

3. **Verify installation**
```bash
# Run tests
python -m pytest tests/ -v

# Check code quality
python -m flake8 --exclude=venv .
```

## Usage

### Pipeline Architecture: READ → DO SOMETHING → WRITE

Each pipeline follows this exact pattern:

1. **READ**: Load detection and location data from parquet files
2. **DO SOMETHING**: Calculate top detected items using RDD operations
3. **WRITE**: Save ranked results to output destination

### Running Pipelines

#### Standard Top Detected Items Pipeline

```bash
# Using default configuration
python pipelines/pipelines/top_detected_by_geo.py

# With custom parameters
python pipelines/pipelines/top_detected_by_geo.py \
    --detection-data-path data/detection_data.parquet \
    --location-data-path data/location_data.parquet \
    --output-path output/results.parquet \
    --top-x 10 \
    --output-format parquet
```

#### Skew-Aware Pipeline

For datasets with known geographical data skew:

```bash
python pipelines/pipelines/top_detected_by_geo_skewed.py \
    --detection-data-path data/skewed_detection_data.parquet \
    --location-data-path data/location_data.parquet \
    --output-path output/skewed_results.parquet \
    --top-x 10 \
    --skewed-location-id 1 \
    --salt-factor 10
```

### Configuration-Driven Approach

Each pipeline uses configuration dictionaries instead of long argument lists:

```python
from pipelines.config.pipeline_configs import get_top_detected_config
from pipelines.pipelines.top_detected_by_geo import run_pipeline

# Load and customize configuration
config_overrides = {
    'detection_data_path': 'my_data/detections.parquet',
    'output_path': 'my_output/results.parquet',
    'top_x': 5
}

# Run pipeline
run_pipeline(config_overrides)
```

### Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `--detection-data-path` | Path to detection data parquet file | Yes | - |
| `--location-data-path` | Path to location data parquet file | Yes | - |
| `--output-path` | Output path for results | Yes | - |
| `--top-x` | Number of top items per location | Yes | - |
| `--output-format` | Output format (parquet/csv/json) | No | parquet |

## Data Schemas

### Input Data Schema

#### Detection Data (Dataset A)
| Column | Type | Description |
|--------|------|-------------|
| geographical_location_oid | bigint | Unique identifier for geographical location |
| video_camera_oid | bigint | Unique identifier for video camera |
| detection_oid | bigint | Unique identifier for detection event |
| item_name | varchar(5000) | Detected item name |
| timestamp_detected | bigint | Detection timestamp |

#### Location Data (Dataset B)
| Column | Type | Description |
|--------|------|-------------|
| geographical_location_oid | bigint | Unique identifier for geographical location |
| geographical_location | varchar(500) | Location name |

### Output Data Schema

| Column | Type | Description |
|--------|------|-------------|
| geographical_location_oid | bigint | Unique identifier for geographical location |
| item_rank | int | Rank of item (1 = most popular) |
| item_name | varchar(5000) | Item name |

## Architecture Deep Dive

### Why READ → DO SOMETHING → WRITE?

This pattern provides several critical benefits:

1. **Clarity**: Every data engineer immediately understands the flow
2. **Debugging**: Easy to isolate issues to specific stages
3. **Testing**: Each stage can be tested independently
4. **Reusability**: READ/WRITE operations can be shared across projects
5. **Scalability**: Business logic (DO SOMETHING) can evolve independently

### Core Processing Logic (DO SOMETHING)

The `TopDetectedOperator` implements a **join-free approach**:

1. **Deduplication**: Remove duplicate `detection_oid` entries using `reduceByKey`
2. **Broadcast Lookup**: Create broadcast variable for location mapping (avoids expensive joins)
3. **Aggregation**: Count item occurrences per geographical location using `reduceByKey`
4. **Ranking**: Sort and rank items within each location using `groupByKey` + `flatMap`

### Key Optimizations

- **Broadcast Join Alternative**: Uses broadcast variables instead of `.join()` operations
- **Efficient Shuffling**: Minimizes data shuffling through strategic key-based operations
- **Memory Management**: Configurable Spark session with adaptive query execution
- **Partitioning**: Leverages natural partitioning by geographical location

### Data Skew Handling Philosophy

**Important**: Skew handling is NOT built into general components. Why?

- **Separation of Concerns**: General operators should be simple and focused
- **Special Cases**: Data skew is a special problem requiring special solutions
- **Maintainability**: Keeping skew logic separate prevents general code complexity
- **Different Approaches**: Each skew situation may require different strategies

The skewed pipeline contains its own skew-handling logic using salting techniques.

## Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test files
python -m pytest tests/test_top_detected_operator.py -v    # Operator unit tests
python -m pytest tests/test_pipeline_integration.py -v    # Pipeline integration tests
```

### Test Coverage

- **Unit Tests**: Individual component testing (data I/O, operator functions)
- **Integration Tests**: End-to-end workflow testing with realistic datasets
- **Edge Cases**: Empty data, duplicates, single location scenarios

## Code Quality

### Linting

```bash
python -m flake8 --exclude=venv .
```

### Clean Code Practices

- **Modular Design**: Clear separation of concerns following READ → DO SOMETHING → WRITE
- **Type Hints**: Function parameters and return types documented
- **Docstrings**: Comprehensive documentation for all functions
- **Error Handling**: Proper exception handling and validation
- **Naming Conventions**: Clear, descriptive variable and function names

## Development Guidelines

### Adding New Operators

Follow the READ → DO SOMETHING → WRITE pattern:

1. **Place business logic in `lib/operators/`** (reusable)
2. **Create pipeline in `pipelines/pipelines/`** (specific implementation)
3. **Add configuration in `pipelines/config/`** (pipeline-specific settings)

### Design Principles

1. **Keep it Simple**: Every data operation is READ → DO SOMETHING → WRITE
2. **Separate Concerns**: General components in `lib/`, specific implementations in `pipelines/`
3. **Configuration-Driven**: Use dictionaries, not hardcoded parameters
4. **Join-Free**: Use broadcast variables instead of expensive joins when possible
5. **Test Everything**: Unit tests for operators, integration tests for pipelines

## Performance Considerations

### Spark Configuration

```python
spark_config = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true', 
    'spark.sql.adaptive.skewJoin.enabled': 'true'
}
```

### Optimization Strategies

1. **Broadcast Variables**: Eliminates expensive shuffle operations for lookups
2. **Adaptive Query Execution**: Enabled for automatic optimization
3. **Memory Management**: Efficient RDD operations and caching strategies

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**: Increase driver/executor memory
2. **File Not Found**: Ensure input paths are accessible
3. **Schema Mismatch**: Verify input data schema matches expected format

### Debugging

Enable detailed logging:
```python
spark.sparkContext.setLogLevel("DEBUG")
```

## Assumptions Made

1. **Data Quality**: Input parquet files are well-formed and readable
2. **Memory**: Available memory sufficient for broadcast variables (location lookup)
3. **Partitioning**: Input data has reasonable partitioning for processing
4. **Tie-Breaking**: Items with same count are ranked alphabetically by item name
5. **Empty Locations**: Locations with no detections are excluded from output
6. **Duplicate Handling**: Only the first occurrence of duplicate `detection_oid` is kept

## License

This project is created for the HTX xData Technical Test (2025).

## Author

Created for HTX xData Technical Test - Data Engineer Position (2025)