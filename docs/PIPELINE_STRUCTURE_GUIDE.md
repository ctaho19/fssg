# ETIP Pipeline Structure Guide

This document outlines the standard structure, patterns, and best practices for creating and maintaining ETIP pipeline projects. Following these guidelines ensures consistency across pipelines and facilitates easier maintenance, testing, and onboarding.

## Directory Structure

A well-structured ETIP pipeline follows this standard directory structure:

```
pl_automated_monitoring_[CONTROL_ID]/
├── avro_schema.json           # Schema definition for data output
├── config.yml                 # Pipeline configuration file
├── pipeline.py                # Main pipeline implementation
└── sql/                       # SQL query directory
    └── monitoring_thresholds.sql  # Core threshold SQL query
    └── [additional queries].sql   # Additional functionality-specific queries
```

## Pipeline Components

### 1. `pipeline.py`

The main pipeline implementation file serves as the entry point and contains the pipeline class definition.

#### Structure and Organization

- **Imports**: Begin with standard library imports, followed by third-party packages, then local/project imports
- **Constants**: Define any constants at the module level
- **Utility Functions**: Define reusable utility functions outside the pipeline class
- **Pipeline Class**: Define the main pipeline class that inherits from `ConfigPipeline`
- **Transformer Functions**: Define transformer functions decorated with `@transformer`
- **Run Function**: Include a standard `run()` function as the entry point

#### Key Components

```python
# Standard import order
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

# Third-party imports
import pandas as pd
from datetime import datetime

# ETIP framework imports
from config_pipeline import ConfigPipeline
from etip_env import Env
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from transform_library import transformer

# Logger setup
logger = logging.getLogger(__name__)

# Utility functions
def get_compliance_status(...):
    """Reusable utility function..."""
    pass

# Pipeline class
class PLAutomatedMonitoring[ControlName](ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        # Pipeline-specific initialization
        
    def _get_api_connector(self) -> OauthApi:
        """Standard API connector method."""
        pass
        
    def transform(self) -> None:
        """Prepare transform stage with API context."""
        pass

# Transformer functions
@transformer
def calculate_metrics(...) -> pd.DataFrame:
    """Transform input data into metrics."""
    pass

# Entry point
def run(env: Env, is_export_test_data: bool = False, is_load: bool = True, dq_actions: bool = True):
    """Instantiate and run the pipeline."""
    pipeline = PLAutomatedMonitoring[ControlName](env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )
```

#### Best Practices

- **Class Naming**: Use `PLAutomatedMonitoring[ControlName]` pattern for class names
- **Error Handling**: Use try-except blocks for error-prone operations (API calls, token refreshes)
- **Function Size**: Break down large functions into smaller, focused helpers
- **Type Hints**: Always include type hints for parameters and return values
- **Docstrings**: Include docstrings for all classes and functions
- **Logging**: Use appropriate log levels (debug, info, warning, error)
- **API Connections**: Implement standard methods for API token refresh and connections
- **Context Variables**: Pass data between stages using the `context` dictionary

#### Common Pitfalls to Avoid

- **Hardcoded Values**: Avoid hardcoding IDs, thresholds, or configurations directly in the code
- **Excessive Logging**: Limit debug logging to necessary information
- **Large Functions**: Avoid monolithic functions that are difficult to test and maintain
- **Inconsistent Error Handling**: Standardize how errors are caught and reported
- **Missing Type Hints**: Always include type hints for better code readability and IDE support
- **Unused Code**: Remove unused imports, functions, and variables that create clutter
- **Dead Code**: Eliminate code paths that are never executed or functions never called

### 2. `config.yml`

The configuration file defines the pipeline's ETL stages and options.

#### Structure

```yaml
pipeline:
  name: pl_automated_monitoring_[CONTROL_ID]
  use_test_data_on_nonprod: false
  dq_strict_mode: false

stages:
  extract:
    # Data sources to extract
    thresholds_raw:
      connector: snowflake
      options:
        sql: "@text:sql/monitoring_thresholds.sql"
    # Additional extracts as needed

  ingress_validation:
    # Validation rules for each extracted dataset
    thresholds_raw:
      - type: count_check
        fatal: true
        envs:
          - qa
          - prod
        options:
          data_location: Snowflake
          threshold: 1
          table_name: [TABLE_NAME]

  transform:
    # Transformation stage definitions
    monitoring_metrics:
      - function: custom/calculate_metrics
        options:
          thresholds_raw: $thresholds_raw
          # Additional parameters as needed
          control_ids:
            - "CTRL-XXXXXXX"

  load:
    # Load stage definitions
    monitoring_metrics:
      - connector: onestream
        options:
          table_name: etip_controls_monitoring_metrics
          file_type: AVRO
          avro_schema: "@json:avro_schema.json"
          business_application: BAENTERPRISETECHINSIGHTS

  prepare_test_data:
    # Test data preparation (optional)
    thresholds_raw:
      - function: head
        options:
          n: 500000

  export_test_data:
    # Test data export definitions (optional)
    thresholds_raw:
```

#### Best Practices

- **Parameter References**: Use `$variable_name` syntax to reference extracted data
- **External Files**: Use `@text:` and `@json:` syntax to reference external files
- **Control IDs**: Define control IDs in the config rather than hardcoding in the pipeline
- **Consistent Names**: Use consistent naming for datasets across all stages
- **Validation Settings**: Include appropriate ingress validation for all data sources
- **Environment Settings**: Specify environments (qa, prod) for validation rules

#### Common Pitfalls to Avoid

- **Inconsistent Names**: Ensure dataset names are consistent across all stages
- **Missing Validations**: Each extract should have corresponding validation rules
- **Hardcoded Parameters**: Avoid hardcoding parameters that should be configurable
- **Incomplete Test Data**: Ensure test data stage includes all necessary datasets

### 3. `avro_schema.json`

The Avro schema defines the structure of the output data.

#### Structure

```json
{
  "type": "record",
  "name": "etip_controls_monitoring_metrics",
  "fields": [
    {"name": "date", "type": "long"},
    {"name": "control_id", "type": "string"},
    {"name": "monitoring_metric_id", "type": "int"},
    {"name": "monitoring_metric_value", "type": "double"},
    {"name": "compliance_status", "type": "string"},
    {"name": "numerator", "type": "int"},
    {"name": "denominator", "type": "int"},
    {"name": "non_compliant_resources", "type": ["null", {"type": "array", "items": "string"}]}
  ]
}
```

#### Best Practices

- **Field Types**: Use appropriate Avro types for each field
- **Nullable Fields**: Use union types `["null", "type"]` for nullable fields
- **Field Naming**: Use snake_case for field names
- **Consistent Structure**: Maintain a consistent field structure across similar pipelines
- **Documentation**: Include comments for fields with complex meanings or requirements

#### Common Pitfalls to Avoid

- **Type Mismatches**: Ensure field types match the data that will be generated
- **Missing Required Fields**: Include all fields required by downstream systems
- **Inconsistent Naming**: Maintain consistent naming with related systems

### 4. SQL Directory (`sql/`)

Contains SQL queries used by the pipeline for data extraction.

#### Key Files

- **monitoring_thresholds.sql**: Core query to retrieve threshold configuration
- **Additional SQL files**: Specific to pipeline functionality (e.g., IAM roles, SLA data)

#### Structure for `monitoring_thresholds.sql`

```sql
SELECT
  monitoring_metric_id,
  control_id,
  monitoring_metric_tier,
  alerting_threshold,
  warning_threshold,
  metric_threshold_start_date
FROM
  CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
WHERE
  METRIC_ACTIVE_STATUS = 'Y'
  -- Additional filters as needed
ORDER BY
  monitoring_metric_id
```

#### Best Practices

- **Comments**: Include comments to explain complex query logic
- **Formatting**: Use consistent indentation and capitalization for SQL keywords
- **Filtering**: Include appropriate WHERE clauses to limit data returned
- **Column Selection**: Only select columns needed by the pipeline
- **Table Aliases**: Use meaningful table aliases for complex joins
- **Performance**: Consider query performance, especially for large tables

#### Common Pitfalls to Avoid

- **SELECT ***: Avoid selecting all columns when only specific ones are needed
- **Missing Filters**: Ensure appropriate filters to limit data volume
- **Complex Logic**: Break down complex queries into manageable parts
- **Hardcoded Values**: Avoid hardcoding values that may change

## Testing Guidelines

### Test File Naming and Location

Test files should be placed in a `tests/pipelines/` directory and follow a naming pattern:

```
tests/pipelines/test_pl_automated_monitoring_[CONTROL_ID].py
```

### Test Structure

```python
import pytest
from unittest.mock import patch, MagicMock

from pl_automated_monitoring_[CONTROL_ID].pipeline import (
    PLAutomatedMonitoring[ControlName],
    calculate_metrics,
)

class TestPLAutomatedMonitoring[ControlName]:
    def test_pipeline_initialization(self):
        # Test pipeline initialization
        pass
        
    def test_api_connector(self):
        # Test API connector methods
        pass
        
    # Additional test methods

def test_calculate_metrics():
    # Test transformer functions
    pass
```

### Best Practices for Testing

- **Mock External Systems**: Use mocks for API calls, database connections
- **Test Each Component**: Create separate tests for each pipeline component
- **Parametrize Tests**: Use pytest's parametrize for testing multiple scenarios
- **Fixture Usage**: Create fixtures for common test data
- **Coverage**: Aim for high test coverage, especially for business logic
- **Edge Cases**: Test edge cases (empty data, error conditions)
- **Utilize All Fixtures/Mocks**: Ensure all defined test fixtures and mock functions are used in tests
- **Remove Unused Mocks**: Delete any unused mock functions instead of leaving them in the codebase

## Onboarding New Controls

When creating a new pipeline for a control, follow these steps:

1. **Copy Reference Pipeline**: Start with a well-structured reference pipeline
2. **Update Control IDs**: Replace control IDs in all files
3. **Update Pipeline Class**: Rename and adjust the pipeline class
4. **Modify Transformer Logic**: Adjust transformer functions for the specific control
5. **Update SQL Queries**: Modify SQL queries as needed
6. **Create Tests**: Create appropriate test files
7. **Documentation**: Update documentation to reflect the new control

## Integration with ETIP Framework

The ETIP framework provides a standardized approach to ETL processes:

- **ConfigPipeline**: Base class that handles pipeline stages
- **Extract Stage**: Retrieves data from various sources
- **Transform Stage**: Processes and transforms extracted data
- **Load Stage**: Loads processed data to target systems
- **Validation**: Performs data validation at various stages
- **Testing**: Supports test data generation and export

## Common Dependencies

- **pandas**: For data manipulation and processing
- **connectors.api**: For API connections
- **connectors.exchange.oauth_token**: For token authentication
- **transform_library**: For transformer decorators
- **etip_env**: For environment configuration

## Additional Resources

- ETIP Framework Documentation
- Snowflake Query Guidelines
- Onestream Connector Documentation
- AVRO Schema Reference
