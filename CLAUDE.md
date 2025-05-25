# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains Enterprise Tech Insights Platform (ETIP) pipelines for automated security control monitoring. Each pipeline collects metrics from cloud resources, determines compliance status, and outputs standardized metrics data.

Each pipeline should have a corresponding test file.

ALWAYS USE THE "pl_automated_monitornig_cloud_custodian" pipeline and "test_pl_automated_monitornig_cloud_custodian.py" test file as reference for what a successful pipeline and test file should be.

## Pipeline Structure

Each pipeline follows a standard structure:

- `pipeline.py` - Main implementation inheriting from ConfigPipeline
- `config.yml` - ETL stage configuration (extract, transform, load)
- `avro_schema.json` - Schema definition for output data
- `sql/` - Directory containing SQL queries

When refactoring existing code into a pipeline, please ensure that the ETL functionality from the orignal script is carried over into the pipeline. The pipeline should still have the expected structure.

Thresholds should always be extracted from the "ETIP_DB.PHDP_ETIP_CONTROLS_MONITORING.ETIP_CONTROLS_MONITORING_METRICS_DETAILS" dataset in the extract phase of the .yml file.


## Architecture Patterns

### 1. Pipeline Class Structure

All pipelines follow this standard pattern:
```python
class PLAutomatedMonitoring[ControlName](ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        # Control-specific initialization
        
    def _get_api_connector(self) -> OauthApi:
        # Standard API connection setup
        
    def transform(self) -> None:
        # Set up API context and call parent transform
```

### 2. Transformer Functions

The core business logic is implemented in transformer functions:
```python
@transformer
def calculate_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    # Extract metric thresholds
    # Fetch resources from APIs
    # Calculate compliance metrics
    # Format output data
```

### 3. API Integration

Pipelines connect to APIs using OAuth authentication:
```python
api_token = refresh(
    client_id=self.env.exchange.client_id,
    client_secret=self.env.exchange.client_secret,
    exchange_url=self.env.exchange.exchange_url,
)
api_connector = OauthApi(
    url=self.api_url,
    api_token=f"Bearer {api_token}",
)
```

## Testing Guidelines

When writing or modifying tests:

1. Always use the `mock` parameter name (not `mocker`)
```python
def test_function(mock):
    mock_api = mock.patch("module.api_call")
```

2. Use freeze_time for timestamp-dependent tests
```python
with freeze_time("2024-11-05 12:09:00"):
    # Test code that generates timestamps
```

3. Initialize the context object in pipeline tests
```python
pipe.context = {}  # Initialize context
```

4. Properly mock API connectors
```python
mock_api = MockOauthApi(url="api_url", api_token="Bearer token")
mock_api.response = mock_response
```

5. Create utility functions for test data
```python
def _mock_threshold_df():
    # Return a DataFrame with test threshold data
```

## Common Pitfalls to Avoid

1. Case sensitivity in import paths (match filesystem exactly)
2. Missing context initialization in pipeline tests
3. DataFrame data type mismatches in test assertions
4. Missing thresholds data validation in transformer functions
5. Division by zero in metrics calculations
6. Hardcoded control IDs (use config file instead)
7. Timestamp handling inconsistencies in tests
8. Using pyspark instead of pandas

## Best Practices

1. Break down large functions into smaller, focused helpers
2. Validate inputs with descriptive error messages
3. Add robust error handling for API operations
4. Use appropriate log levels (info vs debug vs error)
5. Follow consistent naming patterns
6. Verify DataFrame types before returning from transformers
7. Document metrics calculations and compliance rules

## Environment Configuration Standards (Based on Zach's QA Implementation)

When setting up pipelines to be QA and PROD ready, follow these standards established by Zach:

### 1. Configuration Structure

Use environment-specific configurations in config.yml:

```yaml
pipeline:
  name: pl_automated_monitoring_[CONTROL_NAME]
  use_test_data_on_nonprod: false
  dq_strict_mode: false

environments:
  prod:
    extract:
      thresholds_raw:
        connector: snowflake
        options:
          sql: "@text:sql/monitoring_thresholds.sql"
    load:
      monitoring_metrics:
        - connector: onestream
          options:
            table_name: etip_controls_monitoring_metrics
            file_type: AVRO
            avro_schema: "@json:avro_schema.json"
            business_application: BAENTERPRISETECHINSIGHTS

  qa: &nonprod_config
    extract:
      thresholds_raw:
        connector: snowflake
        options:
          sql: "@text:sql/monitoring_thresholds_qa.sql"
    load:
      monitoring_metrics:
        - connector: onestream
          options:
            table_name: fact_controls_monitoring_metrics_daily_v4
            file_type: AVRO
            avro_schema: "@json:avro_schema.json"
            business_application: BAENTERPRISETECHINSIGHTS

  local: *nonprod_config
```

### 2. SQL File Environment Mapping

- **Production**: Use `sql/monitoring_thresholds.sql` with database `etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details`
- **QA**: Use `sql/monitoring_thresholds_qa.sql` with database `etip_db.qhdp_techcos.dim_controls_monitoring_metrics`

### 3. Table Name Standards

- **Production**: `etip_controls_monitoring_metrics`
- **QA**: `fact_controls_monitoring_metrics_daily_v4`

### 4. API Configuration

#### API URL Construction
```python
# Use environment-specific API URL
self.cloudradar_api_url = f"https://{self.env.exchange.exchange_url}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
```

#### API Headers
```python
headers = {
    "Accept": "application/json;v=1",  # Note: v=1 not v=1.0
    "Authorization": api_connector.api_token,
    "Content-Type": "application/json"
}
```

### 5. Standard Output Field Names

All environments use consistent field names:
- `control_monitoring_utc_timestamp` (datetime object)
- `control_id`
- `monitoring_metric_id`
- `monitoring_metric_value`
- `monitoring_metric_status`
- `metric_value_numerator`
- `metric_value_denominator`
- `resources_info`

### 6. Output Data Format

```python
# Use consistent field names across all environments
now = datetime.now()
results = [{
    "control_monitoring_utc_timestamp": now,
    "control_id": ctrl_id,
    "monitoring_metric_id": metric_id,
    "monitoring_metric_value": float(metric_value),
    "monitoring_metric_status": status,
    "metric_value_numerator": int(numerator),
    "metric_value_denominator": int(denominator),
    "resources_info": resources
}]
```

### 7. Data Quality Checks

Always include comprehensive data quality validation:

```yaml
ingress_validation:
  thresholds_raw:
    - type: schema_check
      fatal: false
      informational: true
      envs: [qa, prod]
      options:
        required_columns:
          - monitoring_metric_id
          - control_id
          - monitoring_metric_tier
          - alerting_threshold
    - type: count_check
      fatal: true
      envs: [qa, prod]
      options:
        data_location: Snowflake
        threshold: 1
    - type: consistency_check
      fatal: false
      informational: true
      envs: [qa, prod]
      options:
        check_type: "data_freshness"
        description: "Ingress data quality check"

egress_validation:
  monitoring_metrics:
    - type: schema_check
      fatal: false
      informational: true
      envs: [qa, prod]
    - type: count_check
      fatal: false
      informational: true
      envs: [qa, prod]
    - type: consistency_check
      fatal: false
      informational: true
      envs: [qa, prod]
      options:
        description: "Egress data will not match ingress data due to API enrichment"
```

### 8. Key Implementation Notes

1. **Set informational: true** for data quality checks where ingress data won't match egress data
2. **Use environment-specific SQL files** for different database schemas
3. **Use consistent field names** across all environments (QA and PROD)
4. **Use proper API header versioning** (v=1 not v=1.0)
5. **Use datetime.now()** for timestamp handling across all environments
6. **Include comprehensive error handling** for API failures
7. **Add negative response testing** in unit tests