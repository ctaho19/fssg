# CLAUDE.md - ETIP Pipeline Development Standards

This document provides comprehensive guidance for developing Enterprise Tech Insights Platform (ETIP) pipelines following Zach's established production-ready architectural patterns and coding standards.

## Repository Overview

This repository contains ETIP pipelines for automated security control monitoring. Each pipeline collects metrics from various data sources (APIs, databases), determines compliance status, and outputs standardized metrics data to support enterprise security monitoring and reporting.

**Reference Implementation**: Always use `pl_automated_monitoring_cloud_custodian` pipeline and `pl_automated_monitoring_cloudradar_controls` pipeline as the gold standard for production-ready pipeline architecture and testing patterns.

## Production-Ready Pipeline Architecture

All new ETIP pipelines must follow the **Extract Override Pattern** which represents Zach's production-ready architecture for API-integrated pipelines.

### Production Architecture Pattern

```python
from typing import Dict, List
import pandas as pd
from datetime import datetime
import json
import ssl
from pathlib import Path
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env

class PLAutomatedMonitoring[ControlName](ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        # Initialize control-specific variables
        self.api_url = f"https://{self.env.exchange.exchange_url}/[api-endpoint-path]"
        
    def _get_api_connector(self) -> OauthApi:
        """Standard OAuth API connector setup following Zach's pattern"""
        api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        
        # Create SSL context if certificate file exists
        ssl_context = None
        if C1_CERT_FILE:
            ssl_context = ssl.create_default_context(cafile=C1_CERT_FILE)
        
        return OauthApi(
            url=self.api_url,
            api_token=f"Bearer {api_token}",
            ssl_context=ssl_context
        )
    
    def extract(self) -> pd.DataFrame:
        """Override extract to integrate API data with SQL data"""
        # First get data from configured SQL sources
        df = super().extract()
        
        # Then enrich with API data using class methods
        df["monitoring_metrics"] = self._calculate_metrics(df["thresholds_raw"])
        return df
    
    def _calculate_metrics(self, thresholds_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Core business logic for calculating metrics
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            
        Returns:
            DataFrame with standardized output schema
        """
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
        
        # Step 2: Create API connector
        api_connector = self._get_api_connector()
        
        # Step 3: API Data Collection with Pagination
        all_resources = []
        next_record_key = None
        
        while True:
            try:
                headers = {
                    "Accept": "application/json;v=1",  # Note: v=1 not v=1.0
                    "Authorization": api_connector.api_token,
                    "Content-Type": "application/json"
                }
                
                payload = {
                    # API-specific parameters
                }
                if next_record_key:
                    payload["nextRecordKey"] = next_record_key
                
                response = api_connector.send_request(
                    url="",  # Empty when full URL is in api_connector.url
                    request_type="post",
                    request_kwargs={
                        "headers": headers,
                        "json": payload,
                        "verify": C1_CERT_FILE,
                        "timeout": 120,
                    },
                    retry_delay=5,
                    retry_count=3
                )
                
                if response.status_code != 200:
                    raise RuntimeError(f"API request failed: {response.status_code} - {response.text}")
                
                data = response.json()
                all_resources.extend(data.get("resources", []))
                
                # Handle pagination
                next_record_key = data.get("nextRecordKey")
                if not next_record_key:
                    break
                    
            except Exception as e:
                raise RuntimeError(f"Failed to fetch resources from API: {str(e)}")
        
        # Step 4: Compliance Calculation
        results = []
        now = datetime.now()
        
        for _, threshold in thresholds_raw.iterrows():
            ctrl_id = threshold["control_id"]
            metric_id = threshold["monitoring_metric_id"]
            
            # Filter and analyze resources based on control requirements
            # Calculate compliance metrics
            # Determine compliance status
            
            # Step 5: Format Output with Standard Fields
            result = {
                "control_monitoring_utc_timestamp": now,
                "control_id": ctrl_id,
                "monitoring_metric_id": metric_id,
                "monitoring_metric_value": float(compliance_percentage),
                "monitoring_metric_status": compliance_status,
                "metric_value_numerator": int(compliant_count),
                "metric_value_denominator": int(total_count),
                "resources_info": [json.dumps(resource) for resource in non_compliant_resources] if non_compliant_resources else None
            }
            results.append(result)
        
        result_df = pd.DataFrame(results)
        
        # Ensure correct data types to match test expectations
        if not result_df.empty:
            result_df = result_df.astype({
                "metric_value_numerator": "int64",
                "metric_value_denominator": "int64",
                "monitoring_metric_value": "float64"
            })
        
        return result_df

def run(env: Env, is_load: bool = True, dq_actions: bool = True):
    """Run the pipeline."""
    pipeline = PLAutomatedMonitoring[ControlName](env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return pipeline.run(load=is_load, dq_actions=dq_actions)

if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception:
        import sys
        sys.exit(1)
```

## Pipeline Development Process

### Step 1: Project Structure Setup

Every pipeline must follow this exact directory structure:

#### QA-Enabled Pipelines (when data source has QA alternative)
```
pl_automated_monitoring_[CONTROL_ID]/
├── pipeline.py           # Main pipeline implementation
├── config.yml           # Environment-specific ETL configuration
├── avro_schema.json     # Production data schema definition
├── avro_schema_qa.json  # QA data schema definition
└── sql/                 # SQL queries directory
    ├── monitoring_thresholds.sql      # Production queries
    └── monitoring_thresholds_qa.sql   # QA environment queries
```

#### Local-Only Pipelines (when data source lacks QA alternative)
```
pl_automated_monitoring_[CONTROL_ID]/
├── pipeline.py           # Main pipeline implementation
├── config.yml           # Production and local-only configuration
├── avro_schema.json     # Production data schema definition
└── sql/                 # SQL queries directory
    └── monitoring_thresholds.sql      # Production queries only
```

**Naming Convention**: Use `pl_automated_monitoring_[CONTROL_ID]` format where `CONTROL_ID` matches the control being monitored (e.g., `CTRL_1077231`).

**QA Environment Determination**: Some pipelines cannot be tested in QA if their data sources do not have QA alternatives. These pipelines will be tested locally only and do not require QA implementation files (`monitoring_thresholds_qa.sql`, `avro_schema_qa.json`) or QA environment configuration.

### Step 2: Configuration Setup (`config.yml`)

#### QA-Enabled Pipeline Configuration Structure

```yaml
pipeline:
  name: pl_automated_monitoring_[CONTROL_ID]
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
            avro_schema: "@json:avro_schema_qa.json"
            business_application: BAENTERPRISETECHINSIGHTS

  local: *nonprod_config

stages:
  - name: ingress_validation
    description: "Validate data quality of input datasets"
    envs: [qa, prod, local]
  - name: extract
    description: "Extract data from Snowflake and external APIs"
    envs: [qa, prod, local]
  - name: transform
    description: "Calculate control metrics"
    envs: [qa, prod, local]
  - name: egress_validation
    description: "Validate transformed metrics before loading"
    envs: [qa, prod, local]
  - name: load
    description: "Load metrics to OneStream"
    envs: [qa, prod, local]

# REQUIRED: Comprehensive Data Quality Validation
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
          - metric_name
          - metric_description
          - warning_threshold
          - alerting_threshold
          - control_executor
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
      options:
        required_columns:
          - control_monitoring_utc_timestamp
          - control_id
          - monitoring_metric_id
          - monitoring_metric_value
          - monitoring_metric_status
          - metric_value_numerator
          - metric_value_denominator
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

#### Local-Only Pipeline Configuration Structure

For pipelines without QA data source alternatives:

```yaml
pipeline:
  name: pl_automated_monitoring_[CONTROL_ID]
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

  local:
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

stages:
  - name: ingress_validation
    description: "Validate data quality of input datasets"
    envs: [local, prod]
  - name: extract
    description: "Extract data from Snowflake and external APIs"
    envs: [local, prod]
  - name: transform
    description: "Calculate control metrics"
    envs: [local, prod]
  - name: egress_validation
    description: "Validate transformed metrics before loading"
    envs: [local, prod]
  - name: load
    description: "Load metrics to OneStream"
    envs: [local, prod]

# Include similar ingress_validation and egress_validation sections
# with envs: [local, prod] instead of [qa, prod]
```

### Step 3: SQL Query Implementation

#### Production SQL (`sql/monitoring_thresholds.sql`)

**Single-Control Pipelines:**
```sql
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold,
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details
where 
    metric_threshold_end_date is null
    and control_id = 'CTRL-1077231'
```

**Multi-Control Pipelines:**
```sql
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold,
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details
where 
    metric_threshold_end_date is null
    and control_id in ('CTRL-1077224', 'CTRL-1077231', 'CTRL-1077125')
```

#### QA SQL (`sql/monitoring_thresholds_qa.sql`)

```sql
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold,
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.qhdp_techcos.dim_controls_monitoring_metrics
where 
    metric_threshold_end_date is null
    and control_id = 'CTRL-1077231'  -- or control_id in (...) for multi-control
```

### Step 4: Unit Test Implementation

#### Test File Structure (`tests/pipelines/test_pl_automated_monitoring_[CONTROL_ID].py`)

```python
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components - ALWAYS use pipelines. prefix
from pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline import (
    PLAutomatedMonitoring[ControlName],
    run
)

# Standard test constants
AVRO_SCHEMA_FIELDS = [
    "control_monitoring_utc_timestamp",
    "control_id", 
    "monitoring_metric_id",
    "monitoring_metric_value",
    "monitoring_metric_status",
    "metric_value_numerator",
    "metric_value_denominator",
    "resources_info"
]

class MockExchangeConfig:
    def __init__(self):
        self.client_id = "test_client"
        self.client_secret = "test_secret"
        self.exchange_url = "test-exchange.com"

class MockEnv:
    def __init__(self):
        self.exchange = MockExchangeConfig()
        self.env = self  # Add self-reference for pipeline framework compatibility

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1077231",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 97.0,
        "alerting_threshold": 95.0
    }])

def generate_mock_api_response(content=None, status_code=200):
    """Generate standardized mock API response."""
    import json
    
    mock_response = Response()
    mock_response.status_code = status_code
    
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        mock_response._content = json.dumps({}).encode("utf-8")
    
    return mock_response

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_success():
    """Test successful metrics calculation using extract method"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    # Setup test data
    thresholds_df = _mock_threshold_df()
    
    # Mock API response
    with patch('pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline.OauthApi') as mock_oauth:
        mock_api_instance = Mock()
        mock_oauth.return_value = mock_api_instance
        
        mock_response = generate_mock_api_response({
            "resources": [{"id": "resource1", "compliant": True}],
            "nextRecordKey": None
        })
        mock_api_instance.send_request.return_value = mock_response
        
        # Call _calculate_metrics directly on pipeline instance
        result = pipeline._calculate_metrics(thresholds_df)
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert list(result.columns) == AVRO_SCHEMA_FIELDS
        
        # Verify data types using pandas type checking
        assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
        assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
        assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    empty_df = pd.DataFrame()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df)

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url

def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_threshold_df()
    mock_df = pd.DataFrame({"thresholds_raw": [mock_thresholds]})
    
    with patch.object(PLAutomatedMonitoring[ControlName], '__bases__', (Mock,)):
        with patch('pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline.ConfigPipeline.extract') as mock_super:
            mock_super.return_value = mock_df
            
            # Mock API calls to avoid actual network requests
            with patch('pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline.OauthApi') as mock_oauth:
                mock_api_instance = Mock()
                mock_oauth.return_value = mock_api_instance
                mock_api_instance.send_request.return_value = generate_mock_api_response({
                    "resources": [{"id": "resource1", "compliant": True}],
                    "nextRecordKey": None
                })
                
                result = pipeline.extract()
                
                # Verify super().extract() was called
                mock_super.assert_called_once()
                
                # Verify the result has monitoring_metrics column
                assert "monitoring_metrics" in result.columns
                
                # Verify that the monitoring_metrics contains a DataFrame
                metrics_df = result["monitoring_metrics"].iloc[0]
                assert isinstance(metrics_df, pd.DataFrame)
                
                # Verify the DataFrame has the correct schema
                if not metrics_df.empty:
                    assert list(metrics_df.columns) == AVRO_SCHEMA_FIELDS

def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    with patch('pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline.OauthApi') as mock_oauth:
        mock_api_instance = Mock()
        mock_oauth.return_value = mock_api_instance
        mock_api_instance.send_request.side_effect = RequestException("Connection error")
        
        with pytest.raises(RuntimeError, match="Failed to fetch"):
            pipeline._calculate_metrics(_mock_threshold_df())

def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_[CONTROL_ID].pipeline.PLAutomatedMonitoring[ControlName]') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = "test_result"
        
        result = run(env, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run.assert_called_once_with(load=False, dq_actions=True)
        assert result == "test_result"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

## Advanced Patterns for Complex Pipelines

### Multi-Control Pipeline Pattern

For pipelines handling multiple related controls, use a consolidated approach with control configuration mapping:

```python
# Control configuration at module level
CONTROL_CONFIGS = {
    "CTRL-1077224": {
        "resource_type": "AWS::KMS::Key",
        "config_key": "supplementaryConfiguration.KeyRotationStatus",
        "config_location": "supplementaryConfiguration",
        "expected_value": "TRUE",
        "apply_kms_exclusions": True,
        "description": "KMS Key Rotation Status"
    },
    "CTRL-1077231": {
        "resource_type": "AWS::EC2::Instance",
        "config_key": "configuration.metadataOptions.httpTokens",
        "config_location": "configurationList",
        "expected_value": "required",
        "apply_kms_exclusions": False,
        "description": "EC2 Instance Metadata Service v2"
    }
}

class PLAutomatedMonitoringMultiControl(ConfigPipeline):
    def _calculate_metrics(self, thresholds_raw: pd.DataFrame) -> pd.DataFrame:
        # Group by control_id for efficient processing
        control_groups = thresholds_raw.groupby('control_id')
        required_controls = list(control_groups.groups.keys())
        
        # Fetch resources once per resource type
        all_resources = self._fetch_resources_by_type(required_controls)
        
        # Process each control
        all_results = []
        for control_id, control_thresholds in control_groups:
            if control_id not in CONTROL_CONFIGS:
                continue
            
            control_config = CONTROL_CONFIGS[control_id]
            resources = all_resources.get(control_config["resource_type"], [])
            control_results = self._calculate_control_compliance(
                control_id, control_thresholds, resources, control_config
            )
            all_results.extend(control_results)
        
        return pd.DataFrame(all_results)
```

## Production Standards & Requirements

### API Integration Standards

1. **OAuth Authentication**: Always use the standardized `refresh()` function with client credentials
2. **Bearer Token Format**: Format as `f"Bearer {api_token}"`
3. **Environment-Aware URLs**: Construct URLs using `self.env.exchange.exchange_url`
4. **Standard Headers**: Use `"application/json;v=1"` (not `v=1.0`)
5. **SSL Certificates**: Implement `C1_CERT_FILE` for secure connections
6. **Pagination Handling**: Process `nextRecordKey` for large result sets
7. **Error Wrapping**: Wrap API exceptions in `RuntimeError` with descriptive context
8. **Use send_request method**: Always use the `send_request` method from OauthApi for proper error handling and retries
9. **Empty URL for relative paths**: When the full URL is in the connector, pass empty string to send_request
10. **Proper parameter naming**: Use `retry_count` (not `max_retries`) for consistency
11. **Certificate handling**: Always include SSL certificate verification when available

### Data Processing Standards

1. **Input Validation**: Always validate thresholds data is not empty
2. **Pandas Usage**: Use pandas DataFrames (not pyspark) for data manipulation
3. **Type Casting**: Explicit type conversion for numeric fields (`int()`, `float()`)
4. **Timestamp Handling**: Use `datetime.now()` for consistent timestamping
5. **JSON Serialization**: Use `json.dumps()` for complex data in `resources_info`
6. **Error Handling**: Implement comprehensive try-catch blocks with descriptive errors

### Output Data Standards

All pipelines must output data with these exact field names and types:

- `control_monitoring_utc_timestamp` (datetime object)
- `control_id` (string)
- `monitoring_metric_id` (string) 
- `monitoring_metric_value` (float)
- `monitoring_metric_status` (string)
- `metric_value_numerator` (int)
- `metric_value_denominator` (int)
- `resources_info` (array of JSON strings or null)

### Environment Configuration Standards

1. **Environment Sections**: 
   - **QA-Enabled Pipelines**: Separate `prod`, `qa`, `local` configurations
   - **Local-Only Pipelines**: Only `prod` and `local` configurations
2. **YAML Anchors**: Use `&nonprod_config` for qa/local inheritance (QA-enabled pipelines only)
3. **SQL File Mapping**:
   - Production: `sql/monitoring_thresholds.sql` → `etip_db.phdp_etip_controls_monitoring`
   - QA: `sql/monitoring_thresholds_qa.sql` → `etip_db.qhdp_techcos` (QA-enabled pipelines only)
4. **Table Names**:
   - Production: `etip_controls_monitoring_metrics`
   - QA: `fact_controls_monitoring_metrics_daily_v4` (QA-enabled pipelines only)
5. **Avro Schema Files**:
   - Production: `@json:avro_schema.json`
   - QA: `@json:avro_schema_qa.json` (QA-enabled pipelines only)
6. **Data Quality Checks**: Comprehensive validation with `informational: true` flags where appropriate
7. **Stages Section**: Include stages section for complex validation requirements

### Testing Requirements

#### Critical Import and Mock Standards

1. **Import Patterns**: ALWAYS use the `pipelines.` prefix for all pipeline imports to ensure proper module resolution
2. **Mock Parameter Usage**: Do NOT add unused `mock` parameters to test functions unless you're actually using pytest-mock functionality
3. **MockEnv Class Standards**: Use complete MockEnv implementation that matches working test patterns:
   ```python
   # ❌ BAD - Missing env attribute causes AttributeError
   class MockEnv:
       def __init__(self):
           self.exchange = MockExchangeConfig()
   
   # ✅ GOOD - Include self-reference for framework compatibility
   class MockEnv:
       def __init__(self):
           self.exchange = MockExchangeConfig()
           self.env = self  # Required by ConfigPipeline framework (expects env.env)
   ```
   The ConfigPipeline framework expects env objects to have an 'env' attribute containing the environment type. Without this, tests will fail with: `AttributeError: 'MockEnv' object has no attribute 'env'`

4. **Test Class Methods Directly**: Test pipeline class methods (_calculate_metrics) instead of transformer functions
5. **Extract Method Testing**: Include tests for the extract() method integration

#### Standard Testing Requirements

1. **Timestamp Testing**: Use `freeze_time` for consistent datetime testing
2. **Mock API Connectors**: Use proper mocking patterns for OauthApi
3. **Utility Functions**: Create `_mock_threshold_df()` and similar helper functions
4. **Coverage Requirements**: Achieve 80%+ code coverage of the pipeline.py file
5. **Error Testing**: Include negative test cases for API failures and empty data
6. **API Response Generation**: Use standardized `generate_mock_api_response()` function
7. **Exception Wrapping**: Wrap all API exceptions in `RuntimeError` with descriptive messages
8. **Data Type Testing**: Use pandas type checking for DataFrame columns

#### Data Type Standards in Tests

**Use pandas type checking for DataFrame outputs:**
```python
def test_data_types():
    result = pipeline._calculate_metrics(thresholds_df)
    # Use pandas type checking for integer columns
    assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
    assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
    assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])
```

#### Pipeline Data Type Enforcement

**Ensure consistent data types in pipeline outputs:**
```python
def _calculate_metrics(self, thresholds_raw: pd.DataFrame) -> pd.DataFrame:
    # ... calculation logic ...
    
    result_df = pd.DataFrame(all_results)
    
    # Ensure correct data types to match test expectations
    if not result_df.empty:
        result_df = result_df.astype({
            "metric_value_numerator": "int64",
            "metric_value_denominator": "int64", 
            "monitoring_metric_value": "float64"
        })
    
    return result_df
```

## Code Quality and Linting Standards

### Python Code Quality Requirements

#### Import Management
1. **No Unused Imports**: All imports must be used. Remove any unused imports immediately.
2. **No Hallucinated Imports**: Never import modules or frameworks that don't exist in the codebase. Always verify imports against existing codebase structure.
3. **No Unused Variables**: All variables must be used. Remove any unused variable assignments.
4. **Import Organization**: Follow standard Python import ordering (standard library, third-party, local application)

#### Code Style Standards
1. **Line Length**: Maximum 88 characters per line (Black standard)
2. **Function Naming**: Use snake_case for all functions and variables
3. **Class Naming**: Use PascalCase for class names
4. **Constant Naming**: Use UPPER_SNAKE_CASE for constants

### SQL Quality Standards

#### SQLFluff Configuration Requirements
All SQL files MUST include the required SQLFluff headers:

```sql
-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
```

#### SQL Formatting Rules
1. **Keywords**: Use lowercase for SQL keywords (`select`, `from`, `where`, `and`, `or`)
2. **Indentation**: Use 4 spaces for indentation
3. **Column Alignment**: Align column names in SELECT statements
4. **Comments**: Include SQLFluff directives at the top of every SQL file
5. **Control ID Filtering**: For single-control pipelines, always include the specific control ID as a filter in SQL queries

### Pre-commit Check Requirements

#### Before Every Commit
1. **Run Flake8**: Check for unused imports and variables
2. **Run isort**: Organize imports properly
3. **Run Black**: Format code consistently
4. **Run SQLFluff**: Validate SQL files

#### Common Linting Violations to Avoid

**F401 - Unused Import**
```python
# ❌ BAD
from etip_env import set_env_vars  # Not used anywhere

# ✅ GOOD - Remove unused import or use it
if __name__ == "__main__":
    from etip_env import set_env_vars
    env = set_env_vars()
```

**F841 - Unused Variable**
```python
# ❌ BAD
def calculate_metrics(thresholds_raw, context):
    thresholds = thresholds_raw.to_dict("records")  # Never used
    return pd.DataFrame([])

# ✅ GOOD - Use the variable or remove assignment
def calculate_metrics(thresholds_raw, context):
    thresholds = thresholds_raw.to_dict("records")
    for threshold in thresholds:  # Now it's used
        # Process threshold
        pass
    return pd.DataFrame(results)
```

#### Critical Code Review Issues to Prevent

**Missing Required Imports**
```python
# ❌ BAD - Missing ssl import when using ssl.create_default_context()
import json
from datetime import datetime
# ssl module missing but used in _get_api_connector()

# ✅ GOOD - Include all required imports
import json
import ssl
from datetime import datetime
from pathlib import Path
```

**Inverted Threshold Logic**
```python
# ❌ BAD - Yellow status is unreachable due to inverted logic
if metric_value >= alert_threshold:      # 95.0
    compliance_status = "Green"
elif metric_value >= warning_threshold:  # 97.0 - Never reached!
    compliance_status = "Yellow"
else:
    compliance_status = "Red"

# ✅ GOOD - Correct threshold order (higher threshold first)
if warning_threshold is not None and metric_value >= warning_threshold:  # 97.0
    compliance_status = "Green"
elif metric_value >= alert_threshold:  # 95.0
    compliance_status = "Yellow"
else:
    compliance_status = "Red"
```

**Series vs DataFrame Parameter Errors**
```python
# ❌ BAD - Passing Series instead of DataFrame
def extract(self) -> pd.DataFrame:
    df = super().extract()
    # df["thresholds_raw"] is a Series, not a DataFrame
    df["monitoring_metrics"] = self._calculate_metrics(df["thresholds_raw"])
    return df

# ✅ GOOD - Extract DataFrame from Series using iloc[0]
def extract(self) -> pd.DataFrame:
    df = super().extract()
    df["monitoring_metrics"] = self._calculate_metrics(df["thresholds_raw"].iloc[0])
    return df
```

**Duplicate OAuth Token Creation**
```python
# ❌ BAD - Creating new OAuth connector instead of using existing method
def _fetch_api_resources(self, resource_type: str):
    api_connector = OauthApi(
        url=self.api_url,
        api_token=refresh(  # Duplicate token refresh
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        ),
    )

# ✅ GOOD - Reuse existing _get_api_connector() method
def _fetch_api_resources(self, resource_type: str):
    api_connector = self._get_api_connector()
```

### Mandatory Validations

1. **Empty Thresholds Check**: Validate input data exists
2. **API Response Validation**: Check status codes and response structure  
3. **DataFrame Type Verification**: Ensure correct data types before returning
4. **Division by Zero Protection**: Handle edge cases in percentage calculations
5. **Configuration Validation**: Verify all required config sections exist

### File Organization Requirements

1. **Directory Structure**: Follow exact structure with `sql/` subdirectory
2. **Naming Conventions**: `pl_automated_monitoring_[CONTROL_ID]` format
3. **SQL Files**: 
   - **QA-Enabled Pipelines**: Both `monitoring_thresholds.sql` and `monitoring_thresholds_qa.sql`
   - **Local-Only Pipelines**: Only `monitoring_thresholds.sql`
4. **Avro Schema Files**:
   - **QA-Enabled Pipelines**: Both `avro_schema.json` and `avro_schema_qa.json`
   - **Local-Only Pipelines**: Only `avro_schema.json`
5. **Import Paths**: Match filesystem exactly (case-sensitive)
6. **Test Files**: Mirror pipeline structure in `tests/pipelines/` directory

## Common Anti-Patterns to Avoid

1. **Hardcoded Values**: Never hardcode control IDs, use config files
2. **Type Mismatches**: Ensure DataFrame columns match expected types
3. **Inconsistent Field Names**: Use standardized output field names across environments
4. **Missing Error Handling**: Always wrap API calls in try-catch blocks
5. **Timestamp Inconsistencies**: Use consistent datetime handling in tests and code
6. **Using @transformer decorator**: Use extract() override method instead for production pipelines
7. **Instantiating pipeline classes in helper functions**: Keep business logic in class methods or standalone functions

This document serves as the definitive guide for ETIP pipeline development. All pipelines must adhere to these production-ready standards to ensure consistency, maintainability, and reliability across the platform.