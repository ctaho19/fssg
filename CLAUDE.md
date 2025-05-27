# CLAUDE.md - ETIP Pipeline Development Standards

This document provides comprehensive guidance for developing Enterprise Tech Insights Platform (ETIP) pipelines following Zach's established architectural patterns and coding standards.

## Repository Overview

This repository contains ETIP pipelines for automated security control monitoring. Each pipeline collects metrics from various data sources (APIs, databases), determines compliance status, and outputs standardized metrics data to support enterprise security monitoring and reporting.

**Reference Implementation**: Always use `pl_automated_monitoring_cloud_custodian` pipeline and `test_pl_automated_monitoring_cloud_custodian.py` as the gold standard for pipeline architecture and testing patterns.

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

### Step 2: Pipeline Class Implementation (`pipeline.py`)

#### Required Class Structure

```python
from typing import Dict, Any
import pandas as pd
from datetime import datetime
import json
import ssl
import os
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env
from transform_library import transformer

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
        
    def transform(self) -> None:
        """Override transform to set up API context"""
        self.context["api_connector"] = self._get_api_connector()
        super().transform()
```

#### Required Transformer Function

```python
@transformer
def calculate_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    """
    Core business logic transformer following Zach's established patterns
    
    Args:
        thresholds_raw: DataFrame containing metric thresholds from SQL query
        context: Pipeline context including API connector
        
    Returns:
        DataFrame with standardized output schema
    """
    
    # Step 1: Input Validation (REQUIRED)
    if thresholds_raw.empty:
        raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
    
    # Step 2: Extract Threshold Configuration
    thresholds = thresholds_raw.to_dict("records")
    api_connector = context["api_connector"]
    
    # Step 3: API Data Collection with Pagination
    all_resources = []
    next_record_key = None
    
    while True:
        try:
            # Standard API request headers
            headers = {
                "Accept": "application/json;v=1",  # Note: v=1 not v=1.0
                "Authorization": api_connector.api_token,
                "Content-Type": "application/json"
            }
            
            # Construct request payload
            payload = {
                # API-specific parameters
            }
            if next_record_key:
                payload["nextRecordKey"] = next_record_key
            
            response = api_connector.post("", json=payload, headers=headers)
            
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
    
    for threshold in thresholds:
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
    
    return pd.DataFrame(results)
```

### Step 3: Configuration Setup (`config.yml`)

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
```

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

### Step 4: SQL Query Implementation

#### Production SQL (`sql/monitoring_thresholds.sql`)

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
    metric_threshold_end_date is Null
    and control_id = '[CONTROL_ID]'
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
    metric_threshold_end_date is Null
    and control_id = '[CONTROL_ID]'
```

### Step 5: Avro Schema Definition

#### Production Schema (`avro_schema.json`)

Production schemas include comprehensive metadata for data governance:

```json
{
  "name": "etip_controls_monitoring_metrics",
  "namespace": "com.capitalone.BAENTERPRISETECHINSIGHTS", 
  "doc": "This dataset is used for analytics and reporting for the Tech Controls Health Monitoring process. Th",
  "type": "record",
  "fields": [
    {
      "name": "monitoring_metric_id",
      "dmslCatalog": {
        "privacyDetails": {
          "category": "NOT_APPLICABLE",
          "exclusionCategory": "NOT_APPLICABLE", 
          "identifyingKey": "NOT_APPLICABLE"
        },
        "dataProtectionType": "NONE",
        "physicalName": "monitoring_metric_id",
        "dataQualityDetails": {
          "allowedValues": [],
          "isOptional": false,
          "orderNumber": 0
        },
        "dataSensitivity": {
          "hasCreditInformation": false,
          "hasPersonallyIdentifiableInformation": false,
          "hasBankAccountNumber": false,
          "hasPaymentCardIndustry": false,
          "hasAssociatePersonalInformation": false,
          "hasNonPublicInformation": false
        }
      },
      "doc": "A unique identifier for a monitoring metric",
      "type": "int",
      "tokenization": {"type": "NOT_APPLICABLE"}
    }
    // Additional fields with similar structure...
  ]
}
```

#### QA Schema (`avro_schema_qa.json`) - Required for QA-Enabled Pipelines Only

QA schemas include additional metadata elements for enhanced validation:

```json
{
  "name": "fact_controls_monitoring_metrics_daily_v4",
  "namespace": "com.capitalone.BAENTERPRISETECHINSIGHTS",
  "doc": "This dataset is used for analytics and reporting for the Tech Controls Health Monitoring process. Th",
  "fields": [
    {
      "name": "monitoring_metric_id",
      "doc": "A unique identifier for a monitoring metric",
      "type": "int",
      "tokenization": {
        "type": "NOT_APPLICABLE"
      },
      "dmslCatalog": {
        "dataProtectionType": "NONE",
        "dataQualityDetails": {
          "isOptional": false,
          "orderNumber": 0
        },
        "dataSensitivity": {
          "hasAssociatePersonalInformation": false,
          "hasBankAccountNumber": false,
          "hasCreditInformation": false,
          "hasNonPublicInformation": false,
          "hasPaymentCardIndustry": false,
          "hasPersonallyIdentifiableInformation": false
        },
        "elementId": "24750646",
        "isAuthoritativeSource": false,
        "isHighPriority": false,
        "physicalName": "monitoring_metric_id",
        "privacyDetails": {
          "category": "NOT_APPLICABLE",
          "exclusionCategory": "NOT_APPLICABLE",
          "identifyingKey": "NOT_APPLICABLE"
        },
        "tokenizationDetails": {
          "tokenizationClassification": "NOT_APPLICABLE"
        }
      },
      "path": ".control_id"
    }
    // Additional fields with similar metadata structure...
  ],
  "type": "record"
}
```

#### Key Differences Between Production and QA Schemas

**Production Schema (`avro_schema.json`)**:
- **Record Name**: `"etip_controls_monitoring_metrics"` (production table name)
- **Compact Format**: Single-line JSON format for efficiency
- **Standard Metadata**: Includes `dmslCatalog`, `privacyDetails`, `dataQualityDetails`, `dataSensitivity`
- **Purpose**: Full data governance compliance for production environment

**QA Schema (`avro_schema_qa.json`)**:
- **Record Name**: `"fact_controls_monitoring_metrics_daily_v4"` (QA table name)
- **Formatted JSON**: Multi-line, readable format for development
- **Enhanced Metadata**: All production metadata PLUS additional QA-specific elements:
  - `elementId` for tracking individual schema elements
  - `isAuthoritativeSource` and `isHighPriority` flags
  - `tokenizationDetails` with classification information
  - `path` field for element mapping
- **Purpose**: Enhanced validation and testing capabilities in QA environment

**Note**: Local-only pipelines (those without QA data source alternatives) only require `avro_schema.json` and do not need `avro_schema_qa.json`.

### Step 6: Unit Test Implementation

#### Test File Structure (`tests/pipelines/test_pl_automated_monitoring_[CONTROL_ID].py`)

```python
import pytest
import pandas as pd
from unittest.mock import Mock
from freezegun import freeze_time
from datetime import datetime

# Import pipeline components
from pl_automated_monitoring_[CONTROL_ID].pipeline import (
    PLAutomatedMonitoring[ControlName],
    calculate_metrics
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

class MockOauthApi:
    def __init__(self, url, api_token, ssl_context=None):
        self.url = url
        self.api_token = api_token
        self.response = None
        self.side_effect = None
    
    def send_request(self, url, request_type, request_kwargs, retry_delay=5):
        """Mock send_request method with proper side_effect handling."""
        if self.side_effect:
            if isinstance(self.side_effect, Exception):
                raise self.side_effect
            elif isinstance(self.side_effect, list) and self.side_effect:
                response = self.side_effect.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response
        
        if self.response:
            return self.response
        
        # Return default response if none configured
        from requests import Response
        default_response = Response()
        default_response.status_code = 200
        return default_response

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "[CONTROL_ID]",
        "monitoring_metric_tier": 1,
        "warning_threshold": 80.0,
        "alerting_threshold": 90.0
    }])

def generate_mock_api_response(content=None, status_code=200):
    """Generate standardized mock API response."""
    from requests import Response
    import json
    
    mock_response = Response()
    mock_response.status_code = status_code
    
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        mock_response._content = json.dumps({}).encode("utf-8")
    
    return mock_response

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_success(mock):
    """Test successful metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    
    # Mock API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [{"id": "resource1", "compliant": True}],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Verify data types
    row = result.iloc[0]
    assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
    assert isinstance(row["monitoring_metric_value"], float)
    assert isinstance(row["metric_value_numerator"], int)
    assert isinstance(row["metric_value_denominator"], int)

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, context)

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url

def test_pipeline_run_method(mock):
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoring[ControlName](env)
    
    # Mock the run method
    mock_run = mock.patch.object(pipeline, 'run')
    pipeline.run()
    
    # Verify run was called with default parameters
    mock_run.assert_called_once_with()

def test_api_error_handling(mock):
    """Test API error handling and exception wrapping"""
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="API request failed"):
        calculate_metrics(_mock_threshold_df(), context)

def test_pagination_handling(mock):
    """Test API pagination with multiple responses"""
    # Create paginated responses
    page1_response = generate_mock_api_response({
        "resources": [{"id": "resource1"}],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "resources": [{"id": "resource2"}],
        "nextRecordKey": None
    })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(_mock_threshold_df(), context)
    
    # Verify both pages were processed
    assert not result.empty

def test_main_function_execution(mock):
    """Test main function execution path"""
    mock_env = mock.Mock()
    
    with mock.patch("etip_env.set_env_vars", return_value=mock_env):
        with mock.patch("pipelines.[module].pipeline.run") as mock_run:
            with mock.patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.[module].pipeline import run
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception as e:
        import sys
        sys.exit(1)
"""
                exec(code)
                
                # Verify success path
                assert not mock_exit.called
                mock_run.assert_called_once_with(env=mock_env, is_load=False, dq_actions=False)
```

## Development Standards & Requirements

### API Integration Standards

1. **OAuth Authentication**: Always use the standardized `refresh()` function with client credentials
2. **Bearer Token Format**: Format as `f"Bearer {api_token}"`
3. **Environment-Aware URLs**: Construct URLs using `self.env.exchange.exchange_url`
4. **Standard Headers**: Use `"application/json;v=1"` (not `v=1.0`)
5. **SSL Certificates**: Implement `C1_CERT_FILE` for secure connections
6. **Pagination Handling**: Process `nextRecordKey` for large result sets
7. **Error Wrapping**: Wrap API exceptions in `RuntimeError` with descriptive context

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
7. **QA Environment Determination**: Pipelines requiring QA testing must have data sources with QA alternatives. Pipelines without QA data source alternatives will be tested locally only.

### Testing Requirements

1. **Parameter Naming**: Use `mock` parameter (not `mocker`)
2. **Timestamp Testing**: Use `freeze_time` for consistent datetime testing
3. **Context Initialization**: Always initialize `pipeline.context = {}` in tests
4. **Mock API Connectors**: Use `MockOauthApi` class pattern with proper `send_request` method
5. **Utility Functions**: Create `_mock_threshold_df()` and similar helper functions
6. **Coverage Requirements**: Achieve 80%+ code coverage of the pipeline.py file with end-to-end testing
7. **Error Testing**: Include negative test cases for API failures and empty data
8. **API Response Generation**: Use standardized `generate_mock_api_response()` function
9. **Exception Wrapping**: Wrap all API exceptions in `RuntimeError` with descriptive messages
10. **Function Parameter Testing**: Only pass parameters that exist in function signatures
11. **Main Function Testing**: Include tests for module execution paths
12. **Proactively Review Unit Tests**: Read each individual unit test and flag any that will fail due to an error and reccomend a fix.

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

### Mandatory Validations

1. **Empty Thresholds Check**: Validate input data exists
2. **API Response Validation**: Check status codes and response structure  
3. **DataFrame Type Verification**: Ensure correct data types before returning
4. **Division by Zero Protection**: Handle edge cases in percentage calculations
5. **Configuration Validation**: Verify all required config sections exist

### Common Anti-Patterns to Avoid

1. **Hardcoded Values**: Never hardcode control IDs, use config files
2. **Missing Context**: Always initialize pipeline context in tests
3. **Type Mismatches**: Ensure DataFrame columns match expected types
4. **Inconsistent Field Names**: Use standardized output field names across environments
5. **Self-Mocking in Tests**: Don't mock the function being tested
6. **Missing Error Handling**: Always wrap API calls in try-catch blocks
7. **Timestamp Inconsistencies**: Use consistent datetime handling in tests and code

This document serves as the definitive guide for ETIP pipeline development. All pipelines must adhere to these standards to ensure consistency, maintainability, and reliability across the platform.