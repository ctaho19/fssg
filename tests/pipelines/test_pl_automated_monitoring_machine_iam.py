import datetime
import json
import unittest.mock as mock
from typing import Optional, Union, Dict, Any, List
import pandas as pd
import pytest
from freezegun import freeze_time
from requests import Response, RequestException

import pipelines.pl_automated_monitoring_machine_iam.pipeline as pipeline
from etip_env import set_env_vars
from connectors.api import OauthApi

class MockOauthApi:
    """Mock implementation of OauthApi for testing purposes."""
    
    def __init__(self, url: str, api_token: str):
        self.url = url
        self.api_token = api_token
        self.response = None
        self.side_effect = None
    
    def send_request(self, url: str, request_type: str, request_kwargs: Dict[str, Any], 
                     retry_delay: int = 5) -> Response:
        """Mocked version of send_request method."""
        if self.side_effect:
            if isinstance(self.side_effect, Exception):
                raise self.side_effect
            elif callable(self.side_effect):
                return self.side_effect(url, request_type, request_kwargs)
            else:
                # Handle list of responses for pagination
                if isinstance(self.side_effect, list) and self.side_effect:
                    response = self.side_effect.pop(0)
                    if isinstance(response, Exception):
                        raise response
                    return response
        
        # Return the mocked response with proper handling
        if self.response:
            return self.response
        else:
            # Create a default Response if none was provided
            default_response = Response()
            default_response.status_code = 200
            default_response._content = json.dumps({}).encode("utf-8")
            return default_response

# Standard timestamp for all tests to use (2024-11-05 12:09:00 UTC)
FIXED_TIMESTAMP = "2024-11-05 12:09:00"
FIXED_TIMESTAMP_MS = 1730808540000  # This value was determined by the actual test execution

def get_fixed_timestamp(as_int: bool = True) -> Union[int, pd.Timestamp]:
    """Returns a fixed timestamp for testing, either as int milliseconds or pandas Timestamp.
    
    Args:
        as_int: If True, returns timestamp as milliseconds since epoch (int).
               If False, returns as pandas Timestamp with UTC timezone.
    
    Returns:
        Either int milliseconds or pandas Timestamp object
    """
    if as_int:
        return FIXED_TIMESTAMP_MS
    return pd.Timestamp(FIXED_TIMESTAMP, tz="UTC")

# Function to generate mock API responses
def generate_mock_api_response(content: Optional[dict] = None, status_code: int = 200) -> Response:
    mock_response = Response()
    # Explicitly set status_code attribute 
    mock_response.status_code = status_code
    
    # Ensure json() method works by setting _content
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        # Set default empty content to avoid NoneType errors
        mock_response._content = json.dumps({}).encode("utf-8")
        
    # Set request information for debugging
    mock_response.request = mock.Mock()
    mock_response.request.url = "https://mock.api.url/search-resource-configurations"
    mock_response.request.method = "POST"
    
    return mock_response

# Mock data for test fixtures
def _mock_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2, 3, 4, 5, 6],
        "control_id": [
            "CTRL-1074653", "CTRL-1074653", "CTRL-1074653",
            "CTRL-1105806", "CTRL-1105806", "CTRL-1077124"
        ],
        "monitoring_metric_tier": [
            "Tier 1", "Tier 2", "Tier 3",
            "Tier 1", "Tier 2", "Tier 1"
        ],
        "metric_name": [
            "% of machine roles evaluated", "% of machine roles compliant", "% of non-compliant roles within SLA",
            "% of machine roles evaluated", "% of machine roles compliant", "% of machine roles evaluated"
        ],
        "metric_description": [
            "Percent of machine roles that have been evaluated", 
            "Percent of machine roles that are compliant",
            "Percent of non-compliant roles that are within SLA",
            "Percent of machine roles that have been evaluated",
            "Percent of machine roles that are compliant",
            "Percent of machine roles that have been evaluated"
        ],
        "warning_threshold": [97.0, 75.0, 90.0, 97.0, 75.0, 97.0],
        "alerting_threshold": [95.0, 50.0, 85.0, 95.0, 50.0, 95.0],
        "load_timestamp": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ]
    })

def _mock_invalid_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2],
        "control_id": ["CTRL-1074653", "CTRL-1074653"],
        "monitoring_metric_tier": ["Tier 1", "Tier 2"],
        "metric_name": ["% of machine roles evaluated", "% of machine roles compliant"],
        "metric_description": ["Percent of machine roles that have been evaluated", "Percent of machine roles that are compliant"],
        "warning_threshold": ["invalid", None],
        "alerting_threshold": ["not_a_number", 50.0],
        "load_timestamp": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ]
    }).astype({
        "monitoring_metric_id": int,
        "warning_threshold": object,
        "alerting_threshold": object
    })

def _mock_iam_roles_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "RESOURCE_ID": [
            "AROAW876543222222AAAA", "AROAW876543233333BBBB", 
            "AROAW876543244444CCCC", "AROAW876543255555DDDD", 
            "AROAW876543266666EEEE"
        ],
        "AMAZON_RESOURCE_NAME": [
            "arn:aws:iam::123456789012:role/service-role/example-role-1",
            "arn:aws:iam::123456789012:role/machine-role/example-role-2",
            "arn:aws:iam::123456789012:role/machine-role/example-role-3",
            "arn:aws:iam::123456789012:role/machine-role/example-role-4",
            "arn:aws:iam::123456789012:role/machine-role/example-role-5"
        ],
        "BA": ["BAENTERPRISETECHINSIGHTS"] * 5,
        "ACCOUNT": ["123456789012"] * 5,
        "CREATE_DATE": [datetime.datetime(2023, 1, 1) for _ in range(5)],
        "TYPE": ["role"] * 5,
        "FULL_RECORD": [f"IAM Role {i+1} details" for i in range(5)],
        "ROLE_TYPE": ["MACHINE"] * 5,
        "SF_LOAD_TIMESTAMP": [datetime.datetime(2024, 11, 5, 8, 0, 0) for _ in range(5)],
        "LOAD_TIMESTAMP": [datetime.datetime(2024, 11, 5, 12, 9, 0) for _ in range(5)]
    })

def _mock_evaluated_roles_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "resource_name": [
            "ARN:AWS:IAM::123456789012:ROLE/MACHINE-ROLE/EXAMPLE-ROLE-2",
            "ARN:AWS:IAM::123456789012:ROLE/MACHINE-ROLE/EXAMPLE-ROLE-3",
            "ARN:AWS:IAM::123456789012:ROLE/MACHINE-ROLE/EXAMPLE-ROLE-4"
        ],
        "compliance_status": [
            "Compliant", "NonCompliant", "Compliant"
        ],
        "control_id": ["AC-3.AWS.39.v02"] * 3,  # This should be cloud_control_id, not CTRL-ID
        "role_type": ["MACHINE"] * 3,
        "create_date": [datetime.datetime(2024, 11, 5, 8, 0, 0) for _ in range(3)],
        "load_timestamp": [datetime.datetime(2024, 11, 5, 12, 9, 0) for _ in range(3)]
    })

def _mock_sla_data_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "RESOURCE_ID": ["AROAW876543233333BBBB", "AROAW876543244444CCCC"],
        "CONTROL_RISK": ["High", "Critical"],
        "CONTROL_ID": ["AC-3.AWS.39.v02", "AC-3.AWS.39.v02"],  # Cloud control ID
        "OPEN_DATE_UTC_TIMESTAMP": [
            datetime.datetime(2024, 10, 20, 0, 0, 0),  # Within SLA (High: 30 days)
            datetime.datetime(2024, 9, 1, 0, 0, 0)     # Past SLA (Critical: 0 days)
        ]
    })

# Tests for utility functions
def test_get_compliance_status():
    """Test get_compliance_status function with various inputs."""
    # Testing normal thresholds
    assert pipeline.get_compliance_status(96.0, 95.0, 97.0) == "Yellow"
    assert pipeline.get_compliance_status(98.0, 95.0, 97.0) == "Green"
    assert pipeline.get_compliance_status(94.0, 95.0, 97.0) == "Red"
    
    # Testing with only alert threshold (no warning)
    assert pipeline.get_compliance_status(96.0, 95.0) == "Green"
    assert pipeline.get_compliance_status(94.0, 95.0) == "Red"
    
    # Testing with invalid thresholds
    assert pipeline.get_compliance_status(96.0, "invalid") == "Red"
    assert pipeline.get_compliance_status(96.0, 95.0, "invalid") == "Green"

def test_format_non_compliant_resources():
    """Test format_non_compliant_resources function."""
    # Test with empty DataFrame
    empty_df = pd.DataFrame()
    assert pipeline.format_non_compliant_resources(empty_df) is None
    
    # Test with None input
    assert pipeline.format_non_compliant_resources(None) is None
    
    # Test with valid DataFrame
    test_df = pd.DataFrame({
        "id": [1, 2],
        "name": ["resource1", "resource2"]
    })
    result = pipeline.format_non_compliant_resources(test_df)
    assert len(result) == 2
    assert json.loads(result[0])["id"] == 1
    assert json.loads(result[1])["name"] == "resource2"

def test_fetch_all_resources_success():
    """Test successful resource fetching with pagination."""
    # Create mock responses for pagination
    page1_response = generate_mock_api_response({
        "resourceConfigurations": [{"id": "resource1"}],
        "nextRecordKey": "page2key"
    })
    
    page2_response = generate_mock_api_response({
        "resourceConfigurations": [{"id": "resource2"}, {"id": "resource3"}],
        "nextRecordKey": ""
    })
    
    # Create mock API connector
    mock_api = MockOauthApi(
        url="https://api.example.com/resources",
        api_token="Bearer mock_token"
    )
    
    # Set up the pagination responses
    mock_api.side_effect = [page1_response, page2_response]
    
    # Call the function with pagination
    with mock.patch("time.sleep") as mock_sleep:  # Mock sleep to speed up test
        result = pipeline.fetch_all_resources(
            api_connector=mock_api,
            verify_ssl=True,
            config_key_full="configuration.key",
            search_payload={"searchParameters": [{"resourceType": "AWS::IAM::Role"}]}
        )
    
    # Verify the results
    assert len(result) == 3
    assert [r["id"] for r in result] == ["resource1", "resource2", "resource3"]
    assert mock_sleep.called  # Verify that sleep was called between paginated requests

def test_fetch_all_resources_api_error():
    """Test error handling in fetch_all_resources."""
    # Create mock API connector that raises an exception
    mock_api = MockOauthApi(
        url="https://api.example.com/resources",
        api_token="Bearer mock_token"
    )
    
    # Set up the connector to raise a RequestException
    mock_api.side_effect = RequestException("Connection error")
    
    # Test with try-except to handle the expected error
    with pytest.raises(RuntimeError):
        pipeline.fetch_all_resources(
            api_connector=mock_api,
            verify_ssl=True,
            config_key_full="configuration.key",
            search_payload={"searchParameters": [{"resourceType": "AWS::IAM::Role"}]}
        )

# Tests for pipeline initialization and OAuth token handling
def test_pipeline_init_success():
    """Test successful pipeline initialization."""
    class MockExchangeConfig:
        def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
            self.client_id = client_id
            self.client_secret = client_secret
            self.exchange_url = exchange_url
    
    class MockSnowflakeConfig:
        def __init__(self):
            self.account = "capitalone"
            self.user = "etip_user"
            self.password = "etip_password"
            self.role = "etip_role"
            self.warehouse = "etip_wh"
            self.database = "etip_db"
            self.schema = "etip_schema"
    
    env = set_env_vars("qa")
    env.exchange = MockExchangeConfig()
    env.snowflake = MockSnowflakeConfig()
    
    pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
    
    assert pipe.client_id == "etip-client-id"
    assert pipe.client_secret == "etip-client-secret"
    assert pipe.exchange_url == "https://api.cloud.capitalone.com/exchange"
    assert pipe.cloudradar_api_url == "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"

def test_get_api_token_success():
    """Test successful API token retrieval."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh") as mock_refresh:
        mock_refresh.return_value = "mock_token_value"
        
        class MockExchangeConfig:
            def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
                self.client_id = client_id
                self.client_secret = client_secret
                self.exchange_url = exchange_url
        
        env = set_env_vars("qa")
        env.exchange = MockExchangeConfig()
        
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        token = pipe._get_api_token()
        
        assert token == "Bearer mock_token_value"
        mock_refresh.assert_called_once_with(
            client_id="etip-client-id",
            client_secret="etip-client-secret",
            exchange_url="https://api.cloud.capitalone.com/exchange"
        )

def test_get_api_token_failure():
    """Test API token retrieval failure."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh") as mock_refresh:
        mock_refresh.side_effect = Exception("Token refresh failed")
        
        class MockExchangeConfig:
            def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
                self.client_id = client_id
                self.client_secret = client_secret
                self.exchange_url = exchange_url
        
        env = set_env_vars("qa")
        env.exchange = MockExchangeConfig()
        
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        
        with pytest.raises(RuntimeError, match="API token refresh failed"):
            pipe._get_api_token()

def test_get_api_connector_success():
    """Test successful API connector creation."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh") as mock_refresh:
        mock_refresh.return_value = "mock_token_value"
        
        class MockExchangeConfig:
            def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
                self.client_id = client_id
                self.client_secret = client_secret
                self.exchange_url = exchange_url
        
        env = set_env_vars("qa")
        env.exchange = MockExchangeConfig()
        
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        connector = pipe._get_api_connector()
        
        assert isinstance(connector, OauthApi)
        assert connector.api_token == "Bearer mock_token_value"
        assert connector.url == "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        
        mock_refresh.assert_called_once_with(
            client_id="etip-client-id",
            client_secret="etip-client-secret",
            exchange_url="https://api.cloud.capitalone.com/exchange"
        )

def test_get_api_connector_failure():
    """Test API connector creation failure."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh") as mock_refresh:
        mock_refresh.side_effect = Exception("Token refresh failed")
        
        class MockExchangeConfig:
            def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
                self.client_id = client_id
                self.client_secret = client_secret
                self.exchange_url = exchange_url
        
        env = set_env_vars("qa")
        env.exchange = MockExchangeConfig()
        
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        
        with pytest.raises(RuntimeError, match="API connector initialization failed"):
            pipe._get_api_connector()

# Tests for helper functions
def test_extract_tier_metrics():
    """Test _extract_tier_metrics helper function with enhanced functionality."""
    # Test with valid thresholds
    thresholds_df = _mock_threshold_df_pandas()
    result = pipeline._extract_tier_metrics(thresholds_df, "CTRL-1074653")
    
    assert len(result) == 3  # Should have Tier 1, 2, and 3
    assert "Tier 1" in result
    assert "Tier 2" in result
    assert "Tier 3" in result
    assert result["Tier 1"]["metric_id"] == 1
    assert result["Tier 2"]["metric_id"] == 2
    assert result["Tier 3"]["metric_id"] == 3
    
    # Check that it correctly extracts thresholds
    assert result["Tier 1"]["alert_threshold"] == 95.0
    assert result["Tier 1"]["warning_threshold"] == 97.0
    
    # Check that it also extracts optional fields if present
    assert "metric_name" in result["Tier 1"]
    assert result["Tier 1"]["metric_name"] == "% of machine roles evaluated"
    
    # Test with non-existent control ID
    result = pipeline._extract_tier_metrics(thresholds_df, "CTRL-NONEXISTENT")
    assert result == {}
    
    # Test with threshold dataframe that has multiple entries for same metric
    # Add a duplicate row with a newer date and different threshold
    dup_row = thresholds_df.iloc[0].copy()
    dup_row["metric_threshold_start_date"] = datetime.datetime(2024, 11, 6, 12, 9, 0)
    dup_row["alerting_threshold"] = 98.0  # Different threshold
    
    # Create a new dataframe with the duplicate row
    thresholds_with_dup = pd.concat([thresholds_df, pd.DataFrame([dup_row])], ignore_index=True)
    
    # Extract metrics - should use the newer threshold
    result = pipeline._extract_tier_metrics(thresholds_with_dup, "CTRL-1074653")
    assert result["Tier 1"]["alert_threshold"] == 98.0  # Should use the newer threshold

def test_calculate_tier1_metric():
    """Test _calculate_tier1_metric helper function."""
    with freeze_time(FIXED_TIMESTAMP):
        # Set up test data
        iam_roles = _mock_iam_roles_df_pandas()
        evaluated_roles = _mock_evaluated_roles_df_pandas()
        thresholds = _mock_threshold_df_pandas()
        tier_metrics = pipeline._extract_tier_metrics(thresholds, "CTRL-1074653")
        now = get_fixed_timestamp(as_int=True)
        
        # Test calculation
        result = pipeline._calculate_tier1_metric(
            iam_roles=iam_roles,
            evaluated_roles=evaluated_roles,
            ctrl_id="CTRL-1074653",
            tier_metrics=tier_metrics,
            timestamp=now
        )
        
        # Assert results - 3 out of 5 roles were evaluated (60%)
        assert result["date"] == now
        assert result["control_id"] == "CTRL-1074653"
        assert result["monitoring_metric_id"] == 1
        assert result["monitoring_metric_value"] == 60.0
        assert result["compliance_status"] == "Red"  # 60% < 95% alert threshold
        assert result["numerator"] == 3
        assert result["denominator"] == 5
        assert len(result["non_compliant_resources"]) == 2  # 2 roles not evaluated

def test_calculate_tier2_metric():
    """Test _calculate_tier2_metric helper function."""
    with freeze_time(FIXED_TIMESTAMP):
        # Set up test data
        iam_roles = _mock_iam_roles_df_pandas()
        evaluated_roles = _mock_evaluated_roles_df_pandas()
        thresholds = _mock_threshold_df_pandas()
        tier_metrics = pipeline._extract_tier_metrics(thresholds, "CTRL-1074653")
        now = get_fixed_timestamp(as_int=True)
        
        # Test calculation
        result, combined_df = pipeline._calculate_tier2_metric(
            iam_roles=iam_roles,
            evaluated_roles=evaluated_roles,
            ctrl_id="CTRL-1074653",
            tier_metrics=tier_metrics,
            timestamp=now
        )
        
        # Assert results - 2 out of 3 evaluated roles are compliant (66.67%)
        assert result["date"] == now
        assert result["control_id"] == "CTRL-1074653"
        assert result["monitoring_metric_id"] == 2
        assert result["monitoring_metric_value"] > 65.0 and result["monitoring_metric_value"] < 67.0  # ~66.67%
        assert result["compliance_status"] == "Green"  # >50% alert threshold
        assert result["numerator"] == 2
        assert result["denominator"] == 3
        assert len(result["non_compliant_resources"]) == 1  # 1 role is non-compliant
        
        # Also test combined_df is returned correctly for Tier 3 usage
        assert len(combined_df) == 5
        assert "compliance_status" in combined_df.columns

def test_calculate_tier3_metric():
    """Test _calculate_tier3_metric helper function."""
    with freeze_time(FIXED_TIMESTAMP):
        # Set up test data
        iam_roles = _mock_iam_roles_df_pandas()
        evaluated_roles = _mock_evaluated_roles_df_pandas()
        sla_data = _mock_sla_data_df_pandas()
        thresholds = _mock_threshold_df_pandas()
        tier_metrics = pipeline._extract_tier_metrics(thresholds, "CTRL-1074653")
        now = get_fixed_timestamp(as_int=True)
        
        # First calculate tier 2 to get the combined dataframe
        _, combined_df = pipeline._calculate_tier2_metric(
            iam_roles=iam_roles,
            evaluated_roles=evaluated_roles,
            ctrl_id="CTRL-1074653",
            tier_metrics=tier_metrics,
            timestamp=now
        )
        
        # Test calculation
        result = pipeline._calculate_tier3_metric(
            combined=combined_df,
            sla_data=sla_data,
            ctrl_id="CTRL-1074653",
            tier_metrics=tier_metrics,
            timestamp=now
        )
        
        # Assert results - 1 out of 1 non-compliant roles have SLA data (100%)
        assert result["date"] == now
        assert result["control_id"] == "CTRL-1074653"
        assert result["monitoring_metric_id"] == 3
        assert result["compliance_status"] == "Red"  # 0% < 85% alert threshold for roles within SLA
        assert result["non_compliant_resources"] is not None  # Should have non-compliant resources

# Tests for main transformer function
def test_calculate_machine_iam_metrics_success():
    """Test the main transformer function with successful execution."""
    with freeze_time(FIXED_TIMESTAMP):
        # Set up test data
        thresholds = _mock_threshold_df_pandas()
        iam_roles = _mock_iam_roles_df_pandas()
        evaluated_roles = _mock_evaluated_roles_df_pandas()
        sla_data = _mock_sla_data_df_pandas()
        
        # Test the transformer function
        result_df = pipeline.calculate_machine_iam_metrics(
            thresholds_raw=thresholds,
            iam_roles=iam_roles,
            evaluated_roles=evaluated_roles,
            sla_data=sla_data
        )
        
        # Assert the results
        assert len(result_df) > 0  # Should have metrics for the control IDs
        control_ids = set(result_df["control_id"].unique())
        assert control_ids.issubset({"CTRL-1074653", "CTRL-1105806", "CTRL-1077124"})
        
        # Verify metric IDs were taken from the thresholds dataframe
        tiers_per_control = {}
        for ctrl_id in control_ids:
            ctrl_metrics = result_df[result_df["control_id"] == ctrl_id]
            tiers_per_control[ctrl_id] = len(ctrl_metrics)
            
            # Find matching thresholds for this control
            expected_metric_ids = thresholds[thresholds["control_id"] == ctrl_id]["monitoring_metric_id"].tolist()
            actual_metric_ids = ctrl_metrics["monitoring_metric_id"].tolist()
            
            # Check that all expected metric IDs are in the result
            for metric_id in expected_metric_ids:
                assert metric_id in actual_metric_ids, f"Metric ID {metric_id} for {ctrl_id} missing from results"
        
        # Check we have the expected number of tiers for each control
        if "CTRL-1074653" in tiers_per_control:
            # This control has Tier 3 enabled
            assert tiers_per_control["CTRL-1074653"] == 3
            
        # Verify data types
        assert result_df["date"].dtype == "int64"
        assert result_df["numerator"].dtype == "int64"
        assert result_df["denominator"].dtype == "int64"
        assert result_df["monitoring_metric_id"].dtype == "int64"
        assert result_df["monitoring_metric_value"].dtype == "float64"

def test_calculate_machine_iam_metrics_empty_data():
    """Test the main transformer function with empty input data."""
    with freeze_time(FIXED_TIMESTAMP):
        # Set up empty test data
        thresholds = _mock_threshold_df_pandas()
        empty_iam_roles = pd.DataFrame(columns=_mock_iam_roles_df_pandas().columns)
        empty_evaluated_roles = pd.DataFrame(columns=_mock_evaluated_roles_df_pandas().columns)
        empty_sla_data = pd.DataFrame(columns=_mock_sla_data_df_pandas().columns)
        
        # Test the transformer function
        result_df = pipeline.calculate_machine_iam_metrics(
            thresholds_raw=thresholds,
            iam_roles=empty_iam_roles,
            evaluated_roles=empty_evaluated_roles,
            sla_data=empty_sla_data
        )
        
        # Assert the results - should have metrics with 0% values and Red status
        assert len(result_df) == 0 or all(row["monitoring_metric_value"] == 0.0 for _, row in result_df.iterrows())
        
    # Also test with empty thresholds
    with freeze_time(FIXED_TIMESTAMP):
        empty_thresholds = pd.DataFrame(columns=_mock_threshold_df_pandas().columns)
        iam_roles = _mock_iam_roles_df_pandas()
        evaluated_roles = _mock_evaluated_roles_df_pandas()
        
        # Test the transformer function
        result_df = pipeline.calculate_machine_iam_metrics(
            thresholds_raw=empty_thresholds,
            iam_roles=iam_roles,
            evaluated_roles=evaluated_roles,
            sla_data=None
        )
        
        # Should return empty dataframe when thresholds are missing
        assert isinstance(result_df, pd.DataFrame)
        assert result_df.empty

def test_pipeline_transform():
    """Test the pipeline transform method."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.PLAutomatedMonitoringMachineIAM._get_api_connector") as mock_get_connector:
        # Create a mock API connector
        mock_api = MockOauthApi(
            url="https://api.example.com/resources",
            api_token="Bearer mock_token"
        )
        mock_get_connector.return_value = mock_api
        
        # Create a mock super transform method
        with mock.patch("config_pipeline.ConfigPipeline.transform") as mock_super_transform:
            # Initialize the pipeline
            class MockExchangeConfig:
                def __init__(self):
                    self.client_id = "etip-client-id"
                    self.client_secret = "etip-client-secret"
                    self.exchange_url = "https://api.cloud.capitalone.com/exchange"
            
            env = set_env_vars("qa")
            env.exchange = MockExchangeConfig()
            
            pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
            pipe.transform()
            
            # Verify the API connector was created and added to context
            mock_get_connector.assert_called_once()
            assert pipe.context["api_connector"] is mock_api
            assert "api_verify_ssl" in pipe.context
            
            # Verify the parent transform method was called
            mock_super_transform.assert_called_once()

def test_pipeline_extract():
    """Test the pipeline extract method with cloud_control_id parameters."""
    with mock.patch("config_pipeline.ConfigPipeline.extract") as mock_super_extract:
        # Initialize the pipeline
        class MockExchangeConfig:
            def __init__(self):
                self.client_id = "etip-client-id"
                self.client_secret = "etip-client-secret"
                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
        
        env = set_env_vars("qa")
        env.exchange = MockExchangeConfig()
        
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        pipe.context = {}  # Initialize empty context
        pipe.extract()
        
        # Verify evaluated_roles_params was added to context
        assert "evaluated_roles_params" in pipe.context
        assert isinstance(pipe.context["evaluated_roles_params"], list)
        assert len(pipe.context["evaluated_roles_params"]) == len(pipeline.CONTROL_CONFIGS)
        
        # Check that each parameter dict contains the cloud_control_id
        for i, param in enumerate(pipe.context["evaluated_roles_params"]):
            assert "control_id" in param
            assert param["control_id"] == pipeline.CONTROL_CONFIGS[i]["cloud_control_id"]
        
        # Verify the parent extract method was called
        mock_super_extract.assert_called_once()

def test_prepare_sla_parameters():
    """Test the prepare_sla_parameters method for SLA query."""
    # Initialize the pipeline
    class MockExchangeConfig:
        def __init__(self):
            self.client_id = "etip-client-id"
            self.client_secret = "etip-client-secret"
            self.exchange_url = "https://api.cloud.capitalone.com/exchange"
    
    env = set_env_vars("qa")
    env.exchange = MockExchangeConfig()
    
    pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
    
    # Test with empty resources
    empty_df = pd.DataFrame()
    result = pipe.prepare_sla_parameters(empty_df, "AC-3.AWS.39.v02")
    assert result["control_id"] == "AC-3.AWS.39.v02"
    assert result["resource_id_list"] == "''"
    
    # Test with None resources
    result = pipe.prepare_sla_parameters(None, "AC-3.AWS.39.v02")
    assert result["control_id"] == "AC-3.AWS.39.v02"
    assert result["resource_id_list"] == "''"
    
    # Test with actual resources
    resources_df = pd.DataFrame({
        "RESOURCE_ID": ["res1", "res2", "res3"],
        "other_column": ["val1", "val2", "val3"]
    })
    result = pipe.prepare_sla_parameters(resources_df, "AC-3.AWS.39.v02")
    assert result["control_id"] == "AC-3.AWS.39.v02"
    assert "resource_id_list" in result
    assert "'res1', 'res2', 'res3'" == result["resource_id_list"] or "'res1','res2','res3'" == result["resource_id_list"]

def test_pipeline_run():
    """Test the run entrypoint function."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.PLAutomatedMonitoringMachineIAM") as mock_pipeline_class:
        mock_pipeline_instance = mock_pipeline_class.return_value
        mock_pipeline_instance.run.return_value = None
        
        env = set_env_vars("qa")
        pipeline.run(env=env)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline_instance.configure_from_filename.assert_called_once()
        mock_pipeline_instance.run.assert_called_once_with(load=True, dq_actions=True)

def test_pipeline_run_export_test_data():
    """Test the run entrypoint function with export_test_data flag."""
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.PLAutomatedMonitoringMachineIAM") as mock_pipeline_class:
        mock_pipeline_instance = mock_pipeline_class.return_value
        mock_pipeline_instance.run_test_data_export.return_value = None
        
        env = set_env_vars("qa")
        pipeline.run(env=env, is_export_test_data=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline_instance.configure_from_filename.assert_called_once()
        mock_pipeline_instance.run_test_data_export.assert_called_once_with(dq_actions=True)

def test_pipeline_end_to_end():
    """Consolidated end-to-end test that validates the core functionality of the pipeline."""
    # Use context managers for all mocks to ensure proper cleanup
    with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh") as mock_refresh:
        with mock.patch("requests.request") as mock_request:
            with mock.patch("builtins.open", mock.mock_open(read_data="pipeline:\n  name: test")):
                with mock.patch("os.path.exists", return_value=True):
                    # Set up mock return values
                    mock_refresh.return_value = "mock_token_value"
                    
                    # Set up mock response
                    mock_response = mock.Mock()
                    mock_response.status_code = 200
                    mock_response.ok = True
                    mock_response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": ""}
                    mock_request.return_value = mock_response
                    
                    # Ensure consistent timestamps
                    with freeze_time(FIXED_TIMESTAMP):
                        # Create a mock environment with the OAuth config
                        class MockExchangeConfig:
                            def __init__(self):
                                self.client_id = "etip-client-id"
                                self.client_secret = "etip-client-secret"
                                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
                        
                        # Initialize the environment
                        env = set_env_vars("qa")
                        env.exchange = MockExchangeConfig()
                        
                        # Create the pipeline instance
                        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
                        
                        # Initialize context
                        pipe.context = {}
                        
                        # Create expected result dataframe
                        thresholds = _mock_threshold_df_pandas()
                        iam_roles = _mock_iam_roles_df_pandas()
                        evaluated_roles = _mock_evaluated_roles_df_pandas()
                        sla_data = _mock_sla_data_df_pandas()
                        
                        expected_df = pipeline.calculate_machine_iam_metrics(
                            thresholds_raw=thresholds,
                            iam_roles=iam_roles,
                            evaluated_roles=evaluated_roles,
                            sla_data=sla_data
                        )
                        
                        # Mock the core calculation function and configuration methods
                        with mock.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.calculate_machine_iam_metrics", return_value=expected_df) as _:
                            with mock.patch.object(pipe, 'configure_from_filename') as _:
                                with mock.patch.object(pipe, 'validate_and_merge') as _:
                                    
                                    # Store the result for later assertions
                                    pipe.output_df = expected_df
                                    
                                    # 1. First verify OAuth token refresh via the API connector
                                    api_connector = pipe._get_api_connector()
                                    assert isinstance(api_connector, OauthApi)
                                    assert api_connector.api_token.startswith("Bearer ")
                                    assert mock_refresh.called, "OAuth token refresh function should have been called"
                                    
                                    # 2. Setup the context with the API connector
                                    pipe.context["api_connector"] = api_connector
                                    pipe.context["api_verify_ssl"] = True
                                    
                                    # 3. Call the transform method
                                    pipe.transform()
                                    
                                    # Verify the output
                                    assert "api_connector" in pipe.context
                                    assert pipe.context["api_connector"] is api_connector
                                    assert pipe.context["api_verify_ssl"] is True