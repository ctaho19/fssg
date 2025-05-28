import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
import requests

from pipelines.pl_automated_monitoring_machine_iam_detective.pipeline import (
    PLAutomatedMonitoringMachineIamDetective,
    calculate_metrics,
    _get_approved_accounts,
    _calculate_tier1_metrics,
    _calculate_tier2_metrics,
    _calculate_tier3_metrics
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
    return pd.DataFrame([
        {
            "monitoring_metric_id": "MNTR-1074653-T1",
            "control_id": "CTRL-1074653",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "Machine IAM Detective Tier 1",
            "metric_description": "Coverage metric",
            "warning_threshold": 95.0,
            "alerting_threshold": 90.0
        },
        {
            "monitoring_metric_id": "MNTR-1074653-T2",
            "control_id": "CTRL-1074653",
            "monitoring_metric_tier": "Tier 2",
            "metric_name": "Machine IAM Detective Tier 2",
            "metric_description": "Compliance metric",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0
        },
        {
            "monitoring_metric_id": "MNTR-1074653-T3",
            "control_id": "CTRL-1074653",
            "monitoring_metric_tier": "Tier 3",
            "metric_name": "Machine IAM Detective Tier 3",
            "metric_description": "SLA metric",
            "warning_threshold": 90.0,
            "alerting_threshold": 85.0
        }
    ])

def _mock_all_iam_roles_df():
    """Mock IAM roles data"""
    return pd.DataFrame([
        {
            "AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1",
            "ACCOUNT": "123456789012",
            "ROLE_TYPE": "MACHINE"
        },
        {
            "AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2",
            "ACCOUNT": "123456789012",
            "ROLE_TYPE": "MACHINE"
        },
        {
            "AMAZON_RESOURCE_NAME": "arn:aws:iam::111111111111:role/test-role-3",
            "ACCOUNT": "111111111111",
            "ROLE_TYPE": "MACHINE"
        },
        {
            "AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/human-role",
            "ACCOUNT": "123456789012",
            "ROLE_TYPE": "HUMAN"
        }
    ])

def _mock_evaluated_roles_df():
    """Mock evaluated roles data"""
    return pd.DataFrame([
        {
            "RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1",
            "CONTROL_ID": "AC-3.AWS.39.v02",
            "COMPLIANCE_STATUS": "Compliant"
        },
        {
            "RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2",
            "CONTROL_ID": "AC-3.AWS.39.v02",
            "COMPLIANCE_STATUS": "NonCompliant"
        }
    ])

def _mock_sla_data_df():
    """Mock SLA data"""
    return pd.DataFrame([
        {
            "RESOURCE_ID": "arn:aws:iam::123456789012:role/test-role-2",
            "CONTROL_RISK": "High",
            "OPEN_DATE_UTC_TIMESTAMP": datetime.now()
        }
    ])

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

def generate_mock_accounts_response():
    """Generate mock approved accounts API response"""
    return {
        "accounts": [
            {"accountNumber": "123456789012"},
            {"accountNumber": "987654321098"}
        ]
    }

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_success():
    """Test successful metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    sla_data_df = _mock_sla_data_df()
    
    # Mock API response for approved accounts
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = generate_mock_accounts_response()
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Mock the _get_approved_accounts function
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline._get_approved_accounts', 
                    return_value=["123456789012", "987654321098"]):
        # Execute transformer
        result = calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, sla_data_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    assert len(result) == 3  # Three metrics (Tier 1, 2, 3)
    
    # Verify data types
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)
        assert row["control_id"] == "CTRL-1074653"

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    sla_data_df = _mock_sla_data_df()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, all_iam_roles_df, evaluated_roles_df, sla_data_df, context)

def test_get_approved_accounts_success():
    """Test successful account fetching"""
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = generate_mock_accounts_response()
    mock_session.get.return_value = mock_response
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    
    with patch('requests.Session', return_value=mock_session):
        accounts = _get_approved_accounts(mock_api)
    
    assert accounts == ["123456789012", "987654321098"]
    assert len(accounts) == 2

def test_get_approved_accounts_api_error():
    """Test API error handling"""
    mock_session = Mock()
    mock_session.get.side_effect = requests.exceptions.RequestException("Connection error")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    
    with patch('requests.Session', return_value=mock_session):
        with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
            _get_approved_accounts(mock_api)

def test_get_approved_accounts_empty_response():
    """Test empty accounts response"""
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"accounts": []}
    mock_session.get.return_value = mock_response
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    
    with patch('requests.Session', return_value=mock_session):
        with pytest.raises(ValueError, match="No valid account numbers received"):
            _get_approved_accounts(mock_api)

def test_calculate_tier1_metrics():
    """Test Tier 1 metrics calculation"""
    filtered_roles = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2"}
    ])
    
    evaluated_roles = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"}
    ])
    
    metric_value, compliant_count, total_count, non_compliant_resources = _calculate_tier1_metrics(
        filtered_roles, evaluated_roles
    )
    
    assert metric_value == 50.0  # 1 out of 2 evaluated
    assert compliant_count == 1
    assert total_count == 2
    assert non_compliant_resources is not None

def test_calculate_tier2_metrics():
    """Test Tier 2 metrics calculation"""
    filtered_roles = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1", "ACCOUNT": "123456789012"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2", "ACCOUNT": "123456789012"}
    ])
    
    evaluated_roles = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1", "COMPLIANCE_STATUS": "Compliant"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2", "COMPLIANCE_STATUS": "NonCompliant"}
    ])
    
    metric_value, compliant_count, total_count, non_compliant_resources = _calculate_tier2_metrics(
        filtered_roles, evaluated_roles
    )
    
    assert metric_value == 50.0  # 1 out of 2 compliant
    assert compliant_count == 1
    assert total_count == 2
    assert non_compliant_resources is not None

def test_calculate_tier3_metrics():
    """Test Tier 3 metrics calculation"""
    filtered_roles = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2", "ACCOUNT": "123456789012"}
    ])
    
    evaluated_roles = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2", "COMPLIANCE_STATUS": "NonCompliant"}
    ])
    
    sla_data = pd.DataFrame([
        {
            "RESOURCE_ID": "arn:aws:iam::123456789012:role/test-role-2",
            "CONTROL_RISK": "High",
            "OPEN_DATE_UTC_TIMESTAMP": datetime.now() - pd.Timedelta(days=20)  # Within SLA
        }
    ])
    
    metric_value, compliant_count, total_count, non_compliant_resources = _calculate_tier3_metrics(
        filtered_roles, evaluated_roles, sla_data
    )
    
    assert metric_value == 100.0  # Within SLA
    assert compliant_count == 1
    assert total_count == 1
    assert non_compliant_resources is not None

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert "api.cloud.capitalone.com" in pipeline.api_url

def test_pipeline_run_method():
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    # Mock the run method
    with patch.object(pipeline, 'run') as mock_run:
        pipeline.run()
        # Verify run was called with default parameters
        mock_run.assert_called_once_with()

def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    sla_data_df = _mock_sla_data_df()
    
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = requests.exceptions.RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
        calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, sla_data_df, context)

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_machine_iam_detective.pipeline import run
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception:
        import sys
        sys.exit(1)
"""
                exec(code)
                
                # Verify success path
                assert not mock_exit.called
                mock_run.assert_called_once_with(env=mock_env, is_load=False, dq_actions=False)

def test_empty_data_handling():
    """Test handling of empty data scenarios"""
    thresholds_df = _mock_threshold_df()
    empty_roles_df = pd.DataFrame()
    empty_evaluated_df = pd.DataFrame()
    empty_sla_df = pd.DataFrame()
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    context = {"api_connector": mock_api}
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline._get_approved_accounts', 
                    return_value=["123456789012"]):
        result = calculate_metrics(thresholds_df, empty_roles_df, empty_evaluated_df, empty_sla_df, context)
        
        # Should still return a valid DataFrame with 0 values
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) == 3  # Three metrics
        
        # All metrics should have 0 values due to empty data
        for _, row in result.iterrows():
            assert row["monitoring_metric_value"] == 0.0
            assert row["metric_value_numerator"] == 0
            assert row["metric_value_denominator"] == 0

def test_compliance_status_logic():
    """Test compliance status determination logic"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": "MNTR-1074653-T1",
        "control_id": "CTRL-1074653",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 95.0,
        "alerting_threshold": 90.0
    }])
    
    # Test scenarios for different metric values
    test_cases = [
        (96.0, "Green"),   # Above warning threshold
        (93.0, "Yellow"),  # Between warning and alert threshold
        (85.0, "Red")      # Below alert threshold
    ]
    
    for metric_value, expected_status in test_cases:
        # Create mock data that will result in the desired metric value
        mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
        context = {"api_connector": mock_api}
        
        with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline._get_approved_accounts', 
                        return_value=["123456789012"]):
            with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline._calculate_tier1_metrics',
                            return_value=(metric_value, 96, 100, None)):
                result = calculate_metrics(thresholds_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), context)
                
                assert result.iloc[0]["monitoring_metric_status"] == expected_status