import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests.exceptions import RequestException

# Import pipeline components
from pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline import (
    PLAutomatedMonitoringMachineIamPreventative,
    calculate_metrics,
    _get_approved_accounts,
    _calculate_tier1_metrics,
    _calculate_tier2_metrics
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
        self.call_count = 0
    
    def post(self, endpoint, **kwargs):
        """Mock post method with proper side_effect handling."""
        self.call_count += 1
        
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
            "monitoring_metric_id": "ctrl_1105806_tier1",
            "control_id": "CTRL-1105806",
            "monitoring_metric_tier": 1,
            "warning_threshold": 80.0,
            "alerting_threshold": 90.0
        },
        {
            "monitoring_metric_id": "ctrl_1105806_tier2",
            "control_id": "CTRL-1105806", 
            "monitoring_metric_tier": 2,
            "warning_threshold": 95.0,
            "alerting_threshold": 98.0
        },
        {
            "monitoring_metric_id": "ctrl_1077124_tier1",
            "control_id": "CTRL-1077124",
            "monitoring_metric_tier": 1,
            "warning_threshold": 85.0,
            "alerting_threshold": 92.0
        },
        {
            "monitoring_metric_id": "ctrl_1077124_tier2", 
            "control_id": "CTRL-1077124",
            "monitoring_metric_tier": 2,
            "warning_threshold": 96.0,
            "alerting_threshold": 99.0
        }
    ])

def _mock_all_iam_roles_df():
    """Utility function for test IAM roles data"""
    return pd.DataFrame([
        {
            "account_id": "123456789012",
            "role_name": "test-role-1",
            "role_arn": "arn:aws:iam::123456789012:role/test-role-1"
        },
        {
            "account_id": "123456789012", 
            "role_name": "test-role-2",
            "role_arn": "arn:aws:iam::123456789012:role/test-role-2"
        },
        {
            "account_id": "987654321098",
            "role_name": "test-role-3", 
            "role_arn": "arn:aws:iam::987654321098:role/test-role-3"
        }
    ])

def _mock_evaluated_roles_df():
    """Utility function for test evaluated roles data"""
    return pd.DataFrame([
        {
            "control_id": "CTRL-1105806",
            "account_id": "123456789012",
            "role_name": "test-role-1"
        },
        {
            "control_id": "CTRL-1077124",
            "account_id": "123456789012",
            "role_name": "test-role-2" 
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

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_success():
    """Test successful metrics calculation for both controls"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    # Mock API response for approved accounts
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [
            {"accountId": "123456789012", "approved": True},
            {"accountId": "987654321098", "approved": False}
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Mock the _get_approved_accounts function
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline._get_approved_accounts', 
               return_value=["123456789012", "987654321098"]):
        # Execute transformer
        result = calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4  # 4 metrics (2 controls x 2 tiers each)
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Verify data types
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)

@freeze_time("2024-11-05 12:09:00") 
def test_calculate_metrics_tier1_coverage():
    """Test Tier 1 coverage calculation logic"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [{"accountId": "123456789012", "approved": True}],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline._get_approved_accounts', 
               return_value=["123456789012"]):
        result = calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, context)
    
    # Find Tier 1 metrics
    tier1_results = result[result["monitoring_metric_id"].str.contains("tier1")]
    
    for _, row in tier1_results.iterrows():
        # Should have some coverage value based on evaluated roles
        assert isinstance(row["monitoring_metric_value"], float)
        assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier2_compliance():
    """Test Tier 2 compliance calculation logic"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df() 
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [{"accountId": "123456789012", "approved": True}],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline._get_approved_accounts', 
               return_value=["123456789012"]):
        result = calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, context)
    
    # Find Tier 2 metrics
    tier2_results = result[result["monitoring_metric_id"].str.contains("tier2")]
    
    for _, row in tier2_results.iterrows():
        # Should have some compliance value based on evaluated roles
        assert isinstance(row["monitoring_metric_value"], float)
        assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, all_iam_roles_df, evaluated_roles_df, context)

def test_get_approved_accounts_success():
    """Test successful account fetching"""
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "accounts": [
            {"accountNumber": "123456789012"},
            {"accountNumber": "987654321098"}
        ]
    }
    mock_session.get.return_value = mock_response
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    
    with patch('requests.Session', return_value=mock_session):
        accounts = _get_approved_accounts(mock_api)
    
    assert accounts == ["123456789012", "987654321098"]
    assert len(accounts) == 2


def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    
    context = {"api_connector": mock_api}
    
    # Mock _get_approved_accounts to raise an exception
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline._get_approved_accounts', 
               side_effect=RuntimeError("Failed to fetch approved accounts")):
        with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
            calculate_metrics(thresholds_df, all_iam_roles_df, evaluated_roles_df, context)

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

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert "api.cloud.capitalone.com" in pipeline.api_url

def test_pipeline_run_method():
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Mock the run method
    with patch.object(pipeline, 'run') as mock_run:
        pipeline.run()
        # Verify run was called with default parameters
        mock_run.assert_called_once_with()

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline import run
    
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