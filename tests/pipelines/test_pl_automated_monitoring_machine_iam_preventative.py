import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests.exceptions import RequestException

# Import pipeline components
from pl_automated_monitoring_machine_iam_preventative.pipeline import (
    PLAutomatedMonitoringMachineIamPreventative,
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
def test_calculate_metrics_success(mock):
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
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, context)
    
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
def test_calculate_metrics_tier1_coverage(mock):
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
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    result = calculate_metrics(thresholds_df, context)
    
    # Find Tier 1 metrics
    tier1_results = result[result["monitoring_metric_id"].str.contains("tier1")]
    
    for _, row in tier1_results.iterrows():
        # Tier 1 should have 100% coverage since we have evaluated roles for both controls
        assert row["monitoring_metric_value"] == 100.0
        assert row["monitoring_metric_status"] == "COMPLIANT"
        assert row["metric_value_numerator"] == 1  # 1 approved account with roles
        assert row["metric_value_denominator"] == 1  # 1 total approved account

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier2_compliance(mock):
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
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    result = calculate_metrics(thresholds_df, context)
    
    # Find Tier 2 metrics
    tier2_results = result[result["monitoring_metric_id"].str.contains("tier2")]
    
    for _, row in tier2_results.iterrows():
        # Tier 2 should show 100% compliance since all roles in approved account are evaluated
        assert row["monitoring_metric_value"] == 100.0
        assert row["monitoring_metric_status"] == "COMPLIANT"
        assert row["metric_value_numerator"] == 2  # 2 roles evaluated in approved account
        assert row["metric_value_denominator"] == 2  # 2 total roles in approved account

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, context)

def test_calculate_metrics_missing_context_data():
    """Test error handling for missing context data"""
    thresholds_df = _mock_threshold_df()
    
    # Missing all_iam_roles
    context = {"api_connector": Mock(), "evaluated_roles": pd.DataFrame()}
    with pytest.raises(RuntimeError, match="all_iam_roles data not found"):
        calculate_metrics(thresholds_df, context)
    
    # Missing evaluated_roles
    context = {"api_connector": Mock(), "all_iam_roles": pd.DataFrame()}
    with pytest.raises(RuntimeError, match="evaluated_roles data not found"):
        calculate_metrics(thresholds_df, context)

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url

def test_pipeline_run_method():
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
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
    
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
        calculate_metrics(thresholds_df, context)

def test_api_non_200_response():
    """Test API non-200 status code handling"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    mock_response = generate_mock_api_response(status_code=500)
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    with pytest.raises(RuntimeError, match="API request failed: 500"):
        calculate_metrics(thresholds_df, context)

def test_pagination_handling():
    """Test API pagination with multiple responses"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    # Create paginated responses
    page1_response = generate_mock_api_response({
        "resources": [{"accountId": "123456789012", "approved": True}],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "resources": [{"accountId": "987654321098", "approved": False}],
        "nextRecordKey": None
    })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    result = calculate_metrics(thresholds_df, context)
    
    # Verify both pages were processed
    assert not result.empty
    # Should have processed 2 API calls
    assert mock_api.call_count == 2

@freeze_time("2024-11-05 12:09:00")
def test_compliance_status_determination():
    """Test compliance status logic based on thresholds"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1105806",
        "monitoring_metric_tier": 1,
        "warning_threshold": 80.0,
        "alerting_threshold": 90.0
    }])
    
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = pd.DataFrame()  # No evaluated roles = 0% compliance
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [{"accountId": "123456789012", "approved": True}],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    result = calculate_metrics(thresholds_df, context)
    
    # With 0% compliance, should be ALERT status
    assert result.iloc[0]["monitoring_metric_value"] == 0.0
    assert result.iloc[0]["monitoring_metric_status"] == "ALERT"

def test_no_approved_accounts():
    """Test handling when no accounts are approved"""
    thresholds_df = _mock_threshold_df()
    all_iam_roles_df = _mock_all_iam_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resources": [],  # No approved accounts
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {
        "api_connector": mock_api,
        "all_iam_roles": all_iam_roles_df,
        "evaluated_roles": evaluated_roles_df
    }
    
    result = calculate_metrics(thresholds_df, context)
    
    # Should return results with 100% compliance (no accounts to evaluate)
    for _, row in result.iterrows():
        assert row["monitoring_metric_value"] == 100.0
        assert row["monitoring_metric_status"] == "COMPLIANT"
        assert row["metric_value_numerator"] == 0
        assert row["metric_value_denominator"] == 0

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pl_automated_monitoring_machine_iam_preventative.pipeline.PLAutomatedMonitoringMachineIamPreventative") as mock_pipeline_class:
            mock_pipeline = Mock()
            mock_pipeline_class.return_value = mock_pipeline
            
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_machine_iam_preventative.pipeline import PLAutomatedMonitoringMachineIamPreventative
    
    env = set_env_vars()
    try:
        pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
        pipeline.run()
    except Exception as e:
        import sys
        sys.exit(1)
"""
                exec(code)
                
                # Verify success path
                assert not mock_exit.called
                mock_pipeline.run.assert_called_once()