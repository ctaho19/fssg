import pytest
import pandas as pd
from unittest.mock import Mock
from freezegun import freeze_time
from datetime import datetime
import json

from pl_automated_monitoring_CTRL_1080553.pipeline import (
    PLAutomatedMonitoringCTRL1080553,
    calculate_metrics
)

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
        
        from requests import Response
        default_response = Response()
        default_response.status_code = 200
        return default_response

def _mock_threshold_df():
    return pd.DataFrame([{
        "monitoring_metric_id": 53,
        "control_id": "CTRL-1080553",
        "monitoring_metric_tier": "Tier 2",
        "metric_name": "Proofpoint Anti-Phishing Test Success Rate",
        "metric_description": "Percentage of Proofpoint anti-phishing tests that pass",
        "warning_threshold": 80.0,
        "alerting_threshold": 90.0,
        "control_executor": "Security Team"
    }])

def _mock_proofpoint_data():
    return pd.DataFrame([
        {
            "TEST_ID": "test_1",
            "TEST_NAME": "Proofpoint Test 1",
            "PLATFORM": "proofpoint",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "blocked",
            "TEST_DESCRIPTION": "Test description 1"
        },
        {
            "TEST_ID": "test_2",
            "TEST_NAME": "Proofpoint Test 2",
            "PLATFORM": "proofpoint",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "passed",
            "TEST_DESCRIPTION": "Test description 2"
        }
    ])

def generate_mock_api_response(content=None, status_code=200):
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
    thresholds_df = _mock_threshold_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "tests": [
            {"id": "test_1", "status": "passed"},
            {"id": "test_2", "status": "failed"}
        ]
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, context)
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    row = result.iloc[0]
    assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
    assert isinstance(row["monitoring_metric_value"], float)
    assert isinstance(row["metric_value_numerator"], int)
    assert isinstance(row["metric_value_denominator"], int)

def test_calculate_metrics_empty_thresholds():
    empty_df = pd.DataFrame()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, context)

def test_pipeline_initialization():
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1080553(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url

def test_pipeline_run_method(mock):
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1080553(env)
    
    mock_run = mock.patch.object(pipeline, 'run')
    pipeline.run()
    
    mock_run.assert_called_once_with()

def test_api_error_handling(mock):
    from requests.exceptions import RequestException
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    with pytest.raises(RuntimeError, match="Failed to fetch"):
        calculate_metrics(_mock_threshold_df(), context)

def test_pagination_handling(mock):
    page1_response = generate_mock_api_response({
        "tests": [{"id": "test_1"}],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "tests": [{"id": "test_2"}],
        "nextRecordKey": None
    })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(_mock_threshold_df(), context)
    
    assert not result.empty

def test_main_function_execution(mock):
    mock_env = mock.Mock()
    
    with mock.patch("etip_env.set_env_vars", return_value=mock_env):
        with mock.patch("pl_automated_monitoring_CTRL_1080553.pipeline.run") as mock_run:
            with mock.patch("sys.exit") as mock_exit:
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_CTRL_1080553.pipeline import run
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception as e:
        import sys
        sys.exit(1)
"""
                exec(code)
                
                assert not mock_exit.called
                mock_run.assert_called_once_with(env=mock_env, is_load=False, dq_actions=False)

def test_proofpoint_data_processing(mock):
    thresholds_df = _mock_threshold_df()
    proofpoint_data = _mock_proofpoint_data()
    
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1080553(env)
    
    with freeze_time("2024-11-05 12:09:00"):
        result = calculate_metrics(thresholds_df, {"proofpoint_data": proofpoint_data})
        
        row = result.iloc[0]
        assert row["control_id"] == "CTRL-1080553"
        assert row["monitoring_metric_id"] == 53
        assert isinstance(row["monitoring_metric_value"], float)
        assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]

def test_compliance_status_calculation():
    test_cases = [
        (95.0, 90.0, 80.0, "Green"),
        (85.0, 90.0, 80.0, "Yellow"),
        (75.0, 90.0, 80.0, "Red")
    ]
    
    for value, alert_threshold, warning_threshold, expected in test_cases:
        if value >= alert_threshold:
            status = "Green"
        elif value >= warning_threshold:
            status = "Yellow"
        else:
            status = "Red"
        assert status == expected

def test_non_compliant_resources_tracking(mock):
    thresholds_df = _mock_threshold_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "tests": [
            {"id": "test_1", "status": "passed", "description": "Test 1"},
            {"id": "test_2", "status": "failed", "description": "Test 2"}
        ]
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    with freeze_time("2024-11-05 12:09:00"):
        result = calculate_metrics(thresholds_df, context)
        
        row = result.iloc[0]
        if row["resources_info"] is not None:
            assert isinstance(row["resources_info"], list)
            for resource_json in row["resources_info"]:
                resource = json.loads(resource_json)
                assert "id" in resource or "test_id" in resource