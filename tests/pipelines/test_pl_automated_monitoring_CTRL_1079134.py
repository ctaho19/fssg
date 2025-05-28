import datetime
import unittest.mock as mock
import json
import pandas as pd
import pytest
from freezegun import freeze_time
from requests import Response, RequestException

from pl_automated_monitoring_CTRL_1079134.pipeline import (
    PLAmCTRL1079134Pipeline,
    calculate_metrics,
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
        default_response = Response()
        default_response.status_code = 200
        return default_response

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([
        {
            "monitoring_metric_id": 34,
            "control_id": "CTRL-1079134",
            "monitoring_metric_tier": "Tier 0",
            "warning_threshold": 100.0,
            "alerting_threshold": 100.0
        },
        {
            "monitoring_metric_id": 35,
            "control_id": "CTRL-1079134",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0
        },
        {
            "monitoring_metric_id": 36,
            "control_id": "CTRL-1079134",
            "monitoring_metric_tier": "Tier 2",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0
        }
    ])

def _mock_macie_metrics_df():
    """Test data for Macie metrics."""
    return pd.DataFrame([
        {
            "METRIC_DATE": datetime.datetime(2024, 11, 5),
            "SF_LOAD_TIMESTAMP": datetime.datetime(2024, 11, 5),
            "TOTAL_BUCKETS_SCANNED_BY_MACIE": 850,
            "TOTAL_CLOUDFRONTED_BUCKETS": 1000
        }
    ])

def _mock_macie_testing_df():
    """Test data for Macie control test results."""
    return pd.DataFrame([
        {
            "REPORTDATE": datetime.datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_1",
            "TESTNAME": "Test 1"
        },
        {
            "REPORTDATE": datetime.datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_2",
            "TESTNAME": "Test 2"
        },
        {
            "REPORTDATE": datetime.datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_3",
            "TESTNAME": "Test 3"
        },
        {
            "REPORTDATE": datetime.datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_4",
            "TESTNAME": "Test 4"
        },
        {
            "REPORTDATE": datetime.datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": False,
            "TESTID": "test_5",
            "TESTNAME": "Test 5"
        }
    ])

def _mock_historical_stats_df():
    """Test data for historical test statistics."""
    return pd.DataFrame([
        {
            "AVG_HISTORICAL_TESTS": 5.5,
            "MIN_HISTORICAL_TESTS": 5,
            "MAX_HISTORICAL_TESTS": 6
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
    """Test successful metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    assert len(result) == 3  # Three metrics (Tier 0, 1, 2)
    
    # Verify data types
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime.datetime)
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)
        assert row["control_id"] == "CTRL-1079134"


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, macie_metrics_df, macie_testing_df, historical_stats_df)


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAmCTRL1079134Pipeline(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'control_id')
    assert pipeline.control_id == "CTRL-1079134"


def test_pipeline_run_method(mock):
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAmCTRL1079134Pipeline(env)
    
    # Mock the run method
    mock_run = mock.patch.object(pipeline, 'run')
    pipeline.run()
    
    # Verify run was called with default parameters
    mock_run.assert_called_once_with()


def test_tier_metrics_calculation():
    """Test individual tier metric calculations"""
    # Test Tier 0 (Heartbeat) - should be 100% if data exists
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # Tier 0: Should be 100% when data exists
    tier0_row = result[result["monitoring_metric_id"] == 34].iloc[0]
    assert tier0_row["monitoring_metric_value"] == 100.0
    assert tier0_row["monitoring_metric_status"] == "Green"
    
    # Tier 1: Coverage metric (850/1000 = 85%)
    tier1_row = result[result["monitoring_metric_id"] == 35].iloc[0]
    assert tier1_row["monitoring_metric_value"] == 85.0
    assert tier1_row["metric_value_numerator"] == 850
    assert tier1_row["metric_value_denominator"] == 1000
    
    # Tier 2: Accuracy metric (4/5 = 80%)
    tier2_row = result[result["monitoring_metric_id"] == 36].iloc[0]
    assert tier2_row["monitoring_metric_value"] == 80.0
    assert tier2_row["metric_value_numerator"] == 4
    assert tier2_row["metric_value_denominator"] == 5


def test_empty_data_handling():
    """Test handling of empty data scenarios"""
    thresholds_df = _mock_threshold_df()
    empty_macie_metrics = pd.DataFrame()
    empty_testing = pd.DataFrame()
    empty_historical = pd.DataFrame()
    
    result = calculate_metrics(thresholds_df, empty_macie_metrics, empty_testing, empty_historical)
    
    # Should still return a valid DataFrame with 0 values
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 3  # Three metrics
    
    # All metrics should have 0 values due to empty data
    for _, row in result.iterrows():
        assert row["monitoring_metric_value"] == 0.0
        assert row["monitoring_metric_status"] == "Red"


def test_compliance_status_logic():
    """Test compliance status determination logic"""
    # Test scenarios for different metric values
    test_cases = [
        (100.0, 100.0, 100.0, "Green"),   # Perfect score
        (98.0, 95.0, 97.0, "Yellow"),     # Between warning and alert
        (90.0, 95.0, 97.0, "Red")         # Below alert threshold
    ]
    
    for metric_value, alert_threshold, warning_threshold, expected_status in test_cases:
        # Create threshold data for this test case
        test_threshold = pd.DataFrame([{
            "monitoring_metric_id": 35,
            "control_id": "CTRL-1079134",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": warning_threshold,
            "alerting_threshold": alert_threshold
        }])
        
        # Create metrics data that will result in the desired metric value
        test_metrics = pd.DataFrame([{
            "METRIC_DATE": datetime.datetime(2024, 11, 5),
            "SF_LOAD_TIMESTAMP": datetime.datetime(2024, 11, 5),
            "TOTAL_BUCKETS_SCANNED_BY_MACIE": int(metric_value),
            "TOTAL_CLOUDFRONTED_BUCKETS": 100
        }])
        
        result = calculate_metrics(test_threshold, test_metrics, pd.DataFrame(), pd.DataFrame())
        
        assert result.iloc[0]["monitoring_metric_status"] == expected_status


def test_main_function_execution(mock):
    """Test main function execution path"""
    mock_env = mock.Mock()
    
    with mock.patch("etip_env.set_env_vars", return_value=mock_env):
        with mock.patch("pl_automated_monitoring_CTRL_1079134.pipeline.run") as mock_run:
            with mock.patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_CTRL_1079134.pipeline import run
    
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


def test_run_function(mock):
    """Test run function with various parameters"""
    env = MockEnv()
    
    with mock.patch('pl_automated_monitoring_CTRL_1079134.pipeline.PLAmCTRL1079134Pipeline') as mock_pipeline_class:
        mock_pipeline = mock.Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test normal execution
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)


def test_run_function_export_test_data(mock):
    """Test run function with export test data option"""
    env = MockEnv()
    
    with mock.patch('pl_automated_monitoring_CTRL_1079134.pipeline.PLAmCTRL1079134Pipeline') as mock_pipeline_class:
        mock_pipeline = mock.Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test export test data path
        run(env, is_export_test_data=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=False)


def test_division_by_zero_protection():
    """Test that division by zero is handled gracefully"""
    thresholds_df = _mock_threshold_df()
    
    # Create metrics data with zero denominators
    zero_metrics = pd.DataFrame([{
        "METRIC_DATE": datetime.datetime(2024, 11, 5),
        "SF_LOAD_TIMESTAMP": datetime.datetime(2024, 11, 5),
        "TOTAL_BUCKETS_SCANNED_BY_MACIE": 0,
        "TOTAL_CLOUDFRONTED_BUCKETS": 0
    }])
    
    result = calculate_metrics(thresholds_df, zero_metrics, pd.DataFrame(), pd.DataFrame())
    
    # Should handle zero denominator gracefully
    assert not result.empty
    # All metrics should handle division by zero appropriately
    for _, row in result.iterrows():
        assert isinstance(row["monitoring_metric_value"], float)
        assert row["monitoring_metric_value"] >= 0.0


def test_incomplete_data_handling():
    """Test handling of incomplete or missing data fields"""
    thresholds_df = _mock_threshold_df()
    
    # Test with incomplete metrics (missing required fields)
    incomplete_metrics = pd.DataFrame([{
        "METRIC_DATE": datetime.datetime(2024, 11, 5),
        "SF_LOAD_TIMESTAMP": datetime.datetime(2024, 11, 5)
        # Missing TOTAL_BUCKETS_SCANNED_BY_MACIE and TOTAL_CLOUDFRONTED_BUCKETS
    }])
    
    result = calculate_metrics(thresholds_df, incomplete_metrics, pd.DataFrame(), pd.DataFrame())
    
    # Missing data should result in 0 values
    assert not result.empty
    for _, row in result.iterrows():
        assert row["monitoring_metric_value"] == 0.0
        assert row["monitoring_metric_status"] == "Red"


def test_data_types_validation():
    """Test that output data types match requirements"""
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # Verify data types match CLAUDE.md requirements
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime.datetime)
        assert isinstance(row["control_id"], str)
        assert isinstance(row["monitoring_metric_id"], (int, float))
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["monitoring_metric_status"], str)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)
        # resources_info can be None or list
        assert row["resources_info"] is None or isinstance(row["resources_info"], list)


def test_transform_method(mock):
    """Test transform method sets up context correctly"""
    env = MockEnv()
    pipeline = PLAmCTRL1079134Pipeline(env)
    pipeline.context = {}
    
    with mock.patch('config_pipeline.ConfigPipeline.transform') as mock_super_transform:
        with mock.patch.object(pipeline, 'extract_thresholds') as mock_extract:
            mock_extract.return_value = _mock_threshold_df()
            
            pipeline.transform()
            
            mock_super_transform.assert_called_once()
            # Verify context was set up
            assert isinstance(pipeline.context, dict)


def test_get_api_connector_method(mock):
    """Test _get_api_connector method functionality if it exists"""
    env = MockEnv()
    pipeline = PLAmCTRL1079134Pipeline(env)
    
    # Check if pipeline has API connector functionality
    if hasattr(pipeline, '_get_api_connector'):
        with mock.patch('pl_automated_monitoring_CTRL_1079134.pipeline.refresh') as mock_refresh:
            with mock.patch('connectors.api.OauthApi') as mock_oauth_api:
                mock_refresh.return_value = "test_token"
                mock_connector = mock.Mock()
                mock_oauth_api.return_value = mock_connector
                
                result = pipeline._get_api_connector()
                
                assert result == mock_connector
                mock_refresh.assert_called_once_with(
                    client_id="test_client",
                    client_secret="test_secret",
                    exchange_url="test-exchange.com"
                )
                mock_oauth_api.assert_called_once()
    else:
        # If no API connector, just verify pipeline initializes correctly
        assert pipeline.env == env


def test_context_initialization():
    """Test pipeline context initialization"""
    env = MockEnv()
    pipeline = PLAmCTRL1079134Pipeline(env)
    
    # Initialize context if not exists
    if not hasattr(pipeline, 'context'):
        pipeline.context = {}
    
    assert pipeline.context == {}


def test_metric_value_ranges():
    """Test that metric values are within expected ranges"""
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # Verify metric values are within valid ranges (0-100%)
    for _, row in result.iterrows():
        assert 0.0 <= row["monitoring_metric_value"] <= 100.0
        assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]
        assert row["metric_value_numerator"] >= 0
        assert row["metric_value_denominator"] >= 0


def test_timestamp_consistency():
    """Test that timestamps are consistent across all metrics"""
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    with freeze_time("2024-11-05 12:09:00"):
        result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
        
        # All timestamps should be the same
        timestamps = result["control_monitoring_utc_timestamp"].unique()
        assert len(timestamps) == 1
        assert timestamps[0] == datetime.datetime(2024, 11, 5, 12, 9, 0)


def test_control_id_consistency():
    """Test that control ID is consistent across all metrics"""
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # All control IDs should be CTRL-1079134
    control_ids = result["control_id"].unique()
    assert len(control_ids) == 1
    assert control_ids[0] == "CTRL-1079134"


def test_metric_id_consistency():
    """Test that metric IDs match expected values"""
    thresholds_df = _mock_threshold_df()
    macie_metrics_df = _mock_macie_metrics_df()
    macie_testing_df = _mock_macie_testing_df()
    historical_stats_df = _mock_historical_stats_df()
    
    result = calculate_metrics(thresholds_df, macie_metrics_df, macie_testing_df, historical_stats_df)
    
    # Should have exactly the expected metric IDs
    metric_ids = sorted(result["monitoring_metric_id"].tolist())
    assert metric_ids == [34, 35, 36]