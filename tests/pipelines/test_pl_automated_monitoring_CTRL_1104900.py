import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
from config_pipeline import ConfigPipeline

# Import pipeline components
from pipelines.pl_automated_monitoring_CTRL_1104900.pipeline import (
    PLAutomatedMonitoringCTRL1104900,
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
        "monitoring_metric_id": 1104900,
        "control_id": "CTRL-1104900",
        "monitoring_metric_tier": "Tier 2",
        "metric_name": "Symantec Proxy Test Success Rate",
        "metric_description": "Percentage of Symantec proxy tests that pass",
        "warning_threshold": 80.0,
        "alerting_threshold": 90.0,
        "control_executor": "Security Team"
    }])

def _mock_symantec_proxy_outcome_df():
    """Test data for Symantec Proxy test outcomes"""
    return pd.DataFrame([
        {
            "TEST_ID": "test_1",
            "TEST_NAME": "Symantec Test 1",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "blocked",
            "TEST_DESCRIPTION": "Test description 1"
        },
        {
            "TEST_ID": "test_2",
            "TEST_NAME": "Symantec Test 2",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "passed",
            "TEST_DESCRIPTION": "Test description 2"
        },
        {
            "TEST_ID": "test_3",
            "TEST_NAME": "Symantec Test 3",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "allowed",
            "ACTUAL_OUTCOME": "allowed",
            "TEST_DESCRIPTION": "Test description 3"
        },
        {
            "TEST_ID": "test_4",
            "TEST_NAME": "Symantec Test 4",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "blocked",
            "TEST_DESCRIPTION": "Test description 4"
        }
    ])

# Test pipeline initialization
def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    assert pipeline.env == env

# Test extract method (the main functionality)
@freeze_time("2024-11-05 12:09:00")
def test_extract_method_success():
    """Test extract method with successful data processing"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Mock ConfigPipeline.extract to return test data
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        # Setup the DataFrame that would come from SQL queries
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Execute extract method
        result_df = pipeline.extract()
        
        # Verify structure
        assert isinstance(result_df, pd.DataFrame)
        assert "monitoring_metrics" in result_df.columns
        assert len(result_df["monitoring_metrics"]) == 1
        
        # Extract the metrics DataFrame
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        assert isinstance(metrics_df, pd.DataFrame)
        assert not metrics_df.empty
        assert list(metrics_df.columns) == AVRO_SCHEMA_FIELDS
        assert len(metrics_df) == 1  # Single metric
        
        # Verify data types
        row = metrics_df.iloc[0]
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["monitoring_metric_value"], float)
        assert pd.api.types.is_integer_dtype(type(row["metric_value_numerator"]))
        assert pd.api.types.is_integer_dtype(type(row["metric_value_denominator"]))
        assert row["control_id"] == "CTRL-1104900"

def test_extract_method_empty_thresholds():
    """Test extract method with empty thresholds data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Mock ConfigPipeline.extract to return empty thresholds
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [pd.DataFrame()],  # Empty DataFrame
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should raise RuntimeError for empty thresholds
        with pytest.raises(RuntimeError, match="No threshold data found"):
            pipeline.extract()

def test_extract_method_symantec_proxy_calculations():
    """Test Symantec Proxy test success calculations"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should have exactly one metric
        assert len(metrics_df) == 1
        row = metrics_df.iloc[0]
        
        # Test data has 4 tests, 3 successful (75%)
        # test_1: blocked/blocked = success
        # test_2: blocked/passed = failure  
        # test_3: allowed/allowed = success
        # test_4: blocked/blocked = success
        assert row["monitoring_metric_value"] == 75.0
        assert row["metric_value_numerator"] == 3
        assert row["metric_value_denominator"] == 4
        assert row["control_id"] == "CTRL-1104900"
        assert row["monitoring_metric_id"] == 1104900

def test_extract_method_empty_test_data():
    """Test handling of empty Symantec Proxy test data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [pd.DataFrame()]  # Empty test data
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should still return a valid DataFrame with 0 values
        assert isinstance(metrics_df, pd.DataFrame)
        assert not metrics_df.empty
        assert len(metrics_df) == 1
        
        row = metrics_df.iloc[0]
        assert row["monitoring_metric_value"] == 0.0
        assert row["metric_value_numerator"] == 0
        assert row["metric_value_denominator"] == 0
        assert row["resources_info"] is not None  # Should have issue message

def test_extract_method_compliance_status_logic():
    """Test compliance status determination logic"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Test scenarios for different metric values
    test_cases = [
        (95.0, 90.0, 80.0, "Green"),   # Above alert threshold
        (85.0, 90.0, 80.0, "Yellow"),  # Between warning and alert
        (75.0, 90.0, 80.0, "Red")      # Below warning threshold
    ]
    
    for expected_value, alert_threshold, warning_threshold, expected_status in test_cases:
        # Create threshold data for this test case
        test_threshold = pd.DataFrame([{
            "monitoring_metric_id": 1104900,
            "control_id": "CTRL-1104900",
            "monitoring_metric_tier": "Tier 2",
            "metric_name": "Symantec Proxy Test Success Rate",
            "warning_threshold": warning_threshold,
            "alerting_threshold": alert_threshold
        }])
        
        # Create test data that will result in the desired success rate
        total_tests = 100
        successful_tests = int(expected_value)
        test_data = []
        
        # Create successful tests
        for i in range(successful_tests):
            test_data.append({
                "TEST_ID": f"test_{i}",
                "EXPECTED_OUTCOME": "blocked",
                "ACTUAL_OUTCOME": "blocked"
            })
        
        # Create failed tests
        for i in range(successful_tests, total_tests):
            test_data.append({
                "TEST_ID": f"test_{i}",
                "EXPECTED_OUTCOME": "blocked", 
                "ACTUAL_OUTCOME": "passed"
            })
        
        test_outcome_df = pd.DataFrame(test_data)
        
        with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [test_threshold],
                "symantec_proxy_outcome": [test_outcome_df]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            row = metrics_df.iloc[0]
            assert row["monitoring_metric_status"] == expected_status
            assert row["monitoring_metric_value"] == expected_value

def test_extract_method_failed_test_reporting():
    """Test that failed tests are properly reported in resources_info"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Create test data with some failures
    test_data = pd.DataFrame([
        {
            "TEST_ID": "success_test",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "blocked"
        },
        {
            "TEST_ID": "failed_test_1",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "passed"
        },
        {
            "TEST_ID": "failed_test_2", 
            "EXPECTED_OUTCOME": "allowed",
            "ACTUAL_OUTCOME": "blocked"
        }
    ])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [test_data]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        row = metrics_df.iloc[0]
        # Should be 33.33% (1 success out of 3 tests)
        assert row["monitoring_metric_value"] == 33.33
        assert row["metric_value_numerator"] == 1
        assert row["metric_value_denominator"] == 3
        
        # Should have resources_info with failed test details
        assert row["resources_info"] is not None
        assert len(row["resources_info"]) == 2  # Two failed tests
        
        # Verify failed test details are JSON strings
        for resource_json in row["resources_info"]:
            resource = json.loads(resource_json)
            assert "test_id" in resource
            assert "expected" in resource
            assert "actual" in resource

def test_extract_method_large_failure_list_truncation():
    """Test that large lists of failed tests are truncated properly"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Create test data with >100 failures to test truncation
    test_data = []
    
    # Add 105 failed tests
    for i in range(105):
        test_data.append({
            "TEST_ID": f"failed_test_{i}",
            "EXPECTED_OUTCOME": "blocked",
            "ACTUAL_OUTCOME": "passed"
        })
    
    test_outcome_df = pd.DataFrame(test_data)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [test_outcome_df]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        row = metrics_df.iloc[0]
        assert row["monitoring_metric_value"] == 0.0  # All failed
        
        # Should have resources_info truncated to 101 items (100 + 1 truncation message)
        assert row["resources_info"] is not None
        assert len(row["resources_info"]) == 101
        
        # Last item should be truncation message
        truncation_message = json.loads(row["resources_info"][-1])
        assert "message" in truncation_message
        assert "5 more failed tests" in truncation_message["message"]

def test_extract_method_data_types_validation():
    """Test that output data types match requirements"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Verify data types match CLAUDE.md requirements
        row = metrics_df.iloc[0]
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["control_id"], str)
        assert isinstance(row["monitoring_metric_id"], (int, float))
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["monitoring_metric_status"], str)
        assert pd.api.types.is_integer_dtype(type(row["metric_value_numerator"]))
        assert pd.api.types.is_integer_dtype(type(row["metric_value_denominator"]))
        # resources_info can be None or list
        assert row["resources_info"] is None or isinstance(row["resources_info"], list)

def test_extract_method_metric_value_ranges():
    """Test that metric values are within expected ranges"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Verify metric values are within valid ranges (0-100%)
        row = metrics_df.iloc[0]
        assert 0.0 <= row["monitoring_metric_value"] <= 100.0
        assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]
        assert row["metric_value_numerator"] >= 0
        assert row["metric_value_denominator"] >= 0

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_timestamp_consistency():
    """Test that timestamps are consistent"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Timestamp should match frozen time
        row = metrics_df.iloc[0]
        assert row["control_monitoring_utc_timestamp"] == datetime(2024, 11, 5, 12, 9, 0)

def test_extract_method_control_id_consistency():
    """Test that control ID is consistent"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Control ID should be CTRL-1104900
        row = metrics_df.iloc[0]
        assert row["control_id"] == "CTRL-1104900"

def test_extract_method_metric_id_consistency():
    """Test that metric ID matches expected value"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should have exactly the expected metric ID
        row = metrics_df.iloc[0]
        assert row["monitoring_metric_id"] == 1104900

def test_extract_method_data_type_enforcement():
    """Test that extract method enforces correct data types"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [_mock_symantec_proxy_outcome_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Check that data types are enforced correctly
        assert metrics_df["metric_value_numerator"].dtype == "int64"
        assert metrics_df["metric_value_denominator"].dtype == "int64" 
        assert metrics_df["monitoring_metric_value"].dtype == "float64"

def test_extract_method_no_test_data():
    """Test handling when there is no test data available"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "symantec_proxy_outcome": [pd.DataFrame()]  # Empty
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        row = metrics_df.iloc[0]
        assert row["monitoring_metric_value"] == 0.0
        assert row["monitoring_metric_status"] == "Red"
        assert row["resources_info"] is not None
        
        # Should contain appropriate error message
        issue_message = json.loads(row["resources_info"][0])
        assert "issue" in issue_message
        assert "No Symantec Proxy outcome data available" in issue_message["issue"]

# Test run function variations
def test_run_function_normal_path():
    """Test run function normal execution path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAutomatedMonitoringCTRL1104900') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)

def test_run_function_export_test_data():
    """Test run function with export test data path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAutomatedMonitoringCTRL1104900') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=False)

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_CTRL_1104900.pipeline import run
    
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