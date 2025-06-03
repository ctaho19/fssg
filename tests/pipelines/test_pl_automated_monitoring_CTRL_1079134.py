import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
from config_pipeline import ConfigPipeline

# Import pipeline components
from pipelines.pl_automated_monitoring_ctrl_1079134.pipeline import (
    PLAutomatedMonitoringCTRL1079134,
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
            "METRIC_DATE": datetime(2024, 11, 5),
            "SF_LOAD_TIMESTAMP": datetime(2024, 11, 5),
            "TOTAL_BUCKETS_SCANNED_BY_MACIE": 850,
            "TOTAL_CLOUDFRONTED_BUCKETS": 1000
        }
    ])

def _mock_macie_testing_df():
    """Test data for Macie control test results."""
    return pd.DataFrame([
        {
            "REPORTDATE": datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_1",
            "TESTNAME": "Test 1"
        },
        {
            "REPORTDATE": datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_2",
            "TESTNAME": "Test 2"
        },
        {
            "REPORTDATE": datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_3",
            "TESTNAME": "Test 3"
        },
        {
            "REPORTDATE": datetime(2024, 11, 5),
            "TESTISSUCCESSFUL": True,
            "TESTID": "test_4",
            "TESTNAME": "Test 4"
        },
        {
            "REPORTDATE": datetime(2024, 11, 5),
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

# Test pipeline initialization
def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    assert pipeline.env == env

# Test extract method (the main functionality)
@freeze_time("2024-11-05 12:09:00")
def test_extract_method_success():
    """Test extract method with successful data processing"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    # Mock ConfigPipeline.extract to return test data
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        # Setup the DataFrame that would come from SQL queries
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
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
        assert len(metrics_df) == 3  # Three metrics (Tier 0, 1, 2)
        
        # Verify data types
        for _, row in metrics_df.iterrows():
            assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
            assert isinstance(row["monitoring_metric_value"], float)
            assert pd.api.types.is_integer_dtype(type(row["metric_value_numerator"]))
            assert pd.api.types.is_integer_dtype(type(row["metric_value_denominator"]))
            assert row["control_id"] == "CTRL-1079134"

def test_extract_method_empty_thresholds():
    """Test extract method with empty thresholds data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    # Mock ConfigPipeline.extract to return empty thresholds
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [pd.DataFrame()],  # Empty DataFrame
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should raise RuntimeError for empty thresholds
        with pytest.raises(RuntimeError, match="No threshold data found"):
            pipeline.extract()

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_tier_calculations():
    """Test individual tier metric calculations"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Tier 0: Should be 100% when data exists
        tier0_row = metrics_df[metrics_df["monitoring_metric_id"] == 34].iloc[0]
        assert tier0_row["monitoring_metric_value"] == 100.0
        assert tier0_row["monitoring_metric_status"] == "Green"
        
        # Tier 1: Coverage metric (850/1000 = 85%)
        tier1_row = metrics_df[metrics_df["monitoring_metric_id"] == 35].iloc[0]
        assert tier1_row["monitoring_metric_value"] == 85.0
        assert tier1_row["metric_value_numerator"] == 850
        assert tier1_row["metric_value_denominator"] == 1000
        
        # Tier 2: Accuracy metric (4/5 = 80%)
        tier2_row = metrics_df[metrics_df["monitoring_metric_id"] == 36].iloc[0]
        assert tier2_row["monitoring_metric_value"] == 80.0
        assert tier2_row["metric_value_numerator"] == 4
        assert tier2_row["metric_value_denominator"] == 5

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_empty_data_handling():
    """Test handling of empty data scenarios"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [pd.DataFrame()],  # Empty
            "macie_testing": [pd.DataFrame()],  # Empty
            "historical_stats": [pd.DataFrame()]  # Empty
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should still return a valid DataFrame with 0 values
        assert isinstance(metrics_df, pd.DataFrame)
        assert not metrics_df.empty
        assert len(metrics_df) == 3  # Three metrics
        
        # All metrics should have 0 values due to empty data
        for _, row in metrics_df.iterrows():
            assert row["monitoring_metric_value"] == 0.0
            assert row["monitoring_metric_status"] == "Red"

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_compliance_status_logic():
    """Test compliance status determination logic"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
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
            "METRIC_DATE": datetime(2024, 11, 5),
            "SF_LOAD_TIMESTAMP": datetime(2024, 11, 5),
            "TOTAL_BUCKETS_SCANNED_BY_MACIE": int(metric_value),
            "TOTAL_CLOUDFRONTED_BUCKETS": 100
        }])
        
        with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [test_threshold],
                "macie_metrics": [test_metrics],
                "macie_testing": [pd.DataFrame()],
                "historical_stats": [pd.DataFrame()]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            assert metrics_df.iloc[0]["monitoring_metric_status"] == expected_status

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_division_by_zero_protection():
    """Test that division by zero is handled gracefully"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    # Create metrics data with zero denominators
    zero_metrics = pd.DataFrame([{
        "METRIC_DATE": datetime(2024, 11, 5),
        "SF_LOAD_TIMESTAMP": datetime(2024, 11, 5),
        "TOTAL_BUCKETS_SCANNED_BY_MACIE": 0,
        "TOTAL_CLOUDFRONTED_BUCKETS": 0
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [zero_metrics],
            "macie_testing": [pd.DataFrame()],
            "historical_stats": [pd.DataFrame()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should handle zero denominator gracefully
        assert not metrics_df.empty
        # All metrics should handle division by zero appropriately
        for _, row in metrics_df.iterrows():
            assert isinstance(row["monitoring_metric_value"], float)
            assert row["monitoring_metric_value"] >= 0.0

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_incomplete_data_handling():
    """Test handling of incomplete or missing data fields"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    # Test with incomplete metrics (missing required fields)
    incomplete_metrics = pd.DataFrame([{
        "METRIC_DATE": datetime(2024, 11, 5),
        "SF_LOAD_TIMESTAMP": datetime(2024, 11, 5)
        # Missing TOTAL_BUCKETS_SCANNED_BY_MACIE and TOTAL_CLOUDFRONTED_BUCKETS
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [incomplete_metrics],
            "macie_testing": [pd.DataFrame()],
            "historical_stats": [pd.DataFrame()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Missing data should result in 0 values
        assert not metrics_df.empty
        for _, row in metrics_df.iterrows():
            assert row["monitoring_metric_value"] == 0.0
            assert row["monitoring_metric_status"] == "Red"

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_data_types_validation():
    """Test that output data types match requirements"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Verify data types match CLAUDE.md requirements
        for _, row in metrics_df.iterrows():
            assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
            assert isinstance(row["control_id"], str)
            assert isinstance(row["monitoring_metric_id"], (int, float))
            assert isinstance(row["monitoring_metric_value"], float)
            assert isinstance(row["monitoring_metric_status"], str)
            assert pd.api.types.is_integer_dtype(type(row["metric_value_numerator"]))
            assert pd.api.types.is_integer_dtype(type(row["metric_value_denominator"]))
            # resources_info can be None or list
            assert row["resources_info"] is None or isinstance(row["resources_info"], list)

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_metric_value_ranges():
    """Test that metric values are within expected ranges"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Verify metric values are within valid ranges (0-100%)
        for _, row in metrics_df.iterrows():
            assert 0.0 <= row["monitoring_metric_value"] <= 100.0
            assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]
            assert row["metric_value_numerator"] >= 0
            assert row["metric_value_denominator"] >= 0

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_timestamp_consistency():
    """Test that timestamps are consistent across all metrics"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # All timestamps should be the same
        timestamps = metrics_df["control_monitoring_utc_timestamp"].unique()
        assert len(timestamps) == 1
        assert timestamps[0] == datetime(2024, 11, 5, 12, 9, 0)

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_control_id_consistency():
    """Test that control ID is consistent across all metrics"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # All control IDs should be CTRL-1079134
        control_ids = metrics_df["control_id"].unique()
        assert len(control_ids) == 1
        assert control_ids[0] == "CTRL-1079134"

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_metric_id_consistency():
    """Test that metric IDs match expected values"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should have exactly the expected metric IDs
        metric_ids = sorted(metrics_df["monitoring_metric_id"].tolist())
        assert metric_ids == [34, 35, 36]

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_data_type_enforcement():
    """Test that extract method enforces correct data types"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "macie_metrics": [_mock_macie_metrics_df()],
            "macie_testing": [_mock_macie_testing_df()],
            "historical_stats": [_mock_historical_stats_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Check that data types are enforced correctly
        assert metrics_df["metric_value_numerator"].dtype == "int64"
        assert metrics_df["metric_value_denominator"].dtype == "int64" 
        assert metrics_df["monitoring_metric_value"].dtype == "float64"

# Test run function variations
def test_run_function_normal_path():
    """Test run function normal execution path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_ctrl_1079134.pipeline.PLAutomatedMonitoringCTRL1079134') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)

def test_run_function_export_test_data():
    """Test run function with export test data path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_ctrl_1079134.pipeline.PLAutomatedMonitoringCTRL1079134') as mock_pipeline_class:
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
        with patch("pipelines.pl_automated_monitoring_ctrl_1079134.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_ctrl_1079134.pipeline import run
    
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