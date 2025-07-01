import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json

# Import pipeline components - ALWAYS use pipelines. prefix
from pipelines.pl_automated_monitoring_dlp_controls.pipeline import (
    PLAutomatedMonitoringDlpControls,
    CONTROL_CONFIGS,
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
            "monitoring_metric_id": "test_metric_1",
            "control_id": "CTRL-1080553",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "Proofpoint DLP Test Success Rate",
            "metric_description": "Percentage of successful DLP tests for Proofpoint",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": "test_metric_2",
            "control_id": "CTRL-1101994",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "Symantec Proxy DLP Test Success Rate",
            "metric_description": "Percentage of successful DLP tests for Symantec Proxy",
            "warning_threshold": 98.0,
            "alerting_threshold": 96.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": "test_metric_3",
            "control_id": "CTRL-1077197",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "Slack CloudSOC DLP Test Success Rate",
            "metric_description": "Percentage of successful DLP tests for Slack CloudSOC",
            "warning_threshold": 99.0,
            "alerting_threshold": 97.0,
            "control_executor": "Security Team"
        }
    ])

def _mock_dlp_outcome_df():
    """Utility function for comprehensive test DLP outcome data"""
    return pd.DataFrame([
        # Proofpoint tests - 50% success rate (1 pass, 1 fail)
        {
            "TEST_ID": "pp_test_1",
            "TEST_NAME": "Proofpoint Email DLP Test",
            "PLATFORM": "proofpoint",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "BLOCK",
            "TEST_DESCRIPTION": "Test email DLP blocking",
            "SF_LOAD_TIMESTAMP": "2024-11-04 10:00:00"
        },
        {
            "TEST_ID": "pp_test_2",
            "TEST_NAME": "Proofpoint File DLP Test",
            "PLATFORM": "proofpoint",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "ALLOW",
            "TEST_DESCRIPTION": "Test file DLP blocking",
            "SF_LOAD_TIMESTAMP": "2024-11-04 10:00:00"
        },
        # Symantec Proxy tests - 100% success rate (2 pass, 0 fail)
        {
            "TEST_ID": "sp_test_1",
            "TEST_NAME": "Symantec Web DLP Test",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "BLOCK",
            "TEST_DESCRIPTION": "Test web DLP blocking",
            "SF_LOAD_TIMESTAMP": "2024-11-04 11:00:00"
        },
        {
            "TEST_ID": "sp_test_2",
            "TEST_NAME": "Symantec Upload DLP Test",
            "PLATFORM": "symantec_proxy",
            "EXPECTED_OUTCOME": "ALLOW",
            "ACTUAL_OUTCOME": "ALLOW",
            "TEST_DESCRIPTION": "Test upload DLP allowance",
            "SF_LOAD_TIMESTAMP": "2024-11-04 11:00:00"
        },
        # Slack CloudSOC tests - 66.67% success rate (2 pass, 1 fail)
        {
            "TEST_ID": "sc_test_1",
            "TEST_NAME": "Slack Message DLP Test",
            "PLATFORM": "slack_cloudsoc",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "BLOCK",
            "TEST_DESCRIPTION": "Test Slack message DLP",
            "SF_LOAD_TIMESTAMP": "2024-11-04 12:00:00"
        },
        {
            "TEST_ID": "sc_test_2",
            "TEST_NAME": "Slack File Share DLP Test",
            "PLATFORM": "slack_cloudsoc",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "BLOCK",
            "TEST_DESCRIPTION": "Test Slack file sharing DLP",
            "SF_LOAD_TIMESTAMP": "2024-11-04 12:00:00"
        },
        {
            "TEST_ID": "sc_test_3",
            "TEST_NAME": "Slack Channel DLP Test",
            "PLATFORM": "slack_cloudsoc",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "ALLOW",
            "TEST_DESCRIPTION": "Test Slack channel DLP",
            "SF_LOAD_TIMESTAMP": "2024-11-04 12:00:00"
        }
    ])

def _mock_empty_dlp_outcome_df():
    """Utility function for empty DLP outcome data"""
    return pd.DataFrame(columns=[
        "TEST_ID", "TEST_NAME", "PLATFORM", "EXPECTED_OUTCOME", 
        "ACTUAL_OUTCOME", "TEST_DESCRIPTION", "SF_LOAD_TIMESTAMP"
    ])

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_all_controls_success():
    """Test successful metrics calculation for all three DLP controls"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Setup test data
    thresholds_df = _mock_threshold_df()
    dlp_outcome_df = _mock_dlp_outcome_df()
    
    # Call _calculate_metrics directly on pipeline instance
    result = pipeline._calculate_metrics(thresholds_df, dlp_outcome_df)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Should have 3 results (one for each control)
    assert len(result) == 3
    
    # Verify data types using pandas type checking
    assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
    assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
    assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])
    
    # Verify control IDs are present
    control_ids = set(result["control_id"].tolist())
    expected_controls = {"CTRL-1080553", "CTRL-1101994", "CTRL-1077197"}
    assert control_ids == expected_controls
    
    # Verify specific metric values for each platform
    for _, row in result.iterrows():
        if row["control_id"] == "CTRL-1080553":  # Proofpoint
            assert row["monitoring_metric_value"] == 50.0  # 1/2 = 50%
            assert row["metric_value_numerator"] == 1
            assert row["metric_value_denominator"] == 2
        elif row["control_id"] == "CTRL-1101994":  # Symantec Proxy
            assert row["monitoring_metric_value"] == 100.0  # 2/2 = 100%
            assert row["metric_value_numerator"] == 2
            assert row["metric_value_denominator"] == 2
        elif row["control_id"] == "CTRL-1077197":  # Slack CloudSOC
            assert row["monitoring_metric_value"] == 66.67  # 2/3 = 66.67%
            assert row["metric_value_numerator"] == 2
            assert row["metric_value_denominator"] == 3

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_single_control_proofpoint():
    """Test metrics calculation for Proofpoint control only"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Setup test data for only Proofpoint control
    single_threshold_df = _mock_threshold_df().iloc[[0]]  # Only proofpoint control
    dlp_outcome_df = _mock_dlp_outcome_df()
    
    result = pipeline._calculate_metrics(single_threshold_df, dlp_outcome_df)
    
    assert len(result) == 1
    assert result["control_id"].iloc[0] == "CTRL-1080553"
    
    # Proofpoint has 2 tests, 1 pass, 1 fail = 50%
    assert result["monitoring_metric_value"].iloc[0] == 50.0
    assert result["metric_value_numerator"].iloc[0] == 1
    assert result["metric_value_denominator"].iloc[0] == 2
    assert result["monitoring_metric_status"].iloc[0] == "Red"  # 50% < 95% alert threshold

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_single_control_symantec():
    """Test metrics calculation for Symantec Proxy control only"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Setup test data for only Symantec control
    single_threshold_df = _mock_threshold_df().iloc[[1]]  # Only symantec control
    dlp_outcome_df = _mock_dlp_outcome_df()
    
    result = pipeline._calculate_metrics(single_threshold_df, dlp_outcome_df)
    
    assert len(result) == 1
    assert result["control_id"].iloc[0] == "CTRL-1101994"
    
    # Symantec has 2 tests, 2 pass, 0 fail = 100%
    assert result["monitoring_metric_value"].iloc[0] == 100.0
    assert result["metric_value_numerator"].iloc[0] == 2
    assert result["metric_value_denominator"].iloc[0] == 2
    assert result["monitoring_metric_status"].iloc[0] == "Green"  # 100% >= 98% warning threshold

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_single_control_slack():
    """Test metrics calculation for Slack CloudSOC control only"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Setup test data for only Slack control
    single_threshold_df = _mock_threshold_df().iloc[[2]]  # Only slack control
    dlp_outcome_df = _mock_dlp_outcome_df()
    
    result = pipeline._calculate_metrics(single_threshold_df, dlp_outcome_df)
    
    assert len(result) == 1
    assert result["control_id"].iloc[0] == "CTRL-1077197"
    
    # Slack has 3 tests, 2 pass, 1 fail = 66.67%
    assert result["monitoring_metric_value"].iloc[0] == 66.67
    assert result["metric_value_numerator"].iloc[0] == 2
    assert result["metric_value_denominator"].iloc[0] == 3
    assert result["monitoring_metric_status"].iloc[0] == "Red"  # 66.67% < 97% alert threshold

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    empty_df = pd.DataFrame()
    dlp_outcome_df = _mock_dlp_outcome_df()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df, dlp_outcome_df)

def test_calculate_metrics_empty_dlp_outcome():
    """Test handling of empty DLP outcome data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    thresholds_df = _mock_threshold_df()
    empty_dlp_df = _mock_empty_dlp_outcome_df()
    
    result = pipeline._calculate_metrics(thresholds_df, empty_dlp_df)
    
    # Should still return results with 0% compliance
    assert len(result) == 3
    for _, row in result.iterrows():
        assert row["monitoring_metric_value"] == 0.0
        assert row["metric_value_numerator"] == 0
        assert row["metric_value_denominator"] == 0
        assert row["resources_info"] is not None
        assert "No" in str(row["resources_info"])  # Should contain error message

def test_dlp_metrics_calculation_proofpoint():
    """Test Proofpoint-specific DLP metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Test proofpoint platform (2 tests, 1 pass)
    proofpoint_data = _mock_dlp_outcome_df()[_mock_dlp_outcome_df()['PLATFORM'] == 'proofpoint']
    metric_value, compliant, total, resources = pipeline._calculate_dlp_metrics(proofpoint_data, "proofpoint")
    
    assert metric_value == 50.0
    assert compliant == 1
    assert total == 2
    assert resources is not None
    assert len(resources) == 1  # One failed test
    
    # Verify failed test details
    failed_detail = json.loads(resources[0])
    assert failed_detail["platform"] == "proofpoint"
    assert failed_detail["expected"] == "BLOCK"
    assert failed_detail["actual"] == "ALLOW"

def test_dlp_metrics_calculation_symantec():
    """Test Symantec-specific DLP metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Test symantec platform (2 tests, 2 pass)
    symantec_data = _mock_dlp_outcome_df()[_mock_dlp_outcome_df()['PLATFORM'] == 'symantec_proxy']
    metric_value, compliant, total, resources = pipeline._calculate_dlp_metrics(symantec_data, "symantec_proxy")
    
    assert metric_value == 100.0
    assert compliant == 2
    assert total == 2
    assert resources is None  # No failed tests

def test_dlp_metrics_calculation_slack():
    """Test Slack CloudSOC-specific DLP metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Test slack platform (3 tests, 2 pass)
    slack_data = _mock_dlp_outcome_df()[_mock_dlp_outcome_df()['PLATFORM'] == 'slack_cloudsoc']
    metric_value, compliant, total, resources = pipeline._calculate_dlp_metrics(slack_data, "slack_cloudsoc")
    
    assert metric_value == 66.67
    assert compliant == 2
    assert total == 3
    assert resources is not None
    assert len(resources) == 1  # One failed test

def test_dlp_metrics_calculation_empty_platform():
    """Test DLP metrics calculation with empty platform data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    empty_df = pd.DataFrame()
    metric_value, compliant, total, resources = pipeline._calculate_dlp_metrics(empty_df, "test_platform")
    
    assert metric_value == 0.0
    assert compliant == 0
    assert total == 0
    assert resources is not None
    assert "No test_platform outcome data available" in str(resources)

def test_compliance_status_logic_green():
    """Test Green compliance status determination"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Create test data with specific thresholds for Green status
    threshold_df = pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1080553",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 95.0,
        "alerting_threshold": 90.0
    }])
    
    # Create DLP outcome that results in 100% compliance
    perfect_dlp_df = pd.DataFrame([{
        "TEST_ID": "test_1",
        "PLATFORM": "proofpoint",
        "EXPECTED_OUTCOME": "BLOCK",
        "ACTUAL_OUTCOME": "BLOCK"
    }])
    
    result = pipeline._calculate_metrics(threshold_df, perfect_dlp_df)
    
    # 100% should be Green (>= warning threshold of 95%)
    assert result["monitoring_metric_status"].iloc[0] == "Green"

def test_compliance_status_logic_yellow():
    """Test Yellow compliance status determination"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Create test data for Yellow status (between alert and warning thresholds)
    threshold_df = pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1080553",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 98.0,
        "alerting_threshold": 92.0
    }])
    
    # Create DLP outcome that results in ~94% compliance (between thresholds)
    yellow_dlp_df = pd.DataFrame([
        {"TEST_ID": "test_1", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_2", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_3", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_4", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_5", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_6", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_7", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_8", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_9", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_10", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_11", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_12", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_13", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_14", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_15", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "BLOCK"},
        {"TEST_ID": "test_16", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "ALLOW"}  # One failure
    ])
    
    result = pipeline._calculate_metrics(threshold_df, yellow_dlp_df)
    
    # 93.75% should be Yellow (>= 92% alert threshold but < 98% warning threshold)
    assert result["monitoring_metric_value"].iloc[0] == 93.75
    assert result["monitoring_metric_status"].iloc[0] == "Yellow"

def test_compliance_status_logic_red():
    """Test Red compliance status determination"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Create test data for Red status (below alert threshold)
    threshold_df = pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1080553",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 95.0,
        "alerting_threshold": 90.0
    }])
    
    # Create DLP outcome that results in low compliance
    red_dlp_df = pd.DataFrame([
        {"TEST_ID": "test_1", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "ALLOW"},
        {"TEST_ID": "test_2", "PLATFORM": "proofpoint", "EXPECTED_OUTCOME": "BLOCK", "ACTUAL_OUTCOME": "ALLOW"}
    ])
    
    result = pipeline._calculate_metrics(threshold_df, red_dlp_df)
    
    # 0% should be Red (< 90% alert threshold)
    assert result["monitoring_metric_value"].iloc[0] == 0.0
    assert result["monitoring_metric_status"].iloc[0] == "Red"

def test_control_configs_mapping():
    """Test that CONTROL_CONFIGS contains all expected controls with correct platforms"""
    expected_controls = {"CTRL-1080553", "CTRL-1101994", "CTRL-1077197"}
    actual_controls = set(CONTROL_CONFIGS.keys())
    
    assert actual_controls == expected_controls
    
    # Verify each config has required fields and correct platform mappings
    assert CONTROL_CONFIGS["CTRL-1080553"]["platform"] == "proofpoint"
    assert CONTROL_CONFIGS["CTRL-1101994"]["platform"] == "symantec_proxy"
    assert CONTROL_CONFIGS["CTRL-1077197"]["platform"] == "slack_cloudsoc"
    
    for control_id, config in CONTROL_CONFIGS.items():
        assert "platform" in config
        assert "description" in config
        assert config["platform"] in ["proofpoint", "symantec_proxy", "slack_cloudsoc"]

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    assert pipeline.env == env

def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_threshold_df()
    mock_dlp_outcome = _mock_dlp_outcome_df()
    mock_df = pd.DataFrame({
        "thresholds_raw": [mock_thresholds],
        "dlp_outcome": [mock_dlp_outcome]
    })
    
    with patch.object(PLAutomatedMonitoringDlpControls, '__bases__', (Mock,)):
        with patch('pipelines.pl_automated_monitoring_dlp_controls.pipeline.ConfigPipeline.extract') as mock_super:
            mock_super.return_value = mock_df
            
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

def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_dlp_controls.pipeline.PLAutomatedMonitoringDlpControls') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = "test_result"
        
        result = run(env, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run.assert_called_once_with(load=False, dq_actions=True)
        assert result == "test_result"

def test_run_function_with_export_test_data():
    """Test pipeline run function with export test data option"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_dlp_controls.pipeline.PLAutomatedMonitoringDlpControls') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run_test_data_export.return_value = "export_result"
        
        result = run(env, is_export_test_data=True, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=True)
        assert result == "export_result"

def test_failed_test_details_with_large_volume():
    """Test handling of large volumes of failed tests (>100)"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringDlpControls(env)
    
    # Create test data with many failed tests
    large_fail_data = []
    for i in range(150):  # 150 failed tests
        large_fail_data.append({
            "TEST_ID": f"test_{i}",
            "PLATFORM": "proofpoint",
            "EXPECTED_OUTCOME": "BLOCK",
            "ACTUAL_OUTCOME": "ALLOW"
        })
    
    large_fail_df = pd.DataFrame(large_fail_data)
    
    metric_value, compliant, total, resources = pipeline._calculate_dlp_metrics(large_fail_df, "proofpoint")
    
    assert metric_value == 0.0  # All failed
    assert compliant == 0
    assert total == 150
    assert resources is not None
    assert len(resources) == 101  # 100 failed tests + 1 summary message
    
    # Verify the summary message is included
    summary_detail = json.loads(resources[100])
    assert "... and" in summary_detail["message"]
    assert "more failed tests" in summary_detail["message"]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])