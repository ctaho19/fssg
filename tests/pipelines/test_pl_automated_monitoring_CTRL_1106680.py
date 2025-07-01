import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime

# Import pipeline components - ALWAYS use pipelines. prefix
from pipelines.pl_automated_monitoring_CTRL_1106680.pipeline import (
    PLAutomatedMonitoringCTRL1106680,
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
            "monitoring_metric_id": "test_metric_tier1",
            "control_id": "CTRL-1106680",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "MFA Compliance Tier 1",
            "metric_description": "Percentage of evaluated roles requiring MFA",
            "warning_threshold": 90.0,
            "alerting_threshold": 80.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": "test_metric_tier2",
            "control_id": "CTRL-1106680",
            "monitoring_metric_tier": "Tier 2",
            "metric_name": "MFA Compliance Tier 2",
            "metric_description": "Percentage of roles with MFA enabled",
            "warning_threshold": 95.0,
            "alerting_threshold": 85.0,
            "control_executor": "Security Team"
        }
    ])

def _mock_in_scope_roles_df():
    """Utility function for in-scope IAM roles test data"""
    return pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role3"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role4"}
    ])

def _mock_evaluated_roles_df():
    """Utility function for evaluated IAM roles test data"""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role3"}
    ])

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier1_success():
    """Test successful Tier 1 metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    result = pipeline._calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df)
    
    # Find Tier 1 metric
    tier1_rows = result[result['monitoring_metric_tier'] == 'Tier 1']
    assert len(tier1_rows) == 1
    tier1_row = tier1_rows.iloc[0]
    
    # 3 out of 4 roles evaluated = 75%
    assert tier1_row['monitoring_metric_value'] == pytest.approx(75.0, abs=0.01)
    assert tier1_row['metric_value_numerator'] == 3
    assert tier1_row['metric_value_denominator'] == 4
    assert tier1_row['monitoring_metric_status'] == "Red"  # Below 80% alert threshold

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier2_success():
    """Test successful Tier 2 metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    result = pipeline._calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df)
    
    # Find Tier 2 metric  
    tier2_rows = result[result['monitoring_metric_tier'] == 'Tier 2']
    assert len(tier2_rows) == 1
    tier2_row = tier2_rows.iloc[0]
    
    # All evaluated roles compliant (simulated logic)
    assert tier2_row['monitoring_metric_value'] == pytest.approx(100.0, abs=0.01)

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    empty_df = pd.DataFrame()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df, in_scope_roles_df, evaluated_roles_df)

def test_calculate_metrics_empty_in_scope_roles():
    """Test handling of empty in-scope roles"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    empty_roles_df = pd.DataFrame()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    result = pipeline._calculate_metrics(thresholds_df, empty_roles_df, evaluated_roles_df)
    
    # Should return 0% compliance for Tier 1
    tier1_row = result[result['monitoring_metric_tier'] == 'Tier 1'].iloc[0]
    assert tier1_row['monitoring_metric_value'] == pytest.approx(0.0, abs=0.01)

def test_calculate_metrics_no_evaluated_roles():
    """Test handling when no roles have been evaluated"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    empty_evaluated_df = pd.DataFrame()
    
    result = pipeline._calculate_metrics(thresholds_df, in_scope_roles_df, empty_evaluated_df)
    
    # Should return 0% compliance for Tier 1
    tier1_row = result[result['monitoring_metric_tier'] == 'Tier 1'].iloc[0]
    assert tier1_row['monitoring_metric_value'] == pytest.approx(0.0, abs=0.01)

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_compliance_status_thresholds():
    """Test compliance status determination based on thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    # Create threshold with specific values for testing status logic
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": "test_metric",
        "control_id": "CTRL-1106680",
        "monitoring_metric_tier": "Tier 1",
        "metric_name": "Test Metric",
        "metric_description": "Test",
        "warning_threshold": 95.0,
        "alerting_threshold": 90.0,
        "control_executor": "Security Team"
    }])
    
    # Create scenario where all roles are evaluated (100% compliance)
    roles_df = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    evaluated_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    
    result = pipeline._calculate_metrics(thresholds_df, roles_df, evaluated_df)
    
    # 100% should be Green (>= 95% warning threshold)
    assert result.iloc[0]['monitoring_metric_value'] == pytest.approx(100.0, abs=0.01)
    assert result.iloc[0]['monitoring_metric_status'] == "Green"

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    assert pipeline.env == env

def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_threshold_df()
    mock_in_scope = _mock_in_scope_roles_df()
    mock_evaluated = _mock_evaluated_roles_df()
    mock_df = pd.DataFrame({
        "thresholds_raw": [mock_thresholds],
        "in_scope_roles": [mock_in_scope],
        "evaluated_roles": [mock_evaluated]
    })
    
    with patch.object(PLAutomatedMonitoringCTRL1106680, '__bases__', (Mock,)):
        with patch('pipelines.pl_automated_monitoring_CTRL_1106680.pipeline.ConfigPipeline.extract') as mock_super:
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

def test_data_type_validation():
    """Test that output data types match Avro schema requirements"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    result = pipeline._calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df)
    
    # Verify data types using pandas type checking (more reliable than isinstance)
    assert pd.api.types.is_datetime64_any_dtype(result["control_monitoring_utc_timestamp"])
    assert pd.api.types.is_string_dtype(result["control_id"])
    assert pd.api.types.is_string_dtype(result["monitoring_metric_id"])
    assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])
    assert pd.api.types.is_string_dtype(result["monitoring_metric_status"])
    assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
    assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
    
    # Test individual row data types for resources_info (which can vary)
    for _, row in result.iterrows():
        # resources_info can be None or list of strings
        if row["resources_info"] is not None:
            assert isinstance(row["resources_info"], list)

def test_edge_case_empty_evaluated_and_in_scope():
    """Test edge case with both empty datasets"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    thresholds_df = _mock_threshold_df()
    empty_df1 = pd.DataFrame()
    empty_df2 = pd.DataFrame()
    
    result = pipeline._calculate_metrics(thresholds_df, empty_df1, empty_df2)
    
    # Should handle empty datasets gracefully
    assert not result.empty
    for _, row in result.iterrows():
        assert row['monitoring_metric_value'] == pytest.approx(0.0, abs=0.01)

def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1106680.pipeline.PLAutomatedMonitoringCTRL1106680') as mock_pipeline_class:
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
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1106680.pipeline.PLAutomatedMonitoringCTRL1106680') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run_test_data_export.return_value = "export_result"
        
        result = run(env, is_export_test_data=True, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=True)
        assert result == "export_result"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])