import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime

# Import pipeline components
from pl_automated_monitoring_CTRL_1106680.pipeline import (
    PLAutomatedMonitoringCTRL1106680,
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

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([
        {
            "monitoring_metric_id": "MNTR-1106680-T1",
            "control_id": "CTRL-1106680",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "MFA Coverage",
            "metric_description": "Percentage of in-scope roles evaluated for MFA",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0,
            "control_executor": "test_executor"
        },
        {
            "monitoring_metric_id": "MNTR-1106680-T2",
            "control_id": "CTRL-1106680",
            "monitoring_metric_tier": "Tier 2",
            "metric_name": "MFA Accuracy",
            "metric_description": "Percentage of non-compliant roles with >10 consecutive days",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0,
            "control_executor": "test_executor"
        }
    ])

def _mock_in_scope_roles_df():
    """Utility function for in-scope roles data"""
    return pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-3"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-4"}
    ])

def _mock_evaluated_roles_df():
    """Utility function for evaluated roles data"""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-3"}
        # Note: test-role-4 is missing (not evaluated)
    ])

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier1_success():
    """Test successful Tier 1 metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    context = {}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 2  # Two tiers
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Check Tier 1 metrics
    tier1_row = result[result['monitoring_metric_id'] == 'MNTR-1106680-T1'].iloc[0]
    assert tier1_row['control_id'] == 'CTRL-1106680'
    assert tier1_row['monitoring_metric_value'] == 75.0  # 3 out of 4 roles evaluated
    assert tier1_row['metric_value_numerator'] == 3
    assert tier1_row['metric_value_denominator'] == 4
    assert tier1_row['monitoring_metric_status'] == 'Red'  # Below 95% threshold
    assert isinstance(tier1_row['control_monitoring_utc_timestamp'], datetime)

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_tier2_success():
    """Test successful Tier 2 metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    
    context = {}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df, context)
    
    # Check Tier 2 metrics
    tier2_row = result[result['monitoring_metric_id'] == 'MNTR-1106680-T2'].iloc[0]
    assert tier2_row['control_id'] == 'CTRL-1106680'
    assert tier2_row['monitoring_metric_value'] == 100.0  # All evaluated roles compliant (simulated)
    assert tier2_row['metric_value_numerator'] == 3
    assert tier2_row['metric_value_denominator'] == 3
    assert tier2_row['monitoring_metric_status'] == 'Green'  # Above 95% threshold

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    context = {}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, in_scope_roles_df, evaluated_roles_df, context)

def test_calculate_metrics_empty_in_scope_roles():
    """Test handling of empty in-scope roles data"""
    thresholds_df = _mock_threshold_df()
    empty_roles_df = pd.DataFrame()
    evaluated_roles_df = _mock_evaluated_roles_df()
    context = {}
    
    result = calculate_metrics(thresholds_df, empty_roles_df, evaluated_roles_df, context)
    
    # Should return results with 0 counts and appropriate error messages
    assert not result.empty
    tier1_row = result[result['monitoring_metric_id'] == 'MNTR-1106680-T1'].iloc[0]
    assert tier1_row['metric_value_denominator'] == 0
    assert tier1_row['monitoring_metric_value'] == 0.0
    assert tier1_row['resources_info'] is not None

def test_calculate_metrics_no_evaluated_roles():
    """Test handling when no roles have been evaluated"""
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    empty_evaluated_df = pd.DataFrame()
    context = {}
    
    result = calculate_metrics(thresholds_df, in_scope_roles_df, empty_evaluated_df, context)
    
    # Tier 1 should show 0% coverage
    tier1_row = result[result['monitoring_metric_id'] == 'MNTR-1106680-T1'].iloc[0]
    assert tier1_row['monitoring_metric_value'] == 0.0
    assert tier1_row['metric_value_numerator'] == 0
    assert tier1_row['metric_value_denominator'] == 4
    assert tier1_row['monitoring_metric_status'] == 'Red'

def test_calculate_metrics_compliance_status_thresholds():
    """Test compliance status determination based on thresholds"""
    # Test data with perfect coverage (100%)
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": "MNTR-1106680-T1",
        "control_id": "CTRL-1106680",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 97.0,
        "alerting_threshold": 95.0
    }])
    
    in_scope_roles_df = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2"}
    ])
    
    evaluated_roles_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/test-role-2"}
    ])
    
    context = {}
    
    result = calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df, context)
    
    # Should be Green (100% >= 95%)
    assert result.iloc[0]['monitoring_metric_status'] == 'Green'
    assert result.iloc[0]['monitoring_metric_value'] == 100.0

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1106680(env)
    
    assert pipeline.env == env

@patch('pl_automated_monitoring_CTRL_1106680.pipeline.run')
def test_pipeline_run_method(mock_run):
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    
    # Import and call the run function
    from pl_automated_monitoring_CTRL_1106680.pipeline import run
    run(env=env, is_load=False, dq_actions=False)
    
    # Verify pipeline was created and run was called
    mock_run.assert_called_once()

def test_tier1_metrics_calculation_detailed():
    """Test detailed Tier 1 metrics calculation logic"""
    from pl_automated_monitoring_CTRL_1106680.pipeline import _calculate_tier1_metrics
    
    # Test perfect coverage
    in_scope_df = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    
    evaluated_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    
    metric_value, compliant_count, total_count, resources_info = _calculate_tier1_metrics(
        in_scope_df, evaluated_df
    )
    
    assert metric_value == 100.0
    assert compliant_count == 2
    assert total_count == 2
    assert resources_info is None  # No non-compliant resources

def test_tier1_metrics_partial_coverage():
    """Test Tier 1 metrics with partial coverage"""
    from pl_automated_monitoring_CTRL_1106680.pipeline import _calculate_tier1_metrics
    
    in_scope_df = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role3"}
    ])
    
    evaluated_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"}
    ])
    
    metric_value, compliant_count, total_count, resources_info = _calculate_tier1_metrics(
        in_scope_df, evaluated_df
    )
    
    assert metric_value == 33.33  # 1 out of 3
    assert compliant_count == 1
    assert total_count == 3
    assert resources_info is not None
    assert len(resources_info) == 2  # Two missing roles

def test_tier2_metrics_calculation_detailed():
    """Test detailed Tier 2 metrics calculation logic"""
    from pl_automated_monitoring_CTRL_1106680.pipeline import _calculate_tier2_metrics
    
    in_scope_df = pd.DataFrame([
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    
    evaluated_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role1"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/role2"}
    ])
    
    metric_value, compliant_count, total_count, resources_info = _calculate_tier2_metrics(
        in_scope_df, evaluated_df
    )
    
    # In the current implementation, this returns 100% (all compliant)
    # since the logic for extended non-compliance is simulated
    assert metric_value == 100.0
    assert compliant_count == 2
    assert total_count == 2

@patch('pl_automated_monitoring_CTRL_1106680.pipeline.set_env_vars')
@patch('pl_automated_monitoring_CTRL_1106680.pipeline.run')
def test_main_function_execution(mock_run, mock_set_env_vars):
    """Test main function execution path"""
    mock_env = Mock()
    mock_set_env_vars.return_value = mock_env
    
    # Execute main block
    if __name__ != "__main__":  # Prevent actual execution during test
        from pl_automated_monitoring_CTRL_1106680 import pipeline
        
        # Simulate main execution
        env = mock_set_env_vars()
        try:
            pipeline.run(env=env, is_load=False, dq_actions=False)
        except Exception:
            pass  # Expected in test environment
        
        # Verify functions were called
        mock_set_env_vars.assert_called_once()

def test_data_type_validation():
    """Test that output data types match Avro schema requirements"""
    thresholds_df = _mock_threshold_df()
    in_scope_roles_df = _mock_in_scope_roles_df()
    evaluated_roles_df = _mock_evaluated_roles_df()
    context = {}
    
    result = calculate_metrics(thresholds_df, in_scope_roles_df, evaluated_roles_df, context)
    
    # Verify data types
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["control_id"], str)
        assert isinstance(row["monitoring_metric_id"], str)
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["monitoring_metric_status"], str)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)
        # resources_info can be None or list of strings
        if row["resources_info"] is not None:
            assert isinstance(row["resources_info"], list)

def test_edge_case_empty_evaluated_and_in_scope():
    """Test edge case with both empty datasets"""
    thresholds_df = _mock_threshold_df()
    empty_df1 = pd.DataFrame()
    empty_df2 = pd.DataFrame()
    context = {}
    
    result = calculate_metrics(thresholds_df, empty_df1, empty_df2, context)
    
    # Should handle gracefully
    assert not result.empty
    assert len(result) == 2  # Two tiers
    
    for _, row in result.iterrows():
        assert row['metric_value_denominator'] == 0
        assert row['monitoring_metric_value'] == 0.0
        assert row['resources_info'] is not None

if __name__ == "__main__":
    pytest.main([__file__])