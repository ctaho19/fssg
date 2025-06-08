import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components
from pipelines.pl_automated_monitoring_scp_controls.pipeline import (
    PLAutomatedMonitoringScpControls,
    CONTROL_CONFIGS,
    run
)
from config_pipeline import ConfigPipeline

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


def _mock_multi_control_thresholds():
    """Generate threshold data for multiple SCP controls"""
    controls = [
        ("CTRL-1077770", "MNTR-1077770-T1", "MNTR-1077770-T2"),
        ("CTRL-1079538", "MNTR-1079538-T1", "MNTR-1079538-T2"),
        ("CTRL-1081889", "MNTR-1081889-T1", "MNTR-1081889-T2"),
    ]
    
    rows = []
    for ctrl_id, tier1_id, tier2_id in controls:
        rows.extend([
            {
                "monitoring_metric_id": tier1_id,
                "control_id": ctrl_id,
                "monitoring_metric_tier": "Tier 1",
                "warning_threshold": 97.0,
                "alerting_threshold": 95.0
            },
            {
                "monitoring_metric_id": tier2_id,
                "control_id": ctrl_id,
                "monitoring_metric_tier": "Tier 2",
                "warning_threshold": 97.0,
                "alerting_threshold": 95.0
            }
        ])
    
    return pd.DataFrame(rows)


def _mock_in_scope_accounts():
    """Generate mock in-scope accounts data"""
    return pd.DataFrame([
        {"ACCOUNT": "123456789012"},
        {"ACCOUNT": "123456789013"},
        {"ACCOUNT": "123456789014"},
        {"ACCOUNT": "123456789015"},
        {"ACCOUNT": "123456789016"},
    ])


def _mock_evaluated_accounts():
    """Generate mock evaluation data for SCP controls"""
    return pd.DataFrame([
        # CTRL-1077770 evaluations
        {"RESOURCE_ID": "123456789012", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.146.v02"},
        {"RESOURCE_ID": "123456789013", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.146.v02"},
        {"RESOURCE_ID": "123456789014", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.146.v02"},
        # CTRL-1079538 evaluations (only 3 accounts evaluated)
        {"RESOURCE_ID": "123456789012", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "CM-2.AWS.20.v02"},
        {"RESOURCE_ID": "123456789013", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "CM-2.AWS.20.v02"},
        {"RESOURCE_ID": "123456789014", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "CompliantControlAllowance", "CONTROL_ID": "CM-2.AWS.20.v02"},
        # CTRL-1081889 evaluations (all accounts)
        {"RESOURCE_ID": "123456789012", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.123.v01"},
        {"RESOURCE_ID": "123456789013", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.123.v01"},
        {"RESOURCE_ID": "123456789014", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.123.v01"},
        {"RESOURCE_ID": "123456789015", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.123.v01"},
        {"RESOURCE_ID": "123456789016", "RESOURCE_TYPE": "Account", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.123.v01"},
    ])


def generate_mock_api_response(content=None, status_code=200):
    """Generate standardized mock API response."""
    import json
    
    mock_response = Response()
    mock_response.status_code = status_code
    
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        mock_response._content = json.dumps({}).encode("utf-8")
    
    return mock_response


@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_multi_control_success():
    """Test successful metrics calculation for multiple SCP controls"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Setup test data
    thresholds_df = _mock_multi_control_thresholds()
    in_scope_accounts_df = _mock_in_scope_accounts()
    evaluated_accounts_df = _mock_evaluated_accounts()
    
    # Call _calculate_metrics directly
    result = pipeline._calculate_metrics(thresholds_df, in_scope_accounts_df, evaluated_accounts_df)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    assert len(result) == 6  # 2 metrics per control * 3 controls
    
    # Verify we have results for all three controls
    control_ids = set(result["control_id"].unique())
    assert control_ids == {"CTRL-1077770", "CTRL-1079538", "CTRL-1081889"}
    
    # Verify data types
    assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
    assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
    assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])


@freeze_time("2024-11-05 12:09:00")
def test_tier1_coverage_calculation():
    """Test Tier 1 (Coverage) metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Setup test data
    active_accounts = {"123456789012", "123456789013", "123456789014", "123456789015", "123456789016"}
    evaluated_accounts = _mock_evaluated_accounts()[_mock_evaluated_accounts()["CONTROL_ID"] == "AC-3.AWS.146.v02"]
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier1_metrics(
        active_accounts, evaluated_accounts, "AC-3.AWS.146.v02"
    )
    
    # 3 out of 5 accounts evaluated = 60%
    assert metric_value == 60.0
    assert compliant_count == 3
    assert total_count == 5
    assert non_compliant_resources is not None
    assert len(non_compliant_resources) == 2  # 2 unevaluated accounts


@freeze_time("2024-11-05 12:09:00")
def test_tier2_compliance_calculation():
    """Test Tier 2 (Compliance) metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Setup test data
    active_accounts = {"123456789012", "123456789013", "123456789014", "123456789015", "123456789016"}
    evaluated_accounts = _mock_evaluated_accounts()[_mock_evaluated_accounts()["CONTROL_ID"] == "AC-3.AWS.146.v02"]
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier2_metrics(
        active_accounts, evaluated_accounts, "AC-3.AWS.146.v02"
    )
    
    # 2 out of 3 evaluated accounts compliant = 66.67%
    assert metric_value == pytest.approx(66.67, 0.01)
    assert compliant_count == 2
    assert total_count == 3
    assert non_compliant_resources is not None
    assert len(non_compliant_resources) == 1  # 1 non-compliant account


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    empty_df = pd.DataFrame()
    in_scope_accounts_df = _mock_in_scope_accounts()
    evaluated_accounts_df = _mock_evaluated_accounts()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df, in_scope_accounts_df, evaluated_accounts_df)


def test_calculate_metrics_empty_accounts():
    """Test error handling for empty in-scope accounts"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    thresholds_df = _mock_multi_control_thresholds()
    empty_accounts_df = pd.DataFrame()
    evaluated_accounts_df = _mock_evaluated_accounts()
    
    with pytest.raises(RuntimeError, match="No in-scope accounts found"):
        pipeline._calculate_metrics(thresholds_df, empty_accounts_df, evaluated_accounts_df)


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'env')


def test_control_configs_structure():
    """Test that all 10 control configurations are properly structured"""
    assert len(CONTROL_CONFIGS) == 10  # Should have 10 controls
    
    expected_control_ids = {
        "CTRL-1077770", "CTRL-1079538", "CTRL-1081889", "CTRL-1102567", 
        "CTRL-1105846", "CTRL-1105996", "CTRL-1105997", "CTRL-1106155",
        "CTRL-1103299", "CTRL-1106425"
    }
    
    actual_control_ids = {config["ctrl_id"] for config in CONTROL_CONFIGS}
    assert actual_control_ids == expected_control_ids
    
    for config in CONTROL_CONFIGS:
        assert "cloud_control_id" in config
        assert "ctrl_id" in config
        assert "metric_ids" in config
        assert "tier1" in config["metric_ids"]
        assert "tier2" in config["metric_ids"]


def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_multi_control_thresholds()
    mock_accounts = _mock_in_scope_accounts()
    mock_evaluations = _mock_evaluated_accounts()
    
    mock_df = pd.DataFrame({
        "thresholds_raw": [mock_thresholds],
        "in_scope_accounts": [mock_accounts],
        "evaluated_accounts": [mock_evaluations]
    })
    
    # Mock the parent class extract method directly
    with patch.object(ConfigPipeline, 'extract', return_value=mock_df):
        result = pipeline.extract()
        
        # Verify the result has monitoring_metrics column
        assert "monitoring_metrics" in result.columns
        
        # Verify that the monitoring_metrics contains a DataFrame
        metrics_df = result["monitoring_metrics"].iloc[0]
        assert isinstance(metrics_df, pd.DataFrame)
        
        # Verify the DataFrame has the correct schema
        if not metrics_df.empty:
            assert list(metrics_df.columns) == AVRO_SCHEMA_FIELDS
            
            # Verify we have results for multiple controls
            assert len(metrics_df) == 6  # 3 controls * 2 tiers each


def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_scp_controls.pipeline.PLAutomatedMonitoringScpControls') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = "test_result"
        
        result = run(env, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run.assert_called_once_with(load=False, dq_actions=True)
        assert result == "test_result"


def test_compliance_status_determination():
    """Test compliance status based on thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Create scenario with different compliance levels
    thresholds_df = pd.DataFrame([
        {"monitoring_metric_id": "MNTR-1077770-T1", "control_id": "CTRL-1077770",
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0}
    ])
    
    # All accounts evaluated (100% coverage)
    in_scope_accounts_df = _mock_in_scope_accounts()[:1]
    evaluated_accounts_df = pd.DataFrame([
        {"RESOURCE_ID": "123456789012", "RESOURCE_TYPE": "Account",
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.146.v02"}
    ])
    
    result = pipeline._calculate_metrics(thresholds_df, in_scope_accounts_df, evaluated_accounts_df)
    
    # 100% coverage should be Green (>= 97% warning threshold)
    assert result.iloc[0]["monitoring_metric_value"] == 100.0
    assert result.iloc[0]["monitoring_metric_status"] == "Green"


def test_no_evaluations_handling():
    """Test handling when no evaluations exist for a control"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Setup test data with no evaluations
    active_accounts = {"123456789012", "123456789013"}
    empty_evaluations = pd.DataFrame()
    
    # Test Tier 1 with no evaluations
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier1_metrics(
        active_accounts, empty_evaluations, "AC-3.AWS.146.v02"
    )
    
    assert metric_value == 0.0
    assert compliant_count == 0
    assert total_count == 2
    assert non_compliant_resources is not None
    
    # Test Tier 2 with no evaluations
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier2_metrics(
        active_accounts, empty_evaluations, "AC-3.AWS.146.v02"
    )
    
    assert metric_value == 0.0
    assert compliant_count == 0
    assert total_count == 0
    assert non_compliant_resources is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])