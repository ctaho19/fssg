import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components
from pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline import (
    PLAutomatedMonitoringMachineIamPreventative,
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
    """Generate threshold data for multiple controls"""
    return pd.DataFrame([
        # CTRL-1105806 thresholds
        {"monitoring_metric_id": "MNTR-1105806-T1", "control_id": "CTRL-1105806", 
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": "MNTR-1105806-T2", "control_id": "CTRL-1105806", 
         "monitoring_metric_tier": "Tier 2", "warning_threshold": 97.0, "alerting_threshold": 95.0},
        # CTRL-1077124 thresholds
        {"monitoring_metric_id": "MNTR-1077124-T1", "control_id": "CTRL-1077124", 
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": "MNTR-1077124-T2", "control_id": "CTRL-1077124", 
         "monitoring_metric_tier": "Tier 2", "warning_threshold": 97.0, "alerting_threshold": 95.0},
    ])


def _mock_iam_roles():
    """Generate mock IAM roles data"""
    return pd.DataFrame([
        {"RESOURCE_ID": "role1", "AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1",
         "BA": "BA1", "ACCOUNT": "123456789012", "ROLE_TYPE": "MACHINE", "TYPE": "role"},
        {"RESOURCE_ID": "role2", "AMAZON_RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine2",
         "BA": "BA2", "ACCOUNT": "123456789012", "ROLE_TYPE": "MACHINE", "TYPE": "role"},
        {"RESOURCE_ID": "role3", "AMAZON_RESOURCE_NAME": "arn:aws:iam::987654321098:role/Machine3",
         "BA": "BA3", "ACCOUNT": "987654321098", "ROLE_TYPE": "MACHINE", "TYPE": "role"},
        {"RESOURCE_ID": "role4", "AMAZON_RESOURCE_NAME": "arn:aws:iam::111111111111:role/Human1",
         "BA": "BA4", "ACCOUNT": "111111111111", "ROLE_TYPE": "HUMAN", "TYPE": "role"},
    ])


def _mock_evaluated_roles():
    """Generate mock evaluated roles data"""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-6.AWS.13.v01"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine2", 
         "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-6.AWS.13.v01"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-6.AWS.35.v02"},
        {"RESOURCE_NAME": "arn:aws:iam::987654321098:role/Machine3", 
         "COMPLIANCE_STATUS": "CompliantControlAllowance", "CONTROL_ID": "AC-6.AWS.35.v02"},
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
    """Test successful metrics calculation for multiple controls"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Setup test data
    thresholds_df = _mock_multi_control_thresholds()
    iam_roles_df = _mock_iam_roles()
    evaluated_roles_df = _mock_evaluated_roles()
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock API response for approved accounts
        with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.OauthApi') as mock_oauth:
            mock_api_instance = Mock()
            mock_oauth.return_value = mock_api_instance
            
            # Mock approved accounts API response
            accounts_response = {
                "accounts": [
                    {"accountNumber": "123456789012", "accountStatus": "Active"},
                    {"accountNumber": "987654321098", "accountStatus": "Active"}
                ]
            }
            mock_api_instance.send_request.return_value = generate_mock_api_response(accounts_response)
            
            # Call _calculate_metrics directly
            result = pipeline._calculate_metrics(thresholds_df, iam_roles_df, evaluated_roles_df)
            
            # Assertions
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert list(result.columns) == AVRO_SCHEMA_FIELDS
            assert len(result) == 4  # 2 metrics per control * 2 controls
            
            # Verify we have results for both controls
            control_ids = set(result["control_id"].unique())
            assert control_ids == {"CTRL-1105806", "CTRL-1077124"}
            
            # Verify data types
            assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
            assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
            assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])


@freeze_time("2024-11-05 12:09:00")
def test_tier1_metrics_calculation():
    """Test Tier 1 (Coverage) metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Setup test data - 3 machine roles, 2 evaluated
    filtered_roles = _mock_iam_roles()[_mock_iam_roles()["ROLE_TYPE"] == "MACHINE"][:3]
    evaluated_roles = _mock_evaluated_roles()[:2]
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier1_metrics(
        filtered_roles, evaluated_roles
    )
    
    # 2 out of 3 roles evaluated = 66.67%
    assert metric_value == pytest.approx(66.67, 0.01)
    assert compliant_count == 2
    assert total_count == 3
    assert non_compliant_resources is not None
    assert len(non_compliant_resources) == 1  # 1 unevaluated role


@freeze_time("2024-11-05 12:09:00")
def test_tier2_metrics_calculation():
    """Test Tier 2 (Compliance) metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Setup test data
    filtered_roles = _mock_iam_roles()[_mock_iam_roles()["ROLE_TYPE"] == "MACHINE"][:2]
    evaluated_roles = _mock_evaluated_roles()[:2]  # 1 compliant, 1 non-compliant
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier2_metrics(
        filtered_roles, evaluated_roles
    )
    
    # 1 out of 2 roles compliant = 50%
    assert metric_value == 50.0
    assert compliant_count == 1
    assert total_count == 2
    assert non_compliant_resources is not None
    assert len(non_compliant_resources) == 1  # 1 non-compliant role


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    empty_df = pd.DataFrame()
    iam_roles_df = _mock_iam_roles()
    evaluated_roles_df = _mock_evaluated_roles()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df, iam_roles_df, evaluated_roles_df)


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'api.cloud.capitalone.com' in pipeline.api_url


def test_control_configs_structure():
    """Test that all control configurations are properly structured"""
    assert len(CONTROL_CONFIGS) == 12  # Should have 12 controls (including CTRL-1100340)
    
    for config in CONTROL_CONFIGS:
        assert "cloud_control_id" in config
        assert "ctrl_id" in config
        assert "metric_ids" in config
        assert "tier1" in config["metric_ids"]
        assert "tier2" in config["metric_ids"]
        assert config["requires_tier3"] is False  # All preventative controls have no Tier 3


def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.OauthApi') as mock_oauth:
        mock_api_instance = Mock()
        mock_oauth.return_value = mock_api_instance
        mock_api_instance.send_request.side_effect = RequestException("Connection error")
        
        with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
            pipeline._get_approved_accounts(mock_api_instance)


def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_multi_control_thresholds()
    mock_iam_roles = _mock_iam_roles()
    mock_evaluated = _mock_evaluated_roles()
    
    mock_df = pd.DataFrame({
        "thresholds_raw": [mock_thresholds],
        "all_iam_roles": [mock_iam_roles],
        "evaluated_roles": [mock_evaluated]
    })
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock the parent class extract method directly
        with patch.object(ConfigPipeline, 'extract', return_value=mock_df):
            
            # Mock API calls to avoid actual network requests
            with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.OauthApi') as mock_oauth:
                mock_api_instance = Mock()
                mock_oauth.return_value = mock_api_instance
                
                # Mock approved accounts API response
                accounts_response = {
                    "accounts": [
                        {"accountNumber": "123456789012", "accountStatus": "Active"},
                        {"accountNumber": "987654321098", "accountStatus": "Active"}
                    ]
                }
                mock_api_instance.send_request.return_value = generate_mock_api_response(accounts_response)
                
                result = pipeline.extract()
                
                # Verify the result has monitoring_metrics column
                assert "monitoring_metrics" in result.columns
                
                # Verify that the monitoring_metrics contains a DataFrame
                metrics_df = result["monitoring_metrics"].iloc[0]
                assert isinstance(metrics_df, pd.DataFrame)
                
                # Verify the DataFrame has the correct schema
                if not metrics_df.empty:
                    assert list(metrics_df.columns) == AVRO_SCHEMA_FIELDS
                    
                    # Verify we have results for multiple controls (should be 4 metrics: 2 controls * 2 tiers)
                    assert len(metrics_df) == 4
                    
                    # Verify control IDs are present
                    control_ids = set(metrics_df["control_id"].unique())
                    expected_controls = {"CTRL-1105806", "CTRL-1077124"}
                    assert control_ids == expected_controls


def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.PLAutomatedMonitoringMachineIamPreventative') as mock_pipeline_class:
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
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    
    # Create scenario with different compliance levels
    thresholds_df = pd.DataFrame([
        {"monitoring_metric_id": "MNTR-1105806-T1", "control_id": "CTRL-1105806",
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0}
    ])
    
    # All roles evaluated (100% coverage)
    iam_roles_df = _mock_iam_roles()[:1]
    evaluated_roles_df = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1",
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-6.AWS.13.v01"}
    ])
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_machine_iam_preventative.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        with patch.object(pipeline, '_get_approved_accounts') as mock_accounts:
            mock_accounts.return_value = ["123456789012"]
            
            result = pipeline._calculate_metrics(thresholds_df, iam_roles_df, evaluated_roles_df)
            
            # 100% coverage should be Green (>= 95% alerting threshold)
            assert result.iloc[0]["monitoring_metric_value"] == 100.0
            assert result.iloc[0]["monitoring_metric_status"] == "Green"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])