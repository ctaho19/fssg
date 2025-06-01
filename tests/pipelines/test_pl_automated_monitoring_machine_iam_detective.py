import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime, timedelta
from requests import Response, RequestException

# Import pipeline components
from pipelines.pl_automated_monitoring_machine_iam_detective.pipeline import (
    PLAutomatedMonitoringMachineIamDetective,
    CONTROL_CONFIG,
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


def _mock_thresholds_with_tier3():
    """Generate threshold data including Tier 3 for detective control"""
    return pd.DataFrame([
        {"monitoring_metric_id": "MNTR-1074653-T1", "control_id": "CTRL-1074653", 
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": "MNTR-1074653-T2", "control_id": "CTRL-1074653", 
         "monitoring_metric_tier": "Tier 2", "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": "MNTR-1074653-T3", "control_id": "CTRL-1074653", 
         "monitoring_metric_tier": "Tier 3 (SLA)", "warning_threshold": 90.0, "alerting_threshold": 85.0},
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
    """Generate mock evaluated roles data with NonCompliant status for Tier 3 testing"""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine2", 
         "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
        {"RESOURCE_NAME": "arn:aws:iam::987654321098:role/Machine3", 
         "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
    ])


def _mock_sla_data():
    """Generate mock SLA data for Tier 3 calculations"""
    base_date = datetime.now()
    return pd.DataFrame([
        {"RESOURCE_ID": "arn:aws:iam::123456789012:role/Machine2", 
         "CONTROL_RISK": "High",
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=20)},  # Within 30-day SLA
        {"RESOURCE_ID": "arn:aws:iam::987654321098:role/Machine3", 
         "CONTROL_RISK": "High",
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=40)},  # Past 30-day SLA
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
def test_calculate_metrics_with_tier3():
    """Test successful metrics calculation including Tier 3 SLA metrics"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    # Setup test data
    thresholds_df = _mock_thresholds_with_tier3()
    iam_roles_df = _mock_iam_roles()
    evaluated_roles_df = _mock_evaluated_roles()
    sla_data_df = _mock_sla_data()
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock API response for approved accounts
        with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.OauthApi') as mock_oauth:
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
            result = pipeline._calculate_metrics(thresholds_df, iam_roles_df, evaluated_roles_df, sla_data_df)
            
            # Assertions
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert list(result.columns) == AVRO_SCHEMA_FIELDS
            assert len(result) == 3  # 3 tiers for 1 control
            
            # Verify all tiers present
            metric_ids = set(result["monitoring_metric_id"].unique())
            assert metric_ids == {"MNTR-1074653-T1", "MNTR-1074653-T2", "MNTR-1074653-T3"}
            
            # Verify data types
            assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
            assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
            assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])


@freeze_time("2024-11-05 12:09:00")
def test_tier3_sla_calculation():
    """Test Tier 3 SLA metrics calculation"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    # Setup test data - 2 non-compliant roles
    evaluated_roles = _mock_evaluated_roles()[1:3]  # 2 non-compliant
    sla_data = _mock_sla_data()  # 1 within SLA, 1 past SLA
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier3_metrics(
        pd.DataFrame(),  # filtered_roles not used in Tier 3
        evaluated_roles,
        sla_data
    )
    
    # 1 out of 2 non-compliant roles within SLA = 50%
    assert metric_value == 50.0
    assert compliant_count == 1  # 1 within SLA
    assert total_count == 2  # 2 total non-compliant
    assert non_compliant_resources is not None
    assert len(non_compliant_resources) == 2  # Details for both non-compliant roles


def test_tier3_all_compliant():
    """Test Tier 3 when all roles are compliant (no non-compliant roles)"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    # All roles are compliant
    evaluated_roles = pd.DataFrame([
        {"RESOURCE_NAME": "arn:aws:iam::123456789012:role/Machine1", 
         "COMPLIANCE_STATUS": "Compliant", "CONTROL_ID": "AC-3.AWS.39.v02"}
    ])
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier3_metrics(
        pd.DataFrame(),
        evaluated_roles,
        pd.DataFrame()  # Empty SLA data
    )
    
    # No non-compliant roles = 100% SLA compliance
    assert metric_value == 100.0
    assert compliant_count == 0
    assert total_count == 0
    assert non_compliant_resources is not None
    assert any("All evaluated roles are compliant" in str(r) for r in non_compliant_resources)


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    empty_df = pd.DataFrame()
    iam_roles_df = _mock_iam_roles()
    evaluated_roles_df = _mock_evaluated_roles()
    sla_data_df = _mock_sla_data()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df, iam_roles_df, evaluated_roles_df, sla_data_df)


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'api.cloud.capitalone.com' in pipeline.api_url


def test_control_config_structure():
    """Test that control configuration is properly structured"""
    assert CONTROL_CONFIG["cloud_control_id"] == "AC-3.AWS.39.v02"
    assert CONTROL_CONFIG["ctrl_id"] == "CTRL-1074653"
    assert "tier1" in CONTROL_CONFIG["metric_ids"]
    assert "tier2" in CONTROL_CONFIG["metric_ids"]
    assert "tier3" in CONTROL_CONFIG["metric_ids"]
    assert CONTROL_CONFIG["requires_tier3"] is True


def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.OauthApi') as mock_oauth:
        mock_api_instance = Mock()
        mock_oauth.return_value = mock_api_instance
        mock_api_instance.send_request.side_effect = RequestException("Connection error")
        
        with pytest.raises(RuntimeError, match="Failed to fetch approved accounts"):
            pipeline._get_approved_accounts(mock_api_instance)


def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    # Mock super().extract() to return test data (as Series containing DataFrames)
    mock_thresholds = _mock_thresholds_with_tier3()
    mock_iam_roles = _mock_iam_roles()
    mock_evaluated = _mock_evaluated_roles()
    mock_sla = _mock_sla_data()
    
    mock_df = pd.DataFrame({
        "thresholds_raw": [mock_thresholds],
        "all_iam_roles": [mock_iam_roles],
        "evaluated_roles": [mock_evaluated],
        "sla_data": [mock_sla]
    })
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock the parent class extract method directly
        with patch.object(ConfigPipeline, 'extract', return_value=mock_df):
            
            # Mock API calls to avoid actual network requests
            with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.OauthApi') as mock_oauth:
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
                    
                    # Verify we have all three tiers for detective control
                    metric_ids = set(metrics_df["monitoring_metric_id"].unique())
                    expected_metric_ids = {"MNTR-1074653-T1", "MNTR-1074653-T2", "MNTR-1074653-T3"}
                    assert metric_ids == expected_metric_ids


def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_machine_iam_detective.pipeline.PLAutomatedMonitoringMachineIamDetective') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = "test_result"
        
        result = run(env, is_load=False, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run.assert_called_once_with(load=False, dq_actions=True)
        assert result == "test_result"


def test_sla_threshold_mapping():
    """Test SLA threshold mapping for different risk levels"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    
    base_date = datetime.now()
    
    # Create evaluated roles with different risk levels
    evaluated_roles = pd.DataFrame([
        {"RESOURCE_NAME": "role1", "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
        {"RESOURCE_NAME": "role2", "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
        {"RESOURCE_NAME": "role3", "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
        {"RESOURCE_NAME": "role4", "COMPLIANCE_STATUS": "NonCompliant", "CONTROL_ID": "AC-3.AWS.39.v02"},
    ])
    
    # Create SLA data with different risk levels and open dates
    sla_data = pd.DataFrame([
        {"RESOURCE_ID": "role1", "CONTROL_RISK": "Critical", 
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=1)},  # Past Critical SLA (0 days)
        {"RESOURCE_ID": "role2", "CONTROL_RISK": "High", 
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=25)},  # Within High SLA (30 days)
        {"RESOURCE_ID": "role3", "CONTROL_RISK": "Medium", 
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=50)},  # Within Medium SLA (60 days)
        {"RESOURCE_ID": "role4", "CONTROL_RISK": "Low", 
         "OPEN_DATE_UTC_TIMESTAMP": base_date - timedelta(days=100)},  # Past Low SLA (90 days)
    ])
    
    metric_value, compliant_count, total_count, non_compliant_resources = pipeline._calculate_tier3_metrics(
        pd.DataFrame(),
        evaluated_roles,
        sla_data
    )
    
    # 2 within SLA (High, Medium), 2 past SLA (Critical, Low) = 50%
    assert metric_value == 50.0
    assert compliant_count == 2
    assert total_count == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])