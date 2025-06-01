import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components
from pipelines.pl_automated_monitoring_cloudradar_controls.pipeline import (
    PLAutomatedMonitoringCloudradarControls,
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
    """Generate threshold data for all three controls"""
    return pd.DataFrame([
        # CTRL-1077224 KMS Key Rotation thresholds
        {"monitoring_metric_id": 24, "control_id": "CTRL-1077224", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 25, "control_id": "CTRL-1077224", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
        # CTRL-1077231 EC2 Metadata thresholds  
        {"monitoring_metric_id": 26, "control_id": "CTRL-1077231", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 27, "control_id": "CTRL-1077231", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
        # CTRL-1077125 KMS Key Origin thresholds
        {"monitoring_metric_id": 28, "control_id": "CTRL-1077125", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 29, "control_id": "CTRL-1077125", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 97.0, "alerting_threshold": 95.0},
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


def _mock_kms_resources():
    """Generate mock KMS resources for testing CTRL-1077224 and CTRL-1077125"""
    return {
        "resourceConfigurations": [
            {
                "resourceId": "kms-key-1",
                "accountResourceId": "account1/kms-key-1",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"},
                    {"configurationName": "configuration.origin", "configurationValue": "AWS_KMS"}
                ],
                "supplementaryConfiguration": [
                    {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", 
                     "supplementaryConfigurationValue": "TRUE"}
                ]
            },
            {
                "resourceId": "kms-key-2",
                "accountResourceId": "account2/kms-key-2",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"},
                    {"configurationName": "configuration.origin", "configurationValue": "EXTERNAL"}
                ],
                "supplementaryConfiguration": [
                    {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", 
                     "supplementaryConfigurationValue": "FALSE"}
                ]
            },
            {
                "resourceId": "kms-key-3",
                "accountResourceId": "account3/kms-key-3",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"},
                    {"configurationName": "configuration.origin", "configurationValue": "AWS_KMS"}
                ],
                "supplementaryConfiguration": []  # No rotation status
            }
        ],
        "nextRecordKey": None
    }


def _mock_ec2_resources():
    """Generate mock EC2 resources for testing CTRL-1077231"""
    return {
        "resourceConfigurations": [
            {
                "resourceId": "i-1234567890abcdef0",
                "accountResourceId": "account1/i-1234567890abcdef0",
                "resourceType": "AWS::EC2::Instance",
                "configurationList": [
                    {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
                ],
                "supplementaryConfiguration": []
            },
            {
                "resourceId": "i-0987654321fedcba0",
                "accountResourceId": "account2/i-0987654321fedcba0",
                "resourceType": "AWS::EC2::Instance",
                "configurationList": [
                    {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "optional"}
                ],
                "supplementaryConfiguration": []
            },
            {
                "resourceId": "i-abcdef1234567890",
                "accountResourceId": "account3/i-abcdef1234567890",
                "resourceType": "AWS::EC2::Instance",
                "configurationList": [],  # No metadata options configured
                "supplementaryConfiguration": []
            }
        ],
        "nextRecordKey": None
    }


@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_multi_control_success():
    """Test successful metrics calculation for multiple controls using extract method"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    # Mock the thresholds data
    thresholds_df = _mock_multi_control_thresholds()
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock API responses
        with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
            mock_api_instance = Mock()
            mock_oauth.return_value = mock_api_instance
            
            # Setup side effect to return different responses based on resource type
            def send_request_side_effect(url, request_type, request_kwargs, **kwargs):
                payload = request_kwargs.get("json", {})
                search_params = payload.get("searchParameters", [])
                
                if search_params and search_params[0].get("resourceType") == "AWS::KMS::Key":
                    return generate_mock_api_response(_mock_kms_resources())
                elif search_params and search_params[0].get("resourceType") == "AWS::EC2::Instance":
                    return generate_mock_api_response(_mock_ec2_resources())
                
                return generate_mock_api_response({"resourceConfigurations": []})
            
            mock_api_instance.send_request.side_effect = send_request_side_effect
            
            # Call _calculate_metrics directly
            result = pipeline._calculate_metrics(thresholds_df)
            
            # Assertions
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert list(result.columns) == AVRO_SCHEMA_FIELDS
            assert len(result) == 6  # 2 metrics per control * 3 controls
            
            # Verify we have results for all three controls
            control_ids = set(result["control_id"].unique())
            assert control_ids == {"CTRL-1077224", "CTRL-1077231", "CTRL-1077125"}
            
            # Verify data types
            assert pd.api.types.is_integer_dtype(result["metric_value_numerator"])
            assert pd.api.types.is_integer_dtype(result["metric_value_denominator"])
            assert pd.api.types.is_float_dtype(result["monitoring_metric_value"])


@freeze_time("2024-11-05 12:09:00")
def test_kms_key_rotation_control_1077224():
    """Test CTRL-1077224 KMS Key Rotation logic"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    thresholds_df = _mock_multi_control_thresholds()
    thresholds_df = thresholds_df[thresholds_df["control_id"] == "CTRL-1077224"]
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
            mock_api_instance = Mock()
            mock_oauth.return_value = mock_api_instance
            mock_api_instance.send_request.return_value = generate_mock_api_response(_mock_kms_resources())
            
            result = pipeline._calculate_metrics(thresholds_df)
            
            tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
            tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
            
            # Tier 1: 2 out of 3 have rotation status (key-1=TRUE, key-2=FALSE, key-3=missing)
            assert tier1_row["metric_value_numerator"] == 2
            assert tier1_row["metric_value_denominator"] == 3
            assert tier1_row["monitoring_metric_value"] == pytest.approx(66.67, 0.01)
            
            # Tier 2: 1 out of 2 (with rotation status) have TRUE
            assert tier2_row["metric_value_numerator"] == 1
            assert tier2_row["metric_value_denominator"] == 2
            assert tier2_row["monitoring_metric_value"] == 50.0


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    empty_df = pd.DataFrame()
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline._calculate_metrics(empty_df)


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url


def test_control_configs_mapping():
    """Test that all expected controls are configured"""
    expected_controls = {"CTRL-1077224", "CTRL-1077231", "CTRL-1077125"}
    assert set(CONTROL_CONFIGS.keys()) == expected_controls
    
    # Verify each control has required configuration
    for control_id, config in CONTROL_CONFIGS.items():
        assert "resource_type" in config
        assert "config_key" in config
        assert "config_location" in config
        assert "expected_value" in config
        assert "apply_kms_exclusions" in config


def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    thresholds_df = _mock_multi_control_thresholds()
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
            mock_api_instance = Mock()
            mock_oauth.return_value = mock_api_instance
            mock_api_instance.send_request.side_effect = RequestException("Connection error")
            
            with pytest.raises(RuntimeError, match="Failed to fetch AWS::KMS::Key resources from API"):
                pipeline._calculate_metrics(thresholds_df)


def test_extract_method_integration():
    """Test the extract method integration with super().extract() and .iloc[0] fix"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    # Mock super().extract() to return thresholds data (as Series containing DataFrame)
    mock_thresholds = _mock_multi_control_thresholds()
    mock_df = pd.DataFrame({"thresholds_raw": [mock_thresholds]})
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock the parent class extract method directly
        with patch.object(ConfigPipeline, 'extract', return_value=mock_df):
            
            # Mock API calls to avoid actual network requests
            with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
                mock_api_instance = Mock()
                mock_oauth.return_value = mock_api_instance
                mock_api_instance.send_request.return_value = generate_mock_api_response({
                    "resourceConfigurations": [], "nextRecordKey": None
                })
                
                result = pipeline.extract()
                
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
    
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.PLAutomatedMonitoringCloudradarControls') as mock_pipeline_class:
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
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    # Create scenario with high compliance (100%)
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 24,
        "control_id": "CTRL-1077224",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 97.0,
        "alerting_threshold": 95.0
    }])
    
    high_compliance_resources = {
        "resourceConfigurations": [
            {
                "resourceId": "compliant-key",
                "accountResourceId": "account1/compliant-key",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                ],
                "supplementaryConfiguration": [
                    {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", 
                     "supplementaryConfigurationValue": "TRUE"}
                ]
            }
        ]
    }
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
            mock_api_instance = Mock()
            mock_oauth.return_value = mock_api_instance
            mock_api_instance.send_request.return_value = generate_mock_api_response(high_compliance_resources)
            
            result = pipeline._calculate_metrics(thresholds_df)
            
            # 100% compliance should be Green (>= 95% alerting threshold)
            assert result.iloc[0]["monitoring_metric_value"] == 100.0
            assert result.iloc[0]["monitoring_metric_status"] == "Green"


@freeze_time("2024-11-05 12:09:00")
def test_end_to_end_pipeline_execution():
    """End-to-end test simulating actual pipeline execution"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    # Setup test data
    test_data = {
        "thresholds_raw": [_mock_multi_control_thresholds()]
    }
    test_df = pd.DataFrame(test_data)
    
    # Mock OAuth token refresh
    with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
        mock_refresh.return_value = "test_token"
        
        # Mock all external dependencies
        with patch.object(ConfigPipeline, 'extract', return_value=test_df):
            
            with patch('pipelines.pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth:
                mock_api_instance = Mock()
                mock_oauth.return_value = mock_api_instance
                
                # Setup API responses
                def send_request_side_effect(url, request_type, request_kwargs, **kwargs):
                    payload = request_kwargs.get("json", {})
                    search_params = payload.get("searchParameters", [])
                    
                    if search_params and search_params[0].get("resourceType") == "AWS::KMS::Key":
                        return generate_mock_api_response(_mock_kms_resources())
                    elif search_params and search_params[0].get("resourceType") == "AWS::EC2::Instance":
                        return generate_mock_api_response(_mock_ec2_resources())
                    
                    return generate_mock_api_response({"resourceConfigurations": []})
                
                mock_api_instance.send_request.side_effect = send_request_side_effect
                
                # Execute pipeline extract
                result = pipeline.extract()
                
                # Verify the pipeline executed correctly
                assert "monitoring_metrics" in result.columns
                metrics_df = result["monitoring_metrics"].iloc[0]
                
                # Verify metrics were calculated
                assert isinstance(metrics_df, pd.DataFrame)
                assert len(metrics_df) == 6  # 2 tiers * 3 controls
                assert list(metrics_df.columns) == AVRO_SCHEMA_FIELDS
                
                # Verify all controls processed
                control_ids = set(metrics_df["control_id"].unique())
                assert control_ids == {"CTRL-1077224", "CTRL-1077231", "CTRL-1077125"}
                
                # Verify timestamp is correct
                assert all(metrics_df["control_monitoring_utc_timestamp"] == datetime(2024, 11, 5, 12, 9, 0))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])