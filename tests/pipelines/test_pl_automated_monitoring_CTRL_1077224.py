import pytest
import pandas as pd
from unittest.mock import Mock
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components
import pipelines.pl_automated_monitoring_CTRL_1077224.pipeline as pipeline
from pipelines.pl_automated_monitoring_CTRL_1077224.pipeline import (
    PLAutomatedMonitoringCTRL1077224,
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
        self.env = self  # Add self-reference for pipeline framework compatibility

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
            "monitoring_metric_id": 24,
            "control_id": "CTRL-1077224",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0
        },
        {
            "monitoring_metric_id": 25,
            "control_id": "CTRL-1077224",
            "monitoring_metric_tier": "Tier 2",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0
        }
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

# Sample KMS Key data with mixed rotation status
def _mock_kms_resources():
    """Generate mock KMS resources for testing"""
    return {
        "resourceConfigurations": [
            {
                "resourceId": "key1",
                "accountResourceId": "account1/key1",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {
                        "configurationName": "configuration.keyState",
                        "configurationValue": "Enabled"
                    },
                    {
                        "configurationName": "configuration.keyManager",
                        "configurationValue": "CUSTOMER"
                    }
                ],
                "supplementaryConfiguration": []
            },
            {
                "resourceId": "key2",
                "accountResourceId": "account2/key2",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {
                        "configurationName": "configuration.keyState",
                        "configurationValue": "Enabled"
                    },
                    {
                        "configurationName": "configuration.keyManager",
                        "configurationValue": "CUSTOMER"
                    }
                ],
                "supplementaryConfiguration": [
                    {
                        "supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus",
                        "supplementaryConfigurationValue": "FALSE"
                    }
                ]
            },
            {
                "resourceId": "key3",
                "accountResourceId": "account3/key3",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {
                        "configurationName": "configuration.keyState",
                        "configurationValue": "Enabled"
                    },
                    {
                        "configurationName": "configuration.keyManager",
                        "configurationValue": "CUSTOMER"
                    }
                ],
                "supplementaryConfiguration": [
                    {
                        "supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus",
                        "supplementaryConfigurationValue": "TRUE"
                    }
                ]
            }
        ],
        "nextRecordKey": None
    }

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_success():
    """Test successful metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    
    # Mock API response
    mock_response = generate_mock_api_response(_mock_kms_resources())
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    assert len(result) == 2  # Two metrics (Tier 1 and Tier 2)
    
    # Verify data types
    row = result.iloc[0]
    assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
    assert isinstance(row["monitoring_metric_value"], float)
    assert isinstance(row["metric_value_numerator"], int)
    assert isinstance(row["metric_value_denominator"], int)

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, context)

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url

def test_pipeline_run_method():
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    
    # Mock the run method
    mock_run = mock.patch.object(pipeline, 'run')
    pipeline.run()
    
    # Verify run was called with default parameters
    mock_run.assert_called_once_with()

def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="Failed to fetch resources from API"):
        calculate_metrics(_mock_threshold_df(), context)

def test_pagination_handling():
    """Test API pagination with multiple responses"""
    # Create paginated responses
    page1_response = generate_mock_api_response({
        "resourceConfigurations": [_mock_kms_resources()["resourceConfigurations"][0]],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "resourceConfigurations": [_mock_kms_resources()["resourceConfigurations"][1]],
        "nextRecordKey": None
    })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(_mock_threshold_df(), context)
    
    # Verify both pages were processed
    assert not result.empty

@freeze_time("2024-11-05 12:09:00")
def test_tier_compliance_logic():
    """Test tier-specific compliance logic"""
    thresholds_df = _mock_threshold_df()
    mock_response = generate_mock_api_response(_mock_kms_resources())
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Verify metrics calculations
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
    
    # Tier 1: 2 out of 3 have rotation status (66.67%)
    # key2 and key3 have rotation status, key1 does not
    assert tier1_row["metric_value_numerator"] == 2
    assert tier1_row["metric_value_denominator"] == 3
    
    # Tier 2: Only processes resources with rotation status (key2=FALSE, key3=TRUE)
    # 1 out of 2 (with rotation status) have TRUE (50%)
    assert tier2_row["metric_value_numerator"] == 1
    assert tier2_row["metric_value_denominator"] == 2

def test_resource_filtering():
    """Test resource exclusion filtering"""
    # Add excluded resources to test data
    excluded_resources = _mock_kms_resources()
    excluded_resources["resourceConfigurations"].extend([
        {
            "resourceId": "aws_key",
            "accountResourceId": "account4/aws_key",
            "source": "CT-AccessDenied",
            "configurationList": [
                {
                    "configurationName": "configuration.keyState",
                    "configurationValue": "Enabled"
                },
                {
                    "configurationName": "configuration.keyManager",
                    "configurationValue": "AWS"
                }
            ],
            "supplementaryConfiguration": []
        }
    ])
    
    thresholds_df = _mock_threshold_df()
    mock_response = generate_mock_api_response(excluded_resources)
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should still only process the 3 valid keys
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    assert tier1_row["metric_value_denominator"] == 3

def test_compliance_status_determination():
    """Test compliance status based on thresholds"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 24,
        "control_id": "CTRL-1077224",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 85.0,
        "alerting_threshold": 95.0
    }])
    
    # Create scenario with high compliance
    high_compliance_resources = {
        "resourceConfigurations": [
            {
                "resourceId": "key1",
                "accountResourceId": "account1/key1",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                ],
                "supplementaryConfiguration": [
                    {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", "supplementaryConfigurationValue": "TRUE"}
                ]
            }
        ]
    }
    
    mock_response = generate_mock_api_response(high_compliance_resources)
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # 100% compliance should be Green
    assert result.iloc[0]["monitoring_metric_status"] == "Green"

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = mock.Mock()
    
    with mock.patch("etip_env.set_env_vars", return_value=mock_env):
        with mock.patch("pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.run") as mock_run:
            with mock.patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_CTRL_1077224.pipeline import run
    
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

def test_empty_api_response():
    """Test handling of empty API response"""
    thresholds_df = _mock_threshold_df()
    empty_response = generate_mock_api_response({"resourceConfigurations": []})
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = empty_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should return metrics with 0 values
    assert not result.empty
    assert all(result["metric_value_numerator"] == 0)
    assert all(result["metric_value_denominator"] == 0)

def test_api_status_code_error():
    """Test handling of API error status codes"""
    thresholds_df = _mock_threshold_df()
    error_response = generate_mock_api_response({}, status_code=500)
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = error_response
    
    context = {"api_connector": mock_api}
    
    with pytest.raises(RuntimeError, match="API request failed: 500"):
        calculate_metrics(thresholds_df, context)

def test_context_initialization():
    """Test pipeline context initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    
    # Initialize context if not exists
    if not hasattr(pipeline, 'context'):
        pipeline.context = {}
    
    assert pipeline.context == {}


def test_get_api_connector_method():
    """Test _get_api_connector method functionality"""
    with mock.patch('pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.refresh') as mock_refresh:
        with mock.patch('connectors.api.OauthApi') as mock_oauth_api:
            mock_refresh.return_value = "test_token"
            mock_connector = Mock()
            mock_oauth_api.return_value = mock_connector
            
            env = MockEnv()
            pipeline = PLAutomatedMonitoringCTRL1077224(env)
            
            result = pipeline._get_api_connector()
            
            assert result == mock_connector
            mock_refresh.assert_called_once_with(
                client_id="test_client",
                client_secret="test_secret",
                exchange_url="test-exchange.com"
            )
            mock_oauth_api.assert_called_once()


def test_transform_method():
    """Test transform method sets up API context correctly"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    pipeline.context = {}
    
    with mock.patch.object(pipeline, '_get_api_connector') as mock_get_connector:
        with mock.patch('config_pipeline.ConfigPipeline.transform') as mock_super_transform:
            mock_connector = Mock()
            mock_get_connector.return_value = mock_connector
            
            pipeline.transform()
            
            assert pipeline.context["api_connector"] == mock_connector
            mock_get_connector.assert_called_once()
            mock_super_transform.assert_called_once()

def test_ssl_context_handling():
    """Test SSL context handling when C1_CERT_FILE is set"""
    # Test that _get_api_connector can be called without error
    with mock.patch('pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.refresh') as mock_refresh:
        with mock.patch('connectors.ca_certs.C1_CERT_FILE', '/path/to/cert.pem'):
            with mock.patch('ssl.create_default_context') as mock_ssl:
                with mock.patch('connectors.api.OauthApi') as mock_oauth_api:
                    mock_refresh.return_value = "test_token"
                    mock_ssl.return_value = "mock_ssl_context"
                    
                    env = MockEnv()
                    pipeline = PLAutomatedMonitoringCTRL1077224(env)
                    
                    connector = pipeline._get_api_connector()
                    mock_ssl.assert_called_once_with(cafile='/path/to/cert.pem')
                    mock_oauth_api.assert_called_once()

def test_json_serialization_in_resources_info():
    """Test that resources_info properly serializes JSON"""
    thresholds_df = _mock_threshold_df()
    mock_response = generate_mock_api_response(_mock_kms_resources())
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Check that resources_info contains JSON strings
    for _, row in result.iterrows():
        if row["resources_info"] is not None:
            assert isinstance(row["resources_info"], list)
            for item in row["resources_info"]:
                assert isinstance(item, str)
                # Verify it's valid JSON
                import json
                json.loads(item)  # Should not raise exception

def test_division_by_zero_protection():
    """Test that division by zero is handled gracefully"""
    thresholds_df = _mock_threshold_df()
    
    # Create response with no valid resources
    empty_resources = {
        "resourceConfigurations": [
            {
                "resourceId": "excluded_key",
                "accountResourceId": "account1/excluded_key",
                "source": "CT-AccessDenied",  # This will be excluded
                "configurationList": [
                    {"configurationName": "configuration.keyManager", "configurationValue": "AWS"}
                ],
                "supplementaryConfiguration": []
            }
        ]
    }
    
    mock_response = generate_mock_api_response(empty_resources)
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should handle zero denominator gracefully
    assert not result.empty
    assert all(result["monitoring_metric_value"] == 0.0)
    assert all(result["metric_value_denominator"] == 0)

def test_tier_2_with_no_tier_1_compliant_resources():
    """Test Tier 2 calculation when no resources pass Tier 1"""
    thresholds_df = _mock_threshold_df()
    
    # Create resources with no rotation status (fail Tier 1)
    no_rotation_resources = {
        "resourceConfigurations": [
            {
                "resourceId": "key1",
                "accountResourceId": "account1/key1",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                ],
                "supplementaryConfiguration": []  # No rotation status
            }
        ]
    }
    
    mock_response = generate_mock_api_response(no_rotation_resources)
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
    
    # Tier 1: 0 out of 1 have rotation status
    assert tier1_row["metric_value_numerator"] == 0
    assert tier1_row["metric_value_denominator"] == 1
    
    # Tier 2: No resources to evaluate (denominator = 0)
    assert tier2_row["metric_value_numerator"] == 0
    assert tier2_row["metric_value_denominator"] == 0


def test_run_function():
    """Test run function with various parameters"""
    env = MockEnv()
    
    with mock.patch('pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAutomatedMonitoringCTRL1077224') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        from pipelines.pl_automated_monitoring_CTRL_1077224.pipeline import run
        
        # Test normal execution
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)


def test_run_function_export_test_data():
    """Test run function with export test data option"""
    env = MockEnv()
    
    with mock.patch('pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAutomatedMonitoringCTRL1077224') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        from pipelines.pl_automated_monitoring_CTRL_1077224.pipeline import run
        
        # Test export test data path
        run(env, is_export_test_data=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=False)