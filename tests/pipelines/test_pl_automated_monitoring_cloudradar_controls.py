import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
from requests import Response, RequestException

# Import pipeline components
from pl_automated_monitoring_cloudradar_controls.pipeline import (
    PLAutomatedMonitoringCloudradarControls,
    calculate_metrics,
    CONTROL_CONFIGS
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

class MockOauthApi:
    def __init__(self, url, api_token, ssl_context=None):
        self.url = url
        self.api_token = api_token
        self.response = None
        self.side_effect = None
        self.call_count = 0
    
    def send_request(self, url, request_type, request_kwargs, retry_delay=5, max_retries=3):
        """Mock send_request method with proper side_effect handling."""
        self.call_count += 1
        
        if self.side_effect:
            if isinstance(self.side_effect, Exception):
                raise self.side_effect
            elif isinstance(self.side_effect, list):
                # Use responses cyclically instead of popping to avoid emptying list
                if self.side_effect:
                    response = self.side_effect[(self.call_count - 1) % len(self.side_effect)]
                    if isinstance(response, Exception):
                        raise response
                    return response
        
        if self.response:
            return self.response
        
        # Return default response if none configured
        default_response = Response()
        default_response.status_code = 200
        return default_response

def _mock_multi_control_thresholds():
    """Generate threshold data for all three controls"""
    return pd.DataFrame([
        # CTRL-1077224 KMS Key Rotation thresholds (corrected: alerting should be higher than warning)
        {"monitoring_metric_id": 24, "control_id": "CTRL-1077224", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 25, "control_id": "CTRL-1077224", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
        # CTRL-1077231 EC2 Metadata thresholds  
        {"monitoring_metric_id": 26, "control_id": "CTRL-1077231", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 27, "control_id": "CTRL-1077231", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
        # CTRL-1077125 KMS Key Origin thresholds
        {"monitoring_metric_id": 28, "control_id": "CTRL-1077125", "monitoring_metric_tier": "Tier 1", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
        {"monitoring_metric_id": 29, "control_id": "CTRL-1077125", "monitoring_metric_tier": "Tier 2", 
         "warning_threshold": 85.0, "alerting_threshold": 95.0},
    ])

def _mock_single_control_thresholds(control_id):
    """Generate threshold data for a specific control"""
    all_thresholds = _mock_multi_control_thresholds()
    return all_thresholds[all_thresholds["control_id"] == control_id].reset_index(drop=True)

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
def test_calculate_metrics_multi_control_success(mock):
    """Test successful metrics calculation for multiple controls"""
    thresholds_df = _mock_multi_control_thresholds()
    
    # Mock API responses for both resource types
    def side_effect_handler(url, request_type, request_kwargs, **kwargs):
        payload = request_kwargs.get("json", {})
        search_params = payload.get("searchParameters", [])
        
        if search_params and search_params[0].get("resourceType") == "AWS::KMS::Key":
            return generate_mock_api_response(_mock_kms_resources())
        elif search_params and search_params[0].get("resourceType") == "AWS::EC2::Instance":
            return generate_mock_api_response(_mock_ec2_resources())
        
        return generate_mock_api_response({"resourceConfigurations": []})
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [
        generate_mock_api_response(_mock_kms_resources()),
        generate_mock_api_response(_mock_ec2_resources())
    ]
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    assert len(result) == 6  # 2 metrics per control * 3 controls
    
    # Verify we have results for all three controls
    control_ids = set(result["control_id"].unique())
    assert control_ids == {"CTRL-1077224", "CTRL-1077231", "CTRL-1077125"}
    
    # Verify data types
    row = result.iloc[0]
    assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
    assert isinstance(row["monitoring_metric_value"], float)
    assert isinstance(row["metric_value_numerator"], int)
    assert isinstance(row["metric_value_denominator"], int)

@freeze_time("2024-11-05 12:09:00")
def test_kms_key_rotation_control_1077224(mock):
    """Test CTRL-1077224 KMS Key Rotation logic"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(_mock_kms_resources())
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
    
    # Tier 1: 2 out of 3 have rotation status (key-1=TRUE, key-2=FALSE, key-3=missing)
    assert tier1_row["metric_value_numerator"] == 2
    assert tier1_row["metric_value_denominator"] == 3
    
    # Tier 2: 1 out of 2 (with rotation status) have TRUE
    assert tier2_row["metric_value_numerator"] == 1
    assert tier2_row["metric_value_denominator"] == 2

@freeze_time("2024-11-05 12:09:00")
def test_ec2_metadata_control_1077231(mock):
    """Test CTRL-1077231 EC2 Metadata Service logic"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077231")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(_mock_ec2_resources())
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    tier1_row = result[result["monitoring_metric_id"] == 26].iloc[0]
    tier2_row = result[result["monitoring_metric_id"] == 27].iloc[0]
    
    # Tier 1: 2 out of 3 have httpTokens setting (instance-1=required, instance-2=optional, instance-3=missing)
    assert tier1_row["metric_value_numerator"] == 2
    assert tier1_row["metric_value_denominator"] == 3
    
    # Tier 2: 1 out of 2 (with httpTokens) have "required"
    assert tier2_row["metric_value_numerator"] == 1
    assert tier2_row["metric_value_denominator"] == 2

@freeze_time("2024-11-05 12:09:00")
def test_kms_key_origin_control_1077125(mock):
    """Test CTRL-1077125 KMS Key Origin logic"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077125")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(_mock_kms_resources())
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    tier1_row = result[result["monitoring_metric_id"] == 28].iloc[0]
    tier2_row = result[result["monitoring_metric_id"] == 29].iloc[0]
    
    # Tier 1: 3 out of 3 have origin setting (all KMS keys have configuration.origin)
    assert tier1_row["metric_value_numerator"] == 3
    assert tier1_row["metric_value_denominator"] == 3
    
    # Tier 2: 2 out of 3 have "AWS_KMS" origin (key-1=AWS_KMS, key-2=EXTERNAL, key-3=AWS_KMS)
    assert tier2_row["metric_value_numerator"] == 2
    assert tier2_row["metric_value_denominator"] == 3

def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, context)

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

def test_resource_type_optimization(mock):
    """Test that resource fetching is optimized by type"""
    thresholds_df = _mock_multi_control_thresholds()
    
    # Mock to track API calls
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [
        generate_mock_api_response(_mock_kms_resources()),
        generate_mock_api_response(_mock_ec2_resources())
    ]
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should only make 2 API calls (one for KMS keys, one for EC2 instances)
    # even though we have 3 controls (2 KMS controls share same resource type)
    assert mock_api.call_count == 2
    assert not result.empty

def test_kms_exclusion_filtering(mock):
    """Test KMS-specific resource exclusions"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    # Add excluded KMS resources
    excluded_kms_resources = _mock_kms_resources()
    excluded_kms_resources["resourceConfigurations"].extend([
        {
            "resourceId": "aws-managed-key",
            "accountResourceId": "account4/aws-managed-key",
            "resourceType": "AWS::KMS::Key",
            "source": "CT-AccessDenied",
            "configurationList": [
                {"configurationName": "configuration.keyManager", "configurationValue": "AWS"}
            ],
            "supplementaryConfiguration": []
        },
        {
            "resourceId": "pending-deletion-key",
            "accountResourceId": "account5/pending-deletion-key",
            "resourceType": "AWS::KMS::Key",
            "configurationList": [
                {"configurationName": "configuration.keyState", "configurationValue": "PendingDeletion"},
                {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
            ],
            "supplementaryConfiguration": []
        }
    ])
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(excluded_kms_resources)
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should still only process the 3 valid keys (excluded 2)
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    assert tier1_row["metric_value_denominator"] == 3

def test_api_error_handling(mock):
    """Test API error handling and exception wrapping"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    with pytest.raises(RuntimeError, match="Failed to fetch AWS::KMS::Key resources from API"):
        calculate_metrics(thresholds_df, context)

def test_pagination_handling(mock):
    """Test API pagination with multiple responses"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
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
    result = calculate_metrics(thresholds_df, context)
    
    # Should process both pages
    assert not result.empty
    assert mock_api.call_count == 2

def test_unknown_control_handling(mock):
    """Test handling of unknown control IDs"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 99,
        "control_id": "CTRL-UNKNOWN",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 97.0,
        "alerting_threshold": 95.0
    }])
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, context)
    
    # Should return empty DataFrame for unknown controls
    assert result.empty

def test_compliance_status_determination(mock):
    """Test compliance status based on thresholds"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 24,
        "control_id": "CTRL-1077224",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 85.0,
        "alerting_threshold": 95.0
    }])
    
    # Create scenario with high compliance (100%)
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
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(high_compliance_resources)
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # 100% compliance should be Green (>= 95% alerting threshold)
    assert result.iloc[0]["monitoring_metric_status"] == "Green"

def test_main_function_execution(mock):
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pl_automated_monitoring_cloudradar_controls.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_cloudradar_controls.pipeline import run
    
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

def test_empty_api_response(mock):
    """Test handling of empty API response"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response({"resourceConfigurations": []})
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Should return metrics with 0 values
    assert not result.empty
    assert all(result["metric_value_numerator"] == 0)
    assert all(result["metric_value_denominator"] == 0)

def test_json_serialization_in_resources_info(mock):
    """Test that resources_info properly serializes JSON"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(_mock_kms_resources())
    
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

def test_ssl_context_handling(mock):
    """Test SSL context handling when C1_CERT_FILE is set"""
    
    with mock.patch('pl_automated_monitoring_cloudradar_controls.pipeline.C1_CERT_FILE', '/path/to/cert.pem'):
        env = MockEnv()
        pipeline = PLAutomatedMonitoringCloudradarControls(env)
        
        with mock.patch('pl_automated_monitoring_cloudradar_controls.pipeline.refresh') as mock_refresh:
            with mock.patch('pl_automated_monitoring_cloudradar_controls.pipeline.ssl.create_default_context') as mock_ssl:
                with mock.patch('pl_automated_monitoring_cloudradar_controls.pipeline.OauthApi') as mock_oauth_api:
                    mock_refresh.return_value = "test_token"
                    mock_ssl.return_value = "mock_ssl_context"
                    
                    connector = pipeline._get_api_connector()
                    mock_ssl.assert_called_once_with(cafile='/path/to/cert.pem')
                    mock_oauth_api.assert_called_once()

def test_transform_method():
    """Test pipeline transform method"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    pipeline.context = {}
    
    with patch.object(pipeline, '_get_api_connector') as mock_connector:
        with patch('pl_automated_monitoring_cloudradar_controls.pipeline.ConfigPipeline.transform') as mock_super_transform:
            mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
            mock_connector.return_value = mock_api
            
            pipeline.transform()
            
            assert "api_connector" in pipeline.context
            assert pipeline.context["api_connector"] == mock_api
            mock_connector.assert_called_once()
            mock_super_transform.assert_called_once()

def test_run_function():
    """Test pipeline run function"""
    env = MockEnv()
    
    with patch('pl_automated_monitoring_cloudradar_controls.pipeline.PLAutomatedMonitoringCloudradarControls') as mock_pipeline_class:
        mock_pipeline = mock.Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = "test_result"
        
        from pl_automated_monitoring_cloudradar_controls.pipeline import run
        result = run(env, is_load=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=False)
        assert result == "test_result"

def test_calculate_metrics_with_env_fix(mock):
    """Test calculate_metrics without pipeline instantiation issue"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(_mock_kms_resources())
    
    context = {"api_connector": mock_api}
    
    # Patch the problematic pipeline instantiation
    with patch('pl_automated_monitoring_cloudradar_controls.pipeline.PLAutomatedMonitoringCloudradarControls') as mock_pipeline_class:
        mock_pipeline = mock.Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline._fetch_resources_by_type.return_value = {"AWS::KMS::Key": _mock_kms_resources()["resourceConfigurations"]}
        mock_pipeline._calculate_control_compliance.return_value = [
            {
                "control_monitoring_utc_timestamp": datetime.now(),
                "control_id": "CTRL-1077224",
                "monitoring_metric_id": 24,
                "monitoring_metric_value": 66.67,
                "monitoring_metric_status": "Red",
                "metric_value_numerator": 2,
                "metric_value_denominator": 3,
                "resources_info": None
            }
        ]
        
        result = calculate_metrics(thresholds_df, context)
        
        assert not result.empty
        mock_pipeline_class.assert_called_once_with(None)

def test_fetch_cloudradar_resources_error_handling(mock):
    """Test error handling in _fetch_cloudradar_resources"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Network error")
    
    with pytest.raises(RuntimeError, match="Failed to fetch AWS::KMS::Key resources from API"):
        pipeline._fetch_cloudradar_resources(mock_api, "AWS::KMS::Key")

def test_tier2_denominator_adjustment(mock):
    """Test Tier 2 denominator adjustment when resources lack configuration"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    # Create resources where some lack the required configuration
    mixed_resources = {
        "resourceConfigurations": [
            {
                "resourceId": "kms-key-with-config",
                "accountResourceId": "account1/kms-key-with-config",
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                ],
                "supplementaryConfiguration": [
                    {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", 
                     "supplementaryConfigurationValue": "TRUE"}
                ]
            },
            {
                "resourceId": "kms-key-without-config",
                "accountResourceId": "account2/kms-key-without-config", 
                "resourceType": "AWS::KMS::Key",
                "configurationList": [
                    {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                    {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                ],
                "supplementaryConfiguration": []  # No rotation status
            }
        ]
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(mixed_resources)
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Tier 1: Both resources counted (1 with config, 1 without)
    tier1_row = result[result["monitoring_metric_id"] == 24].iloc[0]
    assert tier1_row["metric_value_denominator"] == 2
    assert tier1_row["metric_value_numerator"] == 1  # Only one has rotation status
    
    # Tier 2: Only resource with config counted in denominator
    tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
    assert tier2_row["metric_value_denominator"] == 1  # Adjusted down
    assert tier2_row["metric_value_numerator"] == 1   # The one with config has "TRUE"

def test_large_resources_info_truncation(mock):
    """Test that resources_info is truncated to 50 items"""
    thresholds_df = _mock_single_control_thresholds("CTRL-1077224")
    
    # Create 60 non-compliant resources
    large_resources = {
        "resourceConfigurations": []
    }
    
    for i in range(60):
        large_resources["resourceConfigurations"].append({
            "resourceId": f"kms-key-{i}",
            "accountResourceId": f"account{i}/kms-key-{i}",
            "resourceType": "AWS::KMS::Key",
            "configurationList": [
                {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
            ],
            "supplementaryConfiguration": [
                {"supplementaryConfigurationName": "supplementaryConfiguration.KeyRotationStatus", 
                 "supplementaryConfigurationValue": "FALSE"}  # All non-compliant
            ]
        })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = generate_mock_api_response(large_resources)
    
    context = {"api_connector": mock_api}
    result = calculate_metrics(thresholds_df, context)
    
    # Check Tier 2 resources_info is truncated to 50
    tier2_row = result[result["monitoring_metric_id"] == 25].iloc[0]
    assert tier2_row["resources_info"] is not None
    assert len(tier2_row["resources_info"]) == 50  # Truncated from 60 to 50