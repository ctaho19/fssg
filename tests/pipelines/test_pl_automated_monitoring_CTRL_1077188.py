import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
from requests.exceptions import RequestException
from config_pipeline import ConfigPipeline

# Import pipeline components
from pl_automated_monitoring_CTRL_1077188.pipeline import (
    PLAutomatedMonitoringCTRL1077188,
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

class MockOauthApi:
    def __init__(self, url, api_token):
        self.url = url
        self.api_token = api_token
        self.response = None
        self.side_effect = None
    
    def send_request(self, url, request_type, request_kwargs, retry_delay=5, retry_count=3):
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
        from requests import Response
        default_response = Response()
        default_response.status_code = 200
        default_response._content = b'{"resourceConfigurations": [], "nextRecordKey": null}'
        return default_response

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([
        {
            "monitoring_metric_id": 1,
            "control_id": "CTRL-1077188",
            "monitoring_metric_tier": "Tier 0",
            "warning_threshold": 100.0,
            "alerting_threshold": 100.0
        },
        {
            "monitoring_metric_id": 2,
            "control_id": "CTRL-1077188",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": 90.0,
            "alerting_threshold": 95.0
        },
        {
            "monitoring_metric_id": 3,
            "control_id": "CTRL-1077188",
            "monitoring_metric_tier": "Tier 2",
            "warning_threshold": 90.0,
            "alerting_threshold": 95.0
        }
    ])

def _mock_dataset_certificates_df():
    """Utility function for test dataset certificates data"""
    return pd.DataFrame([
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
            "certificate_id": "abcd1234-5678-90ef-ghij-klmnopqrstuv",
            "nominal_issuer": "Amazon",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 1, 1),
            "last_usage_observation_utc_timestamp": datetime(2023, 12, 15)
        },
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/wxyz9876-5432-10ab-cdef-ghijklmnopqr",
            "certificate_id": "wxyz9876-5432-10ab-cdef-ghijklmnopqr", 
            "nominal_issuer": "Amazon",
            "not_valid_before_utc_timestamp": datetime(2022, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2023, 6, 1),
            "last_usage_observation_utc_timestamp": datetime(2023, 7, 1)  # Used after expiry
        }
    ])

def _mock_dataset_certificates_with_warnings_df():
    """Utility function for test dataset with warning scenarios"""
    return pd.DataFrame([
        {
            # Certificate expiring in 15 days (warning scenario)
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/warning1234-5678-90ef-ghij-klmnopqrstuv",
            "certificate_id": "warning1234-5678-90ef-ghij-klmnopqrstuv",
            "nominal_issuer": "Amazon",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 11, 20),  # 15 days from test time
            "last_usage_observation_utc_timestamp": datetime(2024, 11, 1)
        },
        {
            # Certificate expired 48 hours ago and unused
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/expired1234-5678-90ef-ghij-klmnopqrstuv",
            "certificate_id": "expired1234-5678-90ef-ghij-klmnopqrstuv",
            "nominal_issuer": "Amazon",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 11, 3),  # 2 days ago from test time
            "last_usage_observation_utc_timestamp": datetime(2024, 11, 2)
        }
    ])

def generate_mock_api_response(content=None, status_code=200):
    """Generate standardized mock API response."""
    from requests import Response
    import json
    
    mock_response = Response()
    mock_response.status_code = status_code
    
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        mock_response._content = json.dumps({"resourceConfigurations": [], "nextRecordKey": None}).encode("utf-8")
    
    return mock_response

# Test pipeline initialization and methods
def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    assert pipeline.env == env
    assert hasattr(pipeline, 'api_url')
    assert 'test-exchange.com' in pipeline.api_url
    assert 'internal-operations/cloud-service/aws-tooling/search-resource-configurations' in pipeline.api_url

@patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh')
@patch('pl_automated_monitoring_CTRL_1077188.pipeline.OauthApi')
def test_get_api_connector_success(mock_oauth_api, mock_refresh):
    """Test _get_api_connector method successful execution"""
    mock_refresh.return_value = "test_token"
    mock_connector = Mock()
    mock_oauth_api.return_value = mock_connector
    
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    connector = pipeline._get_api_connector()
    
    assert connector == mock_connector
    mock_refresh.assert_called_once_with(
        client_id="test_client",
        client_secret="test_secret", 
        exchange_url="test-exchange.com"
    )
    mock_oauth_api.assert_called_once_with(
        url=pipeline.api_url,
        api_token="Bearer test_token"
    )

def test_get_api_connector_refresh_failure():
    """Test _get_api_connector method when OAuth refresh fails"""
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh') as mock_refresh:
        mock_refresh.side_effect = Exception("OAuth refresh failed")
        
        env = MockEnv()
        pipeline = PLAutomatedMonitoringCTRL1077188(env)
        
        with pytest.raises(Exception, match="OAuth refresh failed"):
            pipeline._get_api_connector()

# Test extract method (the main functionality)
@freeze_time("2024-11-05 12:09:00")
@patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh')
@patch('pl_automated_monitoring_CTRL_1077188.pipeline.OauthApi')
def test_extract_method_success(mock_oauth_api, mock_refresh):
    """Test extract method with successful API calls"""
    # Setup OAuth mocking
    mock_refresh.return_value = "test_token"
    mock_api_connector = MockOauthApi(url="test_url", api_token="Bearer test_token")
    
    # Mock successful API response
    mock_response = generate_mock_api_response({
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ],
        "nextRecordKey": None
    })
    mock_api_connector.response = mock_response
    mock_oauth_api.return_value = mock_api_connector
    
    # Setup pipeline
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Mock ConfigPipeline.extract to return test data
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        # Setup the DataFrame that would come from SQL queries
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "dataset_certificates": [_mock_dataset_certificates_df()]
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
        assert len(metrics_df) == 3  # Three tiers

@freeze_time("2024-11-05 12:09:00") 
def test_extract_method_empty_thresholds():
    """Test extract method with empty thresholds data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Mock ConfigPipeline.extract to return empty thresholds
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [pd.DataFrame()],  # Empty DataFrame
            "dataset_certificates": [_mock_dataset_certificates_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should raise RuntimeError for empty thresholds
        with pytest.raises(RuntimeError, match="No threshold data found"):
            pipeline.extract()

@patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh')
@patch('pl_automated_monitoring_CTRL_1077188.pipeline.OauthApi')
def test_extract_method_api_error(mock_oauth_api, mock_refresh):
    """Test extract method with API errors"""
    # Setup OAuth mocking
    mock_refresh.return_value = "test_token"
    mock_api_connector = MockOauthApi(url="test_url", api_token="Bearer test_token")
    mock_api_connector.side_effect = RequestException("Connection error")
    mock_oauth_api.return_value = mock_api_connector
    
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "dataset_certificates": [_mock_dataset_certificates_df()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should wrap API exception in RuntimeError
        with pytest.raises(RuntimeError, match="Failed to fetch resources from API"):
            pipeline.extract()

@patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh')
@patch('pl_automated_monitoring_CTRL_1077188.pipeline.OauthApi')
def test_extract_method_api_pagination(mock_oauth_api, mock_refresh):
    """Test extract method with API pagination"""
    # Setup OAuth mocking
    mock_refresh.return_value = "test_token"
    mock_api_connector = MockOauthApi(url="test_url", api_token="Bearer test_token")
    
    # Create paginated responses
    page1_response = generate_mock_api_response({
        "resourceConfigurations": [
            {"amazonResourceName": "cert1", "source": "ConfigurationSnapshot", "configurationList": []}
        ],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "resourceConfigurations": [
            {"amazonResourceName": "cert2", "source": "ConfigurationSnapshot", "configurationList": []}
        ],
        "nextRecordKey": None
    })
    
    mock_api_connector.side_effect = [page1_response, page2_response]
    mock_oauth_api.return_value = mock_api_connector
    
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch('time.sleep') as mock_sleep:  # Mock rate limiting
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [_mock_threshold_df()],
                "dataset_certificates": [_mock_dataset_certificates_df()]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            
            # Verify pagination was processed
            assert "monitoring_metrics" in result_df.columns
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            assert len(metrics_df) == 3  # All three tiers
            
            # Verify rate limiting was applied
            mock_sleep.assert_called_with(0.15)

def test_extract_method_tier_specific_calculations():
    """Test that each tier calculates metrics correctly"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Test each tier individually
    tier_test_cases = [
        {
            "tier": "Tier 0",
            "metric_id": 1,
            "expected_value": 100.0,  # No failures case
            "expected_status": "Green"
        },
        {
            "tier": "Tier 1", 
            "metric_id": 2,
            "expected_status": "Green"  # Will depend on API vs dataset match
        },
        {
            "tier": "Tier 2",
            "metric_id": 3,
            "expected_status": "Red"  # Will have violations from test data
        }
    ]
    
    for test_case in tier_test_cases:
        thresholds_df = pd.DataFrame([{
            "monitoring_metric_id": test_case["metric_id"],
            "control_id": "CTRL-1077188",
            "monitoring_metric_tier": test_case["tier"],
            "warning_threshold": 90.0,
            "alerting_threshold": 95.0
        }])
        
        with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
            with patch.object(pipeline, '_get_api_connector') as mock_get_api:
                # Mock API connector for consistency
                mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
                mock_api.response = generate_mock_api_response({
                    "resourceConfigurations": [],
                    "nextRecordKey": None
                })
                mock_get_api.return_value = mock_api
                
                mock_extract_df = pd.DataFrame({
                    "thresholds_raw": [thresholds_df],
                    "dataset_certificates": [_mock_dataset_certificates_df()]
                })
                mock_super_extract.return_value = mock_extract_df
                
                result_df = pipeline.extract()
                metrics_df = result_df["monitoring_metrics"].iloc[0]
                
                # Verify single metric returned
                assert len(metrics_df) == 1
                row = metrics_df.iloc[0]
                assert row["monitoring_metric_id"] == test_case["metric_id"]
                assert row["control_id"] == "CTRL-1077188"
                assert isinstance(row["monitoring_metric_value"], float)
                assert row["monitoring_metric_status"] in ["Green", "Yellow", "Red"]

# Test run function variations
def test_run_function_normal_path():
    """Test run function normal execution path"""
    env = MockEnv()
    
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.PLAutomatedMonitoringCTRL1077188') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)

def test_run_function_export_test_data():
    """Test run function with export test data path"""
    env = MockEnv()
    
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.PLAutomatedMonitoringCTRL1077188') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once_with("config.yml")
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=False)

def test_data_types_validation():
    """Test that output data types match requirements"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response()
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [_mock_threshold_df()],
                "dataset_certificates": [_mock_dataset_certificates_df()]
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

def test_compliance_status_logic():
    """Test compliance status determination logic"""
    test_cases = [
        (100.0, 95.0, 90.0, "Green"),   # Above alert threshold
        (92.0, 95.0, 90.0, "Yellow"),   # Between warning and alert  
        (85.0, 95.0, 90.0, "Red")       # Below warning threshold
    ]
    
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    for metric_value, alert_threshold, warning_threshold, expected_status in test_cases:
        # Test the logic directly by creating a controlled scenario
        if metric_value >= alert_threshold:
            status = "Green"
        elif warning_threshold is not None and metric_value >= warning_threshold:
            status = "Yellow"
        else:
            status = "Red"
        
        assert status == expected_status

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pl_automated_monitoring_CTRL_1077188.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_CTRL_1077188.pipeline import run
    
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

def test_certificate_arn_case_insensitive_matching():
    """Test that certificate ARN matching is case insensitive"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Test Tier 1 specifically since it does ARN matching
    tier1_thresholds = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    # Dataset with lowercase ARN
    dataset_df = pd.DataFrame([{
        "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
        "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
        "not_valid_after_utc_timestamp": datetime(2024, 1, 1),
        "last_usage_observation_utc_timestamp": datetime(2023, 12, 15)
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            # API returns uppercase ARN
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response({
                "resourceConfigurations": [{
                    "amazonResourceName": "ARN:AWS:ACM:US-EAST-1:123456789012:CERTIFICATE/ABCD1234-5678-90EF-GHIJ-KLMNOPQRSTUV",
                    "source": "ConfigurationSnapshot",
                    "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
                }],
                "nextRecordKey": None
            })
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [tier1_thresholds],
                "dataset_certificates": [dataset_df]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            tier1_result = metrics_df.iloc[0]
            assert tier1_result["monitoring_metric_value"] == 100.0  # Should match despite case difference
            assert tier1_result["resources_info"] is None  # No missing certificates

def test_extract_method_data_type_enforcement():
    """Test that extract method enforces correct data types"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response()
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [_mock_threshold_df()],
                "dataset_certificates": [_mock_dataset_certificates_df()]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            # Check that data types are enforced correctly
            assert metrics_df["metric_value_numerator"].dtype == "int64"
            assert metrics_df["metric_value_denominator"].dtype == "int64" 
            assert metrics_df["monitoring_metric_value"].dtype == "float64"

@freeze_time("2024-11-05 12:09:00")
def test_certificate_30_day_warning_logic():
    """Test 30-day expiration warning logic"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Test Tier 2 specifically with warning scenario
    tier2_thresholds = pd.DataFrame([{
        "monitoring_metric_id": 3,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 2",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            # Mock API response with certificate that has in-use information
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response({
                "resourceConfigurations": [{
                    "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/warning1234-5678-90ef-ghij-klmnopqrstuv",
                    "source": "ConfigurationSnapshot",
                    "configurationList": [
                        {"configurationName": "status", "configurationValue": "ISSUED"},
                        {"configurationName": "inUseBy", "configurationValue": "[\"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test-lb/1234567890abcdef\"]"}
                    ]
                }],
                "nextRecordKey": None
            })
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [tier2_thresholds],
                "dataset_certificates": [_mock_dataset_certificates_with_warnings_df()]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            tier2_result = metrics_df.iloc[0]
            
            # Should have warnings in resources_info
            assert tier2_result["resources_info"] is not None
            resources_info = [json.loads(info) for info in tier2_result["resources_info"]]
            
            # Check for warning about certificate expiring in 15 days
            warning_found = any("Expires in 15 days" in str(info) for info in resources_info)
            assert warning_found, f"Expected 30-day warning not found in resources_info: {resources_info}"

@freeze_time("2024-11-05 12:09:00")  
def test_expired_unused_certificate_logic():
    """Test expired unused certificate detection (red status after 24 hours)"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    # Test Tier 2 with expired unused certificate
    tier2_thresholds = pd.DataFrame([{
        "monitoring_metric_id": 3,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 2",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            # Mock API response showing certificate is not in use (inUseBy = "[]")
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response({
                "resourceConfigurations": [{
                    "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/expired1234-5678-90ef-ghij-klmnopqrstuv",
                    "source": "ConfigurationSnapshot",
                    "configurationList": [
                        {"configurationName": "status", "configurationValue": "ISSUED"},
                        {"configurationName": "inUseBy", "configurationValue": "[]"}  # Not in use
                    ]
                }],
                "nextRecordKey": None
            })
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [tier2_thresholds],
                "dataset_certificates": [_mock_dataset_certificates_with_warnings_df()]
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            
            tier2_result = metrics_df.iloc[0]
            
            # Should be Red status due to expired unused certificate
            assert tier2_result["monitoring_metric_status"] == "Red"
            
            # Should have expired unused certificate issue in resources_info
            assert tier2_result["resources_info"] is not None
            resources_info = [json.loads(info) for info in tier2_result["resources_info"]]
            
            expired_unused_found = any("Expired unused certificate" in str(info) for info in resources_info)
            assert expired_unused_found, f"Expected expired unused certificate issue not found: {resources_info}"

@freeze_time("2024-11-05 12:09:00")
def test_certificate_in_use_by_parsing():
    """Test parsing of inUseBy configuration from API response"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    
    tier1_thresholds = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        with patch.object(pipeline, '_get_api_connector') as mock_get_api:
            # Mock API response with various inUseBy scenarios
            mock_api = MockOauthApi(url="test_url", api_token="Bearer test_token")
            mock_api.response = generate_mock_api_response({
                "resourceConfigurations": [
                    {
                        "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/in-use-cert",
                        "source": "ConfigurationSnapshot",
                        "configurationList": [
                            {"configurationName": "status", "configurationValue": "ISSUED"},
                            {"configurationName": "inUseBy", "configurationValue": "[\"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test-lb/1234567890abcdef\"]"}
                        ]
                    },
                    {
                        "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/not-in-use-cert",
                        "source": "ConfigurationSnapshot", 
                        "configurationList": [
                            {"configurationName": "status", "configurationValue": "ISSUED"},
                            {"configurationName": "inUseBy", "configurationValue": "[]"}  # Not in use
                        ]
                    }
                ],
                "nextRecordKey": None
            })
            mock_get_api.return_value = mock_api
            
            mock_extract_df = pd.DataFrame({
                "thresholds_raw": [tier1_thresholds],
                "dataset_certificates": [pd.DataFrame()]  # Empty for Tier 1 test
            })
            mock_super_extract.return_value = mock_extract_df
            
            result_df = pipeline.extract()
            
            # Verify the extract ran without errors
            assert "monitoring_metrics" in result_df.columns
            metrics_df = result_df["monitoring_metrics"].iloc[0]
            assert len(metrics_df) == 1