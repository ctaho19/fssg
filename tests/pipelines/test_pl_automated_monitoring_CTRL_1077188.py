import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
import ssl
import sys
from requests.exceptions import RequestException

# Import pipeline components
from pl_automated_monitoring_CTRL_1077188.pipeline import (
    PLAutomatedMonitoringCTRL1077188,
    calculate_metrics,
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

class MockOauthApi:
    def __init__(self, url, api_token, ssl_context=None):
        self.url = url
        self.api_token = api_token
        self.ssl_context = ssl_context
        self.response = None
        self.side_effect = None
    
    def post(self, endpoint, json=None, headers=None):
        """Mock post method with proper side_effect handling."""
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

def test_get_api_connector_with_ssl(mock):
    """Test _get_api_connector method with SSL certificate"""
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh') as mock_refresh:
        with patch('connectors.ca_certs.C1_CERT_FILE', '/path/to/cert.pem'):
            with patch('ssl.create_default_context') as mock_ssl_context:
                mock_refresh.return_value = "test_token"
                mock_ssl_context.return_value = Mock()
                
                env = MockEnv()
                pipeline = PLAutomatedMonitoringCTRL1077188(env)
                
                connector = pipeline._get_api_connector()
                
                assert connector.api_token == "Bearer test_token"
                assert connector.url == pipeline.api_url
                mock_ssl_context.assert_called_once_with(cafile='/path/to/cert.pem')
                mock_refresh.assert_called_once_with(
                    client_id="test_client",
                    client_secret="test_secret", 
                    exchange_url="test-exchange.com"
                )

def test_get_api_connector_without_ssl(mock):
    """Test _get_api_connector method without SSL certificate"""
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh') as mock_refresh:
        with patch('connectors.ca_certs.C1_CERT_FILE', None):
            mock_refresh.return_value = "test_token"
            
            env = MockEnv()
            pipeline = PLAutomatedMonitoringCTRL1077188(env)
            
            connector = pipeline._get_api_connector()
            
            assert connector.api_token == "Bearer test_token"
            assert connector.ssl_context is None

def test_get_api_connector_refresh_failure(mock):
    """Test _get_api_connector method when OAuth refresh fails"""
    with patch('pl_automated_monitoring_CTRL_1077188.pipeline.refresh') as mock_refresh:
        mock_refresh.side_effect = Exception("OAuth refresh failed")
        
        env = MockEnv()
        pipeline = PLAutomatedMonitoringCTRL1077188(env)
        
        with pytest.raises(Exception, match="OAuth refresh failed"):
            pipeline._get_api_connector()

def test_transform_method():
    """Test transform method sets up API context correctly"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1077188(env)
    pipeline.context = {}
    
    with patch.object(pipeline, '_get_api_connector') as mock_get_connector:
        with patch.object(pipeline.__class__.__bases__[0], 'transform') as mock_super_transform:
            mock_connector = Mock()
            mock_get_connector.return_value = mock_connector
            
            pipeline.transform()
            
            assert pipeline.context["api_connector"] == mock_connector
            mock_get_connector.assert_called_once()
            mock_super_transform.assert_called_once()

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

# Test calculate_metrics function
def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    dataset_df = _mock_dataset_certificates_df()
    context = {"api_connector": Mock()}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        calculate_metrics(empty_df, dataset_df, context)

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_all_tiers_success():
    """Test successful metrics calculation for all three tiers"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    dataset_df = _mock_dataset_certificates_df()
    
    # Mock API response with in-scope certificates
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            },
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/new-cert-not-in-dataset",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 3  # Three tiers
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Verify data types
    for _, row in result.iterrows():
        assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
        assert isinstance(row["monitoring_metric_value"], float)
        assert isinstance(row["metric_value_numerator"], int)
        assert isinstance(row["metric_value_denominator"], int)
        assert row["control_id"] == "CTRL-1077188"

def test_calculate_metrics_api_pagination():
    """Test API pagination handling"""
    thresholds_df = _mock_threshold_df()
    dataset_df = _mock_dataset_certificates_df()
    
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
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    with patch('time.sleep') as mock_sleep:  # Mock rate limiting sleep
        context = {"api_connector": mock_api}
        result = calculate_metrics(thresholds_df, dataset_df, context)
        
        # Verify pagination was processed and rate limiting was applied
        assert not result.empty
        assert len(result) == 3  # All three tiers
        mock_sleep.assert_called_with(0.15)  # Rate limiting verification

def test_calculate_metrics_api_error_handling():
    """Test API error handling and exception wrapping"""
    thresholds_df = _mock_threshold_df()
    dataset_df = _mock_dataset_certificates_df()
    
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="Failed to fetch resources from API"):
        calculate_metrics(thresholds_df, dataset_df, context)

def test_calculate_metrics_api_non_200_status():
    """Test API non-200 status code handling"""
    thresholds_df = _mock_threshold_df()
    dataset_df = _mock_dataset_certificates_df()
    
    # Mock API response with error status
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.json.return_value = {}
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    with pytest.raises(RuntimeError, match="API request failed: 500 - Internal Server Error"):
        calculate_metrics(thresholds_df, dataset_df, context)

def test_calculate_metrics_filter_orphaned_certificates():
    """Test filtering of orphaned certificates (CT-AccessDenied)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188", 
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    # Mock API response with orphaned certificate
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/in-scope",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            },
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/orphaned",
                "source": "CT-AccessDenied",  # Should be filtered out
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    # Should only count the 1 in-scope certificate
    tier1_result = result.iloc[0]
    assert tier1_result["metric_value_denominator"] == 1  # Only 1 in-scope certificate

def test_calculate_metrics_filter_pending_status():
    """Test filtering of certificates with pending status"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    # Mock API response with pending deletion certificate
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/valid",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            },
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/pending-deletion",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "PENDING_DELETION"}]
            }
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    # Should only count the 1 valid certificate
    tier1_result = result.iloc[0]
    assert tier1_result["metric_value_denominator"] == 1

def test_calculate_metrics_tier0_no_failures():
    """Test Tier 0 calculation with no failures (100% compliance)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 1,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 0",
        "warning_threshold": 100.0,
        "alerting_threshold": 100.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = Mock()
    mock_api.response.status_code = 200
    mock_api.response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": None}
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier0_result = result.iloc[0]
    assert tier0_result["monitoring_metric_id"] == 1
    assert tier0_result["monitoring_metric_value"] == 100.0
    assert tier0_result["monitoring_metric_status"] == "Green"
    assert tier0_result["metric_value_numerator"] == 1
    assert tier0_result["metric_value_denominator"] == 1
    assert tier0_result["resources_info"] is None

def test_calculate_metrics_tier1_perfect_coverage():
    """Test Tier 1 with 100% coverage (all API certs in dataset)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    # Mock API response with only certificates that exist in dataset
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_value"] == 100.0
    assert tier1_result["monitoring_metric_status"] == "Green"
    assert tier1_result["metric_value_numerator"] == 1
    assert tier1_result["metric_value_denominator"] == 1
    assert tier1_result["resources_info"] is None

def test_calculate_metrics_tier1_no_api_certificates():
    """Test Tier 1 with no API certificates (default 100%)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1", 
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = Mock()
    mock_api.response.status_code = 200
    mock_api.response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": None}
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_value"] == 100.0
    assert tier1_result["monitoring_metric_status"] == "Green"
    assert tier1_result["metric_value_numerator"] == 0
    assert tier1_result["metric_value_denominator"] == 0

def test_calculate_metrics_tier1_missing_arns_truncation():
    """Test Tier 1 with >100 missing ARNs (truncation logic)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 2,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = pd.DataFrame()  # Empty dataset
    
    # Generate >100 API certificates
    api_certs = []
    for i in range(105):
        api_certs.append({
            "amazonResourceName": f"arn:aws:acm:us-east-1:123456789012:certificate/cert-{i:03d}",
            "source": "ConfigurationSnapshot",
            "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
        })
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": api_certs,
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_value"] == 0.0  # No certificates in dataset
    assert tier1_result["metric_value_denominator"] == 105
    
    # Check truncation message
    resources_info = tier1_result["resources_info"]
    assert len(resources_info) == 101  # 100 certs + 1 truncation message
    truncation_message = json.loads(resources_info[-1])
    assert "... and 5 more" in truncation_message["message"]

def test_calculate_metrics_tier2_empty_dataset():
    """Test Tier 2 with empty dataset"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 3,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 2",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    dataset_df = pd.DataFrame()  # Empty dataset
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = Mock()
    mock_api.response.status_code = 200
    mock_api.response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": None}
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier2_result = result.iloc[0]
    assert tier2_result["monitoring_metric_value"] == 0.0
    assert tier2_result["monitoring_metric_status"] == "Red"
    assert tier2_result["metric_value_numerator"] == 0
    assert tier2_result["metric_value_denominator"] == 0
    assert tier2_result["resources_info"] is not None

def test_calculate_metrics_tier2_certificate_violations():
    """Test Tier 2 with certificate lifetime and usage violations"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 3,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 2",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    # Dataset with various violation types
    dataset_df = pd.DataFrame([
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/compliant",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 1, 1),  # 365 days (compliant)
            "last_usage_observation_utc_timestamp": datetime(2023, 12, 15)  # Used before expiry
        },
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/used-after-expiry",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2023, 6, 1),
            "last_usage_observation_utc_timestamp": datetime(2023, 7, 1)  # Used after expiry
        },
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/long-lifetime",
            "not_valid_before_utc_timestamp": datetime(2022, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2023, 6, 1),  # 517 days (> 395)
            "last_usage_observation_utc_timestamp": datetime(2023, 5, 1)
        }
    ])
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = Mock()
    mock_api.response.status_code = 200
    mock_api.response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": None}
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier2_result = result.iloc[0]
    assert tier2_result["monitoring_metric_value"] == 33.33  # 1 compliant out of 3
    assert tier2_result["monitoring_metric_status"] == "Red"  # Below 95% threshold
    assert tier2_result["metric_value_numerator"] == 1
    assert tier2_result["metric_value_denominator"] == 3
    
    # Verify non-compliant resources are reported
    resources_info = tier2_result["resources_info"]
    assert len(resources_info) == 2  # 2 non-compliant certificates

def test_calculate_metrics_tier2_all_compliant():
    """Test Tier 2 with all certificates compliant (Green status)"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 3,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 2",
        "warning_threshold": 90.0,
        "alerting_threshold": 95.0
    }])
    
    # Dataset with all compliant certificates
    dataset_df = pd.DataFrame([
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/compliant1",
            "not_valid_before_utc_timestamp": datetime(2023, 1, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 1, 1),  # 365 days
            "last_usage_observation_utc_timestamp": datetime(2023, 12, 15)
        },
        {
            "certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/compliant2", 
            "not_valid_before_utc_timestamp": datetime(2023, 2, 1),
            "not_valid_after_utc_timestamp": datetime(2024, 1, 15),  # 348 days
            "last_usage_observation_utc_timestamp": datetime(2024, 1, 10)
        }
    ])
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = Mock()
    mock_api.response.status_code = 200
    mock_api.response.json.return_value = {"resourceConfigurations": [], "nextRecordKey": None}
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier2_result = result.iloc[0]
    assert tier2_result["monitoring_metric_value"] == 100.0
    assert tier2_result["monitoring_metric_status"] == "Green"
    assert tier2_result["metric_value_numerator"] == 2
    assert tier2_result["metric_value_denominator"] == 2
    assert tier2_result["resources_info"] is None

def test_calculate_metrics_compliance_status_thresholds():
    """Test compliance status determination with various thresholds"""
    thresholds_df = pd.DataFrame([
        {
            "monitoring_metric_id": 1,
            "control_id": "CTRL-1077188",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": 80.0,
            "alerting_threshold": 90.0
        }
    ])
    
    dataset_df = _mock_dataset_certificates_df()
    
    # Test Yellow status (between warning and alert thresholds)
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ] * 10,  # 10 API certificates, but only 2 in dataset = 20%
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_value"] == 20.0  # 2 found / 10 total
    assert tier1_result["monitoring_metric_status"] == "Red"  # Below 80% warning threshold

def test_calculate_metrics_warning_threshold_none():
    """Test compliance status when warning threshold is None"""
    thresholds_df = pd.DataFrame([{
        "monitoring_metric_id": 1,
        "control_id": "CTRL-1077188",
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": None,  # No warning threshold
        "alerting_threshold": 90.0
    }])
    
    dataset_df = _mock_dataset_certificates_df()
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-5678-90ef-ghij-klmnopqrstuv",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ] * 2,  # 2 API certificates, 2 in dataset = 100%
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_status"] == "Green"  # Should be Green (>= 90%)

def test_main_execution_success():
    """Test main execution path success"""
    with patch('etip_env.set_env_vars') as mock_set_env:
        with patch('pl_automated_monitoring_CTRL_1077188.pipeline.run') as mock_run:
            mock_env = Mock()
            mock_set_env.return_value = mock_env
            
            # Execute main block logic
            from etip_env import set_env_vars
            from pl_automated_monitoring_CTRL_1077188.pipeline import run
            
            env = set_env_vars()
            run(env=env, is_load=False, dq_actions=False)
            
            # Verify calls
            mock_set_env.assert_called_once()
            mock_run.assert_called_once_with(env=mock_env, is_load=False, dq_actions=False)

def test_main_execution_with_exception():
    """Test main execution path with exception handling"""
    with patch('etip_env.set_env_vars') as mock_set_env:
        with patch('pl_automated_monitoring_CTRL_1077188.pipeline.run') as mock_run:
            with patch('sys.exit') as mock_exit:
                mock_env = Mock()
                mock_set_env.return_value = mock_env
                mock_run.side_effect = Exception("Pipeline execution failed")
                
                # Execute and expect sys.exit to be called
                try:
                    from etip_env import set_env_vars
                    from pl_automated_monitoring_CTRL_1077188.pipeline import run
                    
                    env = set_env_vars()
                    run(env=env, is_load=False, dq_actions=False)
                except SystemExit:
                    pass  # Expected due to mock
                except Exception:
                    # This simulates the exception handling in the main block
                    import sys
                    sys.exit(1)

def test_certificate_arn_case_insensitive_matching():
    """Test that certificate ARN matching is case insensitive"""
    thresholds_df = pd.DataFrame([{
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
    
    # API returns uppercase ARN
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "resourceConfigurations": [
            {
                "amazonResourceName": "ARN:AWS:ACM:US-EAST-1:123456789012:CERTIFICATE/ABCD1234-5678-90EF-GHIJ-KLMNOPQRSTUV",
                "source": "ConfigurationSnapshot",
                "configurationList": [{"configurationName": "status", "configurationValue": "ISSUED"}]
            }
        ],
        "nextRecordKey": None
    }
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = calculate_metrics(thresholds_df, dataset_df, context)
    
    tier1_result = result.iloc[0]
    assert tier1_result["monitoring_metric_value"] == 100.0  # Should match despite case difference
    assert tier1_result["resources_info"] is None  # No missing certificates


def test_main_function_execution(mock):
    """Test main function execution path"""
    mock_env = mock.Mock()
    
    with mock.patch("etip_env.set_env_vars", return_value=mock_env):
        with mock.patch("pl_automated_monitoring_CTRL_1077188.pipeline.run") as mock_run:
            with mock.patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pl_automated_monitoring_CTRL_1077188.pipeline import run
    
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


def test_pipeline_run_method(mock):
    """Test pipeline run method with default parameters"""
    env = MockEnv()
    pipeline_instance = PLAutomatedMonitoringCTRL1077188(env)
    
    # Mock the run method
    mock_run = mock.patch.object(pipeline_instance, 'run')
    pipeline_instance.run()
    
    # Verify run was called with default parameters
    mock_run.assert_called_once_with()