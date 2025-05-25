import datetime
import json
import unittest.mock as mock
from typing import Any, Dict, Optional, Union

import pandas as pd
import pytest
from freezegun import freeze_time
from requests import RequestException, Response

import pipelines.pl_automated_monitoring_CTRL_1077125.pipeline as pipeline
from pipeline_framework.connectors.oauth_api import OauthApi
from etip_env import set_env_vars


class MockOauthApi:
    """Mock implementation of OauthApi for testing purposes."""

    def __init__(self, url: str, api_token: str, ssl_context=None):
        self.url = url
        self.api_token = api_token
        self.response = None
        self.side_effect = None

    def send_request(
        self,
        url: str,
        request_type: str,
        request_kwargs: Dict[str, Any],
        retry_delay: int = 5,
        max_retries: int = 3,
    ) -> Response:
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
        default_response._content = json.dumps({}).encode("utf-8")
        return default_response


# Standard timestamp for all tests to use (2024-11-05 12:09:00 UTC)
FIXED_TIMESTAMP = "2024-11-05 12:09:00"


def get_fixed_timestamp(as_int: bool = True) -> Union[int, pd.Timestamp]:
    """Returns a fixed timestamp for testing, either as milliseconds or pandas Timestamp.

    Args:
        as_int: If True, returns timestamp as milliseconds since epoch (int).
               If False, returns as pandas Timestamp with UTC timezone.

    Returns:
        Either int milliseconds or pandas Timestamp object
    """
    if as_int:
        return int(pd.Timestamp(FIXED_TIMESTAMP).timestamp())
    return pd.Timestamp(FIXED_TIMESTAMP, tz="UTC")


# Standard Avro schema fields following CLAUDE.md specification
AVRO_SCHEMA_FIELDS = [
    "control_monitoring_utc_timestamp",
    "control_id",
    "monitoring_metric_id",
    "monitoring_metric_value",
    "monitoring_metric_status",
    "metric_value_numerator",
    "metric_value_denominator",
    "resources_info",
]


class MockExchangeConfig:
    def __init__(self):
        self.client_id = "test_client"
        self.client_secret = "test_secret"
        self.exchange_url = "test-exchange.com"


class MockEnv:
    def __init__(self):
        self.exchange = MockExchangeConfig()


def generate_mock_api_response(content: Optional[dict] = None, status_code: int = 200) -> Response:
    """Generate standardized mock API response."""
    mock_response = Response()
    mock_response.status_code = status_code
    
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    else:
        mock_response._content = json.dumps({}).encode("utf-8")
    
    # Set request information for debugging
    mock_response.request = mock.Mock()
    mock_response.request.url = "https://mock.api.url/search-resource-configurations"
    mock_response.request.method = "POST"
    
    return mock_response


def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([
        {
            "monitoring_metric_id": 25,
            "control_id": "CTRL-1077125",
            "monitoring_metric_tier": "Tier 1",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0,
            "control_executor": "Individual_1",
            "metric_threshold_start_date": datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            "metric_threshold_end_date": None,
        },
        {
            "monitoring_metric_id": 26,
            "control_id": "CTRL-1077125",
            "monitoring_metric_tier": "Tier 2",
            "warning_threshold": 97.0,
            "alerting_threshold": 95.0,
            "control_executor": "Individual_1",
            "metric_threshold_start_date": datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            "metric_threshold_end_date": None,
        }
    ]).astype({
        "monitoring_metric_id": int,
        "warning_threshold": float,
        "alerting_threshold": float,
    })


# Test API response data
KMS_KEYS_RESPONSE_DATA = {
    "resourceConfigurations": [
        {
            "resourceId": "key1",
            "accountResourceId": "account1/key1",
            "resourceType": "AWS::KMS::Key",
            "awsRegion": "us-east-1",
            "accountName": "account1",
            "awsAccountId": "123456789012",
            "configurationList": [
                {
                    "configurationName": "configuration.origin",
                    "configurationValue": "AWS_KMS"
                },
                {
                    "configurationName": "configuration.keyState",
                    "configurationValue": "Enabled"
                },
                {
                    "configurationName": "configuration.keyManager",
                    "configurationValue": "CUSTOMER"
                }
            ],
            "source": "CloudRadar",
        },
        {
            "resourceId": "key2",
            "accountResourceId": "account2/key2",
            "resourceType": "AWS::KMS::Key",
            "awsRegion": "us-east-1",
            "accountName": "account2",
            "awsAccountId": "123456789013",
            "configurationList": [
                {
                    "configurationName": "configuration.origin",
                    "configurationValue": "AWS_KMS"
                },
                {
                    "configurationName": "configuration.keyState",
                    "configurationValue": "Enabled"
                },
                {
                    "configurationName": "configuration.keyManager",
                    "configurationValue": "CUSTOMER"
                }
            ],
            "source": "CloudRadar",
        },
        {
            "resourceId": "key3",
            "accountResourceId": "account3/key3",
            "resourceType": "AWS::KMS::Key",
            "awsRegion": "us-east-1",
            "accountName": "account3",
            "awsAccountId": "123456789014",
            "configurationList": [
                {
                    "configurationName": "configuration.origin",
                    "configurationValue": "EXTERNAL"
                },
                {
                    "configurationName": "configuration.keyState",
                    "configurationValue": "Enabled"
                },
                {
                    "configurationName": "configuration.keyManager",
                    "configurationValue": "CUSTOMER"
                }
            ],
            "source": "CloudRadar",
        },
    ],
    "nextRecordKey": "",
}

EMPTY_API_RESPONSE_DATA = {"resourceConfigurations": [], "nextRecordKey": ""}


@freeze_time(FIXED_TIMESTAMP)
def test_calculate_metrics_success(mock):
    """Test successful metrics calculation"""
    # Setup test data
    thresholds_df = _mock_threshold_df()
    
    # Mock API response
    mock_response = generate_mock_api_response(KMS_KEYS_RESPONSE_DATA)
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    # Execute transformer - use correct function name
    result = pipeline.calculate_metrics(thresholds_df, context)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == AVRO_SCHEMA_FIELDS
    
    # Verify data types
    row = result.iloc[0]
    assert isinstance(row["control_monitoring_utc_timestamp"], datetime.datetime)
    assert isinstance(row["monitoring_metric_value"], float)
    assert isinstance(row["metric_value_numerator"], int)
    assert isinstance(row["metric_value_denominator"], int)


def test_calculate_metrics_empty_thresholds():
    """Test error handling for empty thresholds"""
    empty_df = pd.DataFrame()
    context = {"api_connector": MockOauthApi(url="test", api_token="test")}
    
    with pytest.raises(RuntimeError, match="No threshold data found"):
        pipeline.calculate_metrics(empty_df, context)


@freeze_time(FIXED_TIMESTAMP)
def test_calculate_metrics_empty_api_response():
    """Test handling of empty API response"""
    thresholds_df = _mock_threshold_df()
    
    mock_response = generate_mock_api_response(EMPTY_API_RESPONSE_DATA)
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.response = mock_response
    
    context = {"api_connector": mock_api}
    
    result = pipeline.calculate_metrics(thresholds_df, context)
    
    # Should return results with zero values
    assert len(result) == 2
    assert result.iloc[0]["monitoring_metric_value"] == 0.0
    assert result.iloc[1]["monitoring_metric_value"] == 0.0
    assert result.iloc[0]["monitoring_metric_status"] == "Red"
    assert result.iloc[1]["monitoring_metric_status"] == "Red"


def test_api_error_handling():
    """Test API error handling and exception wrapping"""
    thresholds_df = _mock_threshold_df()
    
    # Test network errors
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = RequestException("Connection error")
    
    context = {"api_connector": mock_api}
    
    # Should wrap exception in RuntimeError
    with pytest.raises(RuntimeError, match="Failed to fetch resources from API"):
        pipeline.calculate_metrics(thresholds_df, context)


def test_pagination_handling():
    """Test API pagination with multiple responses"""
    thresholds_df = _mock_threshold_df()
    
    # Create paginated responses
    page1_response = generate_mock_api_response({
        "resourceConfigurations": [KMS_KEYS_RESPONSE_DATA["resourceConfigurations"][0]],
        "nextRecordKey": "page2_key"
    })
    page2_response = generate_mock_api_response({
        "resourceConfigurations": KMS_KEYS_RESPONSE_DATA["resourceConfigurations"][1:],
        "nextRecordKey": ""
    })
    
    mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
    mock_api.side_effect = [page1_response, page2_response]
    
    context = {"api_connector": mock_api}
    result = pipeline.calculate_metrics(thresholds_df, context)
    
    # Verify both pages were processed
    assert not result.empty
    assert len(result) == 2


def test_missing_tier_data():
    """Test error handling for missing tier metric data"""
    # Create thresholds missing Tier 2 data
    incomplete_df = pd.DataFrame([{
        "monitoring_metric_id": 25,
        "control_id": "CTRL-1077125", 
        "monitoring_metric_tier": "Tier 1",
        "warning_threshold": 97.0,
        "alerting_threshold": 95.0,
        "control_executor": "Individual_1",
        "metric_threshold_start_date": datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
        "metric_threshold_end_date": None,
    }])
    
    context = {"api_connector": MockOauthApi(url="test", api_token="test")}
    
    with pytest.raises(RuntimeError, match="Tier 2 metric data not found"):
        pipeline.calculate_metrics(incomplete_df, context)


def test_filter_resources():
    """Test the _filter_resources function"""
    test_resources = [
        {
            "resourceId": "key1",
            "configurationList": [
                {"configurationName": "configuration.origin", "configurationValue": "AWS_KMS"},
                {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
            ],
            "source": "CloudRadar"
        },
        {
            "resourceId": "key2", 
            "configurationList": [
                {"configurationName": "configuration.origin", "configurationValue": "EXTERNAL"},
                {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
            ],
            "source": "CloudRadar"
        }
    ]
    
    tier1_num, tier2_num, tier1_non, tier2_non = pipeline._filter_resources(
        test_resources, "configuration.origin", "AWS_KMS"
    )
    
    assert tier1_num == 2  # Both keys have origin value
    assert tier2_num == 1  # Only one has AWS_KMS
    assert len(tier1_non) == 0  # No tier1 non-compliant (both have values)
    assert len(tier2_non) == 1  # One tier2 non-compliant (EXTERNAL)


def test_get_compliance_status():
    """Test the _get_compliance_status function"""
    # Test Green status
    assert pipeline._get_compliance_status(0.96, 95.0, 97.0) == "Green"  # 96% >= 95% alert
    
    # Test Yellow status 
    assert pipeline._get_compliance_status(0.96, 97.0, 95.0) == "Yellow"  # 96% >= 95% warning but < 97% alert
    
    # Test Red status
    assert pipeline._get_compliance_status(0.80, 95.0, 97.0) == "Red"  # 80% < 95% alert
    
    # Test invalid thresholds
    assert pipeline._get_compliance_status(0.80, "invalid", 97.0) == "Red"
    assert pipeline._get_compliance_status(0.80, 95.0, "invalid") == "Red"


def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipe = pipeline.PLAutomatedMonitoringCTRL1077125(env)
    
    assert pipe.env == env
    assert hasattr(pipe, 'api_url')
    assert 'test-exchange.com' in pipe.api_url
    assert pipe.control_id == "CTRL-1077125"
    assert pipe.origin_config_key == "configuration.origin"
    assert pipe.origin_expected_value == "AWS_KMS"


def test_get_api_connector_success():
    """Test successful API connector creation"""
    with mock.patch("pipelines.pl_automated_monitoring_CTRL_1077125.pipeline.refresh") as mock_refresh:
        mock_refresh.return_value = "mock_token_value"
        
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCTRL1077125(env)
        connector = pipe._get_api_connector()
        
        assert isinstance(connector, OauthApi)
        assert connector.api_token == "Bearer mock_token_value"
        mock_refresh.assert_called_once_with(
            client_id="test_client",
            client_secret="test_secret",
            exchange_url="test-exchange.com",
        )


def test_get_api_connector_failure():
    """Test handling of API connector creation failure"""
    with mock.patch("pipelines.pl_automated_monitoring_CTRL_1077125.pipeline.refresh") as mock_refresh:
        mock_refresh.side_effect = Exception("Token refresh failed")
        
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCTRL1077125(env)
        
        # The actual implementation doesn't wrap this exception, so expect the original
        with pytest.raises(Exception, match="Token refresh failed"):
            pipe._get_api_connector()


def test_transform_method():
    """Test pipeline transform method"""
    with mock.patch("pipelines.pl_automated_monitoring_CTRL_1077125.pipeline.refresh") as mock_refresh:
        mock_refresh.return_value = "mock_token"
        
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCTRL1077125(env)
        pipe.context = {}  # Initialize context
        
        # Mock the super().transform() call
        with mock.patch("pipeline_framework.ConfigPipeline.transform"):
            pipe.transform()
            
            # Verify API connector was added to context
            assert "api_connector" in pipe.context
            assert isinstance(pipe.context["api_connector"], OauthApi)


@freeze_time(FIXED_TIMESTAMP)
def test_end_to_end_pipeline():
    """Consolidated end-to-end test for pipeline functionality"""
    with mock.patch("pipelines.pl_automated_monitoring_CTRL_1077125.pipeline.refresh") as mock_refresh:
        mock_refresh.return_value = "mock_token_value"
        
        # Use consistent timestamps
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCTRL1077125(env)
        pipe.context = {}  # Initialize context
        
        # Test the API connector creation
        api_connector = pipe._get_api_connector()
        assert isinstance(api_connector, OauthApi)
        assert api_connector.api_token.startswith("Bearer ")
        assert mock_refresh.called, "OAuth token refresh should be called"
        
        # Setup context and call transformer directly
        pipe.context["api_connector"] = api_connector
        
        # Mock the API response
        mock_api = MockOauthApi(url="test_url", api_token="Bearer token")
        mock_api.response = generate_mock_api_response(KMS_KEYS_RESPONSE_DATA)
        pipe.context["api_connector"] = mock_api
        
        result_df = pipeline.calculate_metrics(
            thresholds_raw=_mock_threshold_df(),
            context=pipe.context,
        )
        
        # Validate results
        assert len(result_df) == 2, "Should have two result rows"
        assert result_df.iloc[0]["monitoring_metric_id"] == 25, "First row should be metric ID 25"
        assert result_df.iloc[1]["monitoring_metric_id"] == 26, "Second row should be metric ID 26"
        
        # Verify timestamp is a datetime object
        assert isinstance(result_df.iloc[0]["control_monitoring_utc_timestamp"], datetime.datetime)
        
        # Verify metric calculations are reasonable
        assert isinstance(result_df.iloc[0]["monitoring_metric_value"], float)
        assert isinstance(result_df.iloc[0]["metric_value_numerator"], int)
        assert isinstance(result_df.iloc[0]["metric_value_denominator"], int)


def test_excluded_resources():
    """Test that excluded resources are properly filtered"""
    test_resources = [
        {
            "resourceId": "excluded1",
            "configurationList": [
                {"configurationName": "configuration.keyState", "configurationValue": "PendingDeletion"}
            ],
            "source": "CloudRadar"
        },
        {
            "resourceId": "excluded2", 
            "configurationList": [
                {"configurationName": "configuration.keyManager", "configurationValue": "AWS"}
            ],
            "source": "CloudRadar"
        },
        {
            "resourceId": "excluded3",
            "source": "CT-AccessDenied"
        }
    ]
    
    tier1_num, tier2_num, tier1_non, tier2_non = pipeline._filter_resources(
        test_resources, "configuration.origin", "AWS_KMS"
    )
    
    # All resources should be excluded
    assert tier1_num == 0
    assert tier2_num == 0
    assert len(tier1_non) == 0  
    assert len(tier2_non) == 0


def test_missing_configuration_values():
    """Test handling of resources with missing configuration values"""
    test_resources = [
        {
            "resourceId": "missing_config",
            "configurationList": [
                {"configurationName": "configuration.keyState", "configurationValue": "Enabled"},
                {"configurationName": "configuration.keyManager", "configurationValue": "CUSTOMER"}
                # Missing configuration.origin
            ],
            "source": "CloudRadar"
        }
    ]
    
    tier1_num, tier2_num, tier1_non, tier2_non = pipeline._filter_resources(
        test_resources, "configuration.origin", "AWS_KMS"
    )
    
    assert tier1_num == 0  # No origin value found
    assert tier2_num == 0
    assert len(tier1_non) == 1  # Should be in non-compliant for tier 1
    assert len(tier2_non) == 0