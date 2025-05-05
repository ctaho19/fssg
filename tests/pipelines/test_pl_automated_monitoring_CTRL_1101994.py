import datetime
import json
import unittest.mock as mock
import pandas as pd
import pytest
from freezegun import freeze_time

import pipelines.pl_automated_monitoring_CTRL_1101994.pipeline as pipeline
from etip_env import set_env_vars
from tests.config_pipeline.helpers import ConfigPipelineTestCase


# --- Expected Schema Fields ---
AVRO_SCHEMA_FIELDS = [
    "date",
    "control_id",
    "monitoring_metric_id",
    "monitoring_metric_value",
    "compliance_status",
    "numerator",
    "denominator",
    "non_compliant_resources"
]


def _tier_threshold_df():
    """Test data for monitoring thresholds."""
    return pd.DataFrame(
        {
            "control_id": [
                "CTRL-1101994",
            ],
            "monitoring_metric_id": [53],
            "monitoring_metric_tier": [
                "Tier 2",
            ],
            "warning_threshold": [
                80.0,
            ],
            "alerting_threshold": [
                90.0,
            ],
            "control_executor": [
                "Individual_1",
            ],
            "metric_threshold_start_date": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
            "metric_threshold_end_date": [
                None,
            ],
        }
    )


def _symantec_outcome_df():
    """Test data for Symantec Proxy test outcomes."""
    return pd.DataFrame(
        {
            "DATE": [datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)],
            "CTRL_ID": ["CTRL-1101994"],
            "MONITORING_METRIC_NUMBER": ["MNTR-1101994-T2"],
            "MONITORING_METRIC": [95.0],
            "COMPLIANCE_STATUS": ["GREEN"],
            "NUMERATOR": [19],
            "DENOMINATOR": [20],
        }
    )


def _expected_output_df():
    """Expected output dataframe for successful test case."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [53],
            "control_id": ["CTRL-1101994"],
            "monitoring_metric_value": [95.0],
            "compliance_status": ["Green"],
            "numerator": [19],
            "denominator": [20],
            "non_compliant_resources": [None],
            "date": [1730764140000],  # This is just a placeholder timestamp
        }
    ).astype({
        "monitoring_metric_id": "int64", 
        "numerator": "int64", 
        "denominator": "int64", 
        "date": "int64"
    })


def _validate_avro_schema(df, expected_fields=None):
    """Validates that a DataFrame conforms to the expected Avro schema structure."""
    if expected_fields is None:
        expected_fields = AVRO_SCHEMA_FIELDS
    
    # Check that all expected fields are present
    for field in expected_fields:
        assert field in df.columns, f"Field {field} missing from DataFrame"
    
    # Validate data types when data is available
    if not df.empty:
        assert df["control_id"].dtype == object
        assert df["monitoring_metric_id"].dtype in ["int64", "int32", "float64"]
        assert df["monitoring_metric_value"].dtype in ["float64", "float32"]
        assert df["compliance_status"].dtype == object
        assert df["numerator"].dtype in ["int64", "int32", "float64"]
        assert df["denominator"].dtype in ["int64", "int32", "float64"]


class Test_CTRL_1101994_Pipeline(ConfigPipelineTestCase):
    """Test class for the Symantec Proxy Monitoring (CTRL-1101994) pipeline."""

    def test_calculate_symantec_proxy_metrics(self):
        """Test the main transformer function that calculates metrics."""
        with freeze_time("2024-11-05 12:09:00.021180"):
            result_df = pipeline.calculate_symantec_proxy_metrics(
                thresholds_raw=_tier_threshold_df(),
                ctrl_id="CTRL-1101994",
                tier2_metric_id=53
            )
            
            # Update the timestamp to match what we get in the actual result
            expected_df = _expected_output_df()
            expected_df["date"] = result_df["date"].iloc[0]
            
            # Validate schema and structure
            _validate_avro_schema(result_df)
            
            # Check basic metric values (the exact timestamp will differ)
            assert result_df["monitoring_metric_id"].iloc[0] == 53
            assert result_df["control_id"].iloc[0] == "CTRL-1101994"
            assert result_df["monitoring_metric_value"].iloc[0] == 95.0
            assert result_df["compliance_status"].iloc[0] == "Green"
            assert result_df["numerator"].iloc[0] == 19
            assert result_df["denominator"].iloc[0] == 20
            assert result_df["non_compliant_resources"].iloc[0] is None
    
    def test_get_compliance_status(self):
        """Test the compliance status determination function."""
        # Test Green status (above alert threshold)
        assert pipeline.get_compliance_status(95.0, 90.0, 80.0) == "Green"
        
        # Test Yellow status (between warning and alert thresholds)
        assert pipeline.get_compliance_status(85.0, 90.0, 80.0) == "Yellow"
        
        # Test Red status (below warning threshold)
        assert pipeline.get_compliance_status(75.0, 90.0, 80.0) == "Red"
        
        # Test handling invalid thresholds
        assert pipeline.get_compliance_status(85.0, "invalid", 80.0) == "Red"
        assert pipeline.get_compliance_status(85.0, 90.0, "invalid") == "Yellow"  # Warning is optional
    
    def test_pipeline_init_success(self):
        """Test that pipeline initialization succeeds with proper environment configuration."""
        class MockExchangeConfig:
            def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", 
                        exchange_url="https://api.cloud.capitalone.com/exchange"):
                self.client_id = client_id
                self.client_secret = client_secret
                self.exchange_url = exchange_url
                
        class MockSnowflakeConfig:
            def __init__(self):
                self.query = lambda sql: pd.DataFrame()
                
        class MockEnv:
            def __init__(self):
                self.exchange = MockExchangeConfig()
                self.snowflake = MockSnowflakeConfig()
                
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1101994(env)
        assert pipe.client_id == "etip-client-id"
        assert pipe.client_secret == "etip-client-secret"
        assert pipe.exchange_url == "https://api.cloud.capitalone.com/exchange"
    
    def test_pipeline_init_missing_oauth_config(self):
        """Test that pipeline initialization fails with missing OAuth config."""
        class MockEnv:
            def __init__(self):
                # No exchange property to trigger the AttributeError
                self.snowflake = mock.Mock()
                
        env = MockEnv()
        with pytest.raises(ValueError, match="Environment object missing expected OAuth attributes"):
            pipeline.PLAutomatedMonitoringCtrl1101994(env)
    
    def test_get_api_token_success(self, mocker):
        """Test that API token refresh succeeds."""
        mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_CTRL_1101994.pipeline.refresh")
        mock_refresh.return_value = "mock_token_value"
        
        class MockExchangeConfig:
            def __init__(self):
                self.client_id = "etip-client-id"
                self.client_secret = "etip-client-secret"
                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
                
        class MockEnv:
            def __init__(self):
                self.exchange = MockExchangeConfig()
                self.snowflake = mock.Mock()
                
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1101994(env)
        token = pipe._get_api_token()
        
        assert token == "Bearer mock_token_value"
        mock_refresh.assert_called_once_with(
            client_id="etip-client-id",
            client_secret="etip-client-secret",
            exchange_url="https://api.cloud.capitalone.com/exchange"
        )
    
    def test_get_api_token_failure(self, mocker):
        """Test that API token refresh failure is handled properly."""
        mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_CTRL_1101994.pipeline.refresh")
        mock_refresh.side_effect = Exception("Token refresh failed")
        
        class MockExchangeConfig:
            def __init__(self):
                self.client_id = "etip-client-id"
                self.client_secret = "etip-client-secret"
                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
                
        class MockEnv:
            def __init__(self):
                self.exchange = MockExchangeConfig()
                self.snowflake = mock.Mock()
                
        env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1101994(env)
        
        with pytest.raises(RuntimeError, match="API token refresh failed"):
            pipe._get_api_token()
    
    def test_full_pipeline_run(self, mocker):
        """Test the full pipeline run with mocked components."""
        # Mock the pipeline components
        mock_configure = mocker.patch("pathlib.Path.__truediv__")
        mock_configure.return_value = "/path/to/config.yml"
        
        # Create a pipeline instance with mocked environment
        class MockExchangeConfig:
            def __init__(self):
                self.client_id = "etip-client-id"
                self.client_secret = "etip-client-secret"
                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
                
        class MockSnowflakeConfig:
            def __init__(self):
                self.query = lambda sql: _tier_threshold_df()
                
        class MockEnv:
            def __init__(self):
                self.exchange = MockExchangeConfig()
                self.snowflake = MockSnowflakeConfig()
                
        env = MockEnv()
        
        # Use patch.multiple to mock multiple methods at once
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1101994.pipeline.PLAutomatedMonitoringCtrl1101994",
            configure_from_filename=mock.DEFAULT,
            run=mock.DEFAULT
        ) as mocks:
            # Configure the mocks
            mocks["configure_from_filename"].return_value = None
            mocks["run"].return_value = None
            
            # Run the pipeline
            pipeline.run(env)
            
            # Verify that the pipeline was configured and run
            mocks["configure_from_filename"].assert_called_once()
            mocks["run"].assert_called_once_with(load=True, dq_actions=True)
    
    def test_run_with_test_data_export(self, mocker):
        """Test running the pipeline in test data export mode."""
        # Mock the pipeline components
        mock_configure = mocker.patch("pathlib.Path.__truediv__")
        mock_configure.return_value = "/path/to/config.yml"
        
        class MockExchangeConfig:
            def __init__(self):
                self.client_id = "etip-client-id"
                self.client_secret = "etip-client-secret"
                self.exchange_url = "https://api.cloud.capitalone.com/exchange"
                
        class MockSnowflakeConfig:
            def __init__(self):
                self.query = lambda sql: _tier_threshold_df()
                
        class MockEnv:
            def __init__(self):
                self.exchange = MockExchangeConfig()
                self.snowflake = MockSnowflakeConfig()
                
        env = MockEnv()
        
        # Use patch.multiple to mock multiple methods at once
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1101994.pipeline.PLAutomatedMonitoringCtrl1101994",
            configure_from_filename=mock.DEFAULT,
            run_test_data_export=mock.DEFAULT
        ) as mocks:
            # Configure the mocks
            mocks["configure_from_filename"].return_value = None
            mocks["run_test_data_export"].return_value = None
            
            # Run the pipeline in test data export mode
            pipeline.run(env, is_export_test_data=True)
            
            # Verify that the pipeline was configured and run in test data export mode
            mocks["configure_from_filename"].assert_called_once()
            mocks["run_test_data_export"].assert_called_once_with(dq_actions=True)