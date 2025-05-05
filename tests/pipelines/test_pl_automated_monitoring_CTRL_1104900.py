import datetime
import unittest.mock as mock
import pandas as pd
from freezegun import freeze_time

import pipelines.pl_automated_monitoring_CTRL_1104900.pipeline as pipeline
from etip_env import set_env_vars
from tests.config_pipeline.helpers import ConfigPipelineTestCase


def _tier_threshold_df():
    """Test data for monitoring thresholds."""
    return pd.DataFrame(
        {
            "control_id": [
                "CTRL-1104900",
            ],
            "monitoring_metric_id": [42],
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


def _symantec_proxy_outcome_df():
    """Test data for Symantec Proxy test outcomes."""
    return pd.DataFrame(
        {
            "TEST_ID": [
                "test_1",
                "test_2",
                "test_3",
                "test_4",
                "test_5",
            ],
            "TEST_NAME": [
                "Symantec Test 1",
                "Symantec Test 2",
                "Symantec Test 3",
                "Symantec Test 4",
                "Symantec Test 5",
            ],
            "PLATFORM": [
                "symantec_proxy",
                "symantec_proxy",
                "symantec_proxy",
                "symantec_proxy",
                "symantec_proxy",
            ],
            "EXPECTED_OUTCOME": [
                "blocked",
                "blocked",
                "blocked",
                "blocked",
                "passed",
            ],
            "ACTUAL_OUTCOME": [
                "blocked",
                "blocked",
                "passed",  # This test failed (expected != actual)
                "blocked",
                "passed",
            ],
            "TEST_DESCRIPTION": [
                "Test description 1",
                "Test description 2",
                "Test description 3",
                "Test description 4",
                "Test description 5",
            ],
            "SF_LOAD_TIMESTAMP": [
                datetime.datetime(2024, 11, 4),
                datetime.datetime(2024, 11, 4),
                datetime.datetime(2024, 11, 4),
                datetime.datetime(2024, 11, 4),
                datetime.datetime(2024, 11, 4),
            ],
        }
    )


def _empty_outcome_df():
    """Empty outcome dataframe for testing."""
    return pd.DataFrame(
        {
            "TEST_ID": [],
            "TEST_NAME": [],
            "PLATFORM": [],
            "EXPECTED_OUTCOME": [],
            "ACTUAL_OUTCOME": [],
            "TEST_DESCRIPTION": [],
            "SF_LOAD_TIMESTAMP": [],
        }
    )


class Test_CTRL_1104900_Pipeline(ConfigPipelineTestCase):
    """Test class for the Symantec Proxy Monitoring (CTRL-1104900) pipeline."""

    def test_extract_symantec_proxy_metrics(self):
        """Test extraction and processing of Symantec Proxy metrics."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1104900Pipeline(env)
            df = pl.extract_symantec_proxy_metrics(
                _tier_threshold_df(),
                _symantec_proxy_outcome_df()
            )
            
            # Verify correct calculation (4/5 = 80% success rate)
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 80.0)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Yellow")  # 80% is Yellow (>= 80 but < 90)
            self.assertEqual(df["metric_value_numerator"].iloc[0], 4)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 5)
    
    def test_extract_symantec_proxy_metrics_empty(self):
        """Test extraction with empty outcome data."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1104900Pipeline(env)
            df = pl.extract_symantec_proxy_metrics(
                _tier_threshold_df(),
                _empty_outcome_df()
            )
            
            # Verify metrics are 0 when no data is available
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 0.0)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Red")
            self.assertEqual(df["metric_value_numerator"].iloc[0], 0)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 0)
    
    def test_extract_with_error(self):
        """Test error handling in extraction."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            with mock.patch.object(pipeline.PLAmCTRL1104900Pipeline, 'extract_symantec_proxy_metrics', 
                                  side_effect=Exception("Test error")):
                pl = pipeline.PLAmCTRL1104900Pipeline(env)
                df = pl.extract()
                
                # The pipeline should return a valid DataFrame even if the extraction fails
                self.assertIn("symantec_proxy_monitoring_df", df)
    
    def test_run_nonprod(self):
        """Test pipeline run in non-prod environment."""
        env = set_env_vars("qa")
        with mock.patch(
            "pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAmCTRL1104900Pipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )
    
    def test_run_prod(self):
        """Test pipeline run in prod environment."""
        env = set_env_vars("prod")
        with mock.patch(
            "pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAmCTRL1104900Pipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )