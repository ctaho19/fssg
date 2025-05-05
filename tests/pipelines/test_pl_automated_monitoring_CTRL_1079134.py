import datetime
import unittest.mock as mock
import json
import pandas as pd
import pytest
from freezegun import freeze_time

import pipelines.pl_automated_monitoring_CTRL_1079134.pipeline as pipeline
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
                "CTRL-1079134",
                "CTRL-1079134",
                "CTRL-1079134",
            ],
            "monitoring_metric_id": [34, 35, 36],
            "monitoring_metric_tier": [
                "Tier 0",
                "Tier 1",
                "Tier 2",
            ],
            "warning_threshold": [
                100.0,
                97.0,
                97.0,
            ],
            "alerting_threshold": [
                100.0,
                95.0,
                95.0,
            ],
            "control_executor": [
                "Individual_1",
                "Individual_1",
                "Individual_1",
            ],
            "metric_threshold_start_date": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
            "metric_threshold_end_date": [
                None,
                None,
                None,
            ],
        }
    )


def _macie_metrics_df():
    """Test data for Macie metrics."""
    return pd.DataFrame(
        {
            "METRIC_DATE": [datetime.datetime(2024, 11, 5)],
            "SF_LOAD_TIMESTAMP": [datetime.datetime(2024, 11, 5)],
            "TOTAL_BUCKETS_SCANNED_BY_MACIE": [850],
            "TOTAL_CLOUDFRONTED_BUCKETS": [1000]
        }
    )


def _macie_testing_df():
    """Test data for Macie control test results."""
    return pd.DataFrame(
        {
            "REPORTDATE": [
                datetime.datetime(2024, 11, 5),
                datetime.datetime(2024, 11, 5),
                datetime.datetime(2024, 11, 5),
                datetime.datetime(2024, 11, 5),
                datetime.datetime(2024, 11, 5),
            ],
            "TESTISSUCCESSFUL": [
                True,
                True,
                True,
                True,
                False,
            ],
            "TESTID": [
                "test_1",
                "test_2",
                "test_3",
                "test_4",
                "test_5",
            ],
            "TESTNAME": [
                "Test 1",
                "Test 2",
                "Test 3",
                "Test 4",
                "Test 5",
            ]
        }
    )


def _historical_stats_df():
    """Test data for historical test statistics."""
    return pd.DataFrame(
        {
            "AVG_HISTORICAL_TESTS": [5.5],
            "MIN_HISTORICAL_TESTS": [5],
            "MAX_HISTORICAL_TESTS": [6]
        }
    )


def _expected_metrics_output_df():
    """Expected output for Macie metrics."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [34, 35, 36],
            "control_id": [
                "CTRL-1079134",
                "CTRL-1079134",
                "CTRL-1079134",
            ],
            "monitoring_metric_value": [100.0, 85.0, 80.0],
            "monitoring_metric_status": [
                "Green",
                "Red",
                "Red",
            ],
            "metric_value_numerator": [850, 850, 4],
            "metric_value_denominator": [1, 1000, 5],
            "resources_info": [
                None,
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


def _empty_metrics_output_df():
    """Expected output when no Macie data is available."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [34, 35, 36],
            "control_id": [
                "CTRL-1079134",
                "CTRL-1079134",
                "CTRL-1079134",
            ],
            "monitoring_metric_value": [0.0, 0.0, 0.0],
            "monitoring_metric_status": [
                "Red",
                "Red",
                "Red",
            ],
            "metric_value_numerator": [0, 0, 0],
            "metric_value_denominator": [1, 0, 0],
            "resources_info": [
                None,
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


def _validate_avro_schema(df, expected_fields=None):
    """Validates that a DataFrame conforms to the expected Avro schema structure."""
    if expected_fields is None:
        expected_fields = AVRO_SCHEMA_FIELDS
    
    # Convert monitoring metric fields to the Avro schema field names
    if "control_monitoring_utc_timestamp" in df.columns:
        df = df.rename(columns={
            "control_monitoring_utc_timestamp": "date",
            "monitoring_metric_status": "compliance_status",
            "metric_value_numerator": "numerator",
            "metric_value_denominator": "denominator",
            "resources_info": "non_compliant_resources"
        })
    
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


class Test_CTRL_1079134_Pipeline(ConfigPipelineTestCase):
    """Test class for the Macie Monitoring (CTRL-1079134) pipeline."""

    def test_extract_macie_metrics(self):
        """Test extraction and processing of Macie metrics."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl.extract_macie_metrics(
                _tier_threshold_df(),
                _macie_metrics_df(),
                _macie_testing_df(),
                _historical_stats_df()
            )
            
            # Verify correct calculation of all tiers
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 100.0)  # Tier 0
            self.assertEqual(df["monitoring_metric_value"].iloc[1], 85.0)   # Tier 1: 850/1000 = 85%
            self.assertEqual(df["monitoring_metric_value"].iloc[2], 80.0)   # Tier 2: 4/5 = 80%
            
            # Verify numerators and denominators
            self.assertEqual(df["metric_value_numerator"].iloc[0], 850)    # Tier 0 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[0], 1)    # Tier 0 denominator
            self.assertEqual(df["metric_value_numerator"].iloc[1], 850)    # Tier 1 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[1], 1000) # Tier 1 denominator
            self.assertEqual(df["metric_value_numerator"].iloc[2], 4)      # Tier 2 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[2], 5)    # Tier 2 denominator
    
    def test_extract_macie_metrics_empty(self):
        """Test the Macie metrics extraction with empty data."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl.extract_macie_metrics(
                _tier_threshold_df(),
                pd.DataFrame(),  # Empty metrics
                pd.DataFrame(),  # Empty testing
                pd.DataFrame()   # Empty historical stats
            )
            
            # Verify metrics are 0 when no data is available
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 0.0)  # Tier 0
            self.assertEqual(df["monitoring_metric_value"].iloc[1], 0.0)  # Tier 1
            self.assertEqual(df["monitoring_metric_value"].iloc[2], 0.0)  # Tier 2
            
            # Verify all statuses are Red
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Red")
            self.assertEqual(df["monitoring_metric_status"].iloc[1], "Red")
            self.assertEqual(df["monitoring_metric_status"].iloc[2], "Red")
    
    def test_calculate_tier0_metrics(self):
        """Test Tier 0 (Heartbeat) metric calculation."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl._calculate_tier0_metrics(
                _macie_metrics_df(),
                "CTRL-1079134",
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
            )
            
            # Verify Tier 0 calculation
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 100.0)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Green")
            self.assertEqual(df["metric_value_numerator"].iloc[0], 850)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 1)
    
    def test_calculate_tier1_metrics(self):
        """Test Tier 1 (Completeness) metric calculation."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl._calculate_tier1_metrics(
                _macie_metrics_df(),
                _tier_threshold_df(),
                "CTRL-1079134",
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
            )
            
            # Verify Tier 1 calculation
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 85.0)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Red")
            self.assertEqual(df["metric_value_numerator"].iloc[0], 850)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 1000)
    
    def test_calculate_tier2_metrics(self):
        """Test Tier 2 (Accuracy) metric calculation."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl._calculate_tier2_metrics(
                _macie_testing_df(),
                _historical_stats_df(),
                _tier_threshold_df(),
                "CTRL-1079134",
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
            )
            
            # Verify Tier 2 calculation
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 80.0)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Red")
            self.assertEqual(df["metric_value_numerator"].iloc[0], 4)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 5)
    
    def test_transform_phase(self):
        """Test the transform phase of the pipeline."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            # Mock Snowflake queries
            mock_sf = mock.MagicMock()
            mock_sf.query.side_effect = [
                _tier_threshold_df(),  # For monitoring thresholds
                _macie_metrics_df(),   # For Macie metrics
                _macie_testing_df(),   # For test results
                _historical_stats_df() # For historical stats
            ]
            env.snowflake = mock_sf
            
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Mock SQL file reading
            with mock.patch("builtins.open", mock.mock_open(read_data="SELECT * FROM test")), \
                 mock.patch("os.path.exists", return_value=True):
                
                # Run the transform phase
                pl.transform()
                
                # Verify the transformed data is in the pipeline context
                self.assertIn("macie_metrics", pl.context)
                
                # Convert the metrics to the Avro schema format for validation
                metrics_df = pl.context["macie_metrics"].copy()
                
                # Validate schema conformance
                _validate_avro_schema(metrics_df)
                
                # Verify metric values
                self.assertEqual(metrics_df["monitoring_metric_value"].iloc[0], 100.0)  # Tier 0
                self.assertEqual(metrics_df["monitoring_metric_value"].iloc[1], 85.0)   # Tier 1
                self.assertEqual(metrics_df["monitoring_metric_value"].iloc[2], 80.0)   # Tier 2
    
    def test_load_phase(self):
        """Test the load phase of the pipeline."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            # Mock Snowflake and exchange for the entire pipeline
            mock_sf = mock.MagicMock()
            mock_sf.query.return_value = _tier_threshold_df()
            mock_ex = mock.MagicMock()
            env.snowflake = mock_sf
            env.exchange = mock_ex
            
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Pre-populate context with transformed data
            pl.context["macie_metrics"] = _expected_metrics_output_df()
            
            # Mock the Avro loading function
            with mock.patch.object(pl, 'load_metrics_to_avro') as mock_load:
                pl.load()
                mock_load.assert_called_once()
                
                # Verify the data passed to load matches expectations
                call_args = mock_load.call_args[0]
                df_arg = call_args[0]
                
                # Validate the DataFrame structure being loaded
                self.assertEqual(len(df_arg), 3)  # Three metrics
                self.assertEqual(list(df_arg["monitoring_metric_id"]), [34, 35, 36])
                self.assertEqual(list(df_arg["control_id"]), ["CTRL-1079134", "CTRL-1079134", "CTRL-1079134"])
    
    def test_data_validation(self):
        """Test data validation for the Macie metrics."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl.extract_macie_metrics(
                _tier_threshold_df(),
                _macie_metrics_df(),
                _macie_testing_df(),
                _historical_stats_df()
            )
            
            # Ensure the data has the expected structure
            self.assertIn("monitoring_metric_id", df.columns)
            self.assertIn("control_id", df.columns)
            self.assertIn("monitoring_metric_value", df.columns)
            self.assertIn("monitoring_metric_status", df.columns)
            self.assertIn("metric_value_numerator", df.columns)
            self.assertIn("metric_value_denominator", df.columns)
            
            # Validate data types
            self.assertEqual(df["monitoring_metric_id"].dtype, "int64")
            self.assertEqual(df["control_id"].dtype, "object")
            self.assertEqual(df["monitoring_metric_value"].dtype, "float64")
            self.assertEqual(df["monitoring_metric_status"].dtype, "object")
            
            # Validate value ranges
            self.assertTrue(all(0 <= val <= 100 for val in df["monitoring_metric_value"]))
            self.assertTrue(all(val in ["Green", "Yellow", "Red"] for val in df["monitoring_metric_status"]))
    
    def test_sql_table_names(self):
        """Test that SQL queries reference the correct table names."""
        # Mock SQL file reading
        sql_content = {
            "monitoring_thresholds.sql": "SELECT * FROM monitoring_thresholds WHERE control_id = %(control_id)s",
            "macie_metrics.sql": "SELECT * FROM macie_metrics WHERE METRIC_DATE = %(metric_date)s",
            "macie_testing.sql": "SELECT * FROM macie_testing WHERE REPORTDATE = %(report_date)s",
            "historical_stats.sql": "SELECT * FROM historical_stats"
        }
        
        def mock_read_file(filename):
            return sql_content.get(filename, "")
        
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1079134Pipeline(env)
        
        # Test with mocked SQL file reading
        with mock.patch.object(pl, '_read_sql_file', side_effect=mock_read_file):
            # Check SQL table references for monitoring thresholds
            query = pl._read_sql_with_params("monitoring_thresholds.sql", {"control_id": "CTRL-1079134"})
            self.assertIn("monitoring_thresholds", query)
            self.assertIn("CTRL-1079134", query)
            
            # Check SQL table references for Macie metrics
            query = pl._read_sql_with_params("macie_metrics.sql", {"metric_date": "2024-11-05"})
            self.assertIn("macie_metrics", query)
            self.assertIn("2024-11-05", query)
    
    def test_error_handling_in_transform(self):
        """Test error handling in transform phase."""
        env = set_env_vars("qa")
        
        # Mock Snowflake queries with errors
        mock_sf = mock.MagicMock()
        mock_sf.query.side_effect = Exception("Database error")
        env.snowflake = mock_sf
        
        pl = pipeline.PLAmCTRL1079134Pipeline(env)
        pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
        
        # With proper error handling, transform should not raise an exception
        with mock.patch("builtins.open", mock.mock_open(read_data="SELECT * FROM test")), \
             mock.patch("os.path.exists", return_value=True):
            
            with self.assertLogs(level='ERROR'):
                pl.transform()
                
                # Context should exist but metrics might be empty
                self.assertIn("macie_metrics", pl.context)
    
    def test_full_pipeline_run(self):
        """Test the full pipeline run with mocked components."""
        env = set_env_vars("qa")
        
        # Set up mocked components
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1079134.pipeline.PLAmCTRL1079134Pipeline",
            extract=mock.DEFAULT,
            transform=mock.DEFAULT,
            load=mock.DEFAULT,
            configure_from_filename=mock.DEFAULT
        ) as mocks:
            # Configure mock returns
            mocks["extract"].return_value = {
                "thresholds": _tier_threshold_df(),
                "macie_metrics": _macie_metrics_df(),
                "macie_testing": _macie_testing_df(),
                "historical_stats": _historical_stats_df()
            }
            mocks["transform"].return_value = None
            mocks["load"].return_value = None
            mocks["configure_from_filename"].return_value = None
            
            # Run the pipeline
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            pl.run()
            
            # Verify all phases were called
            mocks["extract"].assert_called_once()
            mocks["transform"].assert_called_once()
            mocks["load"].assert_called_once()
    
    def test_edge_case_missing_data(self):
        """Test handling of edge cases with missing or invalid data."""
        env = set_env_vars("qa")
        
        # Test with incomplete Macie metrics data (missing some values)
        incomplete_metrics = pd.DataFrame({
            "METRIC_DATE": [datetime.datetime(2024, 11, 5)],
            "SF_LOAD_TIMESTAMP": [datetime.datetime(2024, 11, 5)],
            # Missing TOTAL_BUCKETS_SCANNED_BY_MACIE
            "TOTAL_CLOUDFRONTED_BUCKETS": [1000]
        })
        
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1079134Pipeline(env)
            df = pl.extract_macie_metrics(
                _tier_threshold_df(),
                incomplete_metrics,
                _macie_testing_df(),
                _historical_stats_df()
            )
            
            # Missing data should result in 0 values for metrics that depend on it
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 0.0)  # Tier 0 - depends on TOTAL_BUCKETS_SCANNED_BY_MACIE
            self.assertEqual(df["monitoring_metric_value"].iloc[1], 0.0)  # Tier 1 - depends on both values
    
    def test_run_nonprod(self):
        """Test pipeline run in non-prod environment."""
        env = set_env_vars("qa")
        with mock.patch(
            "pipelines.pl_automated_monitoring_CTRL_1079134.pipeline.PLAmCTRL1079134Pipeline"
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
            "pipelines.pl_automated_monitoring_CTRL_1079134.pipeline.PLAmCTRL1079134Pipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )