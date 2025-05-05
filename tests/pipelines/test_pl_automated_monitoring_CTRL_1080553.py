import datetime
import json
import unittest.mock as mock
import pandas as pd
import pytest
from freezegun import freeze_time

import pipelines.pl_automated_monitoring_CTRL_1080553.pipeline as pipeline
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
                "CTRL-1080553",
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


def _proofpoint_outcome_df():
    """Test data for Proofpoint test outcomes."""
    return pd.DataFrame(
        {
            "TEST_ID": [
                "test_1",
                "test_2",
                "test_3",
                "test_4",
                "test_5",
                "test_6",
            ],
            "TEST_NAME": [
                "Proofpoint Test 1",
                "Proofpoint Test 2",
                "Proofpoint Test 3",
                "Proofpoint Test 4",
                "Proofpoint Test 5",
                "Proofpoint Test 6",
            ],
            "PLATFORM": [
                "proofpoint",
                "proofpoint",
                "proofpoint",
                "proofpoint",
                "proofpoint",
                "proofpoint",
            ],
            "EXPECTED_OUTCOME": [
                "blocked",
                "blocked",
                "blocked",
                "blocked",
                "passed",
                "blocked",
            ],
            "ACTUAL_OUTCOME": [
                "blocked",
                "blocked",
                "passed",  # This test failed (expected != actual)
                "blocked",
                "passed",
                "passed",  # This test failed (expected != actual)
            ],
            "TEST_DESCRIPTION": [
                "Test description 1",
                "Test description 2",
                "Test description 3",
                "Test description 4",
                "Test description 5",
                "Test description 6",
            ],
            "SF_LOAD_TIMESTAMP": [
                datetime.datetime(2024, 11, 4),
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


def _expected_output_df():
    """Expected output dataframe for successful test case."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [53],
            "control_id": ["CTRL-1080553"],
            "monitoring_metric_value": [66.67],
            "monitoring_metric_status": ["Red"],
            "metric_value_numerator": [4],
            "metric_value_denominator": [6],
            "resources_info": [[
                json.dumps({
                    "test_id": "test_3",
                    "test_name": "Proofpoint Test 3",
                    "test_description": "Test description 3",
                    "expected_outcome": "blocked",
                    "actual_outcome": "passed"
                }),
                json.dumps({
                    "test_id": "test_6",
                    "test_name": "Proofpoint Test 6",
                    "test_description": "Test description 6",
                    "expected_outcome": "blocked",
                    "actual_outcome": "passed"
                })
            ]],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
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


class Test_CTRL_1080553_Pipeline(ConfigPipelineTestCase):
    """Test class for the Proofpoint Monitoring (CTRL-1080553) pipeline."""

    def test_extract_proofpoint_metrics(self):
        """Test extraction and processing of Proofpoint metrics."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            df = pl.extract_proofpoint_metrics(
                _tier_threshold_df(),
                _proofpoint_outcome_df()
            )
            
            # Verify correct calculation (4/6 = 66.67% success rate)
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 66.67)
            self.assertEqual(df["monitoring_metric_status"].iloc[0], "Red")  # 66.67% is Red (< 80)
            self.assertEqual(df["metric_value_numerator"].iloc[0], 4)
            self.assertEqual(df["metric_value_denominator"].iloc[0], 6)
    
    def test_extract_proofpoint_metrics_empty(self):
        """Test extraction with empty outcome data."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            df = pl.extract_proofpoint_metrics(
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
            with mock.patch.object(pipeline.PLAmCTRL1080553Pipeline, 'extract_proofpoint_metrics', 
                                  side_effect=Exception("Test error")):
                pl = pipeline.PLAmCTRL1080553Pipeline(env)
                df = pl.extract()
                
                # The pipeline should return a valid DataFrame even if the extraction fails
                self.assertIn("proofpoint_monitoring_df", df)
    
    def test_transform_phase(self):
        """Test the transform phase of the pipeline."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            # Mock Snowflake queries
            mock_sf = mock.MagicMock()
            mock_sf.query.side_effect = [
                _tier_threshold_df(),    # For monitoring thresholds
                _proofpoint_outcome_df() # For test outcomes
            ]
            env.snowflake = mock_sf
            
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Mock SQL file reading
            with mock.patch("builtins.open", mock.mock_open(read_data="SELECT * FROM test")), \
                 mock.patch("os.path.exists", return_value=True):
                
                # Run the transform phase
                pl.transform()
                
                # Verify the transformed data is in the pipeline context
                self.assertIn("proofpoint_monitoring_metrics", pl.context)
                
                # Convert the metrics to the Avro schema format for validation
                metrics_df = pl.context["proofpoint_monitoring_metrics"].copy()
                
                # Validate schema conformance
                _validate_avro_schema(metrics_df)
                
                # Verify metric values
                self.assertEqual(metrics_df["monitoring_metric_value"].iloc[0], 66.67)
                self.assertEqual(metrics_df["metric_value_numerator"].iloc[0], 4)
                self.assertEqual(metrics_df["metric_value_denominator"].iloc[0], 6)
    
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
            
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Pre-populate context with transformed data
            pl.context["proofpoint_monitoring_metrics"] = _expected_output_df()
            
            # Mock the Avro loading function
            with mock.patch.object(pl, 'load_metrics_to_avro') as mock_load:
                pl.load()
                mock_load.assert_called_once()
                
                # Verify the data passed to load matches expectations
                call_args = mock_load.call_args[0]
                df_arg = call_args[0]
                
                # Validate the DataFrame structure being loaded
                self.assertEqual(len(df_arg), 1)  # One metric
                self.assertEqual(df_arg["monitoring_metric_id"].iloc[0], 53)
                self.assertEqual(df_arg["control_id"].iloc[0], "CTRL-1080553")
    
    def test_data_validation(self):
        """Test data validation for the Proofpoint metrics."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            df = pl.extract_proofpoint_metrics(
                _tier_threshold_df(),
                _proofpoint_outcome_df()
            )
            
            # Ensure the data has the expected structure
            self.assertIn("monitoring_metric_id", df.columns)
            self.assertIn("control_id", df.columns)
            self.assertIn("monitoring_metric_value", df.columns)
            self.assertIn("monitoring_metric_status", df.columns)
            self.assertIn("metric_value_numerator", df.columns)
            self.assertIn("metric_value_denominator", df.columns)
            self.assertIn("resources_info", df.columns)
            
            # Validate data types
            self.assertEqual(df["monitoring_metric_id"].dtype, "int64")
            self.assertEqual(df["control_id"].dtype, "object")
            self.assertEqual(df["monitoring_metric_value"].dtype, "float64")
            self.assertEqual(df["monitoring_metric_status"].dtype, "object")
            
            # Validate value ranges
            self.assertTrue(all(0 <= val <= 100 for val in df["monitoring_metric_value"]))
            self.assertTrue(all(val in ["Green", "Yellow", "Red"] for val in df["monitoring_metric_status"]))
    
    def test_non_compliant_resources(self):
        """Test the non-compliant resources tracking."""
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            df = pl.extract_proofpoint_metrics(
                _tier_threshold_df(),
                _proofpoint_outcome_df()
            )
            
            # Verify non-compliant resources are tracked correctly
            resources_info = df["resources_info"].iloc[0]
            
            # Should have 2 failed tests
            self.assertEqual(len(resources_info), 2)
            
            # Check test IDs of failed tests
            test_ids = []
            for resource in resources_info:
                resource_dict = json.loads(resource)
                test_ids.append(resource_dict["test_id"])
            
            self.assertIn("test_3", test_ids)
            self.assertIn("test_6", test_ids)
    
    def test_sql_table_names(self):
        """Test that SQL queries reference the correct table names."""
        # Mock SQL file reading
        sql_content = {
            "monitoring_thresholds.sql": "SELECT * FROM monitoring_thresholds WHERE control_id = %(control_id)s",
            "proofpoint_outcome.sql": "SELECT * FROM proofpoint_outcome WHERE SF_LOAD_TIMESTAMP >= %(date_threshold)s"
        }
        
        def mock_read_file(filename):
            return sql_content.get(filename, "")
        
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1080553Pipeline(env)
        
        # Test with mocked SQL file reading
        with mock.patch.object(pl, '_read_sql_file', side_effect=mock_read_file):
            # Check SQL table references for monitoring thresholds
            query = pl._read_sql_with_params("monitoring_thresholds.sql", {"control_id": "CTRL-1080553"})
            self.assertIn("monitoring_thresholds", query)
            self.assertIn("CTRL-1080553", query)
            
            # Check SQL table references for Proofpoint outcomes
            query = pl._read_sql_with_params("proofpoint_outcome.sql", {"date_threshold": "2024-11-01"})
            self.assertIn("proofpoint_outcome", query)
            self.assertIn("2024-11-01", query)
    
    def test_error_handling_in_transform(self):
        """Test error handling in transform phase."""
        env = set_env_vars("qa")
        
        # Mock Snowflake queries with errors
        mock_sf = mock.MagicMock()
        mock_sf.query.side_effect = Exception("Database error")
        env.snowflake = mock_sf
        
        pl = pipeline.PLAmCTRL1080553Pipeline(env)
        pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
        
        # With proper error handling, transform should not raise an exception
        with mock.patch("builtins.open", mock.mock_open(read_data="SELECT * FROM test")), \
             mock.patch("os.path.exists", return_value=True):
            
            with self.assertLogs(level='ERROR'):
                pl.transform()
                
                # Context should exist but metrics might be empty or have default values
                self.assertIn("proofpoint_monitoring_metrics", pl.context)
    
    def test_compliance_status_determination(self):
        """Test the compliance status determination for different metric values."""
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1080553Pipeline(env)
        
        # Test various scenarios
        # Red: Below alerting threshold (90%)
        self.assertEqual(pl._get_compliance_status(85.0, 90.0, 80.0), "Red")
        
        # Yellow: Between alerting and warning thresholds
        self.assertEqual(pl._get_compliance_status(85.0, 90.0, 80.0, reversed_thresholds=True), "Yellow")
        
        # Green: Above warning threshold
        self.assertEqual(pl._get_compliance_status(95.0, 90.0, 80.0, reversed_thresholds=True), "Green")
    
    def test_full_pipeline_run(self):
        """Test the full pipeline run with mocked components."""
        env = set_env_vars("qa")
        
        # Set up mocked components
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1080553.pipeline.PLAmCTRL1080553Pipeline",
            extract=mock.DEFAULT,
            transform=mock.DEFAULT,
            load=mock.DEFAULT,
            configure_from_filename=mock.DEFAULT
        ) as mocks:
            # Configure mock returns
            mocks["extract"].return_value = {
                "thresholds": _tier_threshold_df(),
                "proofpoint_outcomes": _proofpoint_outcome_df()
            }
            mocks["transform"].return_value = None
            mocks["load"].return_value = None
            mocks["configure_from_filename"].return_value = None
            
            # Run the pipeline
            pl = pipeline.PLAmCTRL1080553Pipeline(env)
            pl.run()
            
            # Verify all phases were called
            mocks["extract"].assert_called_once()
            mocks["transform"].assert_called_once()
            mocks["load"].assert_called_once()
    
    def test_run_nonprod(self):
        """Test pipeline run in non-prod environment."""
        env = set_env_vars("qa")
        with mock.patch(
            "pipelines.pl_automated_monitoring_CTRL_1080553.pipeline.PLAmCTRL1080553Pipeline"
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
            "pipelines.pl_automated_monitoring_CTRL_1080553.pipeline.PLAmCTRL1080553Pipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )