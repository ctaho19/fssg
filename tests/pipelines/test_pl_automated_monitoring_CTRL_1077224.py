import datetime
import json
import unittest.mock as mock
from typing import Dict, List, Optional
import pandas as pd
import pytest
from freezegun import freeze_time
from requests import Response

import pipelines.pl_automated_monitoring_CTRL_1077224.pipeline as pipeline
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
                "CTRL-1077224",
                "CTRL-1077224",
            ],
            "monitoring_metric_id": [24, 25],
            "monitoring_metric_tier": [
                "Tier 1",
                "Tier 2",
            ],
            "warning_threshold": [
                97.0,
                97.0,
            ],
            "alerting_threshold": [
                95.0,
                95.0,
            ],
            "control_executor": [
                "Individual_1",
                "Individual_1",
            ],
            "metric_threshold_start_date": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
            "metric_threshold_end_date": [
                None,
                None,
            ],
        }
    )


def _kms_key_rotation_output_df():
    """Expected output for KMS Key Rotation metrics."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [24, 25],
            "control_id": [
                "CTRL-1077224",
                "CTRL-1077224",
            ],
            "monitoring_metric_value": [80.0, 75.0],
            "monitoring_metric_status": [
                "Red",
                "Red",
            ],
            "metric_value_numerator": [8, 6],
            "metric_value_denominator": [10, 8],
            "resources_info": [
                [
                    '{"resourceId": "key1", "accountResourceId": "account1/key1", "keyState": "Enabled", "rotationStatus": "N/A (Not Found)"}'
                ],
                [
                    '{"resourceId": "key3", "accountResourceId": "account3/key3", "keyState": "Enabled", "rotationStatus": "FALSE"}'
                ],
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


def _empty_kms_metric_output_df():
    """Expected output when no KMS keys are found."""
    return pd.DataFrame(
        {
            "monitoring_metric_id": [24, 25],
            "control_id": [
                "CTRL-1077224",
                "CTRL-1077224",
            ],
            "monitoring_metric_value": [0.0, 0.0],
            "monitoring_metric_status": [
                "Red",
                "Red",
            ],
            "metric_value_numerator": [0, 0],
            "metric_value_denominator": [0, 0],
            "resources_info": [
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


# Mock API response data
EMPTY_API_RESPONSE_DATA = {"resourceConfigurations": []}

# Sample KMS Key data with mixed rotation status
KMS_KEYS_RESPONSE_DATA = {
    "resourceConfigurations": [
        # 10 total keys, with varying compliance states:
        # - 2 with missing KeyRotationStatus (Tier 1 fail)
        # - 2 with FALSE KeyRotationStatus (Tier 2 fail)
        # - 6 with TRUE KeyRotationStatus (fully compliant)
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
            "supplementaryConfiguration": []
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
                    "supplementaryConfigurationValue": "FALSE"
                }
            ]
        },
        {
            "resourceId": "key4",
            "accountResourceId": "account4/key4",
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
            "resourceId": "key5",
            "accountResourceId": "account5/key5",
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
        },
        {
            "resourceId": "key6",
            "accountResourceId": "account6/key6",
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
        },
        {
            "resourceId": "key7",
            "accountResourceId": "account7/key7",
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
        },
        {
            "resourceId": "key8",
            "accountResourceId": "account8/key8",
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
        },
        {
            "resourceId": "key9",
            "accountResourceId": "account9/key9",
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
        },
        {
            "resourceId": "key10",
            "accountResourceId": "account10/key10",
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
    ]
}


def generate_mock_response(content: Optional[Dict] = None, status_code: int = 200):
    """Generate a mock HTTP response with the given content and status code."""
    mock_response = Response()
    mock_response.status_code = status_code
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    mock_response.request = mock.Mock()
    mock_response.request.url = "https://mock.api.url/search-resource-configurations"
    mock_response.request.method = "POST"
    return mock_response


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


class Test_CTRL_1077224_Pipeline(ConfigPipelineTestCase):
    """Test class for the KMS Key Rotation (CTRL-1077224) pipeline."""

    @mock.patch("requests.post")
    def test_extract_kms_key_rotation(self, mock_post):
        """Test the KMS key rotation data extraction with mocked API responses."""
        # Set up mocked API response
        mock_post.return_value = generate_mock_response(KMS_KEYS_RESPONSE_DATA, 200)

        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            df = pl.extract_kms_key_rotation(_tier_threshold_df())
            
            # Verify metrics are calculated properly (80% Tier1, 75% Tier2)
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 80.0)  # Tier1: 8/10 = 80%
            self.assertEqual(df["monitoring_metric_value"].iloc[1], 75.0)  # Tier2: 6/8 = 75%
            
            # Verify numerators and denominators
            self.assertEqual(df["metric_value_numerator"].iloc[0], 8)     # Tier1 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[0], 10)  # Tier1 denominator
            self.assertEqual(df["metric_value_numerator"].iloc[1], 6)     # Tier2 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[1], 8)   # Tier2 denominator
            
            # Verify resource info contains data about non-compliant resources
            self.assertIsNotNone(df["resources_info"].iloc[0])  # Tier 1 non-compliant
            self.assertIsNotNone(df["resources_info"].iloc[1])  # Tier 2 non-compliant
    
    @mock.patch("requests.post")
    def test_extract_kms_key_rotation_empty(self, mock_post):
        """Test the KMS key rotation data extraction with empty API response."""
        # Set up mocked API response - empty resource set
        mock_post.return_value = generate_mock_response(EMPTY_API_RESPONSE_DATA, 200)

        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            df = pl.extract_kms_key_rotation(_tier_threshold_df())
            
            # Verify metrics are 0 when no resources found
            self.assertEqual(df["monitoring_metric_value"].iloc[0], 0.0)  # Tier1
            self.assertEqual(df["monitoring_metric_value"].iloc[1], 0.0)  # Tier2
            
            # Verify numerators and denominators are 0
            self.assertEqual(df["metric_value_numerator"].iloc[0], 0)    # Tier1 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[0], 0)  # Tier1 denominator
            self.assertEqual(df["metric_value_numerator"].iloc[1], 0)    # Tier2 numerator
            self.assertEqual(df["metric_value_denominator"].iloc[1], 0)  # Tier2 denominator
            
            # Verify resource info is None when no resources
            self.assertIsNone(df["resources_info"].iloc[0])  # Tier 1
            self.assertIsNone(df["resources_info"].iloc[1])  # Tier 2
    
    @mock.patch("requests.post")
    def test_extract_with_error(self, mock_post):
        """Test error handling in extraction."""
        # Set up mocked API response to simulate an error
        mock_post.side_effect = Exception("API Error")

        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            df = pl.extract_kms_key_rotation(_tier_threshold_df())
            
            # Verify empty dataframe is returned on error
            self.assertTrue(df.empty)
    
    def test_get_compliance_status(self):
        """Test compliance status determination."""
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1077224Pipeline(env)
        
        # Test Green status
        self.assertEqual(pl.get_compliance_status(0.96, 95.0, 97.0), "Green")  # 96% >= 95% alert
        
        # Test Yellow status 
        self.assertEqual(pl.get_compliance_status(0.96, 97.0, 95.0), "Yellow")  # 96% >= 95% warning
        
        # Test Red status
        self.assertEqual(pl.get_compliance_status(0.80, 95.0, 97.0), "Red")     # 80% < 95% alert
    
    @mock.patch("requests.post")
    def test_transform_phase(self, mock_post):
        """Test the transform phase of the pipeline."""
        # Set up mocked API response
        mock_post.return_value = generate_mock_response(KMS_KEYS_RESPONSE_DATA, 200)

        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            # Mock Snowflake query to return threshold data
            mock_sf = mock.MagicMock()
            mock_sf.query.return_value = _tier_threshold_df()
            env.snowflake = mock_sf
            
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Run the transform phase
            pl.transform()
            
            # Verify the transformed data is in the pipeline context
            self.assertIn("kms_key_rotation_metrics", pl.context)
            
            # Convert the metrics to the Avro schema format for validation
            metrics_df = pl.context["kms_key_rotation_metrics"].copy()
            
            # Validate schema conformance
            _validate_avro_schema(metrics_df)
            
            # Verify metric values
            self.assertEqual(metrics_df["monitoring_metric_value"].iloc[0], 80.0)  # Tier1: 8/10 = 80%
            self.assertEqual(metrics_df["monitoring_metric_value"].iloc[1], 75.0)  # Tier2: 6/8 = 75%
    
    @mock.patch("requests.post")
    def test_load_phase(self, mock_post):
        """Test the load phase of the pipeline."""
        mock_post.return_value = generate_mock_response(KMS_KEYS_RESPONSE_DATA, 200)
        
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            # Mock Snowflake and exchange for the entire pipeline
            mock_sf = mock.MagicMock()
            mock_sf.query.return_value = _tier_threshold_df()
            mock_ex = mock.MagicMock()
            env.snowflake = mock_sf
            env.exchange = mock_ex
            
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
            
            # Pre-populate context with transformed data
            pl.context["kms_key_rotation_metrics"] = _kms_key_rotation_output_df()
            
            # Mock the Avro loading function
            with mock.patch.object(pl, 'load_metrics_to_avro') as mock_load:
                pl.load()
                mock_load.assert_called_once()
                
                # Verify the data passed to load matches expectations
                call_args = mock_load.call_args[0]
                df_arg = call_args[0]
                
                # Validate the DataFrame structure being loaded
                self.assertEqual(len(df_arg), 2)  # Two metrics
                self.assertEqual(list(df_arg["monitoring_metric_id"]), [24, 25])
                self.assertEqual(list(df_arg["control_id"]), ["CTRL-1077224", "CTRL-1077224"])
    
    @mock.patch("requests.post")
    def test_data_validation(self, mock_post):
        """Test data validation for the KMS key rotation metrics."""
        mock_post.return_value = generate_mock_response(KMS_KEYS_RESPONSE_DATA, 200)
        
        env = set_env_vars("qa")
        with freeze_time("2024-11-05 12:09:00.021180"):
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            df = pl.extract_kms_key_rotation(_tier_threshold_df())
            
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
    
    @mock.patch("requests.post")
    def test_sql_table_names(self, mock_post):
        """Test that SQL queries reference the correct table names."""
        mock_post.return_value = generate_mock_response(KMS_KEYS_RESPONSE_DATA, 200)
        
        # Mock SQL file reading
        sql_content = "SELECT * FROM monitoring_thresholds WHERE control_id = %(control_id)s"
        
        with mock.patch("builtins.open", mock.mock_open(read_data=sql_content)):
            env = set_env_vars("qa")
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            
            # Test with mocked SQL file reading
            with mock.patch("os.path.exists", return_value=True):
                pl.configure_from_dict({"pipeline": {"name": "test-pipeline"}})
                
                # Check SQL table references
                query = pl._read_sql_with_params("monitoring_thresholds.sql", {"control_id": "CTRL-1077224"})
                
                # The exact SQL might vary, but it should contain the table name
                self.assertIn("monitoring_thresholds", query)
                self.assertIn("CTRL-1077224", query)
    
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAmCTRL1077224Pipeline._execute_api_request")
    def test_error_handling_api_failure(self, mock_api):
        """Test error handling when API requests fail."""
        # Set up various API failure scenarios
        mock_api.side_effect = [
            Exception("Connection error"),  # First call fails
            Exception("Timeout"),           # Second call fails
            Exception("Authentication error")  # Third call fails
        ]
        
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1077224Pipeline(env)
        
        # Test with various error types
        for _ in range(3):
            with self.assertLogs(level='ERROR'):
                result = pl.extract_kms_key_rotation(_tier_threshold_df())
                self.assertTrue(result.empty)
    
    def test_pagination_handling(self):
        """Test handling of paginated API responses."""
        env = set_env_vars("qa")
        pl = pipeline.PLAmCTRL1077224Pipeline(env)
        
        # Create a mocked paginated response
        page1 = generate_mock_response({
            "resourceConfigurations": KMS_KEYS_RESPONSE_DATA["resourceConfigurations"][:5],
            "nextRecordKey": "next_page_token"
        })
        page2 = generate_mock_response({
            "resourceConfigurations": KMS_KEYS_RESPONSE_DATA["resourceConfigurations"][5:],
            "nextRecordKey": ""
        })
        
        # Mock the API request function to return paginated responses
        with mock.patch.object(pl, '_execute_api_request', side_effect=[page1, page2]):
            result = pl._get_aws_resources("AWS::KMS::Key")
            
            # Verify all resources from both pages are included
            self.assertEqual(len(result), 10)
            resource_ids = [r["resourceId"] for r in result]
            self.assertIn("key1", resource_ids)  # From page 1
            self.assertIn("key10", resource_ids)  # From page 2
    
    def test_full_pipeline_run(self):
        """Test the full pipeline run with mocked components."""
        env = set_env_vars("qa")
        
        # Set up mocked components
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAmCTRL1077224Pipeline",
            extract=mock.DEFAULT,
            transform=mock.DEFAULT,
            load=mock.DEFAULT,
            configure_from_filename=mock.DEFAULT
        ) as mocks:
            # Configure mock returns
            mocks["extract"].return_value = {"thresholds": _tier_threshold_df()}
            mocks["transform"].return_value = None
            mocks["load"].return_value = None
            mocks["configure_from_filename"].return_value = None
            
            # Run the pipeline
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            pl.run()
            
            # Verify all phases were called
            mocks["extract"].assert_called_once()
            mocks["transform"].assert_called_once()
            mocks["load"].assert_called_once()
    
    def test_run_with_disabled_load(self):
        """Test running the pipeline with load disabled."""
        env = set_env_vars("qa")
        
        # Set up mocked components
        with mock.patch.multiple(
            "pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAmCTRL1077224Pipeline",
            extract=mock.DEFAULT,
            transform=mock.DEFAULT,
            load=mock.DEFAULT,
            configure_from_filename=mock.DEFAULT
        ) as mocks:
            # Configure mock returns
            mocks["extract"].return_value = {"thresholds": _tier_threshold_df()}
            mocks["transform"].return_value = None
            mocks["load"].return_value = None
            mocks["configure_from_filename"].return_value = None
            
            # Run the pipeline with load disabled
            pl = pipeline.PLAmCTRL1077224Pipeline(env)
            pl.run(load=False)
            
            # Verify load was not called
            mocks["extract"].assert_called_once()
            mocks["transform"].assert_called_once()
            mocks["load"].assert_not_called()
    
    def test_run_nonprod(self):
        """Test pipeline run in non-prod environment."""
        env = set_env_vars("qa")
        with mock.patch(
            "pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAmCTRL1077224Pipeline"
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
            "pipelines.pl_automated_monitoring_CTRL_1077224.pipeline.PLAmCTRL1077224Pipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )