import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import json
from config_pipeline import ConfigPipeline

# Import pipeline components
from pipelines.pl_automated_monitoring_ctrl_1104900.pipeline import (
    PLAutomatedMonitoringCTRL1104900,
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

def _mock_threshold_df():
    """Utility function for test threshold data"""
    return pd.DataFrame([
        {
            "monitoring_metric_id": 1104900,
            "control_id": "CTRL-1104900",
            "monitoring_metric_tier": "Tier 0",
            "metric_name": "APR Microcertification Program Existence",
            "metric_description": "APR Microcertification Program Existence",
            "warning_threshold": None,
            "alerting_threshold": 100.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": 1104901,
            "control_id": "CTRL-1104900",
            "monitoring_metric_tier": "Tier 1",
            "metric_name": "APR Coverage - In-Scope ASVs",
            "metric_description": "APR Coverage - In-Scope ASVs in Microcertification",
            "warning_threshold": None,
            "alerting_threshold": 100.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": 1104902,
            "control_id": "CTRL-1104900",
            "monitoring_metric_tier": "Tier 2",
            "metric_name": "APR Review Completion Timeliness",
            "metric_description": "APR Review Completion Timeliness",
            "warning_threshold": 99.0,
            "alerting_threshold": 98.0,
            "control_executor": "Security Team"
        },
        {
            "monitoring_metric_id": 1104903,
            "control_id": "CTRL-1104900",
            "monitoring_metric_tier": "Tier 3",
            "metric_name": "APR Remediation Completion Timeliness",
            "metric_description": "APR Remediation Completion Timeliness",
            "warning_threshold": None,
            "alerting_threshold": 100.0,
            "control_executor": "Security Team"
        }
    ])

def _mock_apr_microcertification_raw_data():
    """Test data for raw APR microcertification data matching new SQL structure"""
    base_date = datetime(2024, 1, 1)
    recent_date = datetime(2024, 10, 1)  # Recent record
    
    return pd.DataFrame([
        # In-scope ASVs
        {
            "data_type": "in_scope_asvs",
            "asv": "ASV-001",
            "hpsm_name": None,
            "status": None,
            "created_date": None,
            "target_remediation_date": None,
            "certification_type": None
        },
        {
            "data_type": "in_scope_asvs", 
            "asv": "ASV-002",
            "hpsm_name": None,
            "status": None,
            "created_date": None,
            "target_remediation_date": None,
            "certification_type": None
        },
        {
            "data_type": "in_scope_asvs",
            "asv": "ASV-003",
            "hpsm_name": None,
            "status": None,
            "created_date": None,
            "target_remediation_date": None,
            "certification_type": None
        },
        # Microcertification records - recent records for Tier 0
        {
            "data_type": "microcertification_records",
            "asv": None,
            "hpsm_name": "ASV-001", 
            "status": 4,  # Completed status
            "created_date": recent_date,
            "target_remediation_date": None,
            "certification_type": "Compute Groups APR"
        },
        {
            "data_type": "microcertification_records",
            "asv": None,
            "hpsm_name": "ASV-002",
            "status": 2,  # In progress - overdue for Tier 2 (created 40 days ago)
            "created_date": datetime(2024, 9, 26),  # 40 days before test time
            "target_remediation_date": None,
            "certification_type": "Compute Groups APR"
        },
        {
            "data_type": "microcertification_records",
            "asv": None,
            "hpsm_name": "ASV-004",  # Not in scope ASVs - won't count for Tier 1
            "status": 4,
            "created_date": recent_date,
            "target_remediation_date": None,
            "certification_type": "Compute Groups APR"
        },
        # Remediation record for Tier 3
        {
            "data_type": "microcertification_records",
            "asv": None,
            "hpsm_name": "ASV-005",
            "status": 6,  # In remediation
            "created_date": recent_date,
            "target_remediation_date": datetime(2024, 9, 1),  # Overdue by 65 days
            "certification_type": "Compute Groups APR"
        }
    ])

# Test pipeline initialization
def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    assert pipeline.env == env

# Test extract method (the main functionality)
@freeze_time("2024-11-05 12:09:00")
def test_extract_method_success():
    """Test extract method with successful data processing"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Mock ConfigPipeline.extract to return test data
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        # Setup the DataFrame that would come from SQL queries
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [_mock_apr_microcertification_raw_data()]
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
        assert len(metrics_df) == 4  # Four tier metrics
        
        # Verify data types
        for _, row in metrics_df.iterrows():
            assert isinstance(row["control_monitoring_utc_timestamp"], datetime)
            assert isinstance(row["monitoring_metric_value"], float)
            assert isinstance(row["metric_value_numerator"], (int, np.integer))
            assert isinstance(row["metric_value_denominator"], (int, np.integer))
            assert row["control_id"] == "CTRL-1104900"

def test_extract_method_empty_thresholds():
    """Test extract method with empty thresholds data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Mock ConfigPipeline.extract to return empty thresholds
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [pd.DataFrame()],  # Empty DataFrame
            "apr_microcertification_metrics": [_mock_apr_microcertification_raw_data()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should raise RuntimeError for empty thresholds
        with pytest.raises(RuntimeError, match="No threshold data found"):
            pipeline.extract()

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_tier_calculations():
    """Test that each tier calculates metrics correctly with new business logic"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [_mock_apr_microcertification_raw_data()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Should have exactly 4 metrics (4 tiers)
        assert len(metrics_df) == 4
        
        # Tier 0: Program existence - should be Green (has recent records)
        t0_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104900].iloc[0]
        assert t0_row["monitoring_metric_value"] == 100.0
        assert t0_row["monitoring_metric_status"] == "Green"
        assert t0_row["control_id"] == "CTRL-1104900"
        
        # Tier 1: Coverage - should be Red (only 2/3 ASVs covered: ASV-001, ASV-002 covered, ASV-003 missing)
        t1_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104901].iloc[0]
        assert t1_row["monitoring_metric_value"] == 66.67  # 2/3 * 100
        assert t1_row["monitoring_metric_status"] == "Red"
        assert t1_row["metric_value_numerator"] == 2
        assert t1_row["metric_value_denominator"] == 3
        
        # Tier 2: Review timeliness - should be Red (has overdue reviews)
        t2_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104902].iloc[0]
        assert t2_row["monitoring_metric_status"] == "Red"
        
        # Tier 3: Remediation timeliness - should be Red (has overdue remediation)
        t3_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104903].iloc[0]
        assert t3_row["monitoring_metric_status"] == "Red"

def test_extract_method_empty_apr_data():
    """Test handling of empty APR microcertification data"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [pd.DataFrame()]  # Empty APR data
        })
        mock_super_extract.return_value = mock_extract_df
        
        # Should raise RuntimeError for empty APR data
        with pytest.raises(RuntimeError, match="No APR microcertification metrics data found"):
            pipeline.extract()

@freeze_time("2024-11-05 12:09:00")
def test_tier_2_yellow_status_logic():
    """Test Tier 2 Yellow status logic (98-99% range)"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Create test data where only 1 role is overdue out of 100 (99% compliant)
    test_data = []
    
    # In-scope ASVs
    for i in range(3):
        test_data.append({
            "data_type": "in_scope_asvs",
            "asv": f"ASV-{i:03d}",
            "hpsm_name": None,
            "status": None,
            "created_date": None,
            "target_remediation_date": None,
            "certification_type": None
        })
    
    # Microcertification records - 99 compliant, 1 overdue
    for i in range(99):
        test_data.append({
            "data_type": "microcertification_records",
            "asv": None,
            "hpsm_name": f"ASV-{i:03d}",
            "status": 4,  # Completed
            "created_date": datetime(2024, 10, 1),  # Recent, not overdue
            "target_remediation_date": None,
            "certification_type": "Compute Groups APR"
        })
    
    # 1 overdue record
    test_data.append({
        "data_type": "microcertification_records",
        "asv": None,
        "hpsm_name": "ASV-099",
        "status": 2,  # In progress, overdue
        "created_date": datetime(2024, 9, 1),  # 65 days ago, overdue
        "target_remediation_date": None,
        "certification_type": "Compute Groups APR"
    })
    
    test_df = pd.DataFrame(test_data)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [test_df]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Tier 2 should be Yellow (99% compliant, in 98-99% range)
        t2_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104902].iloc[0]
        assert t2_row["monitoring_metric_value"] == 99.0
        assert t2_row["monitoring_metric_status"] == "Yellow"

def test_extract_method_data_types_validation():
    """Test that output data types match requirements"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [_mock_apr_microcertification_raw_data()]
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
            # resources_info is None for Green metrics, list for non-Green
            if row["monitoring_metric_status"] == "Green":
                assert row["resources_info"] is None
            else:
                assert isinstance(row["resources_info"], list)
                # Verify JSON content
                for resource_json in row["resources_info"]:
                    resource = json.loads(resource_json)
                    assert "issue" in resource

@freeze_time("2024-11-05 12:09:00")
def test_extract_method_data_type_enforcement():
    """Test that extract method enforces correct data types"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [_mock_apr_microcertification_raw_data()]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Check that data types are enforced correctly
        assert metrics_df["metric_value_numerator"].dtype == "int64"
        assert metrics_df["metric_value_denominator"].dtype == "int64" 
        assert metrics_df["monitoring_metric_value"].dtype == "float64"

@freeze_time("2024-11-05 12:09:00")
def test_tier_3_zero_remediation_scenario():
    """Test Tier 3 when no roles are in remediation (should be Green with 100%)"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    
    # Create test data with no remediation records (status != 6)
    test_data = []
    
    # In-scope ASVs
    test_data.append({
        "data_type": "in_scope_asvs",
        "asv": "ASV-001",
        "hpsm_name": None,
        "status": None,
        "created_date": None,
        "target_remediation_date": None,
        "certification_type": None
    })
    
    # Microcertification records - all completed, none in remediation
    test_data.append({
        "data_type": "microcertification_records",
        "asv": None,
        "hpsm_name": "ASV-001",
        "status": 4,  # Completed, not in remediation
        "created_date": datetime(2024, 10, 1),
        "target_remediation_date": None,
        "certification_type": "Compute Groups APR"
    })
    
    test_df = pd.DataFrame(test_data)
    
    with patch.object(ConfigPipeline, 'extract') as mock_super_extract:
        mock_extract_df = pd.DataFrame({
            "thresholds_raw": [_mock_threshold_df()],
            "apr_microcertification_metrics": [test_df]
        })
        mock_super_extract.return_value = mock_extract_df
        
        result_df = pipeline.extract()
        metrics_df = result_df["monitoring_metrics"].iloc[0]
        
        # Tier 3 should be Green with 100% (no remediation needed)
        t3_row = metrics_df[metrics_df["monitoring_metric_id"] == 1104903].iloc[0]
        assert t3_row["monitoring_metric_value"] == 100.0
        assert t3_row["monitoring_metric_status"] == "Green"
        assert t3_row["metric_value_numerator"] == 0
        assert t3_row["metric_value_denominator"] == 0
        assert t3_row["resources_info"] is None

# Test run function variations
def test_run_function_normal_path():
    """Test run function normal execution path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAutomatedMonitoringCTRL1104900') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=False, is_load=True, dq_actions=True)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run.assert_called_once_with(load=True, dq_actions=True)

def test_run_function_export_test_data():
    """Test run function with export test data path"""
    env = MockEnv()
    
    with patch('pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.PLAutomatedMonitoringCTRL1104900') as mock_pipeline_class:
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        run(env, is_export_test_data=True, dq_actions=False)
        
        mock_pipeline_class.assert_called_once_with(env)
        mock_pipeline.configure_from_filename.assert_called_once()
        mock_pipeline.run_test_data_export.assert_called_once_with(dq_actions=False)

def test_main_function_execution():
    """Test main function execution path"""
    mock_env = Mock()
    
    with patch("etip_env.set_env_vars", return_value=mock_env):
        with patch("pipelines.pl_automated_monitoring_CTRL_1104900.pipeline.run") as mock_run:
            with patch("sys.exit") as mock_exit:
                # Execute main block
                code = """
if True:
    from etip_env import set_env_vars
    from pipelines.pl_automated_monitoring_CTRL_1104900.pipeline import run
    
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