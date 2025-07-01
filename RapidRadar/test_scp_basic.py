#!/usr/bin/env python3
"""
Basic test to verify SCP controls pipeline functionality without full environment dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from freezegun import freeze_time
from datetime import datetime
import sys
import os

# Mock the dependencies that are missing
class MockConfigPipeline:
    def __init__(self, env):
        self.env = env
    
    def extract(self):
        # Return mock DataFrame that simulates the SQL extraction
        return pd.DataFrame({
            "thresholds_raw": [pd.DataFrame([
                {"monitoring_metric_id": "MNTR-1077770-T1", "control_id": "CTRL-1077770", 
                 "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0}
            ])],
            "in_scope_accounts": [pd.DataFrame([{"ACCOUNT": "123456789012"}])],
            "in_scope_asvs": [pd.DataFrame([{"ASV": "ASV001"}])],
            "microcertification_asvs": [pd.DataFrame([{"ASV": "ASV001"}])],
            "evaluated_accounts": [pd.DataFrame([
                {"RESOURCE_ID": "123456789012", "CONTROL_ID": "AC-3.AWS.146.v02", "COMPLIANCE_STATUS": "Compliant"}
            ])]
        })

class MockEnv:
    pass

# Mock the module imports
sys.modules['config_pipeline'] = Mock()
sys.modules['config_pipeline'].ConfigPipeline = MockConfigPipeline
sys.modules['etip_env'] = Mock()
sys.modules['etip_env'].Env = MockEnv

# Import the pipeline
from pipelines.pl_automated_monitoring_scp_controls.pipeline import PLAutomatedMonitoringScpControls, CONTROL_CONFIGS

def test_pipeline_import():
    """Test that pipeline can be imported successfully"""
    assert PLAutomatedMonitoringScpControls is not None
    assert CONTROL_CONFIGS is not None
    assert len(CONTROL_CONFIGS) == 10

def test_pipeline_initialization():
    """Test pipeline class initialization"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    assert pipeline.env == env

@freeze_time("2024-11-05 12:09:00")
def test_calculate_metrics_signature():
    """Test that _calculate_metrics method has correct signature and works"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Create test data
    thresholds_df = pd.DataFrame([
        {"monitoring_metric_id": "MNTR-1077770-T1", "control_id": "CTRL-1077770", 
         "monitoring_metric_tier": "Tier 1", "warning_threshold": 97.0, "alerting_threshold": 95.0}
    ])
    in_scope_accounts_df = pd.DataFrame([{"ACCOUNT": "123456789012"}])
    in_scope_asvs_df = pd.DataFrame([{"ASV": "ASV001"}])
    microcertification_asvs_df = pd.DataFrame([{"ASV": "ASV001"}])
    evaluated_accounts_df = pd.DataFrame([
        {"RESOURCE_ID": "123456789012", "CONTROL_ID": "AC-3.AWS.146.v02", "COMPLIANCE_STATUS": "Compliant"}
    ])
    
    # Test method call - this is where the original error occurred
    result = pipeline._calculate_metrics(
        thresholds_df, 
        in_scope_accounts_df, 
        in_scope_asvs_df, 
        microcertification_asvs_df, 
        evaluated_accounts_df
    )
    
    # Verify result
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "control_id" in result.columns
    assert "monitoring_metric_id" in result.columns
    assert "monitoring_metric_value" in result.columns

def test_extract_method():
    """Test the extract method works"""
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    result = pipeline.extract()
    
    assert isinstance(result, pd.DataFrame)
    assert "monitoring_metrics" in result.columns

if __name__ == "__main__":
    print("Running basic SCP controls tests...")
    
    try:
        test_pipeline_import()
        print("✓ test_pipeline_import passed")
        
        test_pipeline_initialization()
        print("✓ test_pipeline_initialization passed")
        
        test_calculate_metrics_signature()
        print("✓ test_calculate_metrics_signature passed")
        
        test_extract_method()
        print("✓ test_extract_method passed")
        
        print("\n🎉 All tests passed! The method signature issue is resolved.")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()