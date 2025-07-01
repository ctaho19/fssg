#!/usr/bin/env python3
"""
Simple test to verify the method signature issue in SCP controls pipeline.
This avoids the dependency issues by creating minimal mocks.
"""

import pandas as pd
from datetime import datetime
from unittest.mock import Mock
import sys
import os

# Add parent directory to path to access pipeline without dependencies
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Mock the dependencies that are missing
class MockConfigPipeline:
    def __init__(self, env):
        self.env = env
    
    def extract(self):
        return pd.DataFrame()

class MockEnv:
    pass

# Mock the module imports
sys.modules['config_pipeline'] = Mock()
sys.modules['config_pipeline'].ConfigPipeline = MockConfigPipeline
sys.modules['etip_env'] = Mock()
sys.modules['etip_env'].Env = MockEnv

# Now import the pipeline
try:
    from pipelines.pl_automated_monitoring_scp_controls.pipeline import PLAutomatedMonitoringScpControls, CONTROL_CONFIGS
    print("✓ Pipeline import successful")
    
    # Test method signature
    env = MockEnv()
    pipeline = PLAutomatedMonitoringScpControls(env)
    
    # Check if the method exists and inspect its signature
    import inspect
    method = getattr(pipeline, '_calculate_metrics')
    sig = inspect.signature(method)
    params = list(sig.parameters.keys())
    
    print(f"✓ Method signature: {params}")
    print(f"✓ Parameter count: {len(params)} (including self)")
    
    # Create mock data
    mock_thresholds = pd.DataFrame([
        {"monitoring_metric_id": "test", "control_id": "CTRL-1077770", "monitoring_metric_tier": "Tier 1"}
    ])
    mock_accounts = pd.DataFrame([{"ACCOUNT": "123456789012"}])
    mock_asvs = pd.DataFrame([{"ASV": "ASV001"}])
    mock_micro_asvs = pd.DataFrame([{"ASV": "ASV001"}])
    mock_evaluations = pd.DataFrame([
        {"RESOURCE_ID": "123456789012", "CONTROL_ID": "AC-3.AWS.146.v02", "COMPLIANCE_STATUS": "Compliant"}
    ])
    
    # Test method call with correct number of arguments
    try:
        result = pipeline._calculate_metrics(
            mock_thresholds, 
            mock_accounts, 
            mock_asvs, 
            mock_micro_asvs, 
            mock_evaluations
        )
        print("✓ Method call successful")
        print(f"✓ Result type: {type(result)}")
        if isinstance(result, pd.DataFrame):
            print(f"✓ Result shape: {result.shape}")
    except Exception as e:
        print(f"✗ Method call failed: {e}")
        import traceback
        traceback.print_exc()
        
except Exception as e:
    print(f"✗ Import failed: {e}")
    import traceback
    traceback.print_exc()