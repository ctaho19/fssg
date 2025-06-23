from typing import Dict, Any
import pandas as pd
from datetime import datetime
import json
from config_pipeline import ConfigPipeline
from etip_env import Env

def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringCTRL1101994(env)
    pipeline.configure_from_filename("config.yml")
    
    if is_export_test_data:
        return pipeline.run_test_data_export(dq_actions=dq_actions)
    else:
        return pipeline.run(load=is_load, dq_actions=dq_actions)

class PLAutomatedMonitoringCTRL1101994(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env

    def _calculate_metrics(self, thresholds_raw: pd.DataFrame, symantec_proxy_outcome: pd.DataFrame) -> pd.DataFrame:
        """
        Core business logic for Symantec Proxy monitoring compliance metrics
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            symantec_proxy_outcome: DataFrame containing Symantec Proxy test outcomes
            
        Returns:
            DataFrame with standardized output schema
        """
    
    # Step 1: Input Validation (REQUIRED)
    if thresholds_raw.empty:
        raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
    
    # Step 2: Extract Threshold Configuration
    thresholds = thresholds_raw.to_dict("records")
    
    # Step 3: Calculate metrics for each threshold
    results = []
    now = datetime.now()
    
    for threshold in thresholds:
        ctrl_id = threshold["control_id"]
        metric_id = threshold["monitoring_metric_id"]
        
        # Calculate Symantec Proxy test success metrics
        metric_value, compliant_count, total_count, non_compliant_resources = _calculate_symantec_proxy_metrics(symantec_proxy_outcome)
        
        # Determine compliance status
        alert_threshold = threshold.get("alerting_threshold", 100.0)
        warning_threshold = threshold.get("warning_threshold")
        
        if metric_value >= alert_threshold:
            compliance_status = "Green"
        elif warning_threshold is not None and metric_value >= warning_threshold:
            compliance_status = "Yellow"
        else:
            compliance_status = "Red"
        
        # Step 4: Format Output with Standard Fields
        result = {
            "control_monitoring_utc_timestamp": now,
            "control_id": ctrl_id,
            "monitoring_metric_id": metric_id,
            "monitoring_metric_value": float(metric_value),
            "monitoring_metric_status": compliance_status,
            "metric_value_numerator": int(compliant_count),
            "metric_value_denominator": int(total_count),
            "resources_info": non_compliant_resources
        }
        results.append(result)
    
        result_df = pd.DataFrame(results)
        
        # Ensure correct data types to match test expectations
        if not result_df.empty:
            result_df = result_df.astype(
                {
                    "metric_value_numerator": "int64",
                    "metric_value_denominator": "int64",
                    "monitoring_metric_value": "float64",
                }
            )
        
        return result_df

def _calculate_symantec_proxy_metrics(symantec_proxy_outcome: pd.DataFrame):
    """Calculate Symantec Proxy test success metrics."""
    metric_value = 0.0
    compliant_count = 0
    total_count = 0
    non_compliant_resources = None
    
    if not symantec_proxy_outcome.empty:
        # Count total tests and successful tests
        total_tests = len(symantec_proxy_outcome)
        successful_tests = sum(symantec_proxy_outcome['EXPECTED_OUTCOME'] == symantec_proxy_outcome['ACTUAL_OUTCOME'])
        
        # Calculate metrics
        if total_tests > 0:
            metric_value = round(100.0 * successful_tests / total_tests, 2)
            compliant_count = successful_tests
            total_count = total_tests
            
            # Report failed tests if any
            if successful_tests < total_tests:
                failed_tests = total_tests - successful_tests
                failed_test_details = []
                
                # Get details of failed tests
                for idx, row in symantec_proxy_outcome.iterrows():
                    if row['EXPECTED_OUTCOME'] != row['ACTUAL_OUTCOME']:
                        failed_test_details.append({
                            "test_id": idx,
                            "expected": row['EXPECTED_OUTCOME'],
                            "actual": row['ACTUAL_OUTCOME']
                        })
                
                # Limit to first 100 failed tests
                if len(failed_test_details) > 100:
                    failed_test_details = failed_test_details[:100]
                    failed_test_details.append({"message": f"... and {len(failed_test_details) - 100} more failed tests"})
                
                non_compliant_resources = [json.dumps(test) for test in failed_test_details]
        else:
            non_compliant_resources = [json.dumps({"issue": "No test data available"})]
    else:
        non_compliant_resources = [json.dumps({"issue": "No Symantec Proxy outcome data available"})]
    
    return metric_value, compliant_count, total_count, non_compliant_resources

    def extract(self) -> pd.DataFrame:
        """Override extract to integrate SQL data with metric calculations"""
        df = super().extract()
        # Wrap the DataFrame in a list to store it as a single value in the cell
        df["monitoring_metrics"] = [self._calculate_metrics(
            df["thresholds_raw"].iloc[0],
            df["symantec_proxy_outcome"].iloc[0]
        )]
        return df

if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception as e:
        import sys
        sys.exit(1)