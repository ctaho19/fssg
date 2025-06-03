import json
from datetime import datetime
import pandas as pd

from config_pipeline import ConfigPipeline
from etip_env import Env


def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringCTRL1079134(env)
    pipeline.configure_from_filename("config.yml")
    
    if is_export_test_data:
        return pipeline.run_test_data_export(dq_actions=dq_actions)
    else:
        return pipeline.run(load=is_load, dq_actions=dq_actions)


class PLAutomatedMonitoringCTRL1079134(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env

    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
        macie_metrics: pd.DataFrame,
        macie_testing: pd.DataFrame,
        historical_stats: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Core business logic transformer for Macie monitoring compliance metrics
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            macie_metrics: DataFrame containing Macie metrics data
            macie_testing: DataFrame containing Macie test results
            historical_stats: DataFrame containing historical test statistics
            context: Pipeline context
            
        Returns:
            DataFrame with standardized output schema
        """
        
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
        
        # Step 2: Extract Threshold Configuration
        thresholds = thresholds_raw.to_dict("records")
        
        # Step 3: Calculate metrics for each tier
        results = []
        now = datetime.now()
        
        for threshold in thresholds:
            ctrl_id = threshold["control_id"]
            metric_id = threshold["monitoring_metric_id"]
            tier = threshold["monitoring_metric_tier"]
            
            if tier == "Tier 0":
                # Tier 0: Heartbeat - Check if Macie data is current and buckets are being scanned
                metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier0_metrics(macie_metrics, now)
                
            elif tier == "Tier 1":
                # Tier 1: Completeness - Calculate percentage of buckets scanned vs total CloudFronted buckets
                metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier1_metrics(macie_metrics, now)
                
            elif tier == "Tier 2":
                # Tier 2: Accuracy - Calculate test success rate with anomaly detection
                metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier2_metrics(
                    macie_testing, historical_stats
                )
            else:
                # Default case for unknown tiers
                metric_value = 0.0
                compliant_count = 0
                total_count = 0
                non_compliant_resources = None
            
            # Determine compliance status
            alert_threshold = threshold.get("alerting_threshold", 100.0)
            warning_threshold = threshold.get("warning_threshold")
            
            if warning_threshold is not None and metric_value >= warning_threshold:
                compliance_status = "Green"
            elif metric_value >= alert_threshold:
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
                "resources_info": non_compliant_resources,
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

    def _calculate_tier0_metrics(self, macie_metrics: pd.DataFrame, current_time: datetime):
        """Calculate Tier 0 (Heartbeat) metrics for Macie monitoring."""
        metric_value = 0.0
        compliant_count = 0
        total_count = 1
        non_compliant_resources = None
        
        if not macie_metrics.empty:
            # Check if data is current (within last day)
            # Using current_time from parent method for consistency with test freezing
            is_current = pd.to_datetime(macie_metrics['SF_LOAD_TIMESTAMP'].iloc[0]).date() >= \
                         (current_time.date() - pd.Timedelta(days=1))
            
            buckets_scanned = macie_metrics['TOTAL_BUCKETS_SCANNED_BY_MACIE'].iloc[0] \
                             if 'TOTAL_BUCKETS_SCANNED_BY_MACIE' in macie_metrics.columns else 0
            
            # Calculate Tier 0 metric
            if is_current and buckets_scanned > 0:
                metric_value = 100.0
                compliant_count = 1
            else:
                issues = []
                if not is_current:
                    issues.append("Data not current (older than 1 day)")
                if buckets_scanned <= 0:
                    issues.append("No buckets being scanned by Macie")
                
                non_compliant_resources = [json.dumps({"issues": issues})] if issues else None
        else:
            non_compliant_resources = [json.dumps({"issue": "No Macie metrics data available"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _calculate_tier1_metrics(self, macie_metrics: pd.DataFrame, current_time: datetime):
        """Calculate Tier 1 (Completeness) metrics for Macie monitoring."""
        metric_value = 0.0
        compliant_count = 0
        total_count = 0
        non_compliant_resources = None
        
        if not macie_metrics.empty:
            # Check if data is current (within last day)
            # Using current_time from parent method for consistency with test freezing
            is_current = pd.to_datetime(macie_metrics['SF_LOAD_TIMESTAMP'].iloc[0]).date() >= \
                         (current_time.date() - pd.Timedelta(days=1))
            
            if is_current:
                buckets_scanned = macie_metrics['TOTAL_BUCKETS_SCANNED_BY_MACIE'].iloc[0] \
                                if 'TOTAL_BUCKETS_SCANNED_BY_MACIE' in macie_metrics.columns else 0
                total_buckets = macie_metrics['TOTAL_CLOUDFRONTED_BUCKETS'].iloc[0] \
                              if 'TOTAL_CLOUDFRONTED_BUCKETS' in macie_metrics.columns else 0
                
                # Calculate Tier 1 metric
                if total_buckets > 0:
                    metric_value = round(100.0 * buckets_scanned / total_buckets, 2)
                    compliant_count = buckets_scanned
                    total_count = total_buckets
                    
                    # Report missing buckets if any
                    missing_buckets = total_buckets - buckets_scanned
                    if missing_buckets > 0:
                        non_compliant_resources = [json.dumps({
                            "buckets_not_scanned": missing_buckets,
                            "total_buckets": total_buckets,
                            "scanned_buckets": buckets_scanned
                        })]
                else:
                    non_compliant_resources = [json.dumps({"issue": "No CloudFronted buckets found"})]
            else:
                non_compliant_resources = [json.dumps({"issue": "Data not current (older than 1 day)"})]
        else:
            non_compliant_resources = [json.dumps({"issue": "No Macie metrics data available"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _calculate_tier2_metrics(self, macie_testing: pd.DataFrame, historical_stats: pd.DataFrame):
        """Calculate Tier 2 (Accuracy) metrics for Macie monitoring."""
        metric_value = 0.0
        compliant_count = 0
        total_count = 0
        non_compliant_resources = None
        
        if not macie_testing.empty:
            # Calculate total tests and successful tests
            total_tests = len(macie_testing)
            successful_tests = macie_testing['TESTISSUCCESSFUL'].sum() if 'TESTISSUCCESSFUL' in macie_testing.columns else 0
            
            # Calculate Tier 2 metric
            if total_tests > 0:
                metric_value = round(100.0 * successful_tests / total_tests, 2)
                compliant_count = successful_tests
                total_count = total_tests
                
                # Check for anomalies in test count if historical data is available
                anomaly_issues = []
                if not historical_stats.empty:
                    avg_historical_tests = historical_stats['AVG_HISTORICAL_TESTS'].iloc[0]
                    min_historical_tests = historical_stats['MIN_HISTORICAL_TESTS'].iloc[0]
                    max_historical_tests = historical_stats['MAX_HISTORICAL_TESTS'].iloc[0]
                    
                    # Test count outside historical range or deviates >20% from average
                    if total_tests < min_historical_tests:
                        anomaly_issues.append(f"Test count ({total_tests}) below historical minimum ({min_historical_tests})")
                    elif total_tests > max_historical_tests:
                        anomaly_issues.append(f"Test count ({total_tests}) above historical maximum ({max_historical_tests})")
                    elif abs(total_tests - avg_historical_tests) > avg_historical_tests * 0.2:
                        anomaly_issues.append(f"Test count ({total_tests}) deviates >20% from historical average ({avg_historical_tests})")
                
                # Report failed tests and anomalies
                issues = []
                if successful_tests < total_tests:
                    failed_tests = total_tests - successful_tests
                    issues.append(f"{failed_tests} out of {total_tests} tests failed")
                
                if anomaly_issues:
                    issues.extend(anomaly_issues)
                    
                if issues:
                    non_compliant_resources = [json.dumps({"issues": issues})]
            else:
                non_compliant_resources = [json.dumps({"issue": "No test data available"})]
        else:
            non_compliant_resources = [json.dumps({"issue": "No Macie test data available"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    # This is the extract portion for the API
    def extract(self) -> pd.DataFrame:
        df = super().extract()
        # Wrap the DataFrame in a list to store it as a single value in the cell
        df["monitoring_metrics"] = [self._calculate_metrics(
            df["thresholds_raw"].iloc[0],
            df["macie_metrics"].iloc[0],
            df["macie_testing"].iloc[0],
            df["historical_stats"].iloc[0]
        )]
        return df

if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception:
        import sys
        sys.exit(1)