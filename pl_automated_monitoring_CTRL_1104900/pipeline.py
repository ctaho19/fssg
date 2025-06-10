import json
from datetime import datetime
import pandas as pd
from pathlib import Path

from config_pipeline import ConfigPipeline
from etip_env import Env


def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringCTRL1104900(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    
    if is_export_test_data:
        return pipeline.run_test_data_export(dq_actions=dq_actions)
    else:
        return pipeline.run(load=is_load, dq_actions=dq_actions)


class PLAutomatedMonitoringCTRL1104900(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env

    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
        apr_microcertification_metrics: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Core business logic for APR microcertification monitoring compliance metrics
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            apr_microcertification_metrics: DataFrame containing raw APR data from SQL
            
        Returns:
            DataFrame with standardized output schema
        """
        
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
        
        if apr_microcertification_metrics.empty:
            raise RuntimeError("No APR microcertification metrics data found. Cannot proceed with metrics calculation.")
        
        # Step 2: Separate data by type
        in_scope_asvs = apr_microcertification_metrics[
            apr_microcertification_metrics['data_type'] == 'in_scope_asvs'
        ]['asv'].dropna().unique()
        
        microcert_records = apr_microcertification_metrics[
            apr_microcertification_metrics['data_type'] == 'microcertification_records'
        ].copy()
        
        # Step 3: Calculate metrics for each tier
        results = []
        now = datetime.now()
        one_year_ago = now.replace(year=now.year - 1)
        
        # Create a mapping of thresholds by tier for lookup
        threshold_map = {
            row["monitoring_metric_tier"]: row 
            for _, row in thresholds_raw.iterrows()
        }
        
        # Tier 0: APR Microcertification Program Existence
        if "Tier 0" in threshold_map:
            threshold = threshold_map["Tier 0"]
            
            # Check if any microcertification records exist within the last year
            recent_records = microcert_records[
                pd.notna(microcert_records['created_date']) & 
                (microcert_records['created_date'] >= one_year_ago)
            ]
            
            if len(recent_records) > 0:
                metric_value = 100.0
                status = "Green"
                numerator = len(recent_records)
                denominator = 1
                resources_info = None
            else:
                metric_value = 0.0
                status = "Red"
                numerator = 0
                denominator = 1
                resources_info = [json.dumps({
                    "issue": "No APR microcertification records found in the last year",
                    "metric": "APR Microcertification Program Existence"
                })]
            
            results.append({
                "control_monitoring_utc_timestamp": now,
                "control_id": "CTRL-1104900",
                "monitoring_metric_id": threshold["monitoring_metric_id"],
                "monitoring_metric_value": metric_value,
                "monitoring_metric_status": status,
                "metric_value_numerator": numerator,
                "metric_value_denominator": denominator,
                "resources_info": resources_info,
            })
        
        # Tier 1: APR Coverage - In-Scope ASVs in Microcertification
        if "Tier 1" in threshold_map:
            threshold = threshold_map["Tier 1"]
            
            # Get unique ASVs in microcertification
            microcert_asvs = set(microcert_records['hpsm_name'].dropna().unique())
            
            # Calculate coverage
            covered_asvs = [asv for asv in in_scope_asvs if asv in microcert_asvs]
            missing_asvs = [asv for asv in in_scope_asvs if asv not in microcert_asvs]
            
            total_asvs = len(in_scope_asvs)
            covered_count = len(covered_asvs)
            
            if total_asvs > 0:
                metric_value = round((covered_count / total_asvs) * 100, 2)
            else:
                metric_value = 0.0
            
            # Determine status
            if metric_value == 100.0:
                status = "Green"
                resources_info = None
            else:
                status = "Red"
                resources_info = [json.dumps({
                    "issue": f"{len(missing_asvs)} ASVs are not in the microcertification table",
                    "missing_asvs": missing_asvs[:10],  # Limit to first 10
                    "metric": "APR Coverage"
                })]
            
            results.append({
                "control_monitoring_utc_timestamp": now,
                "control_id": "CTRL-1104900",
                "monitoring_metric_id": threshold["monitoring_metric_id"],
                "monitoring_metric_value": metric_value,
                "monitoring_metric_status": status,
                "metric_value_numerator": covered_count,
                "metric_value_denominator": total_asvs,
                "resources_info": resources_info,
            })
        
        # Tier 2: APR Review Completion Timeliness
        if "Tier 2" in threshold_map:
            threshold = threshold_map["Tier 2"]
            
            # Filter records not in remediation (status != 6)
            review_records = microcert_records[
                pd.notna(microcert_records['status']) & 
                (microcert_records['status'] != 6)
            ].copy()
            
            total_roles = len(review_records)
            compliant_roles = 0
            overdue_roles = []
            
            for _, record in review_records.iterrows():
                if pd.notna(record['created_date']):
                    days_since_created = (now - record['created_date']).days
                    
                    # Check if overdue (status in [0,1,2,3] and >= 30 days)
                    if record['status'] in [0, 1, 2, 3] and days_since_created >= 30:
                        overdue_roles.append({
                            'hpsm_name': record['hpsm_name'],
                            'status': record['status'],
                            'days_overdue': days_since_created
                        })
                    else:
                        compliant_roles += 1
                else:
                    compliant_roles += 1
            
            # Check for null status records (compliance violation)
            null_status_records = microcert_records[
                microcert_records['status'].isna() & 
                pd.notna(microcert_records['created_date']) &
                (microcert_records['created_date'] >= one_year_ago)
            ]
            
            if total_roles > 0:
                metric_value = round((compliant_roles / total_roles) * 100, 2)
            else:
                metric_value = 0.0
            
            # Determine status
            if len(null_status_records) > 0:
                status = "Red"
            elif compliant_roles == total_roles:
                status = "Green"
            elif metric_value >= 99:
                status = "Green"
            elif metric_value >= 98:
                status = "Yellow"
            else:
                status = "Red"
            
            # Resources info
            resources_info = None
            if status != "Green":
                issues = []
                if len(overdue_roles) > 0:
                    issues.append(f"{len(overdue_roles)} roles have overdue reviews")
                if len(null_status_records) > 0:
                    issues.append(f"{len(null_status_records)} roles have null status")
                
                resources_info = [json.dumps({
                    "issues": issues,
                    "overdue_count": len(overdue_roles),
                    "null_status_count": len(null_status_records),
                    "metric": "APR Review Completion Timeliness"
                })]
            
            results.append({
                "control_monitoring_utc_timestamp": now,
                "control_id": "CTRL-1104900",
                "monitoring_metric_id": threshold["monitoring_metric_id"],
                "monitoring_metric_value": metric_value,
                "monitoring_metric_status": status,
                "metric_value_numerator": compliant_roles,
                "metric_value_denominator": total_roles,
                "resources_info": resources_info,
            })
        
        # Tier 3: APR Remediation Completion Timeliness
        if "Tier 3" in threshold_map:
            threshold = threshold_map["Tier 3"]
            
            # Filter records in remediation (status == 6)
            remediation_records = microcert_records[
                pd.notna(microcert_records['status']) & 
                (microcert_records['status'] == 6)
            ].copy()
            
            total_remediation_roles = len(remediation_records)
            compliant_remediation_roles = 0
            overdue_remediation = []
            
            for _, record in remediation_records.iterrows():
                if pd.notna(record['target_remediation_date']):
                    days_past_target = (now - record['target_remediation_date']).days
                    
                    # Check if overdue (>= 30 days past target remediation date)
                    if days_past_target >= 30:
                        overdue_remediation.append({
                            'hpsm_name': record['hpsm_name'],
                            'days_past_target': days_past_target
                        })
                    else:
                        compliant_remediation_roles += 1
                else:
                    compliant_remediation_roles += 1
            
            if total_remediation_roles == 0:
                metric_value = 100.0
                status = "Green"
                resources_info = None
            else:
                metric_value = round((compliant_remediation_roles / total_remediation_roles) * 100, 2)
                
                if compliant_remediation_roles == total_remediation_roles:
                    status = "Green"
                    resources_info = None
                else:
                    status = "Red"
                    resources_info = [json.dumps({
                        "issue": f"{len(overdue_remediation)} roles have overdue remediation",
                        "overdue_count": len(overdue_remediation),
                        "metric": "APR Remediation Completion Timeliness"
                    })]
            
            results.append({
                "control_monitoring_utc_timestamp": now,
                "control_id": "CTRL-1104900",
                "monitoring_metric_id": threshold["monitoring_metric_id"],
                "monitoring_metric_value": metric_value,
                "monitoring_metric_status": status,
                "metric_value_numerator": compliant_remediation_roles,
                "metric_value_denominator": total_remediation_roles,
                "resources_info": resources_info,
            })
        
        result_df = pd.DataFrame(results)
        
        # Ensure correct data types to match test expectations
        if not result_df.empty:
            result_df = result_df.astype({
                "metric_value_numerator": "int64",
                "metric_value_denominator": "int64",
                "monitoring_metric_value": "float64",
            })
        
        return result_df

    def extract(self) -> pd.DataFrame:
        """Override extract to integrate SQL data with metric calculations"""
        df = super().extract()
        # Wrap the DataFrame in a list to store it as a single value in the cell
        df["monitoring_metrics"] = [self._calculate_metrics(
            df["thresholds_raw"].iloc[0],
            df["apr_microcertification_metrics"].iloc[0]
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