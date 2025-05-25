import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env

logger = logging.getLogger(name)


def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringCtrl1077231(env)
    pipeline.configure_from_filename(str(Path(file).parent / "config.yml"))
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )


class PLAutomatedMonitoringCtrl1077231(ConfigPipeline):
    def init(
        self,
        env: Env,
    ) -> None:
        super().init(env)
        self.env = env
        # Add client properties for test compatibility
        self.client_id = env.exchange.client_id
        self.client_secret = env.exchange.client_secret
        self.exchange_url = env.exchange.exchange_url
        self.cloudradar_api_url = (
            f"https://{self.env.exchange.exchange_url}/internal-operations/"
            "cloud-service/aws-tooling/search-resource-configurations"
        )

    def _calculate_ctrl1077231_metrics(
        self, thresholds_raw: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculate metrics for CTRL-1077231 based on resource configurations and thresholds."""
        # Constants for this control
        resource_type = "AWS::EC2::Instance"
        config_key = "metadataOptions.httpTokens"
        config_value = "required"
        ctrl_id = "CTRL-1077231"

        def fetch_all_resources(
            api_connector: OauthApi,
            verify_ssl,
            config_key_full: str,
            search_payload: Dict,
        ) -> List[Dict]:
            """Fetch all resources using the OauthApi connector with pagination support."""
            all_resources = []
            next_record_key = ""
            response_fields = [
                "resourceId",
                "amazonResourceName",
                "resourceType",
                "awsRegion",
                "accountName",
                "awsAccountId",
                "configurationList",
                config_key_full,
            ]
            fetch_payload = {
                "searchParameters": search_payload.get("searchParameters", [{}]),
                "responseFields": response_fields,
            }

            headers = {
                "Accept": "application/json;v=1",
                "Authorization": api_connector.api_token,
                "Content-Type": "application/json",
            }

            while True:
                params = {
                    "limit": 10000,
                    "nextRecordKey": "",
                }  # Modifying here, feel free to tweak as needed
                if next_record_key:
                    params["nextRecordKey"] = next_record_key

                request_kwargs = {
                    "params": params,
                    "headers": headers,
                    "json": fetch_payload,
                    "verify": verify_ssl,
                }

                response = api_connector.send_request(
                    url=api_connector.url,
                    request_type="post",
                    request_kwargs=request_kwargs,
                    retry_delay=20,
                )

                # Ensure response exists and has a status_code before checking
                if response is None:
                    raise RuntimeError("API response is None")

                if not hasattr(response, "status_code"):
                    raise RuntimeError(
                        "API response does not have status_code attribute"
                    )

                if response.status_code > 299:
                    err_msg = (
                        f"Error occurred while retrieving resources with status code "
                        f"{response.status_code}."
                    )
                    raise RuntimeError(err_msg)

                data = response.json()
                resources = data.get("resourceConfigurations", [])
                new_next_record_key = data.get("nextRecordKey", "")
                all_resources.extend(resources)
                next_record_key = new_next_record_key

                if not next_record_key:
                    break

                # Small delay to avoid rate limits
                time.sleep(0.15)

            return all_resources

        def filter_resources(
            resources: List[Dict], config_key: str, config_value: str
        ) -> Tuple[int, int, List[Dict], List[Dict]]:
            """Filter and categorize resources based on their configuration values."""
            tier1_numerator = 0
            tier2_numerator = 0
            tier1_non_compliant = []
            tier2_non_compliant = []
            config_key_full = f"configuration.{config_key}"
            fields_to_keep = [
                "resourceId",
                "amazonResourceName",
                "resourceType",
                "awsRegion",
                "accountName",
                "awsAccountId",
                config_key_full,
            ]

            for resource in resources:
                config_list = resource.get("configurationList", [])
                config_item = next(
                    (
                        c
                        for c in config_list
                        if c.get("configurationName") == config_key_full
                    ),
                    None,
                )
                actual_value = (
                    config_item.get("configurationValue") if config_item else None
                )

                if actual_value and str(actual_value).strip():
                    tier1_numerator += 1
                    if str(actual_value) == config_value:
                        tier2_numerator += 1
                    else:
                        non_compliant_info = {
                            f: resource.get(f, "N/A") for f in fields_to_keep
                        }
                        non_compliant_info[config_key_full] = actual_value
                        tier2_non_compliant.append(non_compliant_info)
                else:
                    non_compliant_info = {
                        f: resource.get(f, "N/A") for f in fields_to_keep
                    }
                    non_compliant_info[config_key_full] = (
                        actual_value if actual_value is not None else "MISSING"
                    )
                    tier1_non_compliant.append(non_compliant_info)

            return (
                tier1_numerator,
                tier2_numerator,
                tier1_non_compliant,
                tier2_non_compliant,
            )

        def get_compliance_status(
            metric: float,
            alert_threshold: float,
            warning_threshold: Optional[float] = None,
        ) -> str:
            """Calculate compliance status based on metric value and thresholds."""
            metric_percentage = metric * 100
            try:
                alert_threshold_f = float(alert_threshold)
            except (TypeError, ValueError):
                return "Red"
            warning_threshold_f = None
            if warning_threshold is not None:
                try:
                    warning_threshold_f = float(warning_threshold)
                except (TypeError, ValueError):
                    warning_threshold_f = None
            if metric_percentage >= alert_threshold_f:
                return "Green"
            elif (
                warning_threshold_f is not None
                and metric_percentage >= warning_threshold_f
            ):
                return "Yellow"
            else:
                return "Red"

        try:
            # Get metric IDs from thresholds DataFrame based on tier
            tier1_metrics = thresholds_raw[
                (thresholds_raw["control_id"] == ctrl_id)
                & (thresholds_raw["monitoring_metric_tier"] == "Tier 1")
            ]
            tier2_metrics = thresholds_raw[
                (thresholds_raw["control_id"] == ctrl_id)
                & (thresholds_raw["monitoring_metric_tier"] == "Tier 2")
            ]
            if tier1_metrics.empty or tier2_metrics.empty:
                raise ValueError(f"Missing threshold data for control {ctrl_id}")

            tier1_metric_id = tier1_metrics.iloc[0]["monitoring_metric_id"]
            tier2_metric_id = tier2_metrics.iloc[0]["monitoring_metric_id"]

            api_token = refresh(
                client_id=self.env.exchange.client_id,
                client_secret=self.env.exchange.client_secret,
                exchange_url=self.env.exchange.exchange_url,
            )
            api_connector = OauthApi(
                url=self.cloudradar_api_url,
                api_token=api_token,
            )
            verify_ssl = C1_CERT_FILE
            config_key_full = f"configuration.{config_key}"
            search_payload = {"searchParameters": [{"resourceType": resource_type}]}

            try:
                resources = fetch_all_resources(
                    api_connector, verify_ssl, config_key_full, search_payload
                )
            except Exception as e:
                logger.error(f"API request failed: {str(e)}")
                raise RuntimeError(f"Critical API fetch failure: {str(e)}") from e

            (
                tier1_numerator,
                tier2_numerator,
                tier1_non_compliant,
                tier2_non_compliant,
            ) = filter_resources(resources, config_key, config_value)
            total_resources = len(resources)
            tier1_metric = (
                tier1_numerator / total_resources if total_resources > 0 else 0
            )
            tier2_metric = (
                tier2_numerator / tier1_numerator if tier1_numerator > 0 else 0
            )

            # Get thresholds for each tier
            t1_threshold = tier1_metrics.iloc[0]
            t2_threshold = tier2_metrics.iloc[0]

            t1_status = get_compliance_status(
                tier1_metric,
                t1_threshold["alerting_threshold"],
                t1_threshold["warning_threshold"],
            )
            t2_status = get_compliance_status(
                tier2_metric,
                t2_threshold["alerting_threshold"],
                t2_threshold["warning_threshold"],
            )
            now = datetime.now()
            results = [
                {
                    "control_monitoring_utc_timestamp": now,
                    "control_id": ctrl_id,
                    "monitoring_metric_id": tier1_metric_id,
                    "monitoring_metric_value": float(tier1_metric * 100),
                    "monitoring_metric_status": t1_status,
                    "metric_value_numerator": int(tier1_numerator),
                    "metric_value_denominator": int(total_resources),
                    "resources_info": (
                        [json.dumps(x) for x in tier1_non_compliant]
                        if tier1_non_compliant
                        else None
                    ),
                },
                {
                    "control_monitoring_utc_timestamp": now,
                    "control_id": ctrl_id,
                    "monitoring_metric_id": tier2_metric_id,
                    "monitoring_metric_value": float(tier2_metric * 100),
                    "monitoring_metric_status": t2_status,
                    "metric_value_numerator": int(tier2_numerator),
                    "metric_value_denominator": int(tier1_numerator),
                    "resources_info": (
                        [json.dumps(x) for x in tier2_non_compliant]
                        if tier2_non_compliant
                        else None
                    ),
                },
            ]
            df = pd.DataFrame(results)
            df["metric_value_numerator"] = df["metric_value_numerator"].astype("int64")
            df["metric_value_denominator"] = df["metric_value_denominator"].astype(
                "int64"
            )
            print(df)
            return df
        except Exception as e:
            if not isinstance(e, RuntimeError):
                logger.error(
                    f"Unexpected error in calculate_ctrl1077231_metrics: {str(e)}"
                )
                raise RuntimeError(f"Failed to calculate metrics: {str(e)}") from e
            raise

    def extract_data(self) -> pd.DataFrame:
        """Extract data and calculate monitoring metrics."""
        df = super().extract()
        df["monitoring_metrics"] = self._calculate_ctrl1077231_metrics(
            df["thresholds_raw"]
        )
        return df