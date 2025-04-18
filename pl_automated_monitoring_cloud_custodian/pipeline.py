import json
import time
from datetime import datetime, timedelta
from pathlib import Pathimport pandas as pd
from pandas.core.api import DataFrame as DataFrameimport pipelines.pl_automated_monitoring_cloud_custodian.transform  # noqa: F401 # register custom transforms
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env# To onboard a control, add the Cloud Custodian control ID and the FUSE control ID to the below dictionary #
dictionary_tier_2 = {
    "SC-8.AWS.43": "CTRL-1077109",
    "SC-8.AWS.42": "CTRL-1077108",
    "AC-3.AWS.80": "CTRL-1078029",
}aws_services_tier_2 = {}columns = [
    "monitoring_metric_id",
    "control_id",
    "monitoring_metric_value",
    "monitoring_metric_status",
    "metric_value_numerator",
    "metric_value_denominator",
    "resources_info",
    "control_monitoring_utc_timestamp",
]# Extract control number
def map_control(control_id, consolidated_controls_dictionary) -> str:
    if control_id in dictionary_tier_2:
        return dictionary_tier_2[control_id]
    elif control_id in consolidated_controls_dictionary:
        return consolidated_controls_dictionary[control_id]def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAmCloudCustodianPipeline(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )class PLAmCloudCustodianPipeline(ConfigPipeline):
    def __init__(
        self,
        env: Env,
    ) -> None:
        super().__init__(env)
        self.ipat_api = (
            f"https://{self.env.exchange.exchange_url}/private/589268/tests/search"
        )

def _ipat_extract(
    self, threshold_df: pd.DataFrame, total_inventory_df: pd.DataFrame
) -> DataFrame:

    # Filter Inventory DF to get cloud control IDs for one-to-many FUSE controls
    consolidated_controls_dictionary = {}
    for service, fuse_id in aws_services_tier_2.items():
        if total_inventory_df.empty:
            filtered_ozone_df = total_inventory_df
        else:
            filtered_ozone_df = total_inventory_df.loc[
                (total_inventory_df["service_name"] == service)
            ]
        for index, value in filtered_ozone_df["control_id"].items():
            consolidated_controls_dictionary[value[:-4]] = fuse_id

    # Perform API call to return synthetic tests
    api_connector = OauthApi(
        url=self.ipat_api,
        api_token=refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        ),
    )
    headers = {
        "Accept": "application/json;v=1",
        "Authorization": api_connector.api_token,
        "Content-Type": "application/json",
    }

    def ipat_api_call(start_of_day_unix, end_of_day_unix):
        # Make request to endpoint
        api_data = {
            # Used to calculate monitoring metric value
            "test_status": {},
            # Used to return resource_info
            "failed_tests": {},
        }
        params = {
            "fromTimestamp": int(start_of_day_unix),
            "toTimestamp": int(end_of_day_unix),
            "nextRecordKey": "",
        }
        while True:
            request_kwargs = {
                "params": params,
                "headers": headers,
                "verify": C1_CERT_FILE,
            }
            response = api_connector.send_request(
                url=api_connector.url,
                request_type="get",
                request_kwargs=request_kwargs,
                retry_delay=20,
            )
            if response.status_code > 299:
                err_msg = f"Error occurred while retrieving IPAT test data with status code {response.status_code}."
                raise RuntimeError(err_msg)
            response_data = response.json()
            for item in response_data["items"]:
                if len(item["controlId"]) > 0:
                    control_id = item["controlId"][0]
                    if (
                        control_id in dictionary_tier_2
                        or control_id in consolidated_controls_dictionary
                    ):
                        fuse_control_id = map_control(
                            control_id, consolidated_controls_dictionary
                        )
                        if fuse_control_id not in api_data["test_status"]:
                            api_data["test_status"][fuse_control_id] = [
                                item["testRunStatus"]
                            ]
                        elif fuse_control_id in api_data["test_status"]:
                            api_data["test_status"][fuse_control_id].append(
                                item["testRunStatus"]
                            )
                        if (
                            item["testRunStatus"] == "FAIL"
                            and fuse_control_id not in api_data["failed_tests"]
                        ):
                            # Define the items to be included in the resource info
                            api_data["failed_tests"][fuse_control_id] = [
                                {
                                    "executionId": item["executionId"],
                                    "awsRegion": item["awsRegion"],
                                    "environment": item["environment"],
                                }
                            ]
                        elif (
                            item["testRunStatus"] == "FAIL"
                            and fuse_control_id in api_data["failed_tests"]
                        ):
                            api_data["failed_tests"][fuse_control_id].append(
                                {
                                    "executionId": item["executionId"],
                                    "awsRegion": item["awsRegion"],
                                    "environment": item["environment"],
                                }
                            )
            if not response_data.get("nextRecordKey"):
                break
            params["nextRecordKey"] = response_data["nextRecordKey"]
        return api_data

    # Define datetimes
    current_date = datetime.now()
    start_of_day = current_date - timedelta(days=8)
    start_of_day = datetime.combine(start_of_day, datetime.min.time())
    end_of_day = start_of_day + timedelta(days=8)
    start_of_day_unix = time.mktime(start_of_day.timetuple())
    end_of_day_unix = time.mktime(end_of_day.timetuple())

    api_data = ipat_api_call(start_of_day_unix, end_of_day_unix)

    # Add controls that did not have synthetic tests to make sure they return Tier 2
    for fuse_control_id in dictionary_tier_2.values():
        if fuse_control_id not in api_data["test_status"]:
            api_data["test_status"][fuse_control_id] = []
    for fuse_control_id in aws_services_tier_2.values():
        if fuse_control_id not in api_data["test_status"]:
            api_data["test_status"][fuse_control_id] = []

    # Perform Tier 2 calculation
    tier_2_metric_df = pd.DataFrame(columns=columns)
    for fuse_control_id, test_statuses in api_data["test_status"].items():
        passes = sum(test_status == "PASS" for test_status in test_statuses)
        fails = sum(test_status == "FAIL" for test_status in test_statuses)
        total = passes + fails
        calculation = 0
        if total > 0:
            calculation = round((passes / (total)) * 100, 2)
        # Get Tier 2 thresholds for control
        filtered_threshold_df = threshold_df.loc[
            (threshold_df["control_id"] == fuse_control_id)
            & (threshold_df["monitoring_metric_tier"] == "Tier 2")
        ]
        # Filter to most recent version of threshold
        filtered_threshold_df = filtered_threshold_df.loc[
            filtered_threshold_df["metric_threshold_start_date"]
            == filtered_threshold_df["metric_threshold_start_date"].max()
        ]
        if calculation < filtered_threshold_df["alerting_threshold"].values[0]:
            monitoring_metric_status = "Red"
        elif (
            calculation >= filtered_threshold_df["alerting_threshold"].values[0]
        ) & (calculation < filtered_threshold_df["warning_threshold"].values[0]):
            monitoring_metric_status = "Yellow"
        else:
            monitoring_metric_status = "Green"
        if fuse_control_id in api_data["failed_tests"]:
            resource_info = api_data["failed_tests"][fuse_control_id]
            empty_array = []
            for x in resource_info:
                x = json.dumps(x)
                empty_array.append(x)
            resource_info = empty_array
        else:
            resource_info = None
        d = [
            [
                filtered_threshold_df["monitoring_metric_id"].values[0],
                fuse_control_id,
                calculation,
                monitoring_metric_status,
                passes,
                total,
                resource_info,
                current_date,
            ]
        ]
        tier_2_metric_df = pd.concat(
            [pd.DataFrame(d, columns=columns), tier_2_metric_df], ignore_index=True
        )
        tier_2_metric_df = tier_2_metric_df.astype(
            {
                "monitoring_metric_value": float,
                "monitoring_metric_id": int,
                "metric_value_numerator": int,
                "metric_value_denominator": int,
            }
        )
    return tier_2_metric_df

# This is the extract portion for the API
def extract(self) -> DataFrame:
    df = super().extract()
    df["cloud_custodian_df"] = self._ipat_extract(
        df["threshold_df"], df["total_inventory_df"]
    )
    return df