"""pipeline_template.py
------------------------
Reusable skeleton for production monitoring pipelines.

Copy this file into a new pipeline package (e.g. ``pipelines/<new_pipeline>/pipeline.py``)
and replace the TODO-marked sections with pipeline-specific logic.

The template is intentionally opinionated to enforce a consistent structure
across all monitoring pipelines:

1. ``run_pipeline`` public helper for orchestrating the pipeline end-to-end.
2. ``<YourPipelineClass>`` inheriting from ``ConfigPipeline`` with the core
   ETL (Extract, Transform, Load) methods.
3. Helper/private methods grouped logically (API access, metric calculations,
   threshold utilities, etc.).
4. Rich type hints + Google-style docstrings for maximum readability.

NOTE: This template assumes the runtime library ecosystem used in existing
pipelines (``pandas``, ``ConfigPipeline``, ``OauthApi`` etc.) is available in
the executing environment.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import pandas as pd

# Third-party / internal dependencies --------------------------------------------------
# Import your internal abstractions here.  They are commented-out so that
# importing this template in a bare environment will not fail. Uncomment and
# adjust the import paths according to your project structure.
if TYPE_CHECKING:
    # These imports are only for static type checkers / IDEs and do not
    # introduce a hard runtime dependency when the template is copied into
    # projects that follow a different module structure.
    from your_project.core.env import Env  # pragma: no cover  # type: ignore
    from your_project.core.oauth_api import OauthApi  # pragma: no cover  # type: ignore
# from your_project.core.env import Env
# from your_project.core.pipeline import ConfigPipeline
# from your_project.core.oauth_api import OauthApi

# --------------------------------------------------------------------------------------
# CONSTANTS & CONFIGURATION
# --------------------------------------------------------------------------------------

#: Default set of control-to-metric mappings. **Replace** with the pipeline-
#: specific mapping loaded from a config file or another source of truth.
CONTROL_CONFIGS: List[Dict[str, Any]] = [
    # Example entry -------------------------------------------------------------
    {
        "cloud_control_id": "AC-X.AWS.YY.Vv1",
        "ctrl_id": "CTRL-0000000",
        "metric_ids": {
            "tier1": "MNTR-0000000-T1",
            "tier2": "MNTR-0000000-T2",
        },
        "requires_tier3": False,
    },
    # ---------------------------------------------------------------------------
]

# --------------------------------------------------------------------------------------
# PUBLIC API
# --------------------------------------------------------------------------------------

def run_pipeline(
    env: "Env",  # type: ignore  # Uncomment Env import when available
    load_env: Optional["Env"] = None,  # type: ignore
    *,
    load: bool = True,
    do_actions: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Entry-point helper used by scheduled jobs & ad-hoc executions.

    Args:
        env: Primary *runtime* environment (e.g. DEV / QA / PROD).
        load_env: Environment to load the transformed data into. Defaults to
            the same as ``env``.
        load: Whether to execute the load step.
        do_actions: Whether to execute any configured *actions* (e.g. alerts).

    Returns:
        Dictionary keyed by table / metric name with the resulting DataFrames
        produced by the pipeline.
    """
    pipeline = TemplateMonitoringPipeline(env=env, load_env=load_env)

    # Example of dynamic configuration via co-located ``config.yml`` ----------
    config_file = Path(__file__).with_name("config.yml")
    if config_file.exists():
        pipeline.configure_from_filename(str(config_file))

    return pipeline.run(load=load, do_actions=do_actions)

# --------------------------------------------------------------------------------------
# PIPELINE IMPLEMENTATION
# --------------------------------------------------------------------------------------

class TemplateMonitoringPipeline("ConfigPipeline"):  # type: ignore  # Un-comment import
    """Base class for monitoring pipelines.

    Replace ``TemplateMonitoringPipeline`` with a descriptive class name and
    flesh-out/override the *extract*, *transform* and *load* phases according
    to your specific requirements.
    """

    # ------------------------------------------------------------------
    # INITIALISATION
    # ------------------------------------------------------------------
    def __init__(
        self,
        env: "Env",  # type: ignore
        *,
        load_env: Optional["Env"] = None,  # type: ignore
    ) -> None:
        super().__init__(env)
        self.env: "Env" = env  # Runtime env (e.g. PROD)
        self._load_env: "Env" = load_env or env  # Target env for LOAD step

        # Example base API endpoint – override in subclass or config -----------
        self._api_url: str = (
            "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-t"
        )

    # ------------------------------------------------------------------
    # ETL HOOKS
    # ------------------------------------------------------------------
    def extract(self) -> Dict[str, pd.DataFrame]:  # type: ignore[override]
        """Fetch raw data.

        Returns a dictionary of *raw* DataFrames keyed by logical table name.
        The default implementation simply calls ``super().extract()`` which
        relies on *ConfigPipeline*'s declarative extraction rules. Override if
        custom extraction logic is required.
        """
        return super().extract()

    def transform(self, extracted_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:  # noqa: D401,E501  # type: ignore[override]
        """Transform raw data into the *modelled* form expected by downstream consumers."""

        # ------------------------------------------------------------------
        # 1. Normalisation & cleansing
        # ------------------------------------------------------------------
        thresholds_raw = extracted_dfs["thresholds_raw"].iloc[0]
        all_iam_roles = extracted_dfs["all_iam_roles"].iloc[0]
        evaluated_roles = extracted_dfs["evaluated_roles"].iloc[0]

        # ------------------------------------------------------------------
        # 2. Metric calculation ------------------------------------------------
        # ------------------------------------------------------------------
        monitoring_metrics_df = self._calculate_metrics(
            thresholds_raw, all_iam_roles, evaluated_roles
        )

        return {
            "monitoring_metrics": monitoring_metrics_df,
        }

    def load(self, transformed_dfs: Dict[str, pd.DataFrame]) -> None:  # type: ignore[override]
        """Persist the *transformed* data into the configured target environment."""
        original_env = self.env
        try:
            self.env = self._load_env  # Switch env for load phase
            super().load(transformed_dfs)  # Delegate to ConfigPipeline for actual write
        finally:
            self.env = original_env  # Always revert to original env

    # ------------------------------------------------------------------
    # PRIVATE HELPERS
    # ------------------------------------------------------------------
    # API-helpers -------------------------------------------------------
    def _get_api_connector(self) -> "OauthApi":  # type: ignore
        """Return an authenticated :class:`OauthApi` instance.

        Replace this implementation with your organisation's authentication
        mechanism (e.g. STS, OAuth2 client credentials, etc.).
        """
        client_id = self.env.exchange.client_id  # type: ignore[attr-defined]
        client_secret = self.env.exchange.client_secret  # type: ignore[attr-defined]
        exchange_url = self.env.exchange.exchange_url  # type: ignore[attr-defined]

        api_token = "<refresh-your-token-here>"  # TODO: Implement real token fetch

        return OauthApi(  # type: ignore  # noqa: F821 – placeholder
            url=self._api_url,
            api_token=api_token,
            client_id=client_id,
            client_secret=client_secret,
            exchange_url=exchange_url,
        )

    # Metric calculation ------------------------------------------------
    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
        all_iam_roles: pd.DataFrame,
        evaluated_roles: pd.DataFrame,
    ) -> pd.DataFrame:
        """Driver method orchestrating Tier-1 & Tier-2 metric calculations."""

        if thresholds_raw.empty:
            raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")

        api_connector = self._get_api_connector()
        approved_accounts = self._get_approved_accounts(api_connector)

        # Column name normalisation -------------------------------------
        all_iam_roles.columns = all_iam_roles.columns.str.lower()
        evaluated_roles.columns = evaluated_roles.columns.str.lower()
        thresholds_raw.columns = thresholds_raw.columns.str.lower()

        # Filter IAM roles for relevant accounts + machine roles --------
        filtered_roles = (
            all_iam_roles.assign(account=lambda df: df["account"].astype(str).str.strip())
            .query("account in @approved_accounts and role_type.str.lower() == 'machine'", engine="python")
            .copy()
        )

        # Augment with uppercase ARN for joins --------------------------
        filtered_roles["amazon_resource_name_upper"] = (
            filtered_roles.get("amazon_resource_name", "").astype(str).str.upper().str.strip()
        )
        filtered_roles.drop_duplicates(subset=["amazon_resource_name_upper"], inplace=True)

        # ------------------------------------------------------------------
        # Iterate per-control + per-threshold-tier ---------------------------
        # ------------------------------------------------------------------
        results: List[Dict[str, Any]] = []
        now = datetime.utcnow()

        for control_id, control_thresholds in thresholds_raw.groupby("control_id"):
            control_config = next(
                (cfg for cfg in CONTROL_CONFIGS if cfg["ctrl_id"] == control_id),
                None,
            )
            if not control_config:
                # Skip controls not present in CONTROL_CONFIGS (might be retired)
                continue

            cloud_control_id = control_config["cloud_control_id"]

            control_evaluated_roles = (
                evaluated_roles.query("control_id == @cloud_control_id") if not evaluated_roles.empty else pd.DataFrame()
            ).copy()

            control_evaluated_roles["resource_name_upper"] = (
                control_evaluated_roles.get("resource_name", "").astype(str).str.upper().str.strip()
            )

            for _, threshold in control_thresholds.iterrows():
                metric_id = threshold["monitoring_metric_id"]
                metric_name = threshold["metric_name"]
                tier = str(threshold.get("monitoring_metric_tier", "")).lower()

                if "tier 1" in tier or "tier1" in metric_name.lower():
                    (
                        metric_value,
                        compliant_count,
                        total_count,
                        non_compliant_resources,
                    ) = self._calculate_tier1_metrics(filtered_roles, control_evaluated_roles)
                elif "tier 2" in tier or "tier2" in metric_name.lower():
                    (
                        metric_value,
                        compliant_count,
                        total_count,
                        non_compliant_resources,
                    ) = self._calculate_tier2_metrics(filtered_roles, control_evaluated_roles, cloud_control_id)
                else:
                    metric_value = 0.0
                    compliant_count = 0
                    total_count = 0
                    non_compliant_resources = None

                alert_threshold = threshold.get("alerting_threshold", 95.0)
                warning_threshold = threshold.get("warning_threshold", 97.0)

                if warning_threshold is not None and metric_value >= warning_threshold:
                    compliance_status = "Green"
                elif metric_value >= alert_threshold:
                    compliance_status = "Yellow"
                else:
                    compliance_status = "Red"

                results.append(
                    {
                        "metric_run_timestamp": now,
                        "monitoring_metric_id": metric_id,
                        "metric_name": metric_name,
                        "monitoring_metric_value": metric_value,
                        "metric_value_numerator": compliant_count,
                        "metric_value_denominator": total_count,
                        "compliance_status": compliance_status,
                        "non_compliant_resources": non_compliant_resources,
                    }
                )

        result_df = pd.DataFrame(results)
        if not result_df.empty:
            result_df = result_df.astype(
                {
                    "metric_value_numerator": "int64",
                    "metric_value_denominator": "int64",
                    "monitoring_metric_value": "float64",
                }
            )
        return result_df

    # ------------------------------------------------------------------
    # Metric tier helpers ----------------------------------------------
    # ------------------------------------------------------------------
    def _calculate_tier1_metrics(
        self,
        filtered_roles: pd.DataFrame,
        control_evaluations: pd.DataFrame,
    ) -> Tuple[float, int, int, Optional[List[str]]]:
        """Calculate Tier-1 metrics (coverage of evaluated machine roles)."""
        total_count = len(filtered_roles)
        if total_count == 0:
            return 0.0, 0, 0, [json.dumps({"issue": "No machine roles found in approved accounts"})]

        evaluated_arns = set(control_evaluations["resource_name_upper"]) if not control_evaluations.empty else set()
        filtered_arns = set(filtered_roles["amazon_resource_name_upper"])
        evaluated_count = len(filtered_arns & evaluated_arns)

        non_compliant_resources: Optional[List[str]] = None
        if evaluated_count < total_count:
            unevaluated_arns = filtered_arns - evaluated_arns
            if unevaluated_arns:
                limited = sorted(list(unevaluated_arns))[:50]
                non_compliant_resources = [json.dumps({"arn": arn, "issue": "Role not evaluated"}) for arn in limited]
                if len(unevaluated_arns) > 50:
                    non_compliant_resources.append(
                        json.dumps({"message": f"... and {len(unevaluated_arns) - 50} more unevaluated roles"})
                    )

        metric_value = round((evaluated_count / total_count) * 100, 2) if total_count else 0.0
        return metric_value, evaluated_count, total_count, non_compliant_resources

    def _calculate_tier2_metrics(
        self,
        filtered_roles: pd.DataFrame,
        control_evaluations: pd.DataFrame,
        cloud_control_id: str,
    ) -> Tuple[float, int, int, Optional[List[str]]]:
        """Calculate Tier-2 metrics (compliance of evaluated controls)."""
        if control_evaluations.empty:
            return 0.0, 0, 0, [json.dumps({"issue": "No evaluation data available for this control"})]

        merged_evals = control_evaluations.merge(
            filtered_roles[["amazon_resource_name_upper", "account", "amazon_resource_name"]],
            left_on="resource_name_upper",
            right_on="amazon_resource_name_upper",
            how="inner",
        )

        total_count = len(merged_evals)
        if total_count == 0:
            return 0.0, 0, 0, [json.dumps({"issue": "No evaluated in-scope roles for this control"})]

        compliant_evaluations = merged_evals[merged_evals["compliance_status"].isin(["Compliant", "CompliantControlAllowance"])]
        compliant_count = len(compliant_evaluations)
        metric_value = round((compliant_count / total_count) * 100, 2)

        non_compliant_resources: Optional[List[str]] = None
        if compliant_count < total_count:
            non_compliant_evaluations = merged_evals[~merged_evals["compliance_status"].isin(["Compliant", "CompliantControlAllowance"])]
            if not non_compliant_evaluations.empty:
                limited = non_compliant_evaluations.head(50)
                non_compliant_resources = [
                    json.dumps(
                        {
                            "arn": row["amazon_resource_name"],
                            "account": str(row["account"]),
                            "compliance_status": row["compliance_status"],
                            "control_id": cloud_control_id,
                        }
                    )
                    for _, row in limited.iterrows()
                ]
                if len(non_compliant_evaluations) > 50:
                    non_compliant_resources.append(
                        json.dumps({"message": f"... and {len(non_compliant_evaluations) - 50} more non-compliant accounts"})
                    )

        return metric_value, compliant_count, total_count, non_compliant_resources

    # ------------------------------------------------------------------
    # Utility helpers ---------------------------------------------------
    # ------------------------------------------------------------------
    def _get_approved_accounts(self, api_connector: "OauthApi") -> List[str]:  # type: ignore
        """Fetch list of approved accounts via internal API."""
        headers = {
            "Authorization": api_connector.api_token,  # type: ignore[attr-defined]
            "Accept": "application/json;v=2.0",
            "Content-Type": "application/json",
            # Add additional headers such as User-Eid etc. as required.
        }
        params = {
            "accountStatus": "Active",
            "region": [
                "us-east-1",
                "us-east-2",
                "us-west-2",
                "eu-west-1",
                "eu-west-2",
                "ca-central-1",
            ],
        }

        # Example retry parameters -------------------------------------
        response = api_connector.send_request(  # type: ignore[attr-defined]
            url=api_connector.url,  # type: ignore[attr-defined]
            request_type="get",
            request_kwargs={
                "headers": headers,
                "params": params,
                "timeout": 120,
                "verify": True,  # Set to path of CA cert bundle if needed
            },
            retry_delay=5,
            retry_count=5,
        )

        if response.status_code != 200:
            raise RuntimeError(f"API request failed: {response.status_code} – {response.text}")

        data = response.json()
        account_numbers = [
            acc.get("accountNumber", "").strip()
            for acc in data.get("accounts", [])
            if acc.get("accountNumber")
        ]
        if not account_numbers:
            raise ValueError("No valid account numbers received from API")

        return account_numbers