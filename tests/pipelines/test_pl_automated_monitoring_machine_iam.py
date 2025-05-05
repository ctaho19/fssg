import pandas as pd
import pytest
from datetime import datetime
import json
from unittest.mock import patch, MagicMock
import pipelines.pl_automated_monitoring_machine_iam.pipeline as pipeline

# --- Test Setup ---

# Use the same control configs as the pipeline for consistency in tests
CONTROL_CONFIGS = pipeline.CONTROL_CONFIGS

# Expected fields in the output DataFrame based on the Avro schema
AVRO_FIELDS = [
    "date",
    "control_id",
    "monitoring_metric_id",
    "monitoring_metric_value",
    "compliance_status",
    "numerator",
    "denominator",
    "non_compliant_resources",
]

# --- Mock Data Helper Functions ---

def _mock_thresholds_df():
    """Creates a mock DataFrame with monitoring thresholds for different metrics."""
    return pd.DataFrame([
        {"monitoring_metric_id": 1, "alerting_threshold": 95.0, "warning_threshold": 97.0},
        {"monitoring_metric_id": 2, "alerting_threshold": 90.0, "warning_threshold": 95.0},
        {"monitoring_metric_id": 3, "alerting_threshold": 80.0, "warning_threshold": 90.0},
    ])

def _mock_all_iam_roles():
    """Creates a mock DataFrame representing IAM roles, including machine and human roles."""
    return pd.DataFrame([
        {"RESOURCE_ID": "r1", "AMAZON_RESOURCE_NAME": "arn:1", "ROLE_TYPE": "MACHINE"},
        {"RESOURCE_ID": "r2", "AMAZON_RESOURCE_NAME": "arn:2", "ROLE_TYPE": "MACHINE"},
        {"RESOURCE_ID": "r3", "AMAZON_RESOURCE_NAME": "arn:3", "ROLE_TYPE": "MACHINE"},
        {"RESOURCE_ID": "r4", "AMAZON_RESOURCE_NAME": "arn:4", "ROLE_TYPE": "HUMAN"},
    ])

def _mock_evaluated_roles_all():
    """Creates a mock DataFrame representing evaluated roles, all compliant."""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:1", "COMPLIANCE_STATUS": "Compliant"},
        {"RESOURCE_NAME": "arn:2", "COMPLIANCE_STATUS": "Compliant"},
        {"RESOURCE_NAME": "arn:3", "COMPLIANCE_STATUS": "Compliant"},
    ])

def _mock_evaluated_roles_some():
    """Creates a mock DataFrame representing evaluated roles, with some non-compliant."""
    return pd.DataFrame([
        {"RESOURCE_NAME": "arn:1", "COMPLIANCE_STATUS": "Compliant"},
        {"RESOURCE_NAME": "arn:2", "COMPLIANCE_STATUS": "NonCompliant"},
    ])

def _mock_evaluated_roles_none():
    """Creates an empty mock DataFrame representing no evaluated roles."""
    return pd.DataFrame([])

def _mock_evaluated_roles_with_compliance():
    """Creates a mock DataFrame representing evaluated roles with compliance status for tier 3 metrics."""
    return pd.DataFrame([
        {"RESOURCE_ID": "r1", "AMAZON_RESOURCE_NAME": "arn:1", "COMPLIANCE_STATUS": "NonCompliant"},
        {"RESOURCE_ID": "r2", "AMAZON_RESOURCE_NAME": "arn:2", "COMPLIANCE_STATUS": "Compliant"},
        {"RESOURCE_ID": "r3", "AMAZON_RESOURCE_NAME": "arn:3", "COMPLIANCE_STATUS": "NonCompliant"},
    ])

def _mock_sla_data():
    """Creates a mock DataFrame representing SLA data for non-compliant resources."""
    now = pd.Timestamp.utcnow()
    return pd.DataFrame([
        {"RESOURCE_ID": "r1", "CONTROL_RISK": "High", "OPEN_DATE_UTC_TIMESTAMP": now - pd.Timedelta(days=10)},
        {"RESOURCE_ID": "r3", "CONTROL_RISK": "High", "OPEN_DATE_UTC_TIMESTAMP": now - pd.Timedelta(days=40)},
    ])

def _mock_thresholds(ctrl_id, metric_ids):
    """Creates a mock DataFrame with monitoring thresholds for specified control and metric IDs."""
    rows = []
    for tier, metric_id in metric_ids.items():
        rows.append({
            "monitoring_metric_id": metric_id,
            "control_id": ctrl_id,
            "monitoring_metric_tier": f"Tier {tier[-1]}",
            "warning_threshold": 97.0 if tier == "tier1" else 95.0,
            "alerting_threshold": 95.0 if tier == "tier1" else 90.0,
        })
    return pd.DataFrame(rows)

def _mock_iam_roles(n=3):
    """Creates a mock DataFrame with n machine IAM roles and one human role."""
    return pd.DataFrame([
        {"RESOURCE_ID": f"r{i+1}", "AMAZON_RESOURCE_NAME": f"arn:{i+1}", "ROLE_TYPE": "MACHINE"} for i in range(n)
    ] + [{"RESOURCE_ID": "rH", "AMAZON_RESOURCE_NAME": "arn:H", "ROLE_TYPE": "HUMAN"}])

def _mock_evaluated_roles(ctrl_id, compliant=2, noncompliant=1):
    """Creates a mock DataFrame with evaluated roles, specifying compliant and non-compliant counts."""
    rows = []
    for i in range(compliant):
        rows.append({"resource_name": f"arn:{i+1}", "compliance_status": "Compliant", "control_id": ctrl_id})
    for i in range(noncompliant):
        rows.append({"resource_name": f"arn:{compliant+i+1}", "compliance_status": "NonCompliant", "control_id": ctrl_id})
    return pd.DataFrame(rows)

def _mock_sla_data(resource_ids, days_open_list, risk_list):
    """Creates a mock DataFrame with SLA data for non-compliant resources, specifying days open and risk."""
    now = pd.Timestamp.utcnow()
    rows = []
    for rid, days_open, risk in zip(resource_ids, days_open_list, risk_list):
        rows.append({
            "RESOURCE_ID": rid,
            "CONTROL_RISK": risk,
            "OPEN_DATE_UTC_TIMESTAMP": now - pd.Timedelta(days=days_open),
        })
    return pd.DataFrame(rows)

def _mock_empty_df(cols):
    """Creates an empty DataFrame with specified columns."""
    return pd.DataFrame(columns=cols)

# --- Validation Helper ---

def _validate_avro_schema(df):
    """Validates that a DataFrame conforms to the expected Avro schema structure and data types."""
    assert list(df.columns) == AVRO_FIELDS
    assert df["date"].dtype in ["int64", "int32"]
    assert df["control_id"].dtype == object
    assert df["monitoring_metric_id"].dtype in ["int64", "int32"]
    assert df["monitoring_metric_value"].dtype in ["float64", "float32"]
    assert df["compliance_status"].dtype == object
    assert df["numerator"].dtype in ["int64", "int32"]
    assert df["denominator"].dtype in ["int64", "int32"]
    assert all(isinstance(x, (type(None), list)) for x in df["non_compliant_resources"])

# --- Test Cases ---

def test_tier1_all_evaluated():
    """Tests Tier 1 metric calculation when all roles are evaluated and compliant."""
    df = pipeline.calculate_tier1_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_all(),
        ctrl_id="CTRL-1074653",
        tier1_metric_id="T1",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 100.0
    assert df.iloc[0]["compliance_status"] == "Green"
    assert df.iloc[0]["numerator"] == 3
    assert df.iloc[0]["denominator"] == 3

def test_tier1_some_evaluated():
    """Tests Tier 1 metric calculation when some roles are evaluated, with mixed compliance."""
    df = pipeline.calculate_tier1_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_some(),
        ctrl_id="CTRL-1074653",
        tier1_metric_id="T1",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 66.67
    assert df.iloc[0]["compliance_status"] == "Red"
    assert df.iloc[0]["numerator"] == 2
    assert df.iloc[0]["denominator"] == 3

def test_tier1_none_evaluated():
    """Tests Tier 1 metric calculation when no roles are evaluated."""
    df = pipeline.calculate_tier1_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_none(),
        ctrl_id="CTRL-1074653",
        tier1_metric_id="T1",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 0.0
    assert df.iloc[0]["compliance_status"] == "Red"
    assert df.iloc[0]["numerator"] == 0
    assert df.iloc[0]["denominator"] == 3

def test_tier2_all_compliant():
    """Tests Tier 2 metric calculation when all evaluated roles are compliant."""
    df = pipeline.calculate_tier2_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_all(),
        ctrl_id="CTRL-1074653",
        tier2_metric_id="T2",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 100.0
    assert df.iloc[0]["compliance_status"] == "Green"
    assert df.iloc[0]["numerator"] == 3
    assert df.iloc[0]["denominator"] == 3

def test_tier2_some_compliant():
    """Tests Tier 2 metric calculation with a mix of compliant and non-compliant roles."""
    df = pipeline.calculate_tier2_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_some(),
        ctrl_id="CTRL-1074653",
        tier2_metric_id="T2",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 33.33
    assert df.iloc[0]["compliance_status"] == "Red"
    assert df.iloc[0]["numerator"] == 1
    assert df.iloc[0]["denominator"] == 3

def test_tier2_none_compliant():
    """Tests Tier 2 metric calculation when no roles are compliant."""
    df = pipeline.calculate_tier2_metrics(
        thresholds_raw=_mock_thresholds_df(),
        all_iam_roles=_mock_all_iam_roles(),
        evaluated_roles=_mock_evaluated_roles_none(),
        ctrl_id="CTRL-1074653",
        tier2_metric_id="T2",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 0.0
    assert df.iloc[0]["compliance_status"] == "Red"
    assert df.iloc[0]["numerator"] == 0
    assert df.iloc[0]["denominator"] == 3

def test_tier3_within_and_past_sla():
    """Tests Tier 3 metric calculation with non-compliant roles both within and past SLA."""
    df = pipeline.calculate_tier3_metrics(
        thresholds_raw=_mock_thresholds_df(),
        evaluated_roles_with_compliance=_mock_evaluated_roles_with_compliance(),
        sla_data=_mock_sla_data(),
        ctrl_id="CTRL-1074653",
        tier3_metric_id="T3",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 50.0
    assert df.iloc[0]["compliance_status"] == "Red"
    assert df.iloc[0]["numerator"] == 1
    assert df.iloc[0]["denominator"] == 2

def test_tier3_all_within_sla():
    """Tests Tier 3 metric calculation when all non-compliant roles are within SLA."""
    sla_data = _mock_sla_data().copy()
    sla_data.loc[0, "OPEN_DATE_UTC_TIMESTAMP"] = pd.Timestamp.utcnow() - pd.Timedelta(days=5)
    sla_data.loc[1, "OPEN_DATE_UTC_TIMESTAMP"] = pd.Timestamp.utcnow() - pd.Timedelta(days=5)
    df = pipeline.calculate_tier3_metrics(
        thresholds_raw=_mock_thresholds_df(),
        evaluated_roles_with_compliance=_mock_evaluated_roles_with_compliance(),
        sla_data=sla_data,
        ctrl_id="CTRL-1074653",
        tier3_metric_id="T3",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 100.0
    assert df.iloc[0]["compliance_status"] == "Green"
    assert df.iloc[0]["numerator"] == 2
    assert df.iloc[0]["denominator"] == 2

def test_tier3_none_noncompliant():
    """Tests Tier 3 metric calculation when there are no non-compliant roles."""
    df = pipeline.calculate_tier3_metrics(
        thresholds_raw=_mock_thresholds_df(),
        evaluated_roles_with_compliance=pd.DataFrame([
            {"RESOURCE_ID": "r2", "AMAZON_RESOURCE_NAME": "arn:2", "COMPLIANCE_STATUS": "Compliant"},
        ]),
        sla_data=_mock_sla_data(),
        ctrl_id="CTRL-1074653",
        tier3_metric_id="T3",
    )
    assert df.iloc[0]["monitoring_metric_value"] == 100.0
    assert df.iloc[0]["compliance_status"] == "Green"
    assert df.iloc[0]["numerator"] == 0
    assert df.iloc[0]["denominator"] == 0

def test_tier1_tier2_all_compliant():
    """Tests combined Tier 1 and Tier 2 calculations when all evaluated roles are compliant."""
    ctrl = CONTROL_CONFIGS[0]
    thresholds = _mock_thresholds(ctrl["ctrl_id"], ctrl["metric_ids"])
    iam_roles = _mock_iam_roles(3)
    evaluated_roles = _mock_evaluated_roles(ctrl["cloud_control_id"], compliant=3, noncompliant=0)
    df = pipeline.calculate_machine_iam_metrics(
        thresholds_raw=thresholds,
        iam_roles=iam_roles,
        evaluated_roles=evaluated_roles,
        ctrl_id=ctrl["ctrl_id"],
        metric_ids=ctrl["metric_ids"],
        requires_tier3=ctrl["requires_tier3"],
        sla_data=_mock_empty_df(["RESOURCE_ID", "CONTROL_RISK", "OPEN_DATE_UTC_TIMESTAMP"]),
    )
    _validate_avro_schema(df)
    assert all(df["compliance_status"] == "Green")
    assert all(df["monitoring_metric_value"] == 100.0)
    assert all(x is None for x in df["non_compliant_resources"])

def test_tier1_tier2_some_noncompliant():
    """Tests combined Tier 1 and Tier 2 calculations with a mix of compliant and non-compliant roles."""
    ctrl = CONTROL_CONFIGS[0]
    thresholds = _mock_thresholds(ctrl["ctrl_id"], ctrl["metric_ids"])
    iam_roles = _mock_iam_roles(3)
    evaluated_roles = _mock_evaluated_roles(ctrl["cloud_control_id"], compliant=2, noncompliant=1)
    df = pipeline.calculate_machine_iam_metrics(
        thresholds_raw=thresholds,
        iam_roles=iam_roles,
        evaluated_roles=evaluated_roles,
        ctrl_id=ctrl["ctrl_id"],
        metric_ids=ctrl["metric_ids"],
        requires_tier3=ctrl["requires_tier3"],
        sla_data=_mock_empty_df(["RESOURCE_ID", "CONTROL_RISK", "OPEN_DATE_UTC_TIMESTAMP"]),
    )
    _validate_avro_schema(df)
    assert df.iloc[0]["compliance_status"] == "Red"  # Tier 1 (all roles evaluated)
    assert df.iloc[1]["compliance_status"] == "Red"  # Tier 2 (some non-compliant)
    assert isinstance(df.iloc[0]["non_compliant_resources"], list)
    assert isinstance(df.iloc[1]["non_compliant_resources"], list)

def test_tier3_within_and_past_sla():
    """Tests Tier 3 calculation with non-compliant roles both within and past SLA in a full pipeline run."""
    ctrl = CONTROL_CONFIGS[0]
    thresholds = _mock_thresholds(ctrl["ctrl_id"], ctrl["metric_ids"])
    iam_roles = _mock_iam_roles(3)
    evaluated_roles = _mock_evaluated_roles(ctrl["cloud_control_id"], compliant=1, noncompliant=2)
    sla_data = _mock_sla_data(["r2", "r3"], [10, 40], ["High", "High"])
    df = pipeline.calculate_machine_iam_metrics(
        thresholds_raw=thresholds,
        iam_roles=iam_roles,
        evaluated_roles=evaluated_roles,
        ctrl_id=ctrl["ctrl_id"],
        metric_ids=ctrl["metric_ids"],
        requires_tier3=ctrl["requires_tier3"],
        sla_data=sla_data,
    )
    _validate_avro_schema(df)
    assert df.iloc[2]["monitoring_metric_value"] == 50.0
    assert df.iloc[2]["compliance_status"] == "Red"
    assert isinstance(df.iloc[2]["non_compliant_resources"], list)
    for ev in df.iloc[2]["non_compliant_resources"]:
        ev_dict = json.loads(ev)
        assert "SLA_STATUS" in ev_dict

def test_tier3_all_within_sla():
    """Tests Tier 3 calculation when all non-compliant roles are within SLA in a full pipeline run."""
    ctrl = CONTROL_CONFIGS[0]
    thresholds = _mock_thresholds(ctrl["ctrl_id"], ctrl["metric_ids"])
    iam_roles = _mock_iam_roles(3)
    evaluated_roles = _mock_evaluated_roles(ctrl["cloud_control_id"], compliant=1, noncompliant=2)
    sla_data = _mock_sla_data(["r2", "r3"], [5, 5], ["High", "High"])
    df = pipeline.calculate_machine_iam_metrics(
        thresholds_raw=thresholds,
        iam_roles=iam_roles,
        evaluated_roles=evaluated_roles,
        ctrl_id=ctrl["ctrl_id"],
        metric_ids=ctrl["metric_ids"],
        requires_tier3=ctrl["requires_tier3"],
        sla_data=sla_data,
    )
    _validate_avro_schema(df)
    assert df.iloc[2]["monitoring_metric_value"] == 100.0
    assert df.iloc[2]["compliance_status"] == "Green"
    assert df.iloc[2]["non_compliant_resources"] is None

def test_tier3_none_noncompliant():
    """Tests Tier 3 calculation when there are no non-compliant roles in a full pipeline run."""
    ctrl = CONTROL_CONFIGS[0]
    thresholds = _mock_thresholds(ctrl["ctrl_id"], ctrl["metric_ids"])
    iam_roles = _mock_iam_roles(3)
    evaluated_roles = _mock_evaluated_roles(ctrl["cloud_control_id"], compliant=3, noncompliant=0)
    sla_data = _mock_empty_df(["RESOURCE_ID", "CONTROL_RISK", "OPEN_DATE_UTC_TIMESTAMP"])
    df = pipeline.calculate_machine_iam_metrics(
        thresholds_raw=thresholds,
        iam_roles=iam_roles,
        evaluated_roles=evaluated_roles,
        ctrl_id=ctrl["ctrl_id"],
        metric_ids=ctrl["metric_ids"],
        requires_tier3=ctrl["requires_tier3"],
        sla_data=sla_data,
    )
    _validate_avro_schema(df)
    assert df.iloc[2]["monitoring_metric_value"] == 100.0
    assert df.iloc[2]["compliance_status"] == "Green"
    assert df.iloc[2]["non_compliant_resources"] is None

def test_pipeline_init_success():
    """Tests that pipeline initialization succeeds with proper environment configuration."""
    class MockSnowflake:
        def __init__(self):
            self.query = lambda sql: pd.DataFrame()
    
    class MockExchange:
        def __init__(self):
            self.client_id = "test_id"
            self.client_secret = "test_secret"
            self.exchange_url = "test_url"
    
    class MockEnv:
        def __init__(self):
            self.snowflake = MockSnowflake()
            self.exchange = MockExchange()
    
    env = MockEnv()
    pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
    assert pipe is not None
    assert pipe.env.snowflake is not None
    assert pipe.env.exchange is not None

def test_pipeline_init_missing_oauth_config():
    """Tests that pipeline initialization fails with missing OAuth config."""
    class MockSnowflake:
        def __init__(self):
            self.query = lambda sql: pd.DataFrame()
    
    class MockEnv:
        def __init__(self):
            self.snowflake = MockSnowflake()
            # Missing exchange configuration
    
    env = MockEnv()
    with pytest.raises(AttributeError, match="'MockEnv' object has no attribute 'exchange'"):
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)

def test_get_api_token_success(mocker):
    """Tests that API token refresh succeeds."""
    mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh")
    mock_refresh.return_value = "test_token"
    
    class MockSnowflake:
        def __init__(self):
            self.query = lambda sql: pd.DataFrame()
    
    class MockExchange:
        def __init__(self):
            self.client_id = "test_id"
            self.client_secret = "test_secret"
            self.exchange_url = "test_url"
    
    class MockEnv:
        def __init__(self):
            self.snowflake = MockSnowflake()
            self.exchange = MockExchange()
    
    env = MockEnv()
    pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
    token = pipe._get_api_token()
    assert token == "Bearer test_token"
    mock_refresh.assert_called_once_with(
        client_id="test_id",
        client_secret="test_secret",
        exchange_url="test_url"
    )

def test_get_api_token_failure(mocker):
    """Tests that API token refresh failure is handled properly."""
    mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.refresh")
    mock_refresh.side_effect = Exception("Test error")
    
    class MockSnowflake:
        def __init__(self):
            self.query = lambda sql: pd.DataFrame()
    
    class MockExchange:
        def __init__(self):
            self.client_id = "test_id"
            self.client_secret = "test_secret"
            self.exchange_url = "test_url"
    
    class MockEnv:
        def __init__(self):
            self.snowflake = MockSnowflake()
            self.exchange = MockExchange()
    
    env = MockEnv()
    pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
    with pytest.raises(RuntimeError, match="API token refresh failed"):
        pipe._get_api_token()

def test_read_sql_file(mocker):
    """Tests the SQL file reading function."""
    # Mock open function to return a simple SQL file
    mock_open = mocker.patch("builtins.open", mocker.mock_open(read_data="SELECT * FROM test WHERE id = %(control_id)s"))
    # Mock Path to ensure it finds the right file
    mock_path = mocker.patch("pathlib.Path.__truediv__", return_value="/path/to/sql/monitoring_thresholds.sql")
    
    result = pipeline.read_sql_file("monitoring_thresholds.sql")
    mock_open.assert_called_once()
    assert "SELECT * FROM test WHERE id = %(control_id)s" == result

def test_parameterize_sql():
    """Tests the SQL parameter substitution function."""
    sql = "SELECT * FROM test WHERE id = %(control_id)s"
    params = {"control_id": "CTRL-1234"}
    result = pipeline.parameterize_sql(sql, params)
    assert "SELECT * FROM test WHERE id = 'CTRL-1234'" == result

def test_run_entrypoint(mocker):
    """Tests the run entry point function."""
    mock_pipeline_class = mocker.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.PLAutomatedMonitoringMachineIAM")
    mock_pipeline_instance = mock_pipeline_class.return_value
    mock_pipeline_instance.run.return_value = None
    
    # Mock path exists and open for configure_from_filename
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("builtins.open", mocker.mock_open(read_data="pipeline:\n  name: test"))
    
    from etip_env import Env
    env = Env()
    
    # Test default parameters
    pipeline.run(env)
    mock_pipeline_class.assert_called_once_with(env)
    mock_pipeline_instance.run.assert_called_once()
    
    # Reset mocks
    mock_pipeline_class.reset_mock()
    mock_pipeline_instance.run.reset_mock()
    
    # Test with export_test_data=True
    pipeline.run(env, is_export_test_data=True)
    mock_pipeline_class.assert_called_once_with(env)
    mock_pipeline_instance.run_test_data_export.assert_called_once()

def test_pipeline_transform(mocker):
    """Tests the main pipeline transform stage by mocking database calls to simulate a full run."""
    class MockSnowflake:
        """Mocks the Snowflake connector query method to return mock data based on query type."""
        def query(self, sql):
            if "monitoring_thresholds" in sql:
                return _mock_thresholds("CTRL-1074653", {"tier1": 1, "tier2": 2, "tier3": 3})
            elif "iam_roles" in sql:
                return _mock_iam_roles(3)
            elif "evaluated_roles" in sql:
                return _mock_evaluated_roles("AC-3.AWS.39.v02", compliant=2, noncompliant=1)
            elif "sla_data" in sql or "OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01" in sql:
                return _mock_sla_data(["r3"], [10], ["High"])
            return pd.DataFrame()

    class MockEnv:
        """Mocks the environment object with a Snowflake connector and exchange configuration."""
        def __init__(self):
            self.snowflake = MockSnowflake()
            self.exchange = MagicMock()
            self.exchange.client_id = "id"
            self.exchange.client_secret = "secret"
            self.exchange.exchange_url = "url"

    # Mock SQL file reading
    mocker.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.read_sql_file", return_value="SELECT * FROM test")
    mocker.patch("pipelines.pl_automated_monitoring_machine_iam.pipeline.parameterize_sql", return_value="SELECT * FROM test WHERE id = 'CTRL-1074653'")
    
    # Use only the first control config for the test
    mocker.patch.object(pipeline, "CONTROL_CONFIGS", [CONTROL_CONFIGS[0]])
    
    env = MockEnv()
    with patch.object(pipeline.PLAutomatedMonitoringMachineIAM, "_execute_sql", side_effect=MockSnowflake().query) as mock_sql:
        pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
        pipe.transform()
        assert mock_sql.call_count >= 3
        df = pipe.context["monitoring_metrics"]
        _validate_avro_schema(df)
        assert not df.empty
        assert len(df) == 3