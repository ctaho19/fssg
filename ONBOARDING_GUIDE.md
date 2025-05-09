# Manual Onboarding Guide: Script to Standard ETIP Pipeline

This guide details the step-by-step process for manually converting an existing Python script (that calculates control monitoring metrics) into the standardized ETIP Config Pipeline structure, ensuring adherence to required formats and conventions.

**Goal:** To create a pipeline folder (e.g., `pl_automated_monitoring_CTRL_XXXXXX`) from a script (e.g., `CTRL_XXXXXX.py`), mirroring the structure and standards of reference pipelines like `pl_automated_monitoring_cloud_custodian`.

**Prerequisites:**

1.  **Existing Metric Script:** Your Python script (`.py` or Databricks notebook export) that successfully fetches data, calculates metrics, and produces the desired output (even if not perfectly formatted yet).
2.  **Reference ETIP Pipeline:** Access to a standard, working ETIP pipeline folder (e.g., `pl_automated_monitoring_cloud_custodian`) to use as a template for structure and conventions.
3.  **ETIP Development Environment:** Your local environment configured for ETIP pipeline development (including necessary libraries like `pandas`, `requests`, and the core ETIP/AxDP framework, `pipenv` setup, etc.).
4.  **Control Information:** The specific Control ID (e.g., `CTRL-1077231`) and associated Monitoring Metric IDs (e.g., `MNTR-1077231-T1`, `MNTR-1077231-T2`).

---

**Steps:**

**Step 1: Create Pipeline Folder Structure**

Create the necessary directories for your new control pipeline. Replace `CTRL_XXXXXX` with your specific control ID.

```bash
mkdir pl_automated_monitoring_CTRL_XXXXXX
mkdir pl_automated_monitoring_CTRL_XXXXXX/sql
```

**Step 2: Define Output Schema (`avro_schema.json`)**

The final output of all automated monitoring pipelines must conform to a single, predefined Avro schema used by the target Snowflake table (`etip_controls_monitoring_metrics`).

1.  **Copy the Standard Schema:** Locate the `avro_schema.json` file within the *reference* pipeline folder (e.g., `pl_automated_monitoring_cloud_custodian/avro_schema.json`).
2.  **Paste Content:** Copy the *entire content* of the reference schema file and paste it into your new pipeline's `pl_automated_monitoring_CTRL_XXXXXX/avro_schema.json` file.
    *   **Do NOT modify this file.** It defines the mandatory output structure your `transform.py` must produce.

**Step 3: Prepare Threshold SQL (`sql/monitoring_thresholds.sql`)**

Thresholds (Alerting, Warning) must be loaded from the standardized ETIP monitoring details table, not hardcoded or loaded from other tables within your transform logic.

1.  **Copy Standard SQL:** Locate the `monitoring_thresholds.sql` file in the *reference* pipeline's `sql/` directory (e.g., `pl_automated_monitoring_cloud_custodian/sql/monitoring_thresholds.sql`).
2.  **Paste and Modify Filter:** Copy the content into your new `pl_automated_monitoring_CTRL_XXXXXX/sql/monitoring_thresholds.sql`. Modify *only* the `WHERE` clause at the bottom to filter for the specific `monitoring_metric_id` values relevant to your control.

    *Example Modification (for CTRL-1077231):*
    ```sql
    -- sqlfluff:dialect:snowflake
    -- sqlfluff:templater:placeholder:param_style:pyformat
    select
        monitoring_metric_id,
        control_id,
        monitoring_metric_tier,
        metric_name,
        metric_description,
        warning_threshold,
        alerting_threshold,
        control_executor,
        metric_threshold_start_date,
        metric_threshold_end_date
    from
        etip_db.phdp_etip_controls_monitoring.etip_controls_monitoring_metrics_details
    where
        metric_threshold_end_date is Null
        -- *** MODIFY THIS LINE ***
        and monitoring_metric_id in ('MNTR-XXXXXX-T1', 'MNTR-XXXXXX-T2') -- Replace with your metric IDs
        -- or potentially filter by control_id if more appropriate:
        -- and control_id = 'CTRL_XXXXXX' -- Replace with your control ID
    ```

**Step 4: Configure the Pipeline (`config.yml`)**

This file defines the pipeline's workflow using the ETIP framework's stage-based configuration.

1.  **Copy Reference Config:** Copy the `config.yml` from the *reference* pipeline (e.g., `pl_automated_monitoring_cloud_custodian/config.yml`) into your `pl_automated_monitoring_CTRL_XXXXXX/config.yml` as a starting template.
2.  **Modify `pipeline` Section:**
    *   Update `name:` to your pipeline's name (e.g., `pl_automated_monitoring_CTRL_XXXXXX`).
    *   Update `description:` accurately.
    *   Adjust `tags:` as needed.
3.  **Modify `stages` Section:**
    *   **`extract` Stage:**
        *   Ensure the `threshold_df` entry points to your SQL file using the correct syntax: `sql: "@text:sql/monitoring_thresholds.sql"`.
        *   Remove any explicit Snowflake connection details here; the framework typically handles connections based on central configuration or the `pipeline.py` setup.
        *   If your *original script* extracted data from other sources (like Snowflake tables *besides* thresholds), define additional extract steps here, referencing necessary SQL files or connector options. *However, note that API calls are often handled within the `transform` stage.*
    *   **`ingress_validation` Stage (Optional but Recommended):**
        *   Adapt validation steps from the reference for `threshold_df` (e.g., `count_check` to ensure thresholds loaded).
        *   Add checks for any other data sources extracted.
    *   **`transform` Stage:**
        *   Define the primary transformation step. This step will execute the main Python logic you'll write in `transform.py`.
        *   Give the output DataFrame a meaningful name (e.g., `monitoring_metrics_df`).
        *   Set the `function:` key to point to the custom transform function you will create in `transform.py`. Use the `custom/` prefix convention (e.g., `function: custom/calculate_ctrlXXXXXX_metrics`).
        *   In `options:`, pass the required inputs to your transform function. Crucially, pass the thresholds: `threshold_df: $threshold_df`. If you extracted other DataFrames, pass them similarly (e.g., `other_data: $other_data_df`).
    *   **`consistency_checks` Stage (Optional):**
        *   Add checks if needed to compare record counts or schemas between stages.
    *   **`load` Stage:**
        *   Configure the loading of the final DataFrame (e.g., `monitoring_metrics_df`).
        *   Use `connector: onestream`.
        *   Set `table_name:` to the standard target table (usually `etip_controls_monitoring_metrics`).
        *   Set `business_application:` (e.g., `BAENTERPRISETECHINSIGHTS`).
        *   Set `file_type: AVRO`.
        *   Reference your schema file correctly: `avro_schema: "@json:avro_schema.json"`.
        *   Add `write_mode: append` (or `overwrite` if appropriate).

**Step 5: Refactor Core Logic (`transform.py`)**

This is where the bulk of your original script's logic is adapted.

1.  **Create File:** Create `pl_automated_monitoring_CTRL_XXXXXX/transform.py`.
2.  **Copy Functions:** Copy the relevant Python functions from your original script into this new file (e.g., API call functions, data processing functions, helper functions).
3.  **Apply Refactoring Principles:**
    *   **Remove Hardcoded Config/Secrets:** Delete any hardcoded API keys, tokens, URLs, file paths, control IDs, metric IDs, etc. These should be passed in via the `context` object from `pipeline.py` or potentially as arguments defined in `config.yml`. Define constants for default values at the top of the file if helpful (e.g., `DEFAULT_CTRL_ID = "CTRL-XXXXXX"`).
    *   **Adapt API Functions:**
        *   Modify functions that call external APIs (like `fetch_all_resources`) to accept necessary parameters like `api_url`, `auth_token`, and SSL verification settings (`verify_ssl`) as arguments.
        *   Remove any code within these functions that *generates* the auth token or hardcodes headers with tokens. They should use the passed-in token.
    *   **Remove Threshold Loading:** Delete any function that directly queries Snowflake (or other sources) to get alert/warning thresholds. This data will now be passed in as the `threshold_df` DataFrame.
    *   **Remove Direct Output Writing:** Delete any code that writes the final results DataFrame to Snowflake or any other destination. This is handled by the `load` stage in `config.yml`.
    *   **Create Main Transform Function:**
        *   Define the single, primary function that `config.yml` calls (e.g., `calculate_ctrlXXXXXX_metrics`).
        *   This function *must* accept the `threshold_df` DataFrame and the pipeline `context` dictionary as arguments: `def calculate_ctrlXXXXXX_metrics(threshold_df: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:`.
        *   Inside this function, orchestrate the logic:
            *   Retrieve API credentials, URLs, and other needed config from the `context` dictionary (e.g., `api_token = context['api_auth_token']`).
            *   Call your refactored API functions (passing the retrieved credentials/config).
            *   Call your data processing/filtering functions.
            *   Use the *passed* `threshold_df` to get threshold values, referencing the standard column names (`alerting_threshold`, `warning_threshold`, `monitoring_metric_id`).
            *   **Crucially:** Construct the final pandas DataFrame to be loaded.
    *   **Format Output DataFrame:** The DataFrame *returned* by your main transform function *must exactly match* the structure defined in `avro_schema.json`.
        *   Ensure column names are identical (`monitoring_metric_id`, `control_id`, `monitoring_metric_value`, `monitoring_metric_status`, `metric_value_numerator`, `metric_value_denominator`, `resources_info`, `control_monitoring_utc_timestamp`).
        *   Ensure data types match the Avro schema (use `.astype()`):
            *   `monitoring_metric_id`: int
            *   `control_id`: string
            *   `monitoring_metric_value`: float
            *   `monitoring_metric_status`: string (nullable)
            *   `metric_value_numerator`: int
            *   `metric_value_denominator`: int
            *   `resources_info`: list of strings (JSON strings) or None (handle nullability)
            *   `control_monitoring_utc_timestamp`: long (milliseconds since epoch)
        *   Generate the timestamp using `int(datetime.now(timezone.utc).timestamp() * 1000)`.
        *   Format the `resources_info` field as a list of JSON strings for any non-compliant resources, or `None` if there are none.
    *   **Register Transform Function:** Add the appropriate decorator above your main transform function (e.g., `@transform_function("custom/calculate_ctrlXXXXXX_metrics")`) so the framework recognizes it. (The exact decorator syntax might vary slightly based on your specific ETIP framework version).

**Step 6: Implement the Orchestrator (`pipeline.py`)**

This script sets up the pipeline execution environment, handles authentication, and runs the process defined in `config.yml`.

1.  **Copy Reference Pipeline:** Copy the `pipeline.py` from the *reference* pipeline into your `pl_automated_monitoring_CTRL_XXXXXX/pipeline.py`.
2.  **Update Imports:** Ensure the import near the top correctly references your *own* transform module: `import pipelines.pl_automated_monitoring_CTRL_XXXXXX.transform # noqa: F401`.
3.  **Update Class Name:** Rename the pipeline class inheriting from `ConfigPipeline` (e.g., `class PLAmCtrlXXXXXXPipeline(ConfigPipeline):`).
4.  **Update `__init__`:**
    *   Modify the API URL(s) (`self.cloud_tooling_api_url` or similar) if they differ from the reference.
    *   Ensure it correctly reads necessary secrets (like `client_id`, `client_secret` for API auth) from the passed `env` object, matching how the reference pipeline does it.
5.  **Verify Auth (`_get_api_token`):** Ensure the logic for refreshing the OAuth token matches the requirements for the specific API your script uses.
6.  **Verify Context Injection (`transform` method):**
    *   The reference pipeline likely overrides the base `transform` method to inject context *before* calling `super().transform()`.
    *   Ensure this overridden method correctly puts the necessary items into `self.context` (e.g., `self.context['api_auth_token'] = self._get_api_token()`, `self.context['cloud_tooling_api_url'] = self.cloud_tooling_api_url`). This is how your `transform.py` function receives API credentials and config.
7.  **Update `run` Function:** Ensure the main `run` function instantiates *your* pipeline class (e.g., `pipeline = PLAmCtrlXXXXXXPipeline(env)`).
8.  **Update Local Testing Block (`if __name__ == "__main__":`)**:
    *   Update mock secrets or environment variable names if needed for local testing.
    *   Ensure it calls `run` with the mock environment.

**Step 7: Final Review**

*   **Consistency:** Double-check all file names, paths, function names, config references (`$threshold_df`, `@text:`, `@json:`), and class names are consistent between `config.yml`, `pipeline.py`, and `transform.py`.
*   **Schema:** Confirm the final DataFrame produced by `transform.py` strictly adheres to `avro_schema.json`.
*   **Auth:** Verify the authentication flow in `pipeline.py` is correct and securely handles credentials.
*   **Thresholds:** Ensure `sql/monitoring_thresholds.sql` uses the correct table and filter, and `transform.py` uses the resulting DataFrame correctly.
*   **Dependencies:** Make sure any libraries used in `transform.py` (e.g., `requests`, `pandas`) are standard ETIP dependencies or added appropriately.

---

## Running Unit Tests

Unit tests are crucial for verifying the core logic of your pipeline, specifically the transformations, without relying on external systems like live APIs or Snowflake connections.

**Purpose:** To test the Python functions within your `transform.py` file in isolation. You want to ensure that given specific inputs (mocked API responses, sample threshold data), your functions produce the expected output DataFrame with correct calculations and compliance statuses.

**Location & Naming:**

*   Tests reside in the main `tests/pipelines/` directory of the repository.
*   The test file should be named following the pattern `test_[pipeline_name].py` (e.g., `tests/pipelines/test_pl_automated_monitoring_CTRL_XXXXXX.py`).

**What to Test:**

1.  **Main Transform Function:** Focus testing on the primary function called by `config.yml` (e.g., `calculate_ctrlXXXXXX_metrics` in `transform.py`).
2.  **Mock Inputs:**
    *   Create sample `pandas` DataFrames mimicking the structure of `threshold_df` with known threshold values.
    *   Create a mock `context` dictionary containing plausible (but potentially fake) values for API tokens, URLs, etc.
    *   If your transform calls API functions (`fetch_all_resources`), you'll need to *mock* these calls using `unittest.mock.patch` to prevent actual network requests. The mocked function should return a predefined list of resource dictionaries representing different scenarios (e.g., compliant, non-compliant T1, non-compliant T2).
3.  **Call the Function:** Invoke your main transform function with the mocked inputs (threshold DataFrame, context dictionary).
4.  **Assert Outputs:**
    *   Verify the structure (column names, data types) of the returned DataFrame matches the Avro schema.
    *   Check the number of rows is correct (usually 2, one for T1, one for T2).
    *   Assert that the calculated `monitoring_metric_value`, `monitoring_metric_status`, `metric_value_numerator`, and `metric_value_denominator` are correct based on your mocked input data and thresholds.
    *   Check the content and format of the `resources_info` column.

**Running the Tests:**

You typically run tests using `pytest` via `pipenv`. Navigate to the root directory of the `etip-data-pipelines` repository in your terminal and run:

```bash
pipenv run pytest ./tests/pipelines/test_pl_automated_monitoring_CTRL_XXXXXX.py
```

(Replace `test_pl_automated_monitoring_CTRL_XXXXXX.py` with the actual name of your test file).

This command executes the tests within the virtual environment managed by `pipenv`, ensuring all necessary dependencies are available. Successful execution with passing tests is required before merging your pipeline code. 