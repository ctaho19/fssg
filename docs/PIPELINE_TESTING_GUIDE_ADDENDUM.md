# Pipeline Testing Guide Addendum: Recent Fixes

## JSON Serialization Issues with Pandas Timestamp Objects

**Problem:**
When working with pandas Timestamp objects in tests, you may encounter JSON serialization errors:
```
TypeError: Object of type Timestamp is not JSON serializable
```

This typically occurs when trying to convert a DataFrame with Timestamp objects to JSON format, especially in functions that format data for evidence or non-compliant resources.

**Solution:**
1. **Convert Timestamps to Strings Before JSON Serialization**:
   ```python
   # In format_non_compliant_resources function
   def format_non_compliant_resources(resources_df: pd.DataFrame) -> Optional[List[str]]:
       """Format non-compliant resources as JSON strings for reporting."""
       if resources_df is None or resources_df.empty:
           return None
       
       # Convert DataFrame to dictionary records with proper timestamp handling
       records = []
       for _, row in resources_df.iterrows():
           # Convert row to dict and handle timestamp objects
           row_dict = {}
           for col, val in row.items():
               # Convert timestamp objects to ISO format strings
               if isinstance(val, (pd.Timestamp, datetime.datetime)):
                   row_dict[col] = val.isoformat()
               else:
                   row_dict[col] = val
           records.append(json.dumps(row_dict))
       
       return records
   ```

2. **Use Custom JSON Encoders**:
   ```python
   class TimestampEncoder(json.JSONEncoder):
       """Custom JSON encoder for handling pandas Timestamp objects."""
       def default(self, obj):
           if isinstance(obj, (pd.Timestamp, datetime.datetime)):
               return obj.isoformat()
           return super().default(obj)
   
   # Use in your format_non_compliant_resources function
   def format_non_compliant_resources(resources_df: pd.DataFrame) -> Optional[List[str]]:
       if resources_df is None or resources_df.empty:
           return None
       
       return [json.dumps(row.to_dict(), cls=TimestampEncoder) for _, row in resources_df.iterrows()]
   ```

3. **Pre-process DataFrames Before Testing Functions that Use JSON**:
   ```python
   # In test setup
   def prepare_test_dataframe():
       df = pd.DataFrame({
           "timestamp_column": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
           "other_column": ["value1", "value2"]
       })
       
       # Convert timestamp columns to strings before passing to functions that use JSON
       df["timestamp_column"] = df["timestamp_column"].dt.strftime("%Y-%m-%dT%H:%M:%S")
       return df
   ```

**Best Practices:**
- Always be aware of data types in your DataFrames, especially when they contain datetime objects
- Consider implementing a custom JSON encoder in your pipeline's utilities
- When testing functions that perform JSON serialization, convert timestamp columns to strings first
- Add defensive programming to handle JSON serialization errors by catching and logging specific exceptions
- Remember that pandas Timestamp objects, numpy arrays, and other non-native Python types need special handling for JSON serialization

## Pipeline Context Initialization Issues

**Problem:**
Tests may fail with errors like:
```
AttributeError: 'PLAutomatedMonitoringMachineIAM' object has no attribute 'context'
```

This happens when test code assumes the pipeline object already has an initialized context attribute, but it hasn't been created yet.

**Solution:**
1. **Always Initialize the Context Before Using It**:
   ```python
   # Before using pipe.context in a test
   pipe = pipeline.PLAutomatedMonitoringMachineIAM(env)
   
   # Initialize context if it might not exist
   if not hasattr(pipe, 'context'):
       pipe.context = {}
   
   # Now safely use the context
   pipe.context["api_connector"] = api_connector
   ```

2. **Add Context Initialization to the Pipeline's __init__ Method**:
   ```python
   def __init__(self, env: Env) -> None:
       super().__init__(env)
       self.env = env
       self.context = {}  # Initialize empty context
       # ... rest of initialization
   ```

## Mock Fixture Naming Convention

When using pytest's mock fixture, always use the parameter name `mock` instead of `mocker`. This is a standardization across our codebase and is required for compatibility with our production environment. The `mocker` fixture is not supported in the production environment, and tests using it will fail when run in production:

```python
# Correct usage:
def test_function(mock):
    mock_api = mock.patch("module.api_call")
    mock_api.return_value = "test"

# Incorrect usage - will fail in production:
def test_function(mocker):  # Don't use 'mocker'
    mock_api = mocker.patch("module.api_call")
    mock_api.return_value = "test"
```

It's critical to search for and replace all instances of `mocker` with `mock` in test files before deployment to production environments. Pay special attention to:

1. Function parameter names: `def test_function(mock)` not `def test_function(mocker)`
2. Mocking method calls: `mock.patch()` not `mocker.patch()`
3. Mock creation helpers: `mock.mock_open()` not `mocker.mock_open()`

## Mock Object Side Effects: Proper Configuration

When using `side_effect` with mock objects to simulate exceptions followed by successful responses, there's a common error pattern that can cause test failures:

### Problem: TypeError: 'list_iterator' object is not subscriptable

```python
# Problematic code - don't do this
mock_request.side_effect = [Exception("Network error"), mock.Mock()]
mock_request.side_effect[1].status_code = 200  # TypeError - list_iterator not subscriptable
mock_request.side_effect[1].ok = True
```

This code fails because `side_effect` doesn't work like a regular list - you can't modify individual items by index after setting the `side_effect`. When a mock object with `side_effect` is called, it consumes one item from the list each time.

### Solution: Create Mock Objects First, Then Configure Side Effect

```python
# Create a mock response object first
mock_response = mock.Mock()
mock_response.status_code = 200
mock_response.ok = True

# Then set the side_effect to first raise an exception, then return our already-configured mock
mock_request.side_effect = [Exception("Network error"), mock_response]
```

This approach ensures the mock response is properly configured before it's added to the side_effect list.

## Solving Main Module Import Issues in Tests

When testing code that runs in the `if __name__ == "__main__"` block or imports modules dynamically, you might encounter errors like:

```
AttributeError: module has no attribute 'set_env_vars'
```

### Problem: Module Import Issues in Dynamic Code Execution

When using `exec()` to test main function execution paths, imports within the executed code may not resolve correctly:

```python
# Problematic code - don't do this
mock_set_env = mock.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.set_env_vars")
...
code = """
if True:
    from pipelines.pl_automated_monitoring_ctrl_1077231.pipeline import set_env_vars, run, logger
    import sys
    
    env = set_env_vars()  # AttributeError: module has no attribute 'set_env_vars'
    ...
"""
exec(code)
```

### Solution: Import from the Original Module, Not the Pipeline Module

```python
# Create mock objects for the original module - do this instead
mock_env = mock.Mock()
mock.patch("etip_env.set_env_vars", return_value=mock_env)
...
code = """
if True:
    import sys
    from etip_env import set_env_vars  # Import from original module
    from pipelines.pl_automated_monitoring_ctrl_1077231.pipeline import run, logger
    
    env = set_env_vars()  # Now correctly uses our mocked function
    ...
"""
exec(code)
```

By importing from the original source module rather than re-importing through the pipeline module, we ensure that our mocks properly intercept all function calls. Also note that we're not storing the mock in a variable (`mock_set_env`) since we don't need to reference it later, avoiding unused variable warnings.

## Integration with Existing Documentation

These patterns complement the best practices outlined in the main Pipeline Testing Guide and specifically address two common test failures encountered during the CTRL_1077231 pipeline test development:

1. TypeError: 'list_iterator' object is not subscriptable 
2. AttributeError: module has no attribute 'set_env_vars'

By applying these specific fixes alongside the existing best practices, we can achieve stable test coverage while avoiding common testing pitfalls.

## Code Coverage Considerations

When evaluating code coverage for ETL pipelines:

1. **Focus on Core Functions**: Functions like `calculate_ctrl1077231_metrics` and `_filter_resources` are essential to test thoroughly since they implement the primary business logic.

2. **Accept Some Coverage Gaps**: Code that handles administrative tasks or extremely rare error conditions might not need exhaustive testing to reach the 80% coverage threshold.

3. **Balance Testing Strategy**: Using a combination of end-to-end pipeline tests and direct function tests often yields the best balance between coverage and test stability.

Following these guidelines, we've successfully fixed failing tests and improved coverage for the CTRL_1077231 pipeline.