# Pipeline Testing Guide Addendum: Recent Fixes

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