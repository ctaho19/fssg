# Pipeline Testing Lessons Learned

This document captures important lessons learned from developing and testing the monitoring pipelines, particularly focusing on common errors and their solutions.

## Recent Fixes Summary

The following issues were recently identified and fixed in the test files:

1. **Time-dependent Tests**: Tests were using `datetime.utcnow()` which caused flaky tests due to timestamps changing between runs.
2. **Inconsistent Mock Behavior**: The pagination test had duplicate mock setups that could lead to unexpected behavior.
3. **Division by Zero Protection**: Tests weren't properly validating the pipeline's protection against division by zero in edge cases.
4. **Missing Zero Resources Tests**: Needed explicit testing of scenarios where resources exist but don't match filter criteria.
5. **Insufficient Error Handling Tests**: The fetch_all_resources function lacked comprehensive error tests.
6. **Inconsistent Date/Timestamp Handling**: Tests used various approaches to timestamps, making maintenance difficult.

## Common Test Issues and Solutions

### 1. Case Sensitivity in Import Paths

**Problem:** 
Import paths in Python are case-sensitive. When a module name in the filesystem uses uppercase characters (e.g., `CTRL_1077231`) but is referenced with lowercase in tests (e.g., `ctrl_1077231`), the import will fail with an error like:
```
AttributeError: module 'pipelines' has no attribute 'pl_automated_monitoring_CTRL_1077231'. Did you mean: 'pl_automated_monitoring_ctrl_1077231'?
```

**Solution:**
- Always match the exact case in import statements with the actual directory/file name
- For automated monitoring pipelines, ensure imports use the same casing as the physical directory:
  ```python
  # Correct
  import pipelines.pl_automated_monitoring_CTRL_1077231.pipeline as pipeline
  
  # Incorrect
  import pipelines.pl_automated_monitoring_ctrl_1077231.pipeline as pipeline
  ```
- Similarly, when mocking functions from these modules, maintain consistent casing:
  ```python
  # Correct
  mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.refresh")
  
  # Incorrect
  mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.refresh")
  ```

### 2. DataFrame Data Type Mismatches

**Problem:**
When comparing expected and actual DataFrames, pandas' `assert_frame_equal` checks data types strictly. Mismatches like `int64` vs `datetime64[ns, UTC]` will fail tests even when values appear correct:
```
AssertionError: Attributes of DataFrame.iloc[:, 0] (column name="date") are different
Attribute "dtype" are different
[left]:  int64
[right]: datetime64[ns, UTC]
```

**Solution:**
- Explicitly set data types in expected output DataFrames to match the pipeline's output
- Pay special attention to timestamp/date fields, which can vary in representation
- Example fix:
  ```python
  # Ensure date is stored as int64 to match pipeline output
  df["date"] = df["date"].astype("int64")
  df["numerator"] = df["numerator"].astype("int64") 
  df["denominator"] = df["denominator"].astype("int64")
  ```
- Consider using `freeze_time` to make timestamp-related tests deterministic:
  ```python
  with freeze_time("2024-11-05 12:09:00.123456"):
      # Test code that generates timestamps
  ```

### 3. Empty DataFrame Comparisons

**Problem:**
When testing functions that might return empty results, direct DataFrame comparisons can fail due to shape differences. The pipeline may return a DataFrame with default values rather than a truly empty DataFrame.
```
AssertionError: DataFrame are different
DataFrame shape mismatch
[left]:  (2, 8)
[right]: (0, 8)
```

**Solution:**
- Understand how the pipeline behaves with empty inputs - many pipelines return a structured DataFrame with default values
- Update expected output generators to match this behavior:
  ```python
  def _expected_output_empty_df_pandas() -> pd.DataFrame:
      # Don't return an empty DataFrame
      # return pd.DataFrame([], columns=AVRO_SCHEMA_FIELDS)
      
      # Instead, return a DataFrame with the expected default structure
      now = int(datetime.datetime.utcnow().timestamp() * 1000)
      data = [
          [now, "CTRL-1077231", 1, 0.0, "Red", 0, 0, None],
          [now, "CTRL-1077231", 2, 0.0, "Red", 0, 0, None],
      ]
      df = pd.DataFrame(data, columns=AVRO_SCHEMA_FIELDS)
      return df
  ```
- Alternatively, test specific attributes rather than exact DataFrame equality
  ```python
  # Assert length and key values instead of full DataFrame comparison
  assert len(result_df) == 2
  assert result_df.iloc[0]["monitoring_metric_value"] == 0.0
  assert result_df.iloc[0]["compliance_status"] == "Red"
  ```

### 4. Mock API Call Issues

**Problem:**
Mocked API calls not being triggered or properly asserted, leading to test failures:
```
AssertionError: assert False
+  where False = <MagicMock name='_make_api_request' id='5153182336'>.called
```

**Solution:**
- Ensure mocks patch the correct module path with exact casing
- For nested testing scenarios, verify the mocks are set at the right scope
- Ensure the patched object is actually called in the test flow
- Improve test by checking specific mock call properties rather than just `.called`:
  ```python
  # Better approach
  mock_refresh.assert_called_once_with(
      client_id="etip-client-id",
      client_secret="etip-client-secret",
      exchange_url="https://api.cloud.capitalone.com/exchange"
  )
  ```

## Potential Issues to Watch For

1. **Timestamp Inconsistency**: 
   - Tests using real timestamps may break on different days or times
   - Use `freeze_time` consistently for timestamp-dependent tests

2. **Directory Structure Changes**:
   - If pipeline directories are renamed or moved, imports will break
   - Consider using relative imports within the codebase

3. **OAuth Token Refresh Mechanism**:
   - Ensure consistent naming of OAuth refresh functions across pipelines
   - The pipeline uses `refresh()` from `connectors.exchange.oauth_token`

4. **Mock Inheritance Issues**:
   - Tests that mock parent class methods may break if inheritance changes
   - Consider mocking at the lowest possible level

5. **API Schema Changes**:
   - If CloudRadar API changes its response format, tests will fail
   - Keep mock responses updated with the actual API schema

6. **Pipeline Default Behaviors**:
   - Understand and document the default behaviors when pipelines encounter empty inputs
   - Always test empty/error scenarios explicitly

7. **Data Type Handling Edge Cases**:
   - Be aware that type conversions may have unexpected results with edge cases
   - Test explicitly with invalid data types and null values

## Advanced Test Solutions

### 1. Time-dependent Test Standardization

**Problem:**
Tests using `datetime.utcnow()` are non-deterministic and lead to flaky test results when run at different times.

**Solution:**
- Create a centralized timestamp utility:
  ```python
  # Standard timestamp for all tests to use
  FIXED_TIMESTAMP = "2024-11-05 12:09:00"
  FIXED_TIMESTAMP_MS = 1730937340000  # Same timestamp in milliseconds
  
  def get_fixed_timestamp(as_int: bool = True) -> Union[int, pd.Timestamp]:
      """Returns a fixed timestamp for testing."""
      if as_int:
          return FIXED_TIMESTAMP_MS
      return pd.Timestamp(FIXED_TIMESTAMP, tz="UTC")
  ```
- Use `freeze_time` consistently with the standard timestamp constant:
  ```python
  with freeze_time(FIXED_TIMESTAMP):
      # Test code that generates timestamps
  ```
- Replace all direct timestamp generation in expected outputs:
  ```python
  # Before
  now = int(datetime.datetime.utcnow().timestamp() * 1000)
  
  # After
  now = get_fixed_timestamp(as_int=True)
  ```

### 2. Mock Setup Best Practices

**Problem:**
Inconsistent or duplicate mock setups can lead to unexpected behavior, especially with complex side effects.

**Solution:**
- Set up mocks once and avoid overriding them:
  ```python
  # Good practice
  mock_post.side_effect = [mock_response1, mock_response2]
  
  # Avoid this pattern
  mock_post.side_effect = [...]
  responses = []
  def _side_effect(*args, **kwargs):
      # Custom side effect logic
  mock_post.side_effect = [...]  # Overriding the setup again!
  ```
- Verify mock calls thoroughly:
  ```python
  assert mock_post.call_count == 2  # Verify called exactly twice
  
  # Verify call parameters
  _, kwargs = mock_post.call_args_list[1]
  assert 'params' in kwargs
  assert kwargs['params'].get('nextRecordKey') == 'expected_value'
  ```

### 3. Division by Zero Protection Testing

**Problem:**
Tests didn't properly validate the pipeline's handling of edge cases where division by zero could occur.

**Solution:**
- Add explicit tests for zero resource scenarios:
  ```python
  # Zero resources from empty API response
  test_transform_logic_empty_api_response()
  
  # Resources exist but none match filter criteria
  test_transform_logic_non_matching_resources()
  ```
- Verify denominator and numerator handling:
  ```python
  # Assert division by zero protection kicks in
  assert result_df.iloc[0]["denominator"] == 0
  assert result_df.iloc[0]["numerator"] == 0
  assert result_df.iloc[0]["monitoring_metric_value"] == 0.0
  ```

### 4. Error Handling Test Coverage

**Problem:**
Error handling tests were incomplete, especially for complex functions with multiple potential failure points.

**Solution:**
- Test multiple error scenarios in a single test function:
  ```python
  def test_fetch_all_resources_error_handling(mocker):
      # Test network errors
      mock_make_api_request.side_effect = requests.exceptions.ConnectionError("API error")
      with pytest.raises(requests.exceptions.ConnectionError):
          pipeline.fetch_all_resources(...)
      
      # Reset and test different error type
      mock_make_api_request.reset_mock()
      mock_response = mock.Mock()
      mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
      mock_make_api_request.return_value = mock_response
      with pytest.raises(json.JSONDecodeError):
          pipeline.fetch_all_resources(...)
  ```
- Verify mocks were actually called to ensure test coverage:
  ```python
  mock_make_api_request.assert_called_once()
  ```

## Best Practices

1. **Use Explicit Type Conversion**:
   - Always use explicit type conversion in both pipeline and test code
   - Example: `df["date"] = df["date"].astype("int64")`

2. **Consistent Freezing of Time**:
   - Use `freeze_time` decorator/context with standardized constants
   - Create utility functions for timestamp generation to ensure consistency

3. **Test Empty and Error Cases**:
   - Create explicit tests for empty inputs, API failures, and invalid data scenarios
   - Test edge cases like zero resources or non-matching resources

4. **Consistent Mocking Approach**:
   - Use consistent patterns for mocking, especially for common services like OAuth
   - Avoid duplicate mock setup that could lead to inconsistent behavior

5. **Check API Response Handling**:
   - Verify that the pipeline handles pagination, rate limiting, and API errors correctly
   - Test each error handling path explicitly

6. **Documentation of Expected DataFrame Structures**:
   - Document the expected shape and types of DataFrames for each pipeline
   - Use comprehensive assertions to validate structure and content

## Summary: Key Changes to Improve Test Quality

The improvements we made to the test suite for the CTRL_1077231 pipeline demonstrate several key principles for high-quality test code:

1. **Deterministic Tests**: By standardizing timestamp handling with utility functions and freeze_time, we eliminated flaky tests that could fail depending on when they were run.

2. **Clear Error Handling Tests**: We added comprehensive tests for error scenarios, including connection errors, JSON parsing errors, and empty resource lists.

3. **Edge Case Coverage**: We specifically added tests for zero resources and resources that don't match filter criteria, which are common scenarios in production.

4. **Mock Consistency**: We improved mock setup to avoid side-effect confusion and added assertions to verify mock call parameters.

5. **Type Safety**: We ensured consistent type handling across test code, matching expected and actual output DataFrame types.

Following these patterns across all pipeline tests will significantly improve reliability and maintainability of the test suite.