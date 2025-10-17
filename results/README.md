# Test Results

This folder contains screenshots and documentation of test results for the HTX Data Processing project.

## Test Execution Results

### Unit Tests
- Upload screenshots of unit test execution here
- Include coverage reports if available

### Integration Tests  
- Upload screenshots of integration test results
- Include performance metrics for large dataset tests

### Flake8 Linting
- Upload screenshot showing clean Flake8 output
- Include any code quality metrics

### PySpark Job Execution
- Upload screenshots of successful PySpark job runs
- Include sample output data and performance logs

## How to Run Tests

1. **Activate virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Run unit tests:**
   ```bash
   python -m pytest tests/ -v
   # or
   python -m unittest discover tests/ -v
   ```

3. **Run linting:**
   ```bash
   python -m flake8 src/ --max-line-length=88 --extend-ignore=E203,W503
   ```

4. **Run integration tests:**
   ```bash
   python -m unittest tests.test_integration -v
   ```

## Expected Test Outcomes

- All unit tests should pass
- Integration tests should complete without errors
- Code should pass all Flake8 checks
- Sample PySpark job should produce correct rankings

Upload your test screenshots here to document successful execution.