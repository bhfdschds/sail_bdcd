# SAIL BDCD Test Suite

This directory contains comprehensive tests for the SAIL BDCD (Better Data, Cleaner Data) R pipeline.

## Overview

The test suite uses the `testthat` framework and covers:

1. **Database Connection Tests** - Verify DB2 connections work correctly
2. **Table Existence Tests** - Ensure all required tables exist in the database
3. **Function Tests** - Unit and integration tests for all core R functions

## Test Structure

```
r-scripts/tests/
├── testthat.R                              # Main test runner
├── testthat/
│   ├── helper-setup.R                      # Test helpers and setup
│   ├── test-database-connection.R          # Database connection tests
│   ├── test-table-existence.R              # Table existence tests
│   ├── test-create-long-format-assets.R    # Core pipeline function tests
│   └── test-read-db2-config-multi-source.R # Multi-source config tests
└── README.md                               # This file
```

## Prerequisites

### Required R Packages

Install the following packages before running tests:

```r
install.packages(c(
  "testthat",
  "DBI",
  "odbc",
  "dplyr",
  "yaml",
  "glue",
  "tidyr",
  "here"
))
```

### Database Setup

Tests require a running DB2 database with:

- **Host**: `db` (or as configured in environment)
- **Port**: 50000
- **Database**: DEVDB
- **Schema**: SAIL
- **Credentials**: Set via environment variables:
  ```bash
  export DB_USER="db2inst1"
  export DB_PASSWORD="mypassword123"
  ```

### Required Tables

The following tables must exist in the `SAIL` schema:

- `PATIENT_ALF_CLEANSED`
- `GP_EVENT_REFORMATTED`
- `GP_EVENT_CODES`
- `GP_EVENT_CLEANSED`
- `WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN`
- `WLGP_CLEANED_GP_REG_MEDIAN`

To create these tables, run:

```bash
Rscript r-scripts/data_generation/Generate_Tables.R
Rscript r-scripts/data_generation/Generate_Data.R
```

## Running Tests

### Run All Tests

From the project root directory:

```r
# In R console
library(testthat)
test_dir("r-scripts/tests/testthat")
```

Or from command line:

```bash
Rscript r-scripts/tests/testthat.R
```

### Run Specific Test Files

```r
library(testthat)

# Database connection tests only
test_file("r-scripts/tests/testthat/test-database-connection.R")

# Table existence tests only
test_file("r-scripts/tests/testthat/test-table-existence.R")

# Function tests only
test_file("r-scripts/tests/testthat/test-create-long-format-assets.R")
test_file("r-scripts/tests/testthat/test-read-db2-config-multi-source.R")
```

### Run Tests Without Database

Some tests require database connectivity. These tests will be automatically skipped if the database is not available.

To run only unit tests (no database required):

```r
# Unit tests that don't require database will still run
# Integration tests will be skipped automatically
test_dir("r-scripts/tests/testthat")
```

Tests that require database access use `skip_if_no_db2()` and will show as "skipped" in the output.

## Test Categories

### 1. Database Connection Tests

**File**: `test-database-connection.R`

Tests the `db2_connection.R` functions:

- ✓ Connection creation
- ✓ Connection validity
- ✓ Connection properties
- ✓ Simple query execution
- ✓ Connection disconnection
- ✓ Environment variable usage
- ✓ Multiple connection management

**Example**:
```r
test_file("r-scripts/tests/testthat/test-database-connection.R")
```

### 2. Table Existence Tests

**File**: `test-table-existence.R`

Verifies database schema and table structure:

- ✓ All required tables exist
- ✓ Tables have expected columns
- ✓ Tables contain data
- ✓ Schema is accessible
- ✓ Tables can be queried

**Example**:
```r
test_file("r-scripts/tests/testthat/test-table-existence.R")
```

### 3. Core Function Tests

**File**: `test-create-long-format-assets.R`

Tests functions from `create_long_format_assets.R`:

#### Unit Tests (No Database Required):
- `standardize_patient_id_column()` - Column renaming
- `get_highest_priority_per_patient()` - Priority selection
- `summarize_long_format_table()` - Summary statistics
- `check_conflicts()` - Conflict detection
- `pivot_to_wide_by_source()` - Data pivoting
- `read_db_config()` - Configuration loading

#### Integration Tests (Database Required):
- `build_source_query()` - SQL query generation
- `create_long_format_asset()` - Full pipeline with database
- `create_all_asset_tables()` - Multi-asset processing

**Example**:
```r
test_file("r-scripts/tests/testthat/test-create-long-format-assets.R")
```

### 4. Multi-Source Config Tests

**File**: `test-read-db2-config-multi-source.R`

Tests functions from `read_db2_config_multi_source.R`:

#### Unit Tests (No Database Required):
- `get_asset_sources()` - Source enumeration
- `select_source_for_asset()` - Source selection logic
- `get_source_columns()` - Column mapping retrieval
- `build_select_query()` - SQL generation
- `resolve_conflicts()` - Conflict resolution
- `combine_ethnicity_sources()` - Data combination
- `print_asset_sources()` - Information display
- `print_project_config()` - Configuration display
- Configuration validation

#### Integration Tests (Database Required):
- `get_asset_data()` - Single source data retrieval
- `get_asset_data_multi_source()` - Multi-source retrieval
- `get_project_data()` - Project-based retrieval
- `compare_sources()` - Source comparison

**Example**:
```r
test_file("r-scripts/tests/testthat/test-read-db2-config-multi-source.R")
```

## Understanding Test Output

### Successful Test Run

```
✓ | OK F W S | Context
✓ |  7       | Database Connection Tests
✓ | 15       | Database Table Existence Tests
✓ | 25       | Create Long Format Assets Tests
✓ | 30       | Multi-Source Config and Data Retrieval Tests

══ Results ═══════════════════════════════════
Duration: 5.3 s

[ FAIL 0 | WARN 0 | SKIP 0 | PASS 77 ]
```

### Skipped Tests (No Database)

```
✓ | OK F W S | Context
✓ |  1     6 | Database Connection Tests (6 skipped)
✓ |       15 | Database Table Existence Tests (15 skipped)
✓ | 15     8 | Create Long Format Assets Tests (8 skipped)
✓ | 20    10 | Multi-Source Config and Data Retrieval Tests (10 skipped)

══ Results ═══════════════════════════════════
Duration: 1.2 s

[ FAIL 0 | WARN 0 | SKIP 39 | PASS 36 ]
```

### Failed Tests

```
── Failure: create_long_format_asset retrieves data ──
  result is NULL
  Expected TRUE, got FALSE

[ FAIL 1 | WARN 0 | SKIP 10 | PASS 66 ]
```

## Test Helpers

The `helper-setup.R` file provides utility functions:

### Database Helpers

```r
# Check if DB2 is available
is_db2_available()

# Skip test if no database
skip_if_no_db2()

# Get test connection
conn <- get_test_connection()

# Clean up connection
cleanup_connection(conn)
```

### Test Data Helpers

```r
# Get minimal test configuration
config <- get_test_config()

# Generate test patient IDs
ids <- generate_test_patient_ids(n = 10)
```

### Path Helpers

```r
# Project root
PROJECT_ROOT

# R scripts path
RSCRIPTS_PATH

# Config file path
CONFIG_PATH
```

## Writing New Tests

### Basic Test Structure

```r
test_that("function does what it should", {
  # Arrange - set up test data
  input_data <- data.frame(x = 1:5)

  # Act - call the function
  result <- my_function(input_data)

  # Assert - verify results
  expect_equal(nrow(result), 5)
  expect_true("output_col" %in% names(result))
})
```

### Database Integration Test

```r
test_that("function queries database correctly", {
  skip_if_no_db2()  # Skip if database not available

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Test with database
  result <- my_db_function(conn, config)

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)

  cleanup_connection(conn)
})
```

## Troubleshooting

### Database Connection Failures

**Problem**: Tests fail with "Database not available" or connection errors

**Solutions**:
1. Verify DB2 is running: `docker ps | grep db2`
2. Check environment variables: `echo $DB_USER`
3. Test connection manually:
   ```r
   source("r-scripts/utility_code/db2_connection.R")
   conn <- create_db2_connection()
   DBI::dbGetQuery(conn, "SELECT 1 FROM SYSIBM.SYSDUMMY1")
   ```

### Missing Tables

**Problem**: Table existence tests fail

**Solutions**:
1. Run table generation scripts:
   ```bash
   Rscript r-scripts/data_generation/Generate_Tables.R
   Rscript r-scripts/data_generation/Generate_Data.R
   ```
2. Verify tables exist:
   ```r
   DBI::dbListTables(conn)
   ```

### Missing Packages

**Problem**: Tests fail with "package not found"

**Solution**:
```r
# Install missing packages
install.packages(c("testthat", "DBI", "odbc", "dplyr",
                   "yaml", "glue", "tidyr", "here"))
```

### Path Issues

**Problem**: Tests can't find configuration files

**Solution**:
- Ensure you run tests from project root directory
- Check that `CONFIG_PATH` in `helper-setup.R` is correct
- Use the `here` package for consistent paths

## Continuous Integration

### Running Tests in CI/CD

Example GitHub Actions workflow:

```yaml
name: R Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      db2:
        image: ibmcom/db2:latest
        env:
          LICENSE: accept
          DB2INST1_PASSWORD: mypassword123
        ports:
          - 50000:50000

    steps:
      - uses: actions/checkout@v2

      - name: Setup R
        uses: r-lib/actions/setup-r@v2

      - name: Install dependencies
        run: |
          install.packages(c("testthat", "DBI", "odbc", "dplyr",
                             "yaml", "glue", "tidyr", "here"))
        shell: Rscript {0}

      - name: Run tests
        env:
          DB_USER: db2inst1
          DB_PASSWORD: mypassword123
        run: |
          library(testthat)
          test_dir("r-scripts/tests/testthat")
        shell: Rscript {0}
```

## Test Coverage

Current test coverage by file:

| File | Functions | Tests | Coverage |
|------|-----------|-------|----------|
| `db2_connection.R` | 1 | 7 | 100% |
| `create_long_format_assets.R` | 14 | 25 | 85% |
| `read_db2_config_multi_source.R` | 15 | 30 | 90% |
| **Total** | **30** | **62+** | **~90%** |

## Best Practices

1. **Isolation**: Each test should be independent
2. **Cleanup**: Always disconnect database connections
3. **Skip Gracefully**: Use `skip_if_no_db2()` for integration tests
4. **Descriptive Names**: Use clear test names that describe what is tested
5. **Small Tests**: Test one thing per test function
6. **Fast Tests**: Keep unit tests fast; mark slow tests appropriately

## Contributing

When adding new R functions:

1. Write tests first (TDD approach)
2. Add unit tests for logic without external dependencies
3. Add integration tests for database operations
4. Update this README with new test descriptions
5. Ensure all tests pass before committing

## Support

For issues or questions:

- Check the troubleshooting section above
- Review test output for specific error messages
- Examine `helper-setup.R` for available test utilities
- Consult testthat documentation: https://testthat.r-lib.org/
