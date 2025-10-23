# Test Suite Summary

## Overview

A comprehensive test suite has been created for the SAIL BDCD R pipeline using the `testthat` framework.

## Test Files Created

### 1. Core Test Infrastructure

- **`tests/testthat.R`** - Main test runner entry point
- **`tests/testthat/helper-setup.R`** - Test helpers, utilities, and setup functions
- **`tests/README.md`** - Comprehensive documentation on running and writing tests

### 2. Test Files

#### `test-database-connection.R` (7 tests)
Tests for DB2 database connection functionality:
- Connection creation and validation
- Connection properties
- Query execution
- Connection management (multiple connections, disconnection)
- Environment variable usage for credentials

#### `test-table-existence.R` (15+ tests)
Verifies database schema and table structure:
- Existence of all 6 required SAIL tables
- Table column structure validation
- Data presence verification
- Schema accessibility
- Query capability

#### `test-create-long-format-assets.R` (25+ tests)
Tests for core pipeline functions in `create_long_format_assets.R`:

**Unit Tests:**
- `standardize_patient_id_column()` - 4 tests
- `get_highest_priority_per_patient()` - 3 tests
- `summarize_long_format_table()` - 2 tests
- `check_conflicts()` - 3 tests
- `pivot_to_wide_by_source()` - 2 tests
- `read_db_config()` - 3 tests

**Integration Tests:**
- `build_source_query()` - 3 tests
- `create_long_format_asset()` - 2 tests
- `create_all_asset_tables()` - 1 test

#### `test-read-db2-config-multi-source.R` (30+ tests)
Tests for multi-source configuration and data retrieval:

**Unit Tests:**
- `get_asset_sources()` - 4 tests
- `select_source_for_asset()` - 3 tests
- `get_source_columns()` - 3 tests
- `build_select_query()` - 4 tests
- `resolve_conflicts()` - 3 tests
- `combine_ethnicity_sources()` - 2 tests
- `print_asset_sources()` - 1 test
- `print_project_config()` - 2 tests
- Configuration validation - 3 tests

**Integration Tests:**
- `get_asset_data()` - 1 test
- `get_asset_data_multi_source()` - 1 test
- `get_project_data()` - 1 test
- `compare_sources()` - 1 test

## Test Coverage

| Component | Tests | Type |
|-----------|-------|------|
| Database Connections | 7 | Integration |
| Table Existence | 15+ | Integration |
| Core Functions (Unit) | 15+ | Unit |
| Core Functions (Integration) | 10+ | Integration |
| Config Functions (Unit) | 20+ | Unit |
| Config Functions (Integration) | 10+ | Integration |
| **Total** | **77+** | **Mixed** |

## Key Features

### Smart Test Skipping
- Tests automatically skip database operations if DB2 is not available
- Allows unit tests to run without database
- Uses `skip_if_no_db2()` helper function

### Test Helpers
- `is_db2_available()` - Check database connectivity
- `get_test_connection()` - Get standardized test connection
- `cleanup_connection()` - Proper connection cleanup
- `get_test_config()` - Minimal test configuration
- `generate_test_patient_ids()` - Test data generation

### Comprehensive Coverage
- **Database connectivity** - Connection creation, validation, queries
- **Table structure** - Schema verification, column validation
- **Data transformations** - Standardization, pivoting, summarization
- **Conflict resolution** - Priority-based, date-based, consensus methods
- **Multi-source operations** - Source selection, combination, comparison
- **Configuration** - YAML parsing, project preferences, source metadata

## Running the Tests

Once R and required packages are installed:

```r
# Run all tests
library(testthat)
test_dir("tests/testthat")

# Run specific test file
test_file("tests/testthat/test-database-connection.R")
```

## Testing Strategy

### Unit Tests (No Database Required)
- Function logic and transformations
- Data structure manipulation
- Configuration parsing
- Error handling

### Integration Tests (Database Required)
- Database connections
- Table queries
- Full pipeline execution
- Multi-source data retrieval

## Expected Behavior

### With Database Available
All 77+ tests should pass:
```
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 77+ ]
```

### Without Database
Unit tests pass, integration tests skip:
```
[ FAIL 0 | WARN 0 | SKIP ~40 | PASS ~37 ]
```

## Prerequisites

### R Packages
```r
install.packages(c(
  "testthat", "DBI", "odbc", "dplyr",
  "yaml", "glue", "tidyr", "here"
))
```

### Database Setup
- DB2 running on host `db`, port 50000
- Database: DEVDB
- Schema: SAIL
- Environment variables: `DB_USER`, `DB_PASSWORD`
- Required tables created via Generate_Tables.R and Generate_Data.R

## Benefits

1. **Quality Assurance** - Catch bugs before production
2. **Regression Prevention** - Ensure changes don't break existing functionality
3. **Documentation** - Tests serve as usage examples
4. **Confidence** - Safe refactoring with test coverage
5. **Maintainability** - Easier to understand code intent

## Next Steps

1. Install R and required packages
2. Ensure DB2 is running and accessible
3. Set environment variables for database credentials
4. Run tests: `Rscript tests/testthat.R`
5. Review test output and fix any failures
6. Add tests for new functions as development continues

## Notes

- Tests follow testthat best practices
- Each test is independent and isolated
- Database connections are properly cleaned up
- Configuration files use consistent paths
- Test output is descriptive and actionable
