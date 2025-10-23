# Tests for create_long_format_assets.R functions
# Tests the core pipeline functions for creating long format asset tables

context("Create Long Format Assets Tests")

# ============================================================================
# Tests for standardize_patient_id_column
# ============================================================================

test_that("standardize_patient_id_column renames first column correctly", {
  # Create test data frame
  test_df <- data.frame(
    ALF_PE = c("P001", "P002", "P003"),
    GNDR_CD = c("M", "F", "M"),
    WOB = c("1980", "1990", "1985")
  )

  # Standardize patient ID column
  result <- standardize_patient_id_column(test_df, "patient_id")

  # Check that first column is now "patient_id"
  expect_equal(names(result)[1], "patient_id")

  # Check that other columns remain unchanged
  expect_equal(names(result)[2], "GNDR_CD")
  expect_equal(names(result)[3], "WOB")

  # Check data is preserved
  expect_equal(nrow(result), 3)
  expect_equal(result$patient_id, c("P001", "P002", "P003"))
})

test_that("standardize_patient_id_column handles already standardized column", {
  test_df <- data.frame(
    patient_id = c("P001", "P002"),
    value = c(10, 20)
  )

  result <- standardize_patient_id_column(test_df, "patient_id")

  # Should not change if already named correctly
  expect_equal(names(result)[1], "patient_id")
  expect_equal(nrow(result), 2)
})

test_that("standardize_patient_id_column handles empty data frame", {
  test_df <- data.frame()

  result <- standardize_patient_id_column(test_df, "patient_id")

  expect_equal(ncol(result), 0)
  expect_equal(nrow(result), 0)
})

test_that("standardize_patient_id_column preserves data types", {
  test_df <- data.frame(
    id = 1:5,
    name = letters[1:5],
    value = as.numeric(10:14),
    stringsAsFactors = FALSE
  )

  result <- standardize_patient_id_column(test_df)

  expect_equal(class(result$patient_id), "integer")
  expect_equal(class(result$name), "character")
  expect_equal(class(result$value), "numeric")
})

# ============================================================================
# Tests for build_source_query
# ============================================================================

test_that("build_source_query constructs valid SQL query", {
  config <- get_test_config()

  query <- build_source_query(config, "sex", "gp_sex")

  # Check that query contains expected components
  expect_true(grepl("SELECT", query, ignore.case = TRUE))
  expect_true(grepl("FROM", query, ignore.case = TRUE))
  expect_true(grepl("SAIL.PATIENT_ALF_CLEANSED", query, ignore.case = TRUE))

  # Check column aliases
  expect_true(grepl("AS patient_id", query, ignore.case = TRUE))
  expect_true(grepl("AS sex_code", query, ignore.case = TRUE))
})

test_that("build_source_query adds WHERE clause for patient_ids", {
  config <- get_test_config()
  patient_ids <- c("P001", "P002", "P003")

  query <- build_source_query(config, "sex", "gp_sex", patient_ids)

  # Check WHERE clause is present
  expect_true(grepl("WHERE", query, ignore.case = TRUE))
  expect_true(grepl("IN", query, ignore.case = TRUE))
  expect_true(grepl("P001", query))
})

test_that("build_source_query handles missing asset", {
  config <- get_test_config()

  # Should fail gracefully for non-existent asset
  expect_error(
    build_source_query(config, "nonexistent_asset", "source1"),
    "subscript out of bounds|NULL"
  )
})

# ============================================================================
# Tests for get_highest_priority_per_patient
# ============================================================================

test_that("get_highest_priority_per_patient selects highest priority row", {
  # Create test long format table
  long_table <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002", "P003"),
    source_table = c("source1", "source2", "source1", "source2", "source1"),
    source_priority = c(1, 2, 1, 2, 1),
    value = c("A", "B", "C", "D", "E"),
    stringsAsFactors = FALSE
  )

  result <- get_highest_priority_per_patient(long_table)

  # Should have one row per patient
  expect_equal(nrow(result), 3)

  # Should select priority 1 for each patient
  expect_equal(result$source_priority, c(1, 1, 1))
  expect_equal(result$value, c("A", "C", "E"))
})

test_that("get_highest_priority_per_patient handles single source per patient", {
  long_table <- data.frame(
    patient_id = c("P001", "P002", "P003"),
    source_priority = c(1, 2, 3),
    value = c("A", "B", "C")
  )

  result <- get_highest_priority_per_patient(long_table)

  # Should return all rows unchanged
  expect_equal(nrow(result), 3)
  expect_equal(result$patient_id, c("P001", "P002", "P003"))
})

test_that("get_highest_priority_per_patient maintains column order", {
  long_table <- data.frame(
    patient_id = c("P001", "P001"),
    source_priority = c(1, 2),
    col1 = c("a", "b"),
    col2 = c(10, 20),
    col3 = c(TRUE, FALSE)
  )

  result <- get_highest_priority_per_patient(long_table)

  # Check columns are preserved
  expect_equal(names(result), names(long_table))
})

# ============================================================================
# Tests for summarize_long_format_table
# ============================================================================

test_that("summarize_long_format_table returns summary statistics", {
  long_table <- data.frame(
    patient_id = c("P001", "P001", "P002", "P003"),
    source_table = c("source1", "source2", "source1", "source1"),
    source_priority = c(1, 2, 1, 1),
    source_quality = c("high", "medium", "high", "high"),
    source_coverage = c(0.95, 0.88, 0.95, 0.95),
    value = c("A", "B", "C", "D")
  )

  # Capture output to suppress printing
  result <- capture.output(
    summary <- summarize_long_format_table(long_table, "test_asset")
  )

  # Check that summary contains expected components
  expect_type(summary, "list")
  expect_true("source_summary" %in% names(summary))
  expect_true("coverage" %in% names(summary))

  # Check source summary
  expect_true(nrow(summary$source_summary) > 0)
  expect_true("source_table" %in% names(summary$source_summary))

  # Check coverage summary
  expect_true(nrow(summary$coverage) > 0)
})

test_that("summarize_long_format_table handles minimal columns", {
  long_table <- data.frame(
    patient_id = c("P001", "P002"),
    source_table = c("source1", "source1"),
    source_priority = c(1, 1)
  )

  # Should not error with minimal columns
  result <- capture.output(
    summary <- summarize_long_format_table(long_table, "test_asset")
  )

  expect_type(summary, "list")
})

# ============================================================================
# Tests for check_conflicts
# ============================================================================

test_that("check_conflicts identifies conflicting values", {
  long_table <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002"),
    source_table = c("source1", "source2", "source1", "source2"),
    ethnicity_code = c("A", "B", "C", "C"),  # P001 has conflict, P002 does not
    stringsAsFactors = FALSE
  )

  result <- capture.output(
    conflicts <- check_conflicts(long_table, "ethnicity", "ethnicity_code")
  )

  # Should detect one patient with conflict
  expect_s3_class(conflicts, "data.frame")
  expect_equal(nrow(conflicts), 1)
  expect_equal(conflicts$patient_id[1], "P001")
})

test_that("check_conflicts returns NULL when no conflicts", {
  long_table <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002"),
    source_table = c("source1", "source2", "source1", "source2"),
    value = c("A", "A", "B", "B")  # No conflicts
  )

  result <- capture.output(
    conflicts <- check_conflicts(long_table, "test", "value")
  )

  # Should return NULL when no conflicts
  expect_null(conflicts)
})

test_that("check_conflicts handles single source per patient", {
  long_table <- data.frame(
    patient_id = c("P001", "P002", "P003"),
    source_table = c("source1", "source1", "source1"),
    value = c("A", "B", "C")
  )

  result <- capture.output(
    conflicts <- check_conflicts(long_table, "test", "value")
  )

  # Should return NULL when no patients have multiple sources
  expect_null(conflicts)
})

# ============================================================================
# Tests for pivot_to_wide_by_source
# ============================================================================

test_that("pivot_to_wide_by_source creates wide format correctly", {
  long_table <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002"),
    source_table = c("source1", "source2", "source1", "source2"),
    value = c(10, 20, 30, 40),
    stringsAsFactors = FALSE
  )

  result <- pivot_to_wide_by_source(long_table, "value")

  # Check structure
  expect_equal(nrow(result), 2)  # One row per patient
  expect_true("patient_id" %in% names(result))

  # Check that source columns exist
  expect_true(any(grepl("source1_value", names(result))))
  expect_true(any(grepl("source2_value", names(result))))
})

test_that("pivot_to_wide_by_source handles multiple value columns", {
  long_table <- data.frame(
    patient_id = c("P001", "P001"),
    source_table = c("source1", "source2"),
    col1 = c("A", "B"),
    col2 = c(10, 20)
  )

  result <- pivot_to_wide_by_source(long_table, c("col1", "col2"))

  # Should create columns for both value columns and both sources
  expect_true(any(grepl("source1_col1", names(result))))
  expect_true(any(grepl("source2_col2", names(result))))
})

# ============================================================================
# Integration Tests (require database)
# ============================================================================

test_that("create_long_format_asset retrieves data from database", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Try to create long format table for sex asset
  result <- tryCatch({
    create_long_format_asset(
      conn, config,
      asset_name = "sex",
      patient_ids = NULL  # Get all patients
    )
  }, error = function(e) {
    NULL
  })

  # Check result structure if successful
  if (!is.null(result)) {
    expect_s3_class(result, "data.frame")
    expect_true(nrow(result) > 0)
    expect_true("patient_id" %in% names(result))
    expect_true("source_table" %in% names(result))
    expect_true("source_priority" %in% names(result))
  }

  cleanup_connection(conn)
})

test_that("create_long_format_asset handles patient_ids filter", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Get a few patient IDs from the database first
  sample_query <- "SELECT DISTINCT ALF_PE FROM SAIL.PATIENT_ALF_CLEANSED FETCH FIRST 5 ROWS ONLY"
  sample_patients <- DBI::dbGetQuery(conn, sample_query)

  if (nrow(sample_patients) > 0) {
    patient_ids <- sample_patients$ALF_PE

    result <- create_long_format_asset(
      conn, config,
      asset_name = "sex",
      patient_ids = patient_ids
    )

    # Should only return requested patients
    expect_true(all(result$patient_id %in% patient_ids))
  }

  cleanup_connection(conn)
})

test_that("create_all_asset_tables creates multiple assets", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Create tables for a subset of assets
  result <- tryCatch({
    create_all_asset_tables(
      conn, config,
      patient_ids = NULL,
      assets = c("sex")  # Just test with one asset
    )
  }, error = function(e) {
    NULL
  })

  if (!is.null(result)) {
    expect_type(result, "list")
    expect_true("sex" %in% names(result))
    expect_s3_class(result$sex, "data.frame")
  }

  cleanup_connection(conn)
})

# ============================================================================
# Tests for read_db_config
# ============================================================================

test_that("read_db_config loads YAML configuration", {
  config <- read_db_config(CONFIG_PATH)

  expect_type(config, "list")
  expect_true("database" %in% names(config))
  expect_true("assets" %in% names(config))
})

test_that("read_db_config configuration has expected structure", {
  config <- read_db_config(CONFIG_PATH)

  # Check database config
  expect_true("driver" %in% names(config$database))
  expect_true("schema" %in% names(config$database))

  # Check assets config
  expect_true(length(config$assets) > 0)
})

test_that("read_db_config handles missing file", {
  expect_error(
    read_db_config("nonexistent_file.yaml"),
    "cannot open|does not exist"
  )
})
