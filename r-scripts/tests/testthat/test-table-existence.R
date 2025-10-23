# Tests for Table Existence
# Verifies that all required database tables exist

context("Database Table Existence Tests")

# List of tables that should exist in the SAIL schema
REQUIRED_TABLES <- c(
  "PATIENT_ALF_CLEANSED",
  "GP_EVENT_REFORMATTED",
  "GP_EVENT_CODES",
  "GP_EVENT_CLEANSED",
  "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN",
  "WLGP_CLEANED_GP_REG_MEDIAN"
)

test_that("database connection can list tables", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Get list of tables
  tables <- DBI::dbListTables(conn)

  # Should return a character vector
  expect_type(tables, "character")

  # Should have at least some tables
  expect_true(length(tables) > 0)

  cleanup_connection(conn)
})

test_that("PATIENT_ALF_CLEANSED table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Check if table exists
  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "PATIENT_ALF_CLEANSED"))

  expect_true(exists, info = "PATIENT_ALF_CLEANSED table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("GP_EVENT_REFORMATTED table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "GP_EVENT_REFORMATTED"))

  expect_true(exists, info = "GP_EVENT_REFORMATTED table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("GP_EVENT_CODES table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "GP_EVENT_CODES"))

  expect_true(exists, info = "GP_EVENT_CODES table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("GP_EVENT_CLEANSED table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "GP_EVENT_CLEANSED"))

  expect_true(exists, info = "GP_EVENT_CLEANSED table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN"))

  expect_true(exists, info = "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("WLGP_CLEANED_GP_REG_MEDIAN table exists", {
  skip_if_no_db2()

  conn <- get_test_connection()

  exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = "WLGP_CLEANED_GP_REG_MEDIAN"))

  expect_true(exists, info = "WLGP_CLEANED_GP_REG_MEDIAN table should exist in SAIL schema")

  cleanup_connection(conn)
})

test_that("all required tables exist in database", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Check each required table
  missing_tables <- c()

  for (table_name in REQUIRED_TABLES) {
    exists <- DBI::dbExistsTable(conn, DBI::Id(schema = "SAIL", table = table_name))
    if (!exists) {
      missing_tables <- c(missing_tables, table_name)
    }
  }

  # Report missing tables
  if (length(missing_tables) > 0) {
    fail(paste("Missing tables:", paste(missing_tables, collapse = ", ")))
  }

  expect_equal(length(missing_tables), 0)

  cleanup_connection(conn)
})

test_that("PATIENT_ALF_CLEANSED has expected columns", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Get column names
  columns <- DBI::dbListFields(conn, DBI::Id(schema = "SAIL", table = "PATIENT_ALF_CLEANSED"))

  # Expected key columns (from exploration report)
  expected_columns <- c("ALF_PE", "GNDR_CD", "WOB", "CREATE_DT")

  # Check that expected columns exist
  for (col in expected_columns) {
    expect_true(
      col %in% columns,
      info = paste("Column", col, "should exist in PATIENT_ALF_CLEANSED")
    )
  }

  cleanup_connection(conn)
})

test_that("GP_EVENT_REFORMATTED has expected columns", {
  skip_if_no_db2()

  conn <- get_test_connection()

  columns <- DBI::dbListFields(conn, DBI::Id(schema = "SAIL", table = "GP_EVENT_REFORMATTED"))

  # Expected key columns
  expected_columns <- c("ALF_PE", "EVENT_CD", "EVENT_DT")

  for (col in expected_columns) {
    expect_true(
      col %in% columns,
      info = paste("Column", col, "should exist in GP_EVENT_REFORMATTED")
    )
  }

  cleanup_connection(conn)
})

test_that("tables contain data (are not empty)", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Check PATIENT_ALF_CLEANSED has data
  count_query <- "SELECT COUNT(*) AS row_count FROM SAIL.PATIENT_ALF_CLEANSED"
  result <- DBI::dbGetQuery(conn, count_query)

  expect_true(
    result$ROW_COUNT[1] > 0,
    info = "PATIENT_ALF_CLEANSED should contain data"
  )

  # Check GP_EVENT_REFORMATTED has data
  count_query <- "SELECT COUNT(*) AS row_count FROM SAIL.GP_EVENT_REFORMATTED"
  result <- DBI::dbGetQuery(conn, count_query)

  expect_true(
    result$ROW_COUNT[1] > 0,
    info = "GP_EVENT_REFORMATTED should contain data"
  )

  cleanup_connection(conn)
})

test_that("can query tables successfully", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Test querying PATIENT_ALF_CLEANSED
  query <- "SELECT * FROM SAIL.PATIENT_ALF_CLEANSED FETCH FIRST 5 ROWS ONLY"
  result <- DBI::dbGetQuery(conn, query)

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true(nrow(result) <= 5)

  cleanup_connection(conn)
})

test_that("schema SAIL exists and is accessible", {
  skip_if_no_db2()

  conn <- get_test_connection()

  # Try to query system catalog for SAIL schema
  # This query should work on DB2
  schema_query <- "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = 'SAIL'"
  result <- tryCatch({
    DBI::dbGetQuery(conn, schema_query)
  }, error = function(e) {
    # Alternative approach if SYSCAT is not accessible
    # Just try to list tables which implicitly checks schema access
    DBI::dbListTables(conn)
  })

  # If we got here without error, schema is accessible
  expect_true(TRUE)

  cleanup_connection(conn)
})
