# Test Helper Setup
# This file is loaded before all tests and provides common setup and utilities

# Load required libraries
library(DBI)
library(odbc)
library(dplyr)
library(yaml)
library(glue)
library(tidyr)
library(testthat)

# Set paths relative to test directory
# Tests are now located at r-scripts/tests/testthat/
# Find project root by searching for r-scripts directory

find_project_root <- function() {
  # Start from current working directory
  current <- getwd()

  # Check if r-scripts exists in current directory
  if (dir.exists(file.path(current, "r-scripts"))) {
    return(current)
  }

  # Check if we're inside r-scripts (tests are at r-scripts/tests/testthat)
  # If so, go up to parent directory
  if (basename(current) == "testthat" &&
      dir.exists(file.path(current, "..", "..", ".."))) {
    potential_root <- normalizePath(file.path(current, "..", "..", ".."))
    if (dir.exists(file.path(potential_root, "r-scripts"))) {
      return(potential_root)
    }
  }

  # Try here::here() as fallback
  root <- here::here()
  if (dir.exists(file.path(root, "r-scripts"))) {
    return(root)
  }

  # Last resort: search upwards from current directory
  search_dir <- current
  for (i in 1:5) {  # Search up to 5 levels
    if (dir.exists(file.path(search_dir, "r-scripts"))) {
      return(normalizePath(search_dir))
    }
    search_dir <- file.path(search_dir, "..")
  }

  stop("Could not find project root with r-scripts directory")
}

PROJECT_ROOT <- find_project_root()
RSCRIPTS_PATH <- file.path(PROJECT_ROOT, "r-scripts")
CONFIG_PATH <- file.path(RSCRIPTS_PATH, "pipeline_code", "db2_config_multi_source.yaml")

# Source the R scripts we need to test
source(file.path(RSCRIPTS_PATH, "utility_code", "db2_connection.R"))
source(file.path(RSCRIPTS_PATH, "pipeline_code", "read_db2_config_multi_source.R"))
source(file.path(RSCRIPTS_PATH, "pipeline_code", "create_long_format_assets.R"))

# Test helper functions

#' Check if DB2 is available for testing
#' @return logical TRUE if DB2 connection can be established
is_db2_available <- function() {
  tryCatch({
    conn <- create_db2_connection()
    DBI::dbDisconnect(conn)
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

#' Skip test if DB2 is not available
skip_if_no_db2 <- function() {
  if (!is_db2_available()) {
    skip("DB2 database not available")
  }
}

#' Get a test database connection
#' @return DBI connection object
get_test_connection <- function() {
  create_db2_connection()
}

#' Clean up test connection
#' @param conn DBI connection object
cleanup_connection <- function(conn) {
  if (!is.null(conn) && DBI::dbIsValid(conn)) {
    DBI::dbDisconnect(conn)
  }
}

#' Create a minimal test configuration for testing
#' @return list with test configuration
get_test_config <- function() {
  list(
    database = list(
      driver = "DB2",
      database = "DEVDB",
      hostname = "db",
      port = 50000,
      protocol = "TCPIP",
      schema = "SAIL"
    ),
    assets = list(
      sex = list(
        description = "Patient sex/gender",
        sources = list(
          gp_sex = list(
            table = "PATIENT_ALF_CLEANSED",
            priority = 1,
            columns = list(
              patient_id = "ALF_PE",
              sex_code = "GNDR_CD",
              record_date = "CREATE_DT"
            )
          )
        )
      )
    )
  )
}

#' Generate test patient IDs
#' @param n number of patient IDs to generate
#' @return character vector of test patient IDs
generate_test_patient_ids <- function(n = 10) {
  paste0("TEST_", sprintf("%05d", 1:n))
}
