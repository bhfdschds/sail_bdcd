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

find_project_root_and_rscripts <- function() {
  # Start from current working directory
  current <- getwd()

  # SCENARIO 1: Check if current directory IS the r-scripts folder
  # (e.g., RStudio mounted directly to r-scripts)
  # Look for characteristic subdirectories: pipeline_code, utility_code, data_generation
  has_pipeline <- dir.exists(file.path(current, "pipeline_code"))
  has_utility <- dir.exists(file.path(current, "utility_code"))
  has_data_gen <- dir.exists(file.path(current, "data_generation"))

  if (has_pipeline && has_utility && has_data_gen) {
    # We ARE in the r-scripts directory
    return(list(
      project_root = current,
      rscripts_path = current
    ))
  }

  # SCENARIO 2: Check if r-scripts exists as subdirectory (normal git repo structure)
  if (dir.exists(file.path(current, "r-scripts"))) {
    return(list(
      project_root = current,
      rscripts_path = file.path(current, "r-scripts")
    ))
  }

  # SCENARIO 3: Check if we're inside tests directory, navigate up
  if (basename(current) == "testthat" || basename(current) == "tests") {
    # Try going up levels to find r-scripts
    for (levels_up in 1:4) {
      parent_path <- normalizePath(file.path(current, paste(rep("..", levels_up), collapse = "/")))

      # Check if parent has r-scripts subdirectory
      if (dir.exists(file.path(parent_path, "r-scripts"))) {
        return(list(
          project_root = parent_path,
          rscripts_path = file.path(parent_path, "r-scripts")
        ))
      }

      # Check if parent IS r-scripts
      has_pipeline <- dir.exists(file.path(parent_path, "pipeline_code"))
      has_utility <- dir.exists(file.path(parent_path, "utility_code"))
      if (has_pipeline && has_utility) {
        return(list(
          project_root = parent_path,
          rscripts_path = parent_path
        ))
      }
    }
  }

  # SCENARIO 4: Search upwards from current directory
  search_dir <- current
  for (i in 1:5) {
    if (dir.exists(file.path(search_dir, "r-scripts"))) {
      return(list(
        project_root = normalizePath(search_dir),
        rscripts_path = file.path(normalizePath(search_dir), "r-scripts")
      ))
    }
    search_dir <- file.path(search_dir, "..")
  }

  stop("Could not find r-scripts directory. Please ensure you're running tests from the project root or r-scripts directory.")
}

# Find paths
paths <- find_project_root_and_rscripts()
PROJECT_ROOT <- paths$project_root
RSCRIPTS_PATH <- paths$rscripts_path
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
