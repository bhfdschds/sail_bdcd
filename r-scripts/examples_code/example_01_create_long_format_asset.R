# Example 01: Creating a Long Format Asset Table
# This example demonstrates the core function: create_long_format_asset()
# Using the 'sex' asset as an example

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source required files
source("scripts/utility_code/db2_connection.R")
source("scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# EXAMPLE 1: Basic Usage - Create Sex Asset Table
# ============================================================================

example_basic_sex_table <- function() {
  cat("\n=== EXAMPLE 1: Basic Sex Asset Table ===\n\n")

  # 1. Load configuration
  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")

  # 2. Connect to database
  conn <- create_db2_connection(config)

  # 3. Create long format sex table from ALL sources
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # 4. View the results
  cat("\n--- Structure of Long Format Table ---\n")
  str(sex_long)

  cat("\n--- First 10 Rows ---\n")
  print(head(sex_long, 10))

  cat("\n--- Column Names ---\n")
  print(names(sex_long))

  # 5. Disconnect
  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# EXAMPLE 2: Filtered by Patient IDs
# ============================================================================

example_filtered_sex_table <- function() {
  cat("\n=== EXAMPLE 2: Filtered Sex Table (Specific Patients) ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create table for only specific patient IDs
  specific_patients <- c(1001, 1002, 1003, 1004, 1005)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = specific_patients
  )

  cat("\n--- Results for Specific Patients ---\n")
  print(sex_long)

  cat("\n--- Unique Patient IDs in Result ---\n")
  print(unique(sex_long$patient_id))

  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# EXAMPLE 3: Include Only Specific Sources
# ============================================================================

example_specific_sources <- function() {
  cat("\n=== EXAMPLE 3: Sex Table from Specific Sources Only ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create table using only GP and HOSPITAL sources
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    include_sources = c("gp_registry", "hospital_demographics")
  )

  cat("\n--- Sources Included ---\n")
  print(unique(sex_long$source_table))

  cat("\n--- Sample Data ---\n")
  print(head(sex_long, 10))

  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# EXAMPLE 4: Without Standardizing Patient ID Column
# ============================================================================

example_no_standardization <- function() {
  cat("\n=== EXAMPLE 4: Keep Original Patient ID Column Name ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create table without standardizing patient ID column name
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1010),
    standardize_patient_id = FALSE
  )

  cat("\n--- First Column Name (Patient ID) ---\n")
  print(names(sex_long)[1])

  cat("\n--- Sample Data ---\n")
  print(head(sex_long, 5))

  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# EXAMPLE 5: Understanding the Long Format Structure
# ============================================================================

example_understand_structure <- function() {
  cat("\n=== EXAMPLE 5: Understanding Long Format Structure ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create table for a few patients
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001, 1002)
  )

  cat("\n--- What is Long Format? ---\n")
  cat("Each row represents ONE source's data for ONE patient.\n")
  cat("If a patient appears in 3 sources, they will have 3 rows.\n\n")

  cat("--- Example: Patient 1001 across all sources ---\n")
  patient_1001 <- sex_long %>% filter(patient_id == 1001)
  print(patient_1001)

  cat("\n--- How many sources have data for each patient? ---\n")
  source_counts <- sex_long %>%
    group_by(patient_id) %>%
    summarise(
      n_sources = n(),
      sources = paste(source_table, collapse = ", ")
    )
  print(source_counts)

  cat("\n--- Column Breakdown ---\n")
  cat("- patient_id: The patient identifier\n")
  cat("- source_table: Name of the source (e.g., 'gp_registry')\n")
  cat("- source_db_table: Actual database table name\n")
  cat("- source_priority: Priority ranking (lower = higher priority)\n")
  cat("- sex_code: The actual sex code from this source\n")
  cat("- sex_description: Human-readable description\n")

  if ("source_quality" %in% names(sex_long)) {
    cat("- source_quality: Data quality rating for this source\n")
  }
  if ("source_coverage" %in% names(sex_long)) {
    cat("- source_coverage: Coverage percentage for this source\n")
  }
  if ("source_last_updated" %in% names(sex_long)) {
    cat("- source_last_updated: When this source was last updated\n")
  }

  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic usage
sex_data <- example_basic_sex_table()

# Filtered by patient IDs
# sex_filtered <- example_filtered_sex_table()

# Specific sources only
# sex_specific_sources <- example_specific_sources()

# No standardization
# sex_no_std <- example_no_standardization()

# Understand the structure
# sex_structure <- example_understand_structure()
