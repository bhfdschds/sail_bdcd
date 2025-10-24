# Example 02: Summarizing Long Format Tables
# This example demonstrates: summarize_long_format_table()
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
# EXAMPLE 1: Basic Summary of Sex Asset
# ============================================================================

example_basic_summary <- function() {
  cat("\n=== EXAMPLE 1: Basic Summary of Sex Data ===\n\n")

  # 1. Create the long format table
  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)  # First 100 patients
  )

  # 2. Generate summary
  summary_results <- summarize_long_format_table(sex_long, "sex")

  # 3. The function returns a list with two components:
  cat("\n--- Summary Components ---\n")
  cat("1. source_summary: Statistics per source\n")
  cat("2. coverage: How many patients have data from N sources\n\n")

  # 4. Explore the results
  cat("--- Source Summary Details ---\n")
  print(summary_results$source_summary)

  cat("\n--- Coverage Details ---\n")
  print(summary_results$coverage)

  DBI::dbDisconnect(conn)

  return(summary_results)
}

# ============================================================================
# EXAMPLE 2: Interpreting Source Summary
# ============================================================================

example_interpret_source_summary <- function() {
  cat("\n=== EXAMPLE 2: Interpreting Source Summary ===\n\n")

  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1200)
  )

  summary_results <- summarize_long_format_table(sex_long, "sex")

  cat("\n--- Understanding Source Summary ---\n\n")

  source_summary <- summary_results$source_summary

  for (i in 1:nrow(source_summary)) {
    source_name <- source_summary$source_table[i]
    n_patients <- source_summary$n_patients[i]
    n_rows <- source_summary$n_rows[i]
    priority <- source_summary$source_priority[i]

    cat(glue("Source: {source_name}\n"))
    cat(glue("  - Priority: {priority} (lower = higher priority)\n"))
    cat(glue("  - Patients: {n_patients}\n"))
    cat(glue("  - Rows: {n_rows}\n"))

    if ("source_quality" %in% names(source_summary)) {
      quality <- source_summary$source_quality[i]
      cat(glue("  - Quality: {quality}\n"))
    }

    if ("source_coverage" %in% names(source_summary)) {
      coverage <- source_summary$source_coverage[i]
      cat(glue("  - Coverage: {coverage}\n"))
    }

    cat("\n")
  }

  DBI::dbDisconnect(conn)

  return(summary_results)
}

# ============================================================================
# EXAMPLE 3: Interpreting Coverage Statistics
# ============================================================================

example_interpret_coverage <- function() {
  cat("\n=== EXAMPLE 3: Interpreting Coverage Statistics ===\n\n")

  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  summary_results <- summarize_long_format_table(sex_long, "sex")

  cat("\n--- Understanding Coverage ---\n\n")
  cat("Coverage shows how many patients have data from N sources.\n")
  cat("This helps identify:\n")
  cat("  - Patients with data from only one source (potential gaps)\n")
  cat("  - Patients with data from multiple sources (potential conflicts)\n\n")

  coverage <- summary_results$coverage

  for (i in 1:nrow(coverage)) {
    n_sources <- coverage$n_sources[i]
    n_patients <- coverage$n_patients[i]

    cat(glue("{n_patients} patients have data from {n_sources} source(s)\n"))
  }

  # Calculate percentages
  total_patients <- sum(coverage$n_patients)
  cat(glue("\n--- Coverage Percentages (Total: {total_patients} patients) ---\n"))

  for (i in 1:nrow(coverage)) {
    n_sources <- coverage$n_sources[i]
    n_patients <- coverage$n_patients[i]
    pct <- round(100 * n_patients / total_patients, 1)

    cat(glue("{pct}% of patients have data from {n_sources} source(s)\n"))
  }

  # Highlight important patterns
  cat("\n--- Key Insights ---\n")

  single_source <- coverage %>% filter(n_sources == 1)
  if (nrow(single_source) > 0) {
    pct <- round(100 * single_source$n_patients / total_patients, 1)
    cat(glue("- {pct}% of patients appear in only ONE source\n"))
    cat("  (These patients have no conflicts but may have lower confidence)\n\n")
  }

  multiple_sources <- coverage %>% filter(n_sources > 1)
  if (nrow(multiple_sources) > 0) {
    n_multi <- sum(multiple_sources$n_patients)
    pct <- round(100 * n_multi / total_patients, 1)
    cat(glue("- {pct}% of patients appear in MULTIPLE sources\n"))
    cat("  (These patients may have conflicts that need resolution)\n\n")
  }

  max_sources <- max(coverage$n_sources)
  cat(glue("- Maximum sources for any patient: {max_sources}\n"))

  DBI::dbDisconnect(conn)

  return(summary_results)
}

# ============================================================================
# EXAMPLE 4: Comparing Multiple Assets
# ============================================================================

example_compare_assets <- function() {
  cat("\n=== EXAMPLE 4: Comparing Summaries Across Assets ===\n\n")

  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  patient_ids <- c(1001:1500)

  # Create tables for multiple assets
  cat("Creating tables...\n")
  sex_long <- create_long_format_asset(conn, config, "sex", patient_ids)
  dob_long <- create_long_format_asset(conn, config, "date_of_birth", patient_ids)
  ethnicity_long <- create_long_format_asset(conn, config, "ethnicity", patient_ids)

  # Summarize each
  cat("\n=== Sex Summary ===\n")
  sex_summary <- summarize_long_format_table(sex_long, "sex")

  cat("\n=== Date of Birth Summary ===\n")
  dob_summary <- summarize_long_format_table(dob_long, "date_of_birth")

  cat("\n=== Ethnicity Summary ===\n")
  ethnicity_summary <- summarize_long_format_table(ethnicity_long, "ethnicity")

  # Compare coverage patterns
  cat("\n\n=== Coverage Comparison Across Assets ===\n\n")

  assets_list <- list(
    sex = sex_summary$coverage,
    date_of_birth = dob_summary$coverage,
    ethnicity = ethnicity_summary$coverage
  )

  for (asset_name in names(assets_list)) {
    coverage <- assets_list[[asset_name]]
    total <- sum(coverage$n_patients)
    multi_source <- sum(coverage$n_patients[coverage$n_sources > 1])
    pct_multi <- round(100 * multi_source / total, 1)

    cat(glue("{asset_name}: {pct_multi}% have multiple sources\n"))
  }

  DBI::dbDisconnect(conn)

  return(list(
    sex = sex_summary,
    dob = dob_summary,
    ethnicity = ethnicity_summary
  ))
}

# ============================================================================
# EXAMPLE 5: Using Summary for Data Quality Assessment
# ============================================================================

example_quality_assessment <- function() {
  cat("\n=== EXAMPLE 5: Data Quality Assessment ===\n\n")

  config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  summary_results <- summarize_long_format_table(sex_long, "sex")

  cat("\n--- Quality Assessment Report ---\n\n")

  # 1. Check for missing values in key columns
  cat("1. Completeness Check:\n")
  missing_sex_code <- sum(is.na(sex_long$sex_code))
  total_rows <- nrow(sex_long)
  pct_complete <- round(100 * (1 - missing_sex_code / total_rows), 1)

  cat(glue("   - Sex code completeness: {pct_complete}%\n"))
  cat(glue("   - Missing values: {missing_sex_code} / {total_rows} rows\n\n"))

  # 2. Check distribution across sources
  cat("2. Source Distribution:\n")
  source_summary <- summary_results$source_summary
  for (i in 1:nrow(source_summary)) {
    source <- source_summary$source_table[i]
    n_patients <- source_summary$n_patients[i]
    cat(glue("   - {source}: {n_patients} patients\n"))
  }

  # 3. Check for adequate coverage
  cat("\n3. Coverage Assessment:\n")
  coverage <- summary_results$coverage
  single_source_only <- coverage %>% filter(n_sources == 1) %>% pull(n_patients)

  if (length(single_source_only) > 0) {
    total_patients <- sum(coverage$n_patients)
    pct_single <- round(100 * single_source_only / total_patients, 1)

    if (pct_single > 50) {
      cat(glue("   WARNING: {pct_single}% of patients have data from only one source\n"))
      cat("   Consider expanding data collection or source integration\n")
    } else {
      cat(glue("   GOOD: Only {pct_single}% rely on single source\n"))
    }
  }

  # 4. Priority distribution
  cat("\n4. Priority Distribution:\n")
  if ("source_priority" %in% names(source_summary)) {
    cat("   Sources by priority (lower = higher priority):\n")
    source_summary_ordered <- source_summary %>% arrange(source_priority)
    for (i in 1:nrow(source_summary_ordered)) {
      source <- source_summary_ordered$source_table[i]
      priority <- source_summary_ordered$source_priority[i]
      n_patients <- source_summary_ordered$n_patients[i]
      cat(glue("   - Priority {priority}: {source} ({n_patients} patients)\n"))
    }
  }

  DBI::dbDisconnect(conn)

  return(summary_results)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic summary
summary1 <- example_basic_summary()

# Interpret source summary
summary2 <- example_interpret_source_summary()

# Interpret coverage
summary3 <- example_interpret_coverage()

# Compare assets
summaries <- example_compare_assets()

# Quality assessment
quality_report <- example_quality_assessment()
