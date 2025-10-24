# Example 03: Checking for Conflicts Across Sources
# This example demonstrates: check_conflicts()
# Using the 'sex' asset as an example

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source required files
source("/workspaces/sail_bdcd/r-scripts/utility_code/db2_connection.R")
source("/workspaces/sail_bdcd/r-scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# EXAMPLE 1: Basic Conflict Detection for Sex
# ============================================================================

example_basic_conflict_check <- function() {
  cat("\n=== EXAMPLE 1: Basic Conflict Detection for Sex ===\n\n")

  # 1. Create the long format table
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:2000)
  )

  # 2. Check for conflicts in the sex_code column
  cat("Checking for conflicts in sex_code...\n")

  conflicts <- check_conflicts(
    long_table = sex_long,
    asset_name = "sex",
    key_column = "sex_code"
  )

  # 3. Interpret results
  if (is.null(conflicts)) {
    cat("\n✓ No conflicts found! All sources agree on sex codes.\n")
  } else {
    cat(glue("\n⚠ Found conflicts for {nrow(conflicts)} patients\n\n"))

    cat("--- Sample Conflicts ---\n")
    print(head(conflicts, 10))

    cat("\n--- Conflict Statistics ---\n")
    cat(glue("Total patients with conflicts: {nrow(conflicts)}\n"))
    cat(glue("Total patients in dataset: {n_distinct(sex_long$patient_id)}\n"))

    conflict_rate <- round(100 * nrow(conflicts) / n_distinct(sex_long$patient_id), 2)
    cat(glue("Conflict rate: {conflict_rate}%\n"))
  }

  DBI::dbDisconnect(conn)

  return(conflicts)
}

# ============================================================================
# EXAMPLE 2: Understanding Conflict Details
# ============================================================================

example_understand_conflicts <- function() {
  cat("\n=== EXAMPLE 2: Understanding Conflict Details ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (!is.null(conflicts)) {
    cat("\n--- What the Conflict Table Shows ---\n\n")

    # Take first conflict as example
    example_patient <- conflicts$patient_id[1]
    cat(glue("Example: Patient {example_patient}\n\n"))

    example_row <- conflicts[1, ]
    cat("Columns in conflict table:\n")
    cat(glue("  - patient_id: {example_row$patient_id}\n"))
    cat(glue("  - n_sources: {example_row$n_sources} (number of sources with data)\n"))
    cat(glue("  - values: '{example_row$values}' (conflicting values separated by 'vs')\n"))
    cat(glue("  - sources: '{example_row$sources}' (which sources have these values)\n\n"))

    # Show full data for this patient
    cat(glue("--- All Data for Patient {example_patient} ---\n"))
    patient_data <- sex_long %>%
      filter(patient_id == example_patient) %>%
      select(patient_id, source_table, source_priority, sex_code, sex_description)

    print(patient_data)

    cat("\nInterpretation:\n")
    cat("This patient has different sex codes in different sources.\n")
    cat("You need to decide which source to trust (usually via source_priority).\n")
  }

  DBI::dbDisconnect(conn)

  return(conflicts)
}

# ============================================================================
# EXAMPLE 3: Analyzing Conflict Patterns
# ============================================================================

example_analyze_conflict_patterns <- function() {
  cat("\n=== EXAMPLE 3: Analyzing Conflict Patterns ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (!is.null(conflicts)) {
    cat("--- Conflict Patterns ---\n\n")

    # 1. Most common conflict patterns
    cat("1. Most Common Conflicting Value Pairs:\n")
    value_patterns <- conflicts %>%
      count(values, sort = TRUE) %>%
      head(10)
    print(value_patterns)

    # 2. Which sources are involved in conflicts?
    cat("\n2. Source Combinations in Conflicts:\n")
    source_patterns <- conflicts %>%
      count(sources, sort = TRUE) %>%
      head(10)
    print(source_patterns)

    # 3. Distribution of number of conflicting sources
    cat("\n3. Distribution of Sources per Conflict:\n")
    n_sources_dist <- conflicts %>%
      count(n_sources, name = "n_patients")
    print(n_sources_dist)

    # 4. Detailed look at specific conflict types
    cat("\n4. Example Conflicts by Type:\n\n")

    # Get unique conflict patterns
    unique_patterns <- unique(conflicts$values)

    for (pattern in head(unique_patterns, 3)) {
      cat(glue("Pattern: {pattern}\n"))

      pattern_conflicts <- conflicts %>%
        filter(values == pattern) %>%
        head(3)

      for (i in 1:nrow(pattern_conflicts)) {
        patient <- pattern_conflicts$patient_id[i]

        # Get full details for this patient
        patient_details <- sex_long %>%
          filter(patient_id == patient) %>%
          select(source_table, source_priority, sex_code, sex_description)

        cat(glue("  Patient {patient}:\n"))
        print(patient_details)
        cat("\n")
      }
    }
  }

  DBI::dbDisconnect(conn)

  return(conflicts)
}

# ============================================================================
# EXAMPLE 4: Investigating Specific Conflicts
# ============================================================================

example_investigate_specific_conflicts <- function() {
  cat("\n=== EXAMPLE 4: Investigating Specific Conflicts ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (!is.null(conflicts)) {
    # Pick a patient with conflicts
    conflict_patient <- conflicts$patient_id[1]

    cat(glue("=== Deep Dive: Patient {conflict_patient} ===\n\n"))

    # Get all data for this patient
    patient_all_data <- sex_long %>%
      filter(patient_id == conflict_patient) %>%
      arrange(source_priority)

    cat("All Sources for This Patient (ordered by priority):\n")
    print(patient_all_data)

    cat("\n--- Analysis ---\n")

    # Show which value would be chosen by priority
    highest_priority_row <- patient_all_data[1, ]
    cat(glue("Highest priority source: {highest_priority_row$source_table}\n"))
    cat(glue("Value from highest priority: {highest_priority_row$sex_code}\n"))
    cat(glue("Description: {highest_priority_row$sex_description}\n\n"))

    # Show conflicting values
    cat("Conflicting values:\n")
    for (i in 1:nrow(patient_all_data)) {
      row <- patient_all_data[i, ]
      cat(glue("  {row$source_table} (priority {row$source_priority}): {row$sex_code} - {row$sex_description}\n"))
    }

    cat("\n--- Recommendation ---\n")
    cat("When conflicts exist, typically use the highest priority source.\n")
    cat(glue("For patient {conflict_patient}, use: {highest_priority_row$sex_code}\n"))
  }

  DBI::dbDisconnect(conn)

  return(conflicts)
}

# ============================================================================
# EXAMPLE 5: Comparing Conflicts Across Different Columns
# ============================================================================

example_compare_column_conflicts <- function() {
  cat("\n=== EXAMPLE 5: Comparing Conflicts Across Columns ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  cat("Checking conflicts in different columns...\n\n")

  # Check conflicts in sex_code
  cat("=== Conflicts in 'sex_code' ===\n")
  conflicts_code <- check_conflicts(sex_long, "sex", "sex_code")

  # Check conflicts in sex_description
  cat("\n=== Conflicts in 'sex_description' ===\n")
  conflicts_desc <- check_conflicts(sex_long, "sex", "sex_description")

  # Compare
  cat("\n--- Comparison ---\n")

  n_conflicts_code <- ifelse(is.null(conflicts_code), 0, nrow(conflicts_code))
  n_conflicts_desc <- ifelse(is.null(conflicts_desc), 0, nrow(conflicts_desc))

  cat(glue("Conflicts in sex_code: {n_conflicts_code} patients\n"))
  cat(glue("Conflicts in sex_description: {n_conflicts_desc} patients\n\n"))

  if (n_conflicts_code != n_conflicts_desc) {
    cat("NOTE: Different numbers of conflicts!\n")
    cat("This can happen when:\n")
    cat("  - Codes are the same but descriptions differ (coding system differences)\n")
    cat("  - Descriptions are the same but codes differ (data entry variations)\n")
  } else if (n_conflicts_code == 0 && n_conflicts_desc == 0) {
    cat("✓ No conflicts in either column - excellent data quality!\n")
  } else {
    cat("Both columns have the same number of conflicts.\n")
    cat("This suggests the conflicts are consistent across code and description.\n")
  }

  DBI::dbDisconnect(conn)

  return(list(
    code_conflicts = conflicts_code,
    description_conflicts = conflicts_desc
  ))
}

# ============================================================================
# EXAMPLE 6: Exporting Conflict Reports
# ============================================================================

example_export_conflict_report <- function() {
  cat("\n=== EXAMPLE 6: Exporting Conflict Report ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (!is.null(conflicts)) {
    # Create detailed conflict report
    conflict_report <- sex_long %>%
      filter(patient_id %in% conflicts$patient_id) %>%
      arrange(patient_id, source_priority) %>%
      select(patient_id, source_table, source_priority, sex_code, sex_description)

    # Export to CSV
    output_file <- "/mnt/user-data/outputs/sex_conflicts_report.csv"
    write.csv(conflict_report, output_file, row.names = FALSE)

    cat(glue("✓ Conflict report exported to: {output_file}\n"))
    cat(glue("  Rows: {nrow(conflict_report)}\n"))
    cat(glue("  Unique patients: {n_distinct(conflict_report$patient_id)}\n\n"))

    cat("--- Report Preview ---\n")
    print(head(conflict_report, 10))

    # Also export summary
    summary_file <- "/mnt/user-data/outputs/sex_conflicts_summary.csv"
    write.csv(conflicts, summary_file, row.names = FALSE)

    cat(glue("\n✓ Conflict summary exported to: {summary_file}\n"))
  } else {
    cat("No conflicts found - no report to export.\n")
  }

  DBI::dbDisconnect(conn)

  return(conflicts)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic conflict check
# conflicts1 <- example_basic_conflict_check()

# Understand conflicts
# conflicts2 <- example_understand_conflicts()

# Analyze patterns
# conflicts3 <- example_analyze_conflict_patterns()

# Investigate specific conflicts
# conflicts4 <- example_investigate_specific_conflicts()

# Compare columns
# conflicts5 <- example_compare_column_conflicts()

# Export report
# conflicts6 <- example_export_conflict_report()
