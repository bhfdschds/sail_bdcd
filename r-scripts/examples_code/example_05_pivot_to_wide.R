# Example 05: Pivoting Long Format to Wide Format
# This example demonstrates: pivot_to_wide_by_source()
# Using the 'sex' asset as an example

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)
library(tidyr)

# Source required files
source("/workspaces/sail_bdcd/r-scripts/utility_code/db2_connection.R")
source("/workspaces/sail_bdcd/r-scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# EXAMPLE 1: Basic Wide Format Conversion
# ============================================================================

example_basic_wide_format <- function() {
  cat("\n=== EXAMPLE 1: Basic Wide Format Conversion ===\n\n")

  # 1. Create the long format table
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1010)
  )

  cat("--- Long Format (Before) ---\n")
  cat(glue("Rows: {nrow(sex_long)}\n"))
  cat(glue("Columns: {ncol(sex_long)}\n"))
  print(head(sex_long))

  # 2. Pivot to wide format
  cat("\n--- Converting to Wide Format ---\n")

  sex_wide <- pivot_to_wide_by_source(
    long_table = sex_long,
    value_columns = c("sex_code", "sex_description")
  )

  cat("\n--- Wide Format (After) ---\n")
  cat(glue("Rows: {nrow(sex_wide)}\n"))
  cat(glue("Columns: {ncol(sex_wide)}\n"))
  cat("Column names:\n")
  print(names(sex_wide))

  cat("\n--- Sample Data ---\n")
  print(head(sex_wide))

  DBI::dbDisconnect(conn)

  return(sex_wide)
}

# ============================================================================
# EXAMPLE 2: Understanding Wide Format Structure
# ============================================================================

example_understand_wide_format <- function() {
  cat("\n=== EXAMPLE 2: Understanding Wide Format Structure ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1005)
  )

  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    c("sex_code", "sex_description")
  )

  cat("--- Wide Format Explained ---\n\n")

  cat("In wide format:\n")
  cat("  - Each row is ONE patient\n")
  cat("  - Each source gets its own columns\n")
  cat("  - Column naming: {source_name}_{value_name}\n\n")

  cat("Example column names:\n")
  for (col in names(sex_wide)) {
    if (col != "patient_id") {
      parts <- strsplit(col, "_")[[1]]
      if (length(parts) >= 2) {
        cat(glue("  - {col}: "))

        if (grepl("code$", col)) {
          cat("sex_code from ", gsub("_sex_code", "", col), "\n")
        } else if (grepl("description$", col)) {
          cat("sex_description from ", gsub("_sex_description", "", col), "\n")
        } else {
          cat("\n")
        }
      }
    }
  }

  cat("\n--- Viewing Data Side-by-Side ---\n")
  cat("This format lets you compare values across sources:\n\n")

  # Show an example patient
  if (nrow(sex_wide) > 0) {
    example_patient <- sex_wide[1, ]
    cat(glue("Patient {example_patient$patient_id}:\n"))

    # Get all source columns
    source_cols <- names(sex_wide)[names(sex_wide) != "patient_id"]

    for (col in source_cols) {
      value <- example_patient[[col]]
      if (!is.na(value)) {
        cat(glue("  {col}: {value}\n"))
      } else {
        cat(glue("  {col}: <NA>\n"))
      }
    }
  }

  DBI::dbDisconnect(conn)

  return(sex_wide)
}

# ============================================================================
# EXAMPLE 3: Identifying Conflicts with Wide Format
# ============================================================================

example_identify_conflicts_wide <- function() {
  cat("\n=== EXAMPLE 3: Identifying Conflicts Using Wide Format ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)
  )

  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    c("sex_code")  # Just use code for simplicity
  )

  cat("--- Wide Format for Conflict Detection ---\n\n")
  cat("In wide format, conflicts are easy to spot:\n")
  cat("Just compare values across source columns for each patient\n\n")

  # Get all sex_code columns
  code_columns <- names(sex_wide)[grepl("_sex_code$", names(sex_wide))]

  cat("Sex code columns:\n")
  for (col in code_columns) {
    cat(glue("  - {col}\n"))
  }

  # Find patients with conflicts
  cat("\n--- Finding Conflicts ---\n")

  # For each patient, check if all non-NA values are the same
  sex_wide_conflicts <- sex_wide

  # Create a function to check for conflicts in a row
  has_conflict <- function(row_values) {
    non_na_values <- na.omit(row_values)
    if (length(non_na_values) <= 1) {
      return(FALSE)
    }
    return(length(unique(non_na_values)) > 1)
  }

  # Apply to each row
  sex_wide_conflicts$has_conflict <- apply(
    sex_wide[, code_columns],
    1,
    has_conflict
  )

  conflicts <- sex_wide_conflicts %>% filter(has_conflict == TRUE)

  if (nrow(conflicts) > 0) {
    cat(glue("Found {nrow(conflicts)} patients with conflicts\n\n"))

    cat("--- Sample Conflicts ---\n")
    conflict_sample <- conflicts %>%
      select(patient_id, all_of(code_columns), has_conflict) %>%
      head(5)

    print(conflict_sample)

    cat("\nYou can easily see which sources disagree!\n")
  } else {
    cat("No conflicts found - all sources agree!\n")
  }

  DBI::dbDisconnect(conn)

  return(sex_wide_conflicts)
}

# ============================================================================
# EXAMPLE 4: Pivoting Multiple Columns
# ============================================================================

example_multiple_columns <- function() {
  cat("\n=== EXAMPLE 4: Pivoting Multiple Value Columns ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1010)
  )

  # Pivot both code and description
  cat("Pivoting both sex_code and sex_description...\n\n")

  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    value_columns = c("sex_code", "sex_description")
  )

  cat("--- Result Columns ---\n")
  for (col in names(sex_wide)) {
    cat(glue("  {col}\n"))
  }

  cat("\n--- Sample Data ---\n")
  print(head(sex_wide, 5))

  cat("\nNotice: Each source now has TWO columns:\n")
  cat("  - {source}_sex_code\n")
  cat("  - {source}_sex_description\n")

  DBI::dbDisconnect(conn)

  return(sex_wide)
}

# ============================================================================
# EXAMPLE 5: Creating a Comparison Report
# ============================================================================

example_comparison_report <- function() {
  cat("\n=== EXAMPLE 5: Creating a Source Comparison Report ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Create wide format for comparison
  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    c("sex_code", "sex_description")
  )

  cat("--- Source Comparison Report ---\n\n")

  # 1. Count how many patients have data from each source
  code_columns <- names(sex_wide)[grepl("_sex_code$", names(sex_wide))]

  cat("1. Data Availability by Source:\n")
  for (col in code_columns) {
    source_name <- gsub("_sex_code$", "", col)
    n_with_data <- sum(!is.na(sex_wide[[col]]))
    pct <- round(100 * n_with_data / nrow(sex_wide), 1)

    cat(glue("   {source_name}: {n_with_data} patients ({pct}%)\n"))
  }

  # 2. Agreement analysis
  cat("\n2. Agreement Analysis:\n")

  if (length(code_columns) >= 2) {
    # Compare first two sources as an example
    source1_col <- code_columns[1]
    source2_col <- code_columns[2]

    source1_name <- gsub("_sex_code$", "", source1_col)
    source2_name <- gsub("_sex_code$", "", source2_col)

    # Find patients with data from both sources
    both_available <- sex_wide %>%
      filter(!is.na(.data[[source1_col]]) & !is.na(.data[[source2_col]]))

    if (nrow(both_available) > 0) {
      # Check agreement
      both_available <- both_available %>%
        mutate(
          agreement = .data[[source1_col]] == .data[[source2_col]]
        )

      n_agree <- sum(both_available$agreement, na.rm = TRUE)
      n_total <- nrow(both_available)
      pct_agree <- round(100 * n_agree / n_total, 1)

      cat(glue("   {source1_name} vs {source2_name}:\n"))
      cat(glue("     Patients with both: {n_total}\n"))
      cat(glue("     Agreement: {n_agree} ({pct_agree}%)\n"))
      cat(glue("     Disagreement: {n_total - n_agree} ({100 - pct_agree}%)\n"))
    }
  }

  # 3. Export the wide format for manual review
  output_file <- "/mnt/user-data/outputs/sex_comparison_wide.csv"
  write.csv(sex_wide, output_file, row.names = FALSE)

  cat(glue("\n✓ Comparison table exported to: {output_file}\n"))
  cat("  Open in Excel to manually review source differences\n")

  DBI::dbDisconnect(conn)

  return(sex_wide)
}

# ============================================================================
# EXAMPLE 6: Combining with Priority Selection
# ============================================================================

example_wide_with_priority <- function() {
  cat("\n=== EXAMPLE 6: Wide Format + Priority Selection ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)
  )

  # 1. Create wide format for comparison
  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    c("sex_code", "sex_description")
  )

  # 2. Get highest priority per patient
  sex_priority <- get_highest_priority_per_patient(sex_long)

  # 3. Combine them
  cat("--- Combining Wide Format with Priority Selection ---\n\n")

  # Add priority-selected values to wide format
  sex_combined <- sex_wide %>%
    left_join(
      sex_priority %>%
        select(patient_id,
               selected_code = sex_code,
               selected_description = sex_description,
               selected_source = source_table),
      by = "patient_id"
    )

  cat("Now you can see:\n")
  cat("  - All source values (wide format columns)\n")
  cat("  - The selected value (selected_code, selected_description)\n")
  cat("  - Which source was selected (selected_source)\n\n")

  cat("--- Sample Combined Table ---\n")
  print(head(sex_combined %>% select(1:6), 5))  # Show first few columns

  cat("\n--- Column Summary ---\n")
  cat("Columns:\n")
  for (col in names(sex_combined)) {
    cat(glue("  - {col}\n"))
  }

  # Export
  output_file <- "/mnt/user-data/outputs/sex_wide_with_priority.csv"
  write.csv(sex_combined, output_file, row.names = FALSE)

  cat(glue("\n✓ Combined table exported to: {output_file}\n"))

  DBI::dbDisconnect(conn)

  return(sex_combined)
}

# ============================================================================
# EXAMPLE 7: Wide Format for Specific Sources Only
# ============================================================================

example_wide_specific_sources <- function() {
  cat("\n=== EXAMPLE 7: Wide Format for Specific Sources ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create long format with only specific sources
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100),
    include_sources = c("gp_registry", "hospital_demographics")
  )

  cat("Creating wide format with only GP and Hospital sources...\n\n")

  sex_wide <- pivot_to_wide_by_source(
    sex_long,
    c("sex_code", "sex_description")
  )

  cat("--- Result ---\n")
  cat(glue("Rows: {nrow(sex_wide)}\n"))
  cat(glue("Columns: {ncol(sex_wide)}\n\n"))

  cat("Column names (only selected sources):\n")
  for (col in names(sex_wide)) {
    cat(glue("  - {col}\n"))
  }

  cat("\n--- Sample Data ---\n")
  print(head(sex_wide, 5))

  DBI::dbDisconnect(conn)

  return(sex_wide)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic wide format
# sex_wide1 <- example_basic_wide_format()

# Understand structure
# sex_wide2 <- example_understand_wide_format()

# Identify conflicts
# conflicts <- example_identify_conflicts_wide()

# Multiple columns
# sex_wide3 <- example_multiple_columns()

# Comparison report
# comparison <- example_comparison_report()

# Wide with priority
# combined <- example_wide_with_priority()

# Specific sources
# sex_wide_specific <- example_wide_specific_sources()
