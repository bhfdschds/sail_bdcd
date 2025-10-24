# Example 04: Getting Highest Priority Values Per Patient
# This example demonstrates: get_highest_priority_per_patient()
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
# EXAMPLE 1: Basic Priority Selection
# ============================================================================

example_basic_priority_selection <- function() {
  cat("\n=== EXAMPLE 1: Basic Priority Selection ===\n\n")

  # 1. Create the long format table
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1020)
  )

  cat("--- Original Long Format Table ---\n")
  cat(glue("Rows: {nrow(sex_long)}\n"))
  cat(glue("Unique patients: {n_distinct(sex_long$patient_id)}\n\n"))

  # 2. Get highest priority per patient
  sex_priority <- get_highest_priority_per_patient(sex_long)

  cat("--- After Priority Selection ---\n")
  cat(glue("Rows: {nrow(sex_priority)}\n"))
  cat(glue("Unique patients: {n_distinct(sex_priority$patient_id)}\n\n"))

  cat("Key difference: Now each patient has exactly ONE row!\n\n")

  # 3. Show the result
  cat("--- First 10 Patients (Highest Priority Values) ---\n")
  print(head(sex_priority, 10))

  DBI::dbDisconnect(conn)

  return(sex_priority)
}

# ============================================================================
# EXAMPLE 2: Comparing Before and After
# ============================================================================

example_compare_before_after <- function() {
  cat("\n=== EXAMPLE 2: Comparing Before and After Priority Selection ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1005)
  )

  # Pick a patient who appears in multiple sources
  multi_source_patients <- sex_long %>%
    group_by(patient_id) %>%
    filter(n() > 1) %>%
    pull(patient_id) %>%
    unique()

  if (length(multi_source_patients) > 0) {
    example_patient <- multi_source_patients[1]

    cat(glue("=== Example Patient: {example_patient} ===\n\n"))

    # Show all sources for this patient
    cat("BEFORE - All sources for this patient:\n")
    before_data <- sex_long %>%
      filter(patient_id == example_patient) %>%
      arrange(source_priority) %>%
      select(patient_id, source_table, source_priority, sex_code, sex_description)

    print(before_data)

    # Get highest priority
    sex_priority <- get_highest_priority_per_patient(sex_long)

    cat("\nAFTER - Only highest priority source:\n")
    after_data <- sex_priority %>%
      filter(patient_id == example_patient) %>%
      select(patient_id, source_table, source_priority, sex_code, sex_description)

    print(after_data)

    cat("\n--- Explanation ---\n")
    cat("The function selected the row with the LOWEST source_priority value\n")
    cat("(lower priority number = higher priority in the ranking)\n")

    selected_source <- after_data$source_table[1]
    selected_priority <- after_data$source_priority[1]
    cat(glue("Selected: {selected_source} (priority {selected_priority})\n"))
  } else {
    cat("All patients in this sample have only one source.\n")
  }

  DBI::dbDisconnect(conn)

  return(sex_priority)
}

# ============================================================================
# EXAMPLE 3: Creating a "Master" Patient Table
# ============================================================================

example_create_master_table <- function() {
  cat("\n=== EXAMPLE 3: Creating a Master Patient Table ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  patient_ids <- c(1001:1200)

  cat("Creating master table with highest priority values for each asset...\n\n")

  # Get highest priority for multiple assets
  sex_long <- create_long_format_asset(conn, config, "sex", patient_ids)
  sex_master <- get_highest_priority_per_patient(sex_long)

  dob_long <- create_long_format_asset(conn, config, "date_of_birth", patient_ids)
  dob_master <- get_highest_priority_per_patient(dob_long)

  ethnicity_long <- create_long_format_asset(conn, config, "ethnicity", patient_ids)
  ethnicity_master <- get_highest_priority_per_patient(ethnicity_long)

  cat("--- Summary of Master Tables ---\n\n")
  cat(glue("Sex: {nrow(sex_master)} patients\n"))
  cat(glue("Date of Birth: {nrow(dob_master)} patients\n"))
  cat(glue("Ethnicity: {nrow(ethnicity_master)} patients\n\n"))

  # Now we can join them to create a comprehensive patient demographics table
  cat("--- Creating Comprehensive Demographics Table ---\n\n")

  # Clean up column names for joining
  sex_clean <- sex_master %>%
    select(patient_id, sex_code, sex_description, sex_source = source_table)

  dob_clean <- dob_master %>%
    select(patient_id, date_of_birth, dob_source = source_table)

  ethnicity_clean <- ethnicity_master %>%
    select(patient_id, ethnicity_code, ethnicity_category,
           ethnicity_source = source_table)

  # Join all together
  demographics_master <- sex_clean %>%
    full_join(dob_clean, by = "patient_id") %>%
    full_join(ethnicity_clean, by = "patient_id")

  cat(glue("Master Demographics Table: {nrow(demographics_master)} patients\n\n"))

  cat("--- Sample of Master Table ---\n")
  print(head(demographics_master, 10))

  # Save the master table
  output_file <- "/mnt/user-data/outputs/demographics_master.csv"
  write.csv(demographics_master, output_file, row.names = FALSE)
  cat(glue("\n✓ Master table saved to: {output_file}\n"))

  DBI::dbDisconnect(conn)

  return(demographics_master)
}

# ============================================================================
# EXAMPLE 4: Handling Ties in Priority
# ============================================================================

example_handling_ties <- function() {
  cat("\n=== EXAMPLE 4: How the Function Handles Priority Ties ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)
  )

  cat("--- Checking for Priority Ties ---\n\n")

  # Check if any patients have multiple sources with the same priority
  ties <- sex_long %>%
    group_by(patient_id) %>%
    filter(n_distinct(source_priority) < n()) %>%
    arrange(patient_id, source_priority)

  if (nrow(ties) > 0) {
    cat(glue("Found {n_distinct(ties$patient_id)} patients with priority ties\n\n"))

    example_patient <- ties$patient_id[1]
    cat(glue("Example: Patient {example_patient}\n"))

    patient_ties <- ties %>%
      filter(patient_id == example_patient) %>%
      select(patient_id, source_table, source_priority, sex_code)

    print(patient_ties)

    cat("\n--- How get_highest_priority_per_patient() Handles This ---\n")
    cat("When multiple sources have the same priority:\n")
    cat("  1. The function sorts by patient_id and source_priority\n")
    cat("  2. It takes the FIRST row for each patient\n")
    cat("  3. This means the first source encountered (alphabetically) wins\n\n")

    sex_priority <- get_highest_priority_per_patient(sex_long)

    selected <- sex_priority %>%
      filter(patient_id == example_patient) %>%
      select(patient_id, source_table, source_priority, sex_code)

    cat("Selected row:\n")
    print(selected)

    cat("\nNOTE: To avoid arbitrary selection, ensure unique priorities in your config!\n")
  } else {
    cat("✓ No priority ties found - all sources have unique priorities per patient\n")
  }

  DBI::dbDisconnect(conn)

  return(sex_long)
}

# ============================================================================
# EXAMPLE 5: Tracking Data Provenance
# ============================================================================

example_track_provenance <- function() {
  cat("\n=== EXAMPLE 5: Tracking Data Provenance ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1500)
  )

  sex_priority <- get_highest_priority_per_patient(sex_long)

  cat("--- Data Provenance Analysis ---\n\n")
  cat("Which sources were selected as highest priority?\n\n")

  # Count by source
  provenance_summary <- sex_priority %>%
    count(source_table, source_priority, name = "n_patients") %>%
    arrange(source_priority)

  print(provenance_summary)

  # Calculate percentages
  total_patients <- sum(provenance_summary$n_patients)

  cat("\n--- Provenance Percentages ---\n")
  provenance_pct <- provenance_summary %>%
    mutate(percentage = round(100 * n_patients / total_patients, 1))

  for (i in 1:nrow(provenance_pct)) {
    source <- provenance_pct$source_table[i]
    pct <- provenance_pct$percentage[i]
    priority <- provenance_pct$source_priority[i]

    cat(glue("{source} (priority {priority}): {pct}% of patients\n"))
  }

  cat("\n--- Interpretation ---\n")
  cat("This shows which sources are actually being used for final values.\n")
  cat("If a high-priority source has low coverage, it may not be used much.\n")

  # Compare to original coverage
  cat("\n--- Comparing to Original Source Coverage ---\n")

  original_coverage <- sex_long %>%
    count(source_table, name = "n_patients_with_data") %>%
    arrange(desc(n_patients_with_data))

  comparison <- provenance_summary %>%
    select(source_table, n_selected = n_patients) %>%
    left_join(original_coverage, by = "source_table") %>%
    mutate(
      selection_rate = round(100 * n_selected / n_patients_with_data, 1)
    )

  print(comparison)

  cat("\nSelection rate = % of times this source was chosen when it had data\n")

  DBI::dbDisconnect(conn)

  return(list(
    priority_table = sex_priority,
    provenance = provenance_summary
  ))
}

# ============================================================================
# EXAMPLE 6: Quality Check After Priority Selection
# ============================================================================

example_quality_check <- function() {
  cat("\n=== EXAMPLE 6: Quality Check After Priority Selection ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  sex_priority <- get_highest_priority_per_patient(sex_long)

  cat("--- Quality Checks ---\n\n")

  # 1. Check: One row per patient
  n_patients_long <- n_distinct(sex_long$patient_id)
  n_rows_priority <- nrow(sex_priority)

  cat(glue("1. One row per patient check:\n"))
  cat(glue("   Unique patients in long format: {n_patients_long}\n"))
  cat(glue("   Rows in priority table: {n_rows_priority}\n"))

  if (n_patients_long == n_rows_priority) {
    cat("   ✓ PASS: One row per patient\n\n")
  } else {
    cat("   ✗ FAIL: Row count mismatch!\n\n")
  }

  # 2. Check: No missing sex codes
  missing_codes <- sum(is.na(sex_priority$sex_code))
  cat(glue("2. Completeness check:\n"))
  cat(glue("   Missing sex codes: {missing_codes}\n"))

  if (missing_codes == 0) {
    cat("   ✓ PASS: No missing values\n\n")
  } else {
    pct_missing <- round(100 * missing_codes / n_rows_priority, 1)
    cat(glue("   ⚠ WARNING: {pct_missing}% missing\n\n"))
  }

  # 3. Check: Distribution of values
  cat("3. Value distribution:\n")
  value_dist <- sex_priority %>%
    count(sex_code, sex_description, sort = TRUE)

  print(value_dist)

  # 4. Check: All patients from long format are in priority table
  cat("\n4. Patient retention check:\n")
  patients_long <- unique(sex_long$patient_id)
  patients_priority <- unique(sex_priority$patient_id)

  missing_patients <- setdiff(patients_long, patients_priority)

  if (length(missing_patients) == 0) {
    cat("   ✓ PASS: All patients retained\n")
  } else {
    cat(glue("   ✗ FAIL: {length(missing_patients)} patients lost\n"))
    cat("   Lost patients:", head(missing_patients, 10), "\n")
  }

  DBI::dbDisconnect(conn)

  return(sex_priority)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic priority selection
# sex_priority1 <- example_basic_priority_selection()

# Compare before and after
# sex_priority2 <- example_compare_before_after()

# Create master table
# demographics <- example_create_master_table()

# Handle ties
# ties_check <- example_handling_ties()

# Track provenance
# provenance <- example_track_provenance()

# Quality check
# quality <- example_quality_check()
