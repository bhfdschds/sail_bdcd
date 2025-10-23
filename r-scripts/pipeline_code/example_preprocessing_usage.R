# Example Script: Using the Data Preprocessing Module
#
# This script demonstrates how to use the preprocessing module with the
# asset creation pipeline. It shows various use cases and patterns.

# Load required libraries
library(dplyr)
library(yaml)

# Source preprocessing modules
source("pipeline_code/data_preprocessing.R")
source("pipeline_code/preprocessing_integration.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")
source("utility_code/db2_connection.R")

# ============================================================================
# EXAMPLE 1: Simple Preprocessing with Code Matching and Validation
# ============================================================================

example_1_simple_preprocessing <- function() {
  cat("\n=== EXAMPLE 1: Simple Code Matching and Validation ===\n")

  # Load configurations
  db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
  preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

  # Create database connection
  conn <- create_db2_connection(db_config)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  # Get cholesterol data with preprocessing
  # This will:
  # 1. Match test codes against lookup table
  # 2. Validate values are within 1.0-15.0 range
  # 3. Convert dates to proper Date format
  cholesterol_data <- get_asset_data_with_preprocessing(
    config = db_config,
    preprocessing_config = preprocessing_config,
    asset_name = "cholesterol",
    source_name = "gp_cholesterol",
    conn = conn,
    apply_preprocessing = TRUE
  )

  # View results
  cat("\nCholesterol Data Summary:\n")
  print(summary(cholesterol_data))

  cat("\nValidation Results:\n")
  print(table(cholesterol_data$cholesterol_valid, useNA = "ifany"))

  cat("\nSample of preprocessed data:\n")
  print(head(cholesterol_data, 10))

  return(cholesterol_data)
}


# ============================================================================
# EXAMPLE 2: Covariate Flagging - Pre-existing Conditions
# ============================================================================

example_2_covariate_flagging <- function() {
  cat("\n=== EXAMPLE 2: Covariate Flagging (Pre-existing Conditions) ===\n")

  # Load configurations
  db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
  preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

  # Create database connection
  conn <- create_db2_connection(db_config)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  # Prepare cohort with baseline dates
  # This is required for covariate/outcome flagging
  cat("\nPreparing cohort data...\n")
  cohort_data <- prepare_cohort_data(preprocessing_config, conn)

  if (!is.null(cohort_data)) {
    cat(sprintf("Cohort prepared: %d patients\n", nrow(cohort_data)))
    cat("Sample baseline dates:\n")
    print(head(cohort_data))
  }

  # Process diagnosis data with covariate flagging
  # This will flag patients with heart disease, cancer, diabetes BEFORE baseline
  diagnosis_data <- get_asset_data_with_preprocessing(
    config = db_config,
    preprocessing_config = preprocessing_config,
    asset_name = "diagnoses",
    source_name = "hospital_diagnoses",
    conn = conn,
    apply_preprocessing = TRUE,
    cohort_data = cohort_data
  )

  # Summarize covariates
  cat("\n--- Covariate Summary ---\n")
  cat("Heart Disease at baseline:\n")
  print(table(diagnosis_data$has_heart_disease, useNA = "ifany"))

  cat("\nCancer at baseline:\n")
  print(table(diagnosis_data$has_cancer, useNA = "ifany"))

  cat("\nDiabetes at baseline:\n")
  print(table(diagnosis_data$has_diabetes, useNA = "ifany"))

  # Cross-tabulation
  cat("\nPatients with multiple conditions:\n")
  multi_condition <- diagnosis_data %>%
    group_by(patient_id) %>%
    summarise(
      n_conditions = sum(c(has_heart_disease, has_cancer, has_diabetes), na.rm = TRUE),
      .groups = "drop"
    )
  print(table(multi_condition$n_conditions))

  return(diagnosis_data)
}


# ============================================================================
# EXAMPLE 3: Outcome Flagging - Follow-up Events
# ============================================================================

example_3_outcome_flagging <- function() {
  cat("\n=== EXAMPLE 3: Outcome Flagging (Follow-up Events) ===\n")

  # Load configurations
  db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
  preprocessing_config <- read_preprocessing_config("pipeline_config/preprocessing_config.yaml")

  # Create database connection
  conn <- create_db2_connection(db_config)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  # Prepare cohort
  cohort_data <- prepare_cohort_data(preprocessing_config, conn)

  # Process admission data with outcome flagging
  # This will flag patients with stroke AFTER baseline and record the date
  admission_data <- get_asset_data_with_preprocessing(
    config = db_config,
    preprocessing_config = preprocessing_config,
    asset_name = "admissions",
    source_name = "hospital_admissions",
    conn = conn,
    apply_preprocessing = TRUE,
    cohort_data = cohort_data
  )

  # Summarize outcomes
  cat("\n--- Outcome Summary ---\n")
  cat("Stroke outcomes:\n")
  print(table(admission_data$has_stroke_outcome, useNA = "ifany"))

  if ("stroke_date" %in% names(admission_data)) {
    cat("\nTime to stroke (days from baseline):\n")
    admission_data_with_time <- admission_data %>%
      left_join(cohort_data, by = "patient_id") %>%
      filter(has_stroke_outcome) %>%
      mutate(days_to_stroke = as.numeric(stroke_date - baseline_date))

    print(summary(admission_data_with_time$days_to_stroke))
  }

  return(admission_data)
}


# ============================================================================
# EXAMPLE 4: Complete Pipeline with Multiple Assets
# ============================================================================

example_4_complete_pipeline <- function() {
  cat("\n=== EXAMPLE 4: Complete Preprocessing Pipeline ===\n")

  # Run complete preprocessing pipeline for multiple assets
  results <- run_preprocessing_pipeline(
    db_config_file = "pipeline_code/db2_config_multi_source.yaml",
    preprocessing_config_file = "pipeline_code/preprocessing_config.yaml",
    asset_names = c("cholesterol", "diagnoses", "prescriptions", "lab_results"),
    output_dir = "/mnt/user-data/outputs/preprocessed/"
  )

  # Summary of all results
  cat("\n--- Pipeline Summary ---\n")
  summary_stats <- lapply(names(results), function(asset_name) {
    data <- results[[asset_name]]
    list(
      asset = asset_name,
      n_rows = nrow(data),
      n_patients = length(unique(data$patient_id)),
      n_columns = ncol(data),
      columns = paste(names(data), collapse = ", ")
    )
  })

  summary_df <- do.call(rbind, lapply(summary_stats, as.data.frame))
  print(summary_df)

  return(results)
}


# ============================================================================
# EXAMPLE 5: Integrating with Existing Long Format Pipeline
# ============================================================================

example_5_long_format_integration <- function() {
  cat("\n=== EXAMPLE 5: Long Format Asset Creation with Preprocessing ===\n")

  # Load configurations
  db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
  preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

  # Create database connection
  conn <- create_db2_connection(db_config)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  # Prepare cohort
  cohort_data <- prepare_cohort_data(preprocessing_config, conn)

  # Create long format asset with preprocessing
  # This combines ALL sources for the asset, applying preprocessing to each
  long_ethnicity <- create_long_format_asset_with_preprocessing(
    config = db_config,
    preprocessing_config = preprocessing_config,
    asset_name = "ethnicity",
    conn = conn,
    apply_preprocessing = TRUE,
    cohort_data = cohort_data
  )

  # Generate summary (existing function)
  cat("\n--- Long Format Summary ---\n")
  summary_data <- summarize_long_format_table(long_ethnicity, "ethnicity")
  print(summary_data)

  # Check conflicts (existing function)
  cat("\n--- Conflict Detection ---\n")
  conflicts <- check_conflicts(long_ethnicity, "ethnicity")
  if (!is.null(conflicts) && nrow(conflicts) > 0) {
    cat(sprintf("Found %d patients with conflicting ethnicity values\n", nrow(conflicts)))
    print(head(conflicts))
  } else {
    cat("No conflicts detected\n")
  }

  # Export (existing function)
  export_asset_table(long_ethnicity, "ethnicity_preprocessed", format = "rds")

  return(long_ethnicity)
}


# ============================================================================
# EXAMPLE 6: Manual Preprocessing (Without Full Pipeline)
# ============================================================================

example_6_manual_preprocessing <- function() {
  cat("\n=== EXAMPLE 6: Manual Preprocessing Application ===\n")

  # This example shows how to apply preprocessing directly to a data frame
  # without using the full pipeline integration

  # Load preprocessing config
  preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

  # Create sample data
  sample_data <- data.frame(
    patient_id = c("001", "002", "003", "004"),
    test_code = c("CHOL001", "CHOL002", "CHOL001", "CHOL003"),
    result_value = c(5.2, 4.8, 25.0, 3.1),  # One outlier (25.0)
    result_date = c("2020-06-15", "2020-07-20", "2020-08-10", "2020-09-05"),
    stringsAsFactors = FALSE
  )

  cat("\nOriginal data:\n")
  print(sample_data)

  # Get preprocessing steps for cholesterol data
  cholesterol_preprocessing <- preprocessing_config$preprocessing$cholesterol_data

  # Create mock database connection (would be real in practice)
  # conn <- create_db2_connection(db_config)

  # Apply preprocessing
  # Note: In practice you would pass a real database connection
  # For this example, we'll just apply validation and transformation
  preprocessed_data <- sample_data

  # Apply value validation manually
  preprocessed_data$cholesterol_valid <- (
    preprocessed_data$result_value >= 1.0 &
      preprocessed_data$result_value <= 15.0
  )

  # Apply date conversion
  preprocessed_data$result_date <- as.Date(preprocessed_data$result_date)

  cat("\nPreprocessed data:\n")
  print(preprocessed_data)

  cat("\nValidation summary:\n")
  cat(sprintf("Valid records: %d/%d\n",
              sum(preprocessed_data$cholesterol_valid),
              nrow(preprocessed_data)))

  return(preprocessed_data)
}


# ============================================================================
# EXAMPLE 7: Custom Preprocessing Configuration
# ============================================================================

example_7_custom_config <- function() {
  cat("\n=== EXAMPLE 7: Custom Preprocessing Configuration ===\n")

  # This example shows how to create a custom preprocessing configuration
  # in R code (without YAML file)

  # Create custom preprocessing config
  custom_preprocessing <- list(
    validate_age = list(
      type = "value_validation",
      column = "age",
      min_value = 0,
      max_value = 120,
      action = "flag",
      flag_column = "age_valid"
    ),
    clean_gender = list(
      type = "data_transformation",
      transform_type = "categorical_mapping",
      column = "gender",
      mapping = list(
        "1" = "Male",
        "2" = "Female",
        "M" = "Male",
        "F" = "Female"
      )
    )
  )

  # Sample data
  sample_data <- data.frame(
    patient_id = c("001", "002", "003", "004", "005"),
    age = c(45, 67, 150, 23, -5),  # Some invalid ages
    gender = c("1", "2", "M", "F", "1"),
    stringsAsFactors = FALSE
  )

  cat("\nOriginal data:\n")
  print(sample_data)

  # Apply preprocessing (would normally use apply_preprocessing function)
  # For demonstration, we'll apply manually:

  # Validate age
  sample_data$age_valid <- (sample_data$age >= 0 & sample_data$age <= 120)

  # Clean gender
  gender_mapping <- c("1" = "Male", "2" = "Female", "M" = "Male", "F" = "Female")
  sample_data$gender <- gender_mapping[sample_data$gender]

  cat("\nPreprocessed data:\n")
  print(sample_data)

  return(sample_data)
}


# ============================================================================
# MAIN FUNCTION - Run All Examples
# ============================================================================

run_all_examples <- function() {
  cat("\n")
  cat("========================================\n")
  cat("DATA PREPROCESSING MODULE - EXAMPLES\n")
  cat("========================================\n")

  examples <- list(
    "1" = list(name = "Simple Code Matching and Validation", func = example_1_simple_preprocessing),
    "2" = list(name = "Covariate Flagging", func = example_2_covariate_flagging),
    "3" = list(name = "Outcome Flagging", func = example_3_outcome_flagging),
    "4" = list(name = "Complete Pipeline", func = example_4_complete_pipeline),
    "5" = list(name = "Long Format Integration", func = example_5_long_format_integration),
    "6" = list(name = "Manual Preprocessing", func = example_6_manual_preprocessing),
    "7" = list(name = "Custom Configuration", func = example_7_custom_config)
  )

  cat("\nAvailable examples:\n")
  for (i in names(examples)) {
    cat(sprintf("%s. %s\n", i, examples[[i]]$name))
  }
  cat("0. Run all examples\n")
  cat("\nTo run a specific example:\n")
  cat("  source('pipeline_code/example_preprocessing_usage.R')\n")
  cat("  example_1_simple_preprocessing()  # or example_2_covariate_flagging(), etc.\n\n")
  cat("To run all examples:\n")
  cat("  run_all_examples()\n\n")

  # Note: Uncomment the following to automatically run all examples
  # for (example in examples) {
  #   tryCatch({
  #     example$func()
  #   }, error = function(e) {
  #     cat(sprintf("\nError in example '%s': %s\n", example$name, e$message))
  #   })
  # }
}

# Display usage information when script is sourced
run_all_examples()
