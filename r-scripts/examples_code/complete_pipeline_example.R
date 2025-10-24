# Complete Pipeline Example
# Demonstrates the full data curation pipeline from raw data to analysis-ready dataset

library(yaml)
library(DBI)
library(dplyr)
library(glue)

# Load all pipeline functions
source("../pipeline_code/create_long_format_assets.R")
source("../pipeline_code/generate_cohort.R")
source("../pipeline_code/generate_covariates.R")
source("../pipeline_code/generate_outcomes.R")
source("../utility_code/db2_connection.R")

# ============================================================================
# STEP 1: Curate Demographics Assets
# ============================================================================

curate_demographics <- function(config_path = "../pipeline_code/db2_config_multi_source.yaml",
                                patient_ids = NULL) {
  cat("\n========================================\n")
  cat("STEP 1: Curating Demographics Assets\n")
  cat("========================================\n\n")

  # Load configuration
  config <- read_db_config(config_path)

  # Connect to database
  conn <- create_db2_connection(config)

  # Create long format tables for demographics
  demographics_assets <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = patient_ids,
    assets = c("date_of_birth", "sex", "ethnicity", "lsoa")
  )

  # Get highest priority per patient for each asset
  dob_clean <- get_highest_priority_per_patient(demographics_assets$date_of_birth)
  sex_clean <- get_highest_priority_per_patient(demographics_assets$sex)
  ethnicity_clean <- get_highest_priority_per_patient(demographics_assets$ethnicity)
  lsoa_clean <- get_highest_priority_per_patient(demographics_assets$lsoa)

  # Combine into single demographics table
  demographics <- combine_demographics(
    dob_asset = dob_clean,
    sex_asset = sex_clean,
    ethnicity_asset = ethnicity_clean,
    lsoa_asset = lsoa_clean
  )

  # Export
  saveRDS(demographics, "/mnt/user-data/outputs/demographics_combined.rds")
  cat("\n✓ Demographics saved to: /mnt/user-data/outputs/demographics_combined.rds\n")

  DBI::dbDisconnect(conn)

  return(demographics)
}

# ============================================================================
# STEP 2: Curate Disease and Treatment Assets
# ============================================================================

curate_disease_treatment <- function(config_path = "../pipeline_code/db2_config_multi_source.yaml",
                                     patient_ids = NULL) {
  cat("\n========================================\n")
  cat("STEP 2: Curating Disease & Treatment Assets\n")
  cat("========================================\n\n")

  # Load configuration
  config <- read_db_config(config_path)

  # Connect to database
  conn <- create_db2_connection(config)

  # Create long format tables for disease/treatment data
  disease_assets <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = patient_ids,
    assets = c("hospital_admissions", "primary_care",
               "primary_care_medicines", "deaths")
  )

  # Export each asset
  saveRDS(disease_assets$hospital_admissions,
          "/mnt/user-data/outputs/hospital_admissions_long_format.rds")
  saveRDS(disease_assets$primary_care,
          "/mnt/user-data/outputs/primary_care_long_format.rds")
  saveRDS(disease_assets$primary_care_medicines,
          "/mnt/user-data/outputs/primary_care_medicines_long_format.rds")
  saveRDS(disease_assets$deaths,
          "/mnt/user-data/outputs/deaths_long_format.rds")

  cat("\n✓ Disease/treatment assets saved\n")

  DBI::dbDisconnect(conn)

  return(disease_assets)
}

# ============================================================================
# STEP 3: Generate Cohort
# ============================================================================

create_study_cohort <- function(demographics,
                                index_date = as.Date("2024-01-01"),
                                min_age = 18,
                                max_age = 100) {
  cat("\n========================================\n")
  cat("STEP 3: Generating Study Cohort\n")
  cat("========================================\n\n")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    min_age = min_age,
    max_age = max_age,
    require_known_sex = TRUE,
    require_known_ethnicity = FALSE,
    require_lsoa = TRUE
  )

  # Export
  saveRDS(cohort, "/mnt/user-data/outputs/study_cohort.rds")
  write.csv(cohort, "/mnt/user-data/outputs/study_cohort.csv", row.names = FALSE)

  cat("\n✓ Cohort saved to: /mnt/user-data/outputs/study_cohort.rds\n")

  return(cohort)
}

# ============================================================================
# STEP 4: Generate Covariates
# ============================================================================

create_covariates <- function(disease_assets, cohort) {
  cat("\n========================================\n")
  cat("STEP 4: Generating Covariates\n")
  cat("========================================\n\n")

  # Create lookup table for covariates
  covariate_lookup <- data.frame(
    code = c(
      # Diabetes codes (ICD10)
      "E10", "E11", "E12", "E13", "E14",
      # Hypertension codes (ICD10)
      "I10", "I11", "I12", "I13", "I15",
      # COPD codes (ICD10)
      "J44", "J43",
      # CHD codes (ICD10)
      "I20", "I21", "I22", "I23", "I24", "I25"
    ),
    name = c(
      "diabetes", "diabetes", "diabetes", "diabetes", "diabetes",
      "hypertension", "hypertension", "hypertension", "hypertension", "hypertension",
      "copd", "copd",
      "chd", "chd", "chd", "chd", "chd", "chd"
    ),
    description = c(
      "Type 1 diabetes", "Type 2 diabetes", "Diabetes related to malnutrition",
      "Other specified diabetes", "Unspecified diabetes",
      "Essential hypertension", "Hypertensive heart disease",
      "Hypertensive renal disease", "Hypertensive heart and renal disease",
      "Secondary hypertension",
      "COPD", "Emphysema",
      "Angina pectoris", "Acute MI", "Subsequent MI",
      "Complications following MI", "Other acute IHD", "Chronic IHD"
    ),
    terminology = "ICD10"
  )

  # Combine hospital and primary care data
  disease_data <- bind_rows(
    disease_assets$hospital_admissions,
    disease_assets$primary_care
  )

  # Generate multiple covariates
  covariates <- generate_multiple_covariates(
    disease_treatment_asset = disease_data,
    cohort = cohort,
    lookup_table = covariate_lookup,
    covariate_names = c("diabetes", "hypertension", "copd", "chd"),
    days_before_start = NULL,  # Any time before
    days_before_end = 0,       # Up to day before index
    selection_method = "min",  # First occurrence
    calculate_days_to_index = TRUE
  )

  # Export
  saveRDS(covariates, "/mnt/user-data/outputs/cohort_with_covariates.rds")
  write.csv(covariates, "/mnt/user-data/outputs/cohort_with_covariates.csv", row.names = FALSE)

  cat("\n✓ Covariates saved to: /mnt/user-data/outputs/cohort_with_covariates.rds\n")

  return(covariates)
}

# ============================================================================
# STEP 5: Generate Outcomes
# ============================================================================

create_outcomes <- function(disease_assets, cohort,
                            follow_up_days = 365) {
  cat("\n========================================\n")
  cat("STEP 5: Generating Outcomes\n")
  cat("========================================\n\n")

  # Create lookup table for outcomes
  outcome_lookup <- data.frame(
    code = c(
      # Heart attack (ICD10)
      "I21", "I22",
      # Stroke (ICD10)
      "I63", "I64",
      # Heart failure (ICD10)
      "I50",
      # Death (all causes)
      "DEATH"
    ),
    name = c(
      "heart_attack", "heart_attack",
      "stroke", "stroke",
      "heart_failure",
      "death"
    ),
    description = c(
      "Acute MI", "Subsequent MI",
      "Cerebral infarction", "Stroke NOS",
      "Heart failure",
      "Death (all causes)"
    ),
    terminology = "ICD10"
  )

  # Combine hospital and death data for outcomes
  outcome_data <- bind_rows(
    disease_assets$hospital_admissions,
    disease_assets$deaths
  )

  # Generate multiple outcomes
  outcomes <- generate_multiple_outcomes(
    disease_treatment_asset = outcome_data,
    cohort = cohort,
    lookup_table = outcome_lookup,
    outcome_names = c("heart_attack", "stroke", "heart_failure", "death"),
    days_after_start = 0,          # From index date
    days_after_end = follow_up_days,  # Within follow-up period
    selection_method = "min",      # First occurrence
    calculate_days_from_index = TRUE
  )

  # Export
  saveRDS(outcomes, "/mnt/user-data/outputs/cohort_with_outcomes.rds")
  write.csv(outcomes, "/mnt/user-data/outputs/cohort_with_outcomes.csv", row.names = FALSE)

  cat("\n✓ Outcomes saved to: /mnt/user-data/outputs/cohort_with_outcomes.rds\n")

  return(outcomes)
}

# ============================================================================
# STEP 6: Combine Final Dataset
# ============================================================================

create_final_dataset <- function(cohort, covariates, outcomes) {
  cat("\n========================================\n")
  cat("STEP 6: Creating Final Analysis Dataset\n")
  cat("========================================\n\n")

  # Join covariates
  final_data <- cohort %>%
    left_join(
      covariates %>% select(-index_date),
      by = "patient_id"
    )

  # Join outcomes
  final_data <- final_data %>%
    left_join(
      outcomes %>% select(-index_date),
      by = "patient_id"
    )

  cat(glue("Final dataset: {nrow(final_data)} patients, {ncol(final_data)} columns\n\n"))

  # Summary statistics
  cat("=== Dataset Summary ===\n\n")

  cat("Demographics:\n")
  cat(glue("  Age: {round(mean(final_data$age_at_index), 1)} ± {round(sd(final_data$age_at_index), 1)} years\n"))

  sex_counts <- final_data %>% count(sex_code)
  cat("  Sex:\n")
  print(sex_counts)

  cat("\nCovariates:\n")
  cat(glue("  Diabetes: {sum(final_data$diabetes_covariate_flag)} ({round(100*mean(final_data$diabetes_covariate_flag), 1)}%)\n"))
  cat(glue("  Hypertension: {sum(final_data$hypertension_covariate_flag)} ({round(100*mean(final_data$hypertension_covariate_flag), 1)}%)\n"))
  cat(glue("  COPD: {sum(final_data$copd_covariate_flag)} ({round(100*mean(final_data$copd_covariate_flag), 1)}%)\n"))
  cat(glue("  CHD: {sum(final_data$chd_covariate_flag)} ({round(100*mean(final_data$chd_covariate_flag), 1)}%)\n"))

  cat("\nOutcomes:\n")
  cat(glue("  Heart attack: {sum(final_data$heart_attack_outcome_flag)} ({round(100*mean(final_data$heart_attack_outcome_flag), 1)}%)\n"))
  cat(glue("  Stroke: {sum(final_data$stroke_outcome_flag)} ({round(100*mean(final_data$stroke_outcome_flag), 1)}%)\n"))
  cat(glue("  Heart failure: {sum(final_data$heart_failure_outcome_flag)} ({round(100*mean(final_data$heart_failure_outcome_flag), 1)}%)\n"))
  cat(glue("  Death: {sum(final_data$death_outcome_flag)} ({round(100*mean(final_data$death_outcome_flag), 1)}%)\n"))

  # Export
  saveRDS(final_data, "/mnt/user-data/outputs/final_analysis_dataset.rds")
  write.csv(final_data, "/mnt/user-data/outputs/final_analysis_dataset.csv", row.names = FALSE)

  cat("\n✓ Final dataset saved to: /mnt/user-data/outputs/final_analysis_dataset.rds\n")

  return(final_data)
}

# ============================================================================
# MAIN PIPELINE FUNCTION
# ============================================================================

run_complete_pipeline <- function(config_path = "../pipeline_code/db2_config_multi_source.yaml",
                                  patient_ids = NULL,
                                  index_date = as.Date("2024-01-01"),
                                  min_age = 18,
                                  max_age = 100,
                                  follow_up_days = 365) {
  # Run the complete data curation pipeline
  #
  # Args:
  #   config_path: Path to YAML configuration file
  #   patient_ids: Optional vector of patient IDs to process (NULL = all)
  #   index_date: Index date for cohort
  #   min_age: Minimum age at index
  #   max_age: Maximum age at index
  #   follow_up_days: Days of follow-up for outcomes
  #
  # Returns:
  #   List with all pipeline outputs

  cat("\n")
  cat("╔════════════════════════════════════════╗\n")
  cat("║  COMPLETE DATA CURATION PIPELINE       ║\n")
  cat("╚════════════════════════════════════════╝\n")

  start_time <- Sys.time()

  # Step 1: Curate demographics
  demographics <- curate_demographics(config_path, patient_ids)

  # Step 2: Curate disease/treatment data
  disease_assets <- curate_disease_treatment(config_path, patient_ids)

  # Step 3: Generate cohort
  cohort <- create_study_cohort(demographics, index_date, min_age, max_age)

  # Step 4: Generate covariates
  covariates <- create_covariates(disease_assets, cohort)

  # Step 5: Generate outcomes
  outcomes <- create_outcomes(disease_assets, cohort, follow_up_days)

  # Step 6: Create final dataset
  final_data <- create_final_dataset(cohort, covariates, outcomes)

  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")

  cat("\n")
  cat("╔════════════════════════════════════════╗\n")
  cat("║  PIPELINE COMPLETE!                    ║\n")
  cat("╚════════════════════════════════════════╝\n")
  cat(glue("\nTotal runtime: {round(duration, 1)} minutes\n"))

  return(list(
    demographics = demographics,
    disease_assets = disease_assets,
    cohort = cohort,
    covariates = covariates,
    outcomes = outcomes,
    final_data = final_data
  ))
}

# ============================================================================
# USAGE EXAMPLES
# ============================================================================

# Example 1: Run complete pipeline with sample of patients
# results <- run_complete_pipeline(
#   patient_ids = 1001:2000,
#   index_date = as.Date("2024-01-01"),
#   min_age = 18,
#   max_age = 100,
#   follow_up_days = 365
# )

# Example 2: Run complete pipeline with all patients
# results <- run_complete_pipeline(
#   patient_ids = NULL,  # All patients
#   index_date = as.Date("2024-01-01"),
#   follow_up_days = 730  # 2 years follow-up
# )

# Example 3: Run individual steps
# demographics <- curate_demographics(patient_ids = 1001:1100)
# cohort <- create_study_cohort(demographics, index_date = as.Date("2024-01-01"))
