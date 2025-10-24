# R Script for Generating Study Cohorts
# Applies demographic restrictions to create a cohort with index dates

library(dplyr)
library(lubridate)
library(glue)

# ============================================================================
# 1. Main Function: Generate Cohort
# ============================================================================

generate_cohort <- function(demographics_asset,
                            index_date,
                            min_age = NULL,
                            max_age = NULL,
                            require_known_sex = TRUE,
                            require_known_ethnicity = FALSE,
                            require_lsoa = FALSE) {
  # Generate cohort by applying demographic restrictions
  #
  # Args:
  #   demographics_asset: Data frame with patient_id, date_of_birth, sex_code,
  #                       ethnicity_code, lsoa_code
  #   index_date: Single date or vector of dates (one per patient)
  #   min_age: Minimum age at index date (optional)
  #   max_age: Maximum age at index date (optional)
  #   require_known_sex: Exclude patients with missing sex (default TRUE)
  #   require_known_ethnicity: Exclude patients with missing ethnicity (default FALSE)
  #   require_lsoa: Exclude patients with missing LSOA (default FALSE)
  #
  # Returns:
  #   Data frame with patient_id, index_date, age_at_index, and demographic fields

  cat("\n=== Generating Cohort ===\n\n")
  cat(glue("Starting with {nrow(demographics_asset)} patients\n"))

  # Ensure index_date is a Date object
  if (length(index_date) == 1) {
    index_date <- as.Date(index_date)
  } else if (length(index_date) != nrow(demographics_asset)) {
    stop("index_date must be either a single date or a vector matching nrow(demographics_asset)")
  }

  # Add index date to data
  cohort <- demographics_asset %>%
    mutate(index_date = index_date)

  # Calculate age at index date
  cohort <- cohort %>%
    mutate(
      age_at_index = as.numeric(
        difftime(index_date, date_of_birth, units = "days")
      ) / 365.25
    )

  # Apply restrictions
  n_excluded <- 0

  # Age restrictions
  if (!is.null(min_age)) {
    n_before <- nrow(cohort)
    cohort <- cohort %>% filter(age_at_index >= min_age)
    n_excluded_age_min <- n_before - nrow(cohort)
    cat(glue("Excluded {n_excluded_age_min} patients with age < {min_age}\n"))
    n_excluded <- n_excluded + n_excluded_age_min
  }

  if (!is.null(max_age)) {
    n_before <- nrow(cohort)
    cohort <- cohort %>% filter(age_at_index <= max_age)
    n_excluded_age_max <- n_before - nrow(cohort)
    cat(glue("Excluded {n_excluded_age_max} patients with age > {max_age}\n"))
    n_excluded <- n_excluded + n_excluded_age_max
  }

  # Known sex restriction
  if (require_known_sex) {
    n_before <- nrow(cohort)
    cohort <- cohort %>% filter(!is.na(sex_code) & sex_code != "")
    n_excluded_sex <- n_before - nrow(cohort)
    cat(glue("Excluded {n_excluded_sex} patients with unknown sex\n"))
    n_excluded <- n_excluded + n_excluded_sex
  }

  # Known ethnicity restriction
  if (require_known_ethnicity) {
    n_before <- nrow(cohort)
    cohort <- cohort %>% filter(!is.na(ethnicity_code) & ethnicity_code != "")
    n_excluded_ethnicity <- n_before - nrow(cohort)
    cat(glue("Excluded {n_excluded_ethnicity} patients with unknown ethnicity\n"))
    n_excluded <- n_excluded + n_excluded_ethnicity
  }

  # LSOA restriction
  if (require_lsoa) {
    n_before <- nrow(cohort)
    cohort <- cohort %>% filter(!is.na(lsoa_code) & lsoa_code != "")
    n_excluded_lsoa <- n_before - nrow(cohort)
    cat(glue("Excluded {n_excluded_lsoa} patients with missing LSOA\n"))
    n_excluded <- n_excluded + n_excluded_lsoa
  }

  cat(glue("\nFinal cohort: {nrow(cohort)} patients ({n_excluded} excluded)\n"))

  # Print summary statistics
  cat("\n=== Cohort Summary ===\n")
  cat(glue("Age range: {round(min(cohort$age_at_index, na.rm=TRUE), 1)} - {round(max(cohort$age_at_index, na.rm=TRUE), 1)} years\n"))
  cat(glue("Mean age: {round(mean(cohort$age_at_index, na.rm=TRUE), 1)} years\n"))

  if ("sex_code" %in% names(cohort)) {
    sex_counts <- cohort %>% count(sex_code)
    cat("\nSex distribution:\n")
    print(sex_counts)
  }

  return(cohort)
}

# ============================================================================
# 2. Helper Function: Combine Demographics Assets
# ============================================================================

combine_demographics <- function(dob_asset, sex_asset, ethnicity_asset = NULL,
                                 lsoa_asset = NULL) {
  # Combine multiple demographic assets into a single table
  #
  # Args:
  #   dob_asset: Date of birth asset (patient_id, date_of_birth)
  #   sex_asset: Sex asset (patient_id, sex_code)
  #   ethnicity_asset: Ethnicity asset (patient_id, ethnicity_code) - optional
  #   lsoa_asset: LSOA asset (patient_id, lsoa_code) - optional
  #
  # Returns:
  #   Combined demographics data frame

  cat("\n=== Combining Demographics Assets ===\n\n")

  # Start with DOB
  demographics <- dob_asset %>%
    select(patient_id, date_of_birth)

  cat(glue("Starting with {nrow(demographics)} patients from DOB asset\n"))

  # Add sex
  demographics <- demographics %>%
    left_join(
      sex_asset %>% select(patient_id, sex_code),
      by = "patient_id"
    )

  cat(glue("Added sex: {sum(!is.na(demographics$sex_code))} patients with known sex\n"))

  # Add ethnicity if provided
  if (!is.null(ethnicity_asset)) {
    demographics <- demographics %>%
      left_join(
        ethnicity_asset %>% select(patient_id, ethnicity_code),
        by = "patient_id"
      )
    cat(glue("Added ethnicity: {sum(!is.na(demographics$ethnicity_code))} patients with known ethnicity\n"))
  }

  # Add LSOA if provided
  if (!is.null(lsoa_asset)) {
    demographics <- demographics %>%
      left_join(
        lsoa_asset %>% select(patient_id, lsoa_code),
        by = "patient_id"
      )
    cat(glue("Added LSOA: {sum(!is.na(demographics$lsoa_code))} patients with known LSOA\n"))
  }

  cat(glue("\nCombined demographics: {nrow(demographics)} patients, {ncol(demographics)} columns\n"))

  return(demographics)
}

# ============================================================================
# 3. Example Usage
# ============================================================================

example_generate_cohort <- function() {
  # Example: Generate cohort with age and sex restrictions

  # Load demographics assets (assuming they've been created)
  dob_asset <- readRDS("/mnt/user-data/outputs/date_of_birth_long_format.rds")
  sex_asset <- readRDS("/mnt/user-data/outputs/sex_long_format.rds")
  ethnicity_asset <- readRDS("/mnt/user-data/outputs/ethnicity_long_format.rds")
  lsoa_asset <- readRDS("/mnt/user-data/outputs/lsoa_long_format.rds")

  # Get highest priority per patient
  dob_clean <- get_highest_priority_per_patient(dob_asset)
  sex_clean <- get_highest_priority_per_patient(sex_asset)
  ethnicity_clean <- get_highest_priority_per_patient(ethnicity_asset)
  lsoa_clean <- get_highest_priority_per_patient(lsoa_asset)

  # Combine demographics
  demographics <- combine_demographics(
    dob_clean, sex_clean, ethnicity_clean, lsoa_clean
  )

  # Generate cohort with restrictions
  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = as.Date("2024-01-01"),
    min_age = 18,
    max_age = 100,
    require_known_sex = TRUE,
    require_known_ethnicity = FALSE,
    require_lsoa = TRUE
  )

  # Export cohort
  saveRDS(cohort, "/mnt/user-data/outputs/study_cohort.rds")
  write.csv(cohort, "/mnt/user-data/outputs/study_cohort.csv", row.names = FALSE)

  return(cohort)
}
