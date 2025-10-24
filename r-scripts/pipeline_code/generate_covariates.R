# R Script for Generating Covariates
# Creates time-windowed covariates from disease/treatment assets

library(dplyr)
library(lubridate)
library(glue)

# ============================================================================
# 1. Main Function: Generate Covariates
# ============================================================================

generate_covariates <- function(disease_treatment_asset,
                                cohort,
                                lookup_table,
                                covariate_name,
                                days_before_start = NULL,
                                days_before_end = 0,
                                selection_method = "min",
                                calculate_days_to_index = TRUE,
                                show_quality_report = TRUE) {
  # Generate covariates from disease/treatment data with temporal windows
  #
  # Args:
  #   disease_treatment_asset: Long format asset (patient_id, event_date, code)
  #   cohort: Cohort with patient_id and index_date
  #   lookup_table: Lookup with code, name, description, terminology
  #   covariate_name: Name to filter from lookup_table
  #   days_before_start: Start of lookback window (NULL = any time before)
  #   days_before_end: End of lookback window (0 = day before index)
  #   selection_method: "min" (earliest) or "max" (latest) date
  #   calculate_days_to_index: Calculate days between covariate and index
  #   show_quality_report: Show quality assessment before filtering
  #
  # Returns:
  #   Data frame with patient_id, covariate_flag, covariate_date, days_to_index

  cat(glue("\n=== Generating Covariate: {covariate_name} ===\n\n"))

  # Filter lookup table for this covariate
  codes <- lookup_table %>%
    filter(name == covariate_name) %>%
    pull(code)

  if (length(codes) == 0) {
    stop(glue("No codes found for covariate name '{covariate_name}'"))
  }

  cat(glue("Found {length(codes)} codes for '{covariate_name}'\n"))

  # Filter disease/treatment asset for relevant codes
  filtered_data <- disease_treatment_asset %>%
    filter(code %in% codes)

  cat(glue("Filtered to {nrow(filtered_data)} records\n"))

  # Quality assessment before filtering dates
  if (show_quality_report) {
    quality_report <- generate_quality_report(
      filtered_data, cohort, lookup_table, covariate_name
    )
    print(quality_report)
  }

  # Join with cohort to get index dates
  data_with_index <- filtered_data %>%
    inner_join(
      cohort %>% select(patient_id, index_date),
      by = "patient_id"
    )

  cat(glue("\nMatched {nrow(data_with_index)} records to {n_distinct(data_with_index$patient_id)} cohort patients\n"))

  # Apply temporal window
  windowed_data <- apply_lookback_window(
    data_with_index,
    days_before_start,
    days_before_end
  )

  cat(glue("After temporal filter: {nrow(windowed_data)} records\n"))

  # Select earliest or latest date per patient
  covariate_data <- select_date_per_patient(
    windowed_data,
    selection_method
  )

  # Create covariate output
  result <- cohort %>%
    select(patient_id, index_date) %>%
    left_join(
      covariate_data %>%
        select(patient_id, event_date) %>%
        rename(covariate_date = event_date),
      by = "patient_id"
    ) %>%
    mutate(covariate_flag = !is.na(covariate_date))

  # Calculate days to index if requested
  if (calculate_days_to_index) {
    result <- result %>%
      mutate(
        days_to_index = as.numeric(
          difftime(covariate_date, index_date, units = "days")
        )
      )
  }

  # Summary
  n_with_covariate <- sum(result$covariate_flag)
  pct_with_covariate <- round(100 * n_with_covariate / nrow(result), 1)

  cat(glue("\n=== Covariate Summary ===\n"))
  cat(glue("Patients with covariate: {n_with_covariate} / {nrow(result)} ({pct_with_covariate}%)\n"))

  if (calculate_days_to_index && n_with_covariate > 0) {
    cat(glue("Mean days before index: {round(abs(mean(result$days_to_index, na.rm=TRUE)), 1)}\n"))
    cat(glue("Median days before index: {round(abs(median(result$days_to_index, na.rm=TRUE)), 1)}\n"))
  }

  return(result)
}

# ============================================================================
# 2. Helper Functions
# ============================================================================

apply_lookback_window <- function(data, days_before_start, days_before_end) {
  # Apply temporal lookback window relative to index date
  #
  # Args:
  #   data: Data with patient_id, event_date, index_date
  #   days_before_start: Start of lookback (NULL = unlimited)
  #   days_before_end: End of lookback (0 = day before index)

  result <- data %>%
    mutate(
      days_from_index = as.numeric(
        difftime(event_date, index_date, units = "days")
      )
    )

  # Filter: event must be before or on (index - days_before_end)
  result <- result %>%
    filter(days_from_index <= -days_before_end)

  # Filter: event must be after or on (index - days_before_start)
  if (!is.null(days_before_start)) {
    result <- result %>%
      filter(days_from_index >= -days_before_start)
  }

  return(result)
}

select_date_per_patient <- function(data, method = "min") {
  # Select earliest or latest event per patient
  #
  # Args:
  #   data: Data with patient_id, event_date
  #   method: "min" (earliest) or "max" (latest)

  if (method == "min") {
    result <- data %>%
      group_by(patient_id) %>%
      slice_min(event_date, n = 1, with_ties = FALSE) %>%
      ungroup()
  } else if (method == "max") {
    result <- data %>%
      group_by(patient_id) %>%
      slice_max(event_date, n = 1, with_ties = FALSE) %>%
      ungroup()
  } else {
    stop("method must be 'min' or 'max'")
  }

  return(result)
}

generate_quality_report <- function(data, cohort, lookup_table, covariate_name) {
  # Generate quality assessment report
  #
  # Shows:
  # - Cohort counts and percentage by name
  # - Timeframe coverage and percentage by name
  # - Counts and percentages for each code and name

  cat("\n--- Quality Report ---\n")

  # 1. Cohort counts by name
  cohort_patients <- data %>%
    inner_join(cohort %>% select(patient_id), by = "patient_id") %>%
    distinct(patient_id)

  n_patients_with_data <- n_distinct(cohort_patients$patient_id)
  pct_coverage <- round(100 * n_patients_with_data / nrow(cohort), 1)

  cat(glue("\n1. Cohort Coverage:\n"))
  cat(glue("   Patients with '{covariate_name}': {n_patients_with_data} / {nrow(cohort)} ({pct_coverage}%)\n"))

  # 2. Timeframe coverage
  date_range <- data %>%
    summarise(
      earliest = min(event_date, na.rm = TRUE),
      latest = max(event_date, na.rm = TRUE)
    )

  cat(glue("\n2. Timeframe Coverage:\n"))
  cat(glue("   Date range: {date_range$earliest} to {date_range$latest}\n"))

  # 3. Counts by code
  code_counts <- data %>%
    left_join(lookup_table, by = "code") %>%
    group_by(code, name) %>%
    summarise(
      n_events = n(),
      n_patients = n_distinct(patient_id),
      .groups = "drop"
    ) %>%
    arrange(desc(n_events))

  cat(glue("\n3. Top Codes (showing first 10):\n"))
  print(head(code_counts, 10))

  return(list(
    cohort_coverage = pct_coverage,
    date_range = date_range,
    code_counts = code_counts
  ))
}

# ============================================================================
# 3. Batch Processing Function
# ============================================================================

generate_multiple_covariates <- function(disease_treatment_asset,
                                         cohort,
                                         lookup_table,
                                         covariate_names,
                                         days_before_start = NULL,
                                         days_before_end = 0,
                                         selection_method = "min",
                                         calculate_days_to_index = TRUE) {
  # Generate multiple covariates and combine into wide format
  #
  # Args:
  #   Same as generate_covariates, but covariate_names is a vector
  #
  # Returns:
  #   Wide format data frame with all covariates

  cat("\n=== Generating Multiple Covariates ===\n")
  cat(glue("Processing {length(covariate_names)} covariates\n\n"))

  all_covariates <- list()

  for (cov_name in covariate_names) {
    covariate_data <- generate_covariates(
      disease_treatment_asset = disease_treatment_asset,
      cohort = cohort,
      lookup_table = lookup_table,
      covariate_name = cov_name,
      days_before_start = days_before_start,
      days_before_end = days_before_end,
      selection_method = selection_method,
      calculate_days_to_index = calculate_days_to_index,
      show_quality_report = FALSE
    )

    # Rename columns to include covariate name
    covariate_data <- covariate_data %>%
      select(-index_date) %>%
      rename_with(
        ~ paste0(cov_name, "_", .x),
        .cols = -patient_id
      )

    all_covariates[[cov_name]] <- covariate_data
  }

  # Combine all covariates
  result <- cohort %>%
    select(patient_id, index_date)

  for (cov_data in all_covariates) {
    result <- result %>%
      left_join(cov_data, by = "patient_id")
  }

  cat(glue("\n=== Combined Covariates ===\n"))
  cat(glue("Final dataset: {nrow(result)} patients, {ncol(result)} columns\n"))

  return(result)
}

# ============================================================================
# 4. Example Usage
# ============================================================================

example_generate_covariates <- function(conn = NULL, use_database = TRUE) {
  # Example: Generate diabetes and hypertension covariates
  # Args:
  #   conn: Database connection (required if use_database = TRUE)
  #   use_database: If TRUE, read from/write to database; if FALSE, use RDS files

  # Source utility functions if using database
  if (use_database && !exists("read_from_db")) {
    source("scripts/utility_code/db_table_utils.R")
  }

  if (use_database) {
    # Load assets from database
    if (is.null(conn)) {
      stop("Database connection required when use_database = TRUE")
    }
    cohort <- read_from_db(conn, "STUDY_COHORT")
    disease_asset <- read_from_db(conn, "PRIMARY_CARE_LONG_FORMAT")
  } else {
    # Legacy: Load assets from RDS files
    cohort <- readRDS("/mnt/user-data/outputs/study_cohort.rds")
    disease_asset <- readRDS("/mnt/user-data/outputs/primary_care_long_format.rds")
  }

  # Create lookup table
  lookup_table <- data.frame(
    code = c("E10", "E11", "I10", "I11"),
    name = c("diabetes", "diabetes", "hypertension", "hypertension"),
    description = c("Type 1 diabetes", "Type 2 diabetes",
                    "Essential hypertension", "Secondary hypertension"),
    terminology = c("ICD10", "ICD10", "ICD10", "ICD10")
  )

  # Generate diabetes covariate (any time before index)
  diabetes_cov <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    covariate_name = "diabetes",
    days_before_start = NULL,
    days_before_end = 0,
    selection_method = "min"
  )

  # Generate hypertension covariate (last 365 days before index)
  hypertension_cov <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    covariate_name = "hypertension",
    days_before_start = 365,
    days_before_end = 0,
    selection_method = "max"
  )

  # Or generate multiple at once
  covariates <- generate_multiple_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    covariate_names = c("diabetes", "hypertension"),
    days_before_start = NULL,
    days_before_end = 0,
    selection_method = "min"
  )

  # Save covariates
  if (use_database && !is.null(conn)) {
    # Save to database
    save_to_db(conn, covariates, "COHORT_WITH_COVARIATES")
  } else {
    # Legacy: Save to file
    saveRDS(covariates, "/mnt/user-data/outputs/cohort_with_covariates.rds")
  }

  return(covariates)
}
