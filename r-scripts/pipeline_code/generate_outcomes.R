# R Script for Generating Outcomes
# Creates time-windowed outcomes from disease/treatment assets

library(dplyr)
library(lubridate)
library(glue)

# ============================================================================
# 1. Main Function: Generate Outcomes
# ============================================================================

generate_outcomes <- function(disease_treatment_asset,
                              cohort,
                              lookup_table,
                              outcome_name,
                              days_after_start = 0,
                              days_after_end = NULL,
                              selection_method = "min",
                              calculate_days_from_index = TRUE,
                              show_quality_report = TRUE) {
  # Generate outcomes from disease/treatment data with temporal windows
  #
  # Args:
  #   disease_treatment_asset: Long format asset (patient_id, event_date, code)
  #   cohort: Cohort with patient_id and index_date
  #   lookup_table: Lookup with code, name, description, terminology
  #   outcome_name: Name to filter from lookup_table
  #   days_after_start: Start of follow-up window (0 = day of index)
  #   days_after_end: End of follow-up window (NULL = any time after)
  #   selection_method: "min" (earliest) or "max" (latest) date
  #   calculate_days_from_index: Calculate days between index and outcome
  #   show_quality_report: Show quality assessment before filtering
  #
  # Returns:
  #   Data frame with patient_id, outcome_flag, outcome_date, days_from_index

  cat(glue("\n=== Generating Outcome: {outcome_name} ===\n\n"))

  # Filter lookup table for this outcome
  codes <- lookup_table %>%
    filter(name == outcome_name) %>%
    pull(code)

  if (length(codes) == 0) {
    stop(glue("No codes found for outcome name '{outcome_name}'"))
  }

  cat(glue("Found {length(codes)} codes for '{outcome_name}'\n"))

  # Filter disease/treatment asset for relevant codes
  filtered_data <- disease_treatment_asset %>%
    filter(code %in% codes)

  cat(glue("Filtered to {nrow(filtered_data)} records\n"))

  # Quality assessment before filtering dates
  if (show_quality_report) {
    quality_report <- generate_quality_report(
      filtered_data, cohort, lookup_table, outcome_name
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
  windowed_data <- apply_followup_window(
    data_with_index,
    days_after_start,
    days_after_end
  )

  cat(glue("After temporal filter: {nrow(windowed_data)} records\n"))

  # Select earliest or latest date per patient
  outcome_data <- select_date_per_patient(
    windowed_data,
    selection_method
  )

  # Create outcome output
  result <- cohort %>%
    select(patient_id, index_date) %>%
    left_join(
      outcome_data %>%
        select(patient_id, event_date) %>%
        rename(outcome_date = event_date),
      by = "patient_id"
    ) %>%
    mutate(outcome_flag = !is.na(outcome_date))

  # Calculate days from index if requested
  if (calculate_days_from_index) {
    result <- result %>%
      mutate(
        days_from_index = as.numeric(
          difftime(outcome_date, index_date, units = "days")
        )
      )
  }

  # Summary
  n_with_outcome <- sum(result$outcome_flag)
  pct_with_outcome <- round(100 * n_with_outcome / nrow(result), 1)

  cat(glue("\n=== Outcome Summary ===\n"))
  cat(glue("Patients with outcome: {n_with_outcome} / {nrow(result)} ({pct_with_outcome}%)\n"))

  if (calculate_days_from_index && n_with_outcome > 0) {
    cat(glue("Mean days after index: {round(mean(result$days_from_index, na.rm=TRUE), 1)}\n"))
    cat(glue("Median days after index: {round(median(result$days_from_index, na.rm=TRUE), 1)}\n"))
  }

  return(result)
}

# ============================================================================
# 2. Helper Functions
# ============================================================================

apply_followup_window <- function(data, days_after_start, days_after_end) {
  # Apply temporal follow-up window relative to index date
  #
  # Args:
  #   data: Data with patient_id, event_date, index_date
  #   days_after_start: Start of follow-up (0 = day of index)
  #   days_after_end: End of follow-up (NULL = unlimited)

  result <- data %>%
    mutate(
      days_from_index = as.numeric(
        difftime(event_date, index_date, units = "days")
      )
    )

  # Filter: event must be on or after (index + days_after_start)
  result <- result %>%
    filter(days_from_index >= days_after_start)

  # Filter: event must be on or before (index + days_after_end)
  if (!is.null(days_after_end)) {
    result <- result %>%
      filter(days_from_index <= days_after_end)
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

generate_quality_report <- function(data, cohort, lookup_table, outcome_name) {
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
  cat(glue("   Patients with '{outcome_name}': {n_patients_with_data} / {nrow(cohort)} ({pct_coverage}%)\n"))

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

generate_multiple_outcomes <- function(disease_treatment_asset,
                                       cohort,
                                       lookup_table,
                                       outcome_names,
                                       days_after_start = 0,
                                       days_after_end = NULL,
                                       selection_method = "min",
                                       calculate_days_from_index = TRUE) {
  # Generate multiple outcomes and combine into wide format
  #
  # Args:
  #   Same as generate_outcomes, but outcome_names is a vector
  #
  # Returns:
  #   Wide format data frame with all outcomes

  cat("\n=== Generating Multiple Outcomes ===\n")
  cat(glue("Processing {length(outcome_names)} outcomes\n\n"))

  all_outcomes <- list()

  for (outcome_name in outcome_names) {
    outcome_data <- generate_outcomes(
      disease_treatment_asset = disease_treatment_asset,
      cohort = cohort,
      lookup_table = lookup_table,
      outcome_name = outcome_name,
      days_after_start = days_after_start,
      days_after_end = days_after_end,
      selection_method = selection_method,
      calculate_days_from_index = calculate_days_from_index,
      show_quality_report = FALSE
    )

    # Rename columns to include outcome name
    outcome_data <- outcome_data %>%
      select(-index_date) %>%
      rename_with(
        ~ paste0(outcome_name, "_", .x),
        .cols = -patient_id
      )

    all_outcomes[[outcome_name]] <- outcome_data
  }

  # Combine all outcomes
  result <- cohort %>%
    select(patient_id, index_date)

  for (outcome_data in all_outcomes) {
    result <- result %>%
      left_join(outcome_data, by = "patient_id")
  }

  cat(glue("\n=== Combined Outcomes ===\n"))
  cat(glue("Final dataset: {nrow(result)} patients, {ncol(result)} columns\n"))

  return(result)
}

# ============================================================================
# 4. Example Usage
# ============================================================================

example_generate_outcomes <- function() {
  # Example: Generate heart attack and stroke outcomes

  # Load assets
  cohort <- readRDS("/mnt/user-data/outputs/study_cohort.rds")
  disease_asset <- readRDS("/mnt/user-data/outputs/hospital_admissions_long_format.rds")

  # Create lookup table
  lookup_table <- data.frame(
    code = c("I21", "I22", "I63", "I64"),
    name = c("heart_attack", "heart_attack", "stroke", "stroke"),
    description = c("Acute MI", "Subsequent MI",
                    "Cerebral infarction", "Stroke NOS"),
    terminology = c("ICD10", "ICD10", "ICD10", "ICD10")
  )

  # Generate heart attack outcome (within 365 days after index)
  heart_attack_out <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    outcome_name = "heart_attack",
    days_after_start = 0,
    days_after_end = 365,
    selection_method = "min"
  )

  # Generate stroke outcome (any time after index)
  stroke_out <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    outcome_name = "stroke",
    days_after_start = 0,
    days_after_end = NULL,
    selection_method = "min"
  )

  # Or generate multiple at once
  outcomes <- generate_multiple_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup_table,
    outcome_names = c("heart_attack", "stroke"),
    days_after_start = 0,
    days_after_end = 365,
    selection_method = "min"
  )

  # Export
  saveRDS(outcomes, "/mnt/user-data/outputs/cohort_with_outcomes.rds")

  return(outcomes)
}
