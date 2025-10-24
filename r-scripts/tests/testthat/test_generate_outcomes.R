# Tests for generate_outcomes.R

library(testthat)
library(dplyr)
library(lubridate)

source("../../pipeline_code/generate_outcomes.R")

# ============================================================================
# Test Data Setup
# ============================================================================

create_test_disease_asset <- function() {
  data.frame(
    patient_id = c(1, 1, 2, 2, 3, 3, 4, 5, 5, 6),
    event_date = as.Date(c(
      "2024-02-15", "2024-06-20", "2024-03-10", "2024-08-05",
      "2024-02-28", "2024-09-12", "2024-07-01", "2024-04-18", "2024-10-22", "2024-05-30"
    )),
    code = c("I21", "I21", "I63", "I21", "I21", "I63", "I21", "I63", "I63", "I21")
  )
}

create_test_cohort <- function() {
  data.frame(
    patient_id = 1:6,
    index_date = as.Date("2024-01-01")
  )
}

create_test_lookup <- function() {
  data.frame(
    code = c("I21", "I63"),
    name = c("heart_attack", "stroke"),
    description = c("Acute MI", "Cerebral infarction"),
    terminology = c("ICD10", "ICD10")
  )
}

# ============================================================================
# Tests for apply_followup_window
# ============================================================================

test_that("apply_followup_window filters correctly with unlimited end", {
  data <- data.frame(
    patient_id = c(1, 1, 1),
    event_date = as.Date(c("2024-01-15", "2024-06-01", "2024-12-01")),
    index_date = as.Date("2024-01-01")
  )

  result <- apply_followup_window(data, days_after_start = 0, days_after_end = NULL)

  # All events on or after index should be included
  expect_equal(nrow(result), 3)
})

test_that("apply_followup_window filters with bounded window", {
  data <- data.frame(
    patient_id = c(1, 1, 1, 1),
    event_date = as.Date(c("2024-01-05", "2024-06-01", "2024-11-01", "2025-01-15")),
    index_date = as.Date("2024-01-01")
  )

  # Only events within 365 days after index
  result <- apply_followup_window(data, days_after_start = 0, days_after_end = 365)

  # Should exclude 2025-01-15 (too far ahead)
  expect_true(nrow(result) < nrow(data))
  expect_true(all(result$event_date <= as.Date("2025-01-01")))
})

test_that("apply_followup_window excludes events before start", {
  data <- data.frame(
    patient_id = c(1, 1, 1),
    event_date = as.Date(c("2024-01-01", "2024-01-05", "2024-02-01")),
    index_date = as.Date("2024-01-01")
  )

  # Start from 7 days after index
  result <- apply_followup_window(data, days_after_start = 7, days_after_end = NULL)

  # Should exclude events before day 7
  expect_true(all(result$event_date >= as.Date("2024-01-08")))
})

# ============================================================================
# Tests for select_date_per_patient
# ============================================================================

test_that("select_date_per_patient selects min correctly", {
  data <- data.frame(
    patient_id = c(1, 1, 2, 2),
    event_date = as.Date(c("2024-02-15", "2024-06-20", "2024-03-10", "2024-08-05"))
  )

  result <- select_date_per_patient(data, method = "min")

  expect_equal(nrow(result), 2)
  expect_equal(result$event_date[result$patient_id == 1], as.Date("2024-02-15"))
  expect_equal(result$event_date[result$patient_id == 2], as.Date("2024-03-10"))
})

test_that("select_date_per_patient selects max correctly", {
  data <- data.frame(
    patient_id = c(1, 1, 2, 2),
    event_date = as.Date(c("2024-02-15", "2024-06-20", "2024-03-10", "2024-08-05"))
  )

  result <- select_date_per_patient(data, method = "max")

  expect_equal(nrow(result), 2)
  expect_equal(result$event_date[result$patient_id == 1], as.Date("2024-06-20"))
  expect_equal(result$event_date[result$patient_id == 2], as.Date("2024-08-05"))
})

# ============================================================================
# Tests for generate_outcomes
# ============================================================================

test_that("generate_outcomes creates correct output structure", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_name = "heart_attack",
    days_after_start = 0,
    days_after_end = NULL,
    selection_method = "min",
    calculate_days_from_index = TRUE,
    show_quality_report = FALSE
  )

  expect_true("patient_id" %in% names(result))
  expect_true("index_date" %in% names(result))
  expect_true("outcome_flag" %in% names(result))
  expect_true("outcome_date" %in% names(result))
  expect_true("days_from_index" %in% names(result))
})

test_that("generate_outcomes filters by outcome name", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_name = "heart_attack",
    show_quality_report = FALSE
  )

  # Only patients with heart_attack codes should have flag = TRUE
  patients_with_outcome <- disease_asset %>%
    filter(code == "I21") %>%
    pull(patient_id) %>%
    unique()

  expect_true(all(result$outcome_flag[result$patient_id %in% patients_with_outcome]))
})

test_that("generate_outcomes calculates days_from_index correctly", {
  disease_asset <- data.frame(
    patient_id = c(1, 2),
    event_date = as.Date(c("2024-02-01", "2024-03-01")),
    code = c("I21", "I21")
  )

  cohort <- data.frame(
    patient_id = 1:2,
    index_date = as.Date("2024-01-01")
  )

  lookup <- create_test_lookup()

  result <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_name = "heart_attack",
    calculate_days_from_index = TRUE,
    show_quality_report = FALSE
  )

  # days_from_index should be positive (after index)
  expect_true(all(result$days_from_index[result$outcome_flag] > 0))
})

test_that("generate_outcomes handles patients without outcome", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_name = "heart_attack",
    show_quality_report = FALSE
  )

  # Should have all cohort patients
  expect_equal(nrow(result), nrow(cohort))

  # Patients without outcome should have flag = FALSE
  expect_true(any(!result$outcome_flag))
})

test_that("generate_outcomes respects temporal window", {
  disease_asset <- data.frame(
    patient_id = c(1, 1, 2, 2),
    event_date = as.Date(c("2024-01-15", "2024-07-01", "2024-02-01", "2024-12-01")),
    code = c("I21", "I21", "I21", "I21")
  )

  cohort <- data.frame(
    patient_id = 1:2,
    index_date = as.Date("2024-01-01")
  )

  lookup <- create_test_lookup()

  # Only within 180 days
  result <- generate_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_name = "heart_attack",
    days_after_start = 0,
    days_after_end = 180,
    selection_method = "min",
    show_quality_report = FALSE
  )

  # Patient 1 should have outcome (2024-01-15 is within 180 days)
  # Patient 2 should have outcome (2024-02-01 is within 180 days)
  expect_true(result$outcome_flag[result$patient_id == 1])
  expect_true(result$outcome_flag[result$patient_id == 2])

  # Dates should be the first occurrence within window
  expect_equal(result$outcome_date[result$patient_id == 1], as.Date("2024-01-15"))
  expect_equal(result$outcome_date[result$patient_id == 2], as.Date("2024-02-01"))
})

# ============================================================================
# Tests for generate_multiple_outcomes
# ============================================================================

test_that("generate_multiple_outcomes creates wide format", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_multiple_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_names = c("heart_attack", "stroke")
  )

  # Should have columns for both outcomes
  expect_true("heart_attack_outcome_flag" %in% names(result))
  expect_true("stroke_outcome_flag" %in% names(result))
  expect_true("heart_attack_outcome_date" %in% names(result))
  expect_true("stroke_outcome_date" %in% names(result))
})

test_that("generate_multiple_outcomes maintains cohort size", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_multiple_outcomes(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    outcome_names = c("heart_attack", "stroke")
  )

  expect_equal(nrow(result), nrow(cohort))
})
