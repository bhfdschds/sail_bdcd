# Tests for generate_covariates.R

library(testthat)
library(dplyr)
library(lubridate)

source("../../pipeline_code/generate_covariates.R")

# ============================================================================
# Test Data Setup
# ============================================================================

create_test_disease_asset <- function() {
  data.frame(
    patient_id = c(1, 1, 2, 2, 3, 3, 4, 5, 5, 6),
    event_date = as.Date(c(
      "2023-01-15", "2023-06-20", "2023-03-10", "2023-08-05",
      "2023-02-28", "2023-09-12", "2023-07-01", "2023-04-18", "2023-10-22", "2023-05-30"
    )),
    code = c("E11", "E11", "I10", "E11", "E11", "I10", "E11", "I10", "I10", "E11")
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
    code = c("E11", "I10"),
    name = c("diabetes", "hypertension"),
    description = c("Type 2 diabetes", "Essential hypertension"),
    terminology = c("ICD10", "ICD10")
  )
}

# ============================================================================
# Tests for apply_lookback_window
# ============================================================================

test_that("apply_lookback_window filters correctly with unlimited start", {
  data <- data.frame(
    patient_id = c(1, 1, 1),
    event_date = as.Date(c("2023-01-01", "2023-06-01", "2023-12-01")),
    index_date = as.Date("2024-01-01")
  )

  result <- apply_lookback_window(data, days_before_start = NULL, days_before_end = 0)

  # All events before index should be included
  expect_equal(nrow(result), 3)
})

test_that("apply_lookback_window filters with bounded window", {
  data <- data.frame(
    patient_id = c(1, 1, 1, 1),
    event_date = as.Date(c("2023-01-01", "2023-06-01", "2023-11-01", "2023-12-20")),
    index_date = as.Date("2024-01-01")
  )

  # Only events within 365 days before index
  result <- apply_lookback_window(data, days_before_start = 365, days_before_end = 0)

  # Should exclude 2023-01-01 (too far back)
  expect_true(nrow(result) < nrow(data))
  expect_true(all(result$event_date >= as.Date("2023-01-01")))
})

# ============================================================================
# Tests for select_date_per_patient
# ============================================================================

test_that("select_date_per_patient selects min correctly", {
  data <- data.frame(
    patient_id = c(1, 1, 2, 2),
    event_date = as.Date(c("2023-01-15", "2023-06-20", "2023-03-10", "2023-08-05"))
  )

  result <- select_date_per_patient(data, method = "min")

  expect_equal(nrow(result), 2)
  expect_equal(result$event_date[result$patient_id == 1], as.Date("2023-01-15"))
  expect_equal(result$event_date[result$patient_id == 2], as.Date("2023-03-10"))
})

test_that("select_date_per_patient selects max correctly", {
  data <- data.frame(
    patient_id = c(1, 1, 2, 2),
    event_date = as.Date(c("2023-01-15", "2023-06-20", "2023-03-10", "2023-08-05"))
  )

  result <- select_date_per_patient(data, method = "max")

  expect_equal(nrow(result), 2)
  expect_equal(result$event_date[result$patient_id == 1], as.Date("2023-06-20"))
  expect_equal(result$event_date[result$patient_id == 2], as.Date("2023-08-05"))
})

# ============================================================================
# Tests for generate_covariates
# ============================================================================

test_that("generate_covariates creates correct output structure", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_name = "diabetes",
    days_before_start = NULL,
    days_before_end = 0,
    selection_method = "min",
    calculate_days_to_index = TRUE,
    show_quality_report = FALSE
  )

  expect_true("patient_id" %in% names(result))
  expect_true("index_date" %in% names(result))
  expect_true("covariate_flag" %in% names(result))
  expect_true("covariate_date" %in% names(result))
  expect_true("days_to_index" %in% names(result))
})

test_that("generate_covariates filters by covariate name", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_name = "diabetes",
    show_quality_report = FALSE
  )

  # Only patients with diabetes codes should have flag = TRUE
  patients_with_diabetes <- disease_asset %>%
    filter(code == "E11") %>%
    pull(patient_id) %>%
    unique()

  expect_true(all(result$covariate_flag[result$patient_id %in% patients_with_diabetes]))
})

test_that("generate_covariates calculates days_to_index correctly", {
  disease_asset <- data.frame(
    patient_id = c(1, 2),
    event_date = as.Date(c("2023-12-01", "2023-11-01")),
    code = c("E11", "E11")
  )

  cohort <- data.frame(
    patient_id = 1:2,
    index_date = as.Date("2024-01-01")
  )

  lookup <- create_test_lookup()

  result <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_name = "diabetes",
    calculate_days_to_index = TRUE,
    show_quality_report = FALSE
  )

  # days_to_index should be negative (before index)
  expect_true(all(result$days_to_index[result$covariate_flag] < 0))
})

test_that("generate_covariates handles patients without covariate", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_name = "diabetes",
    show_quality_report = FALSE
  )

  # Should have all cohort patients
  expect_equal(nrow(result), nrow(cohort))

  # Patients without covariate should have flag = FALSE
  expect_true(any(!result$covariate_flag))
})

# ============================================================================
# Tests for generate_multiple_covariates
# ============================================================================

test_that("generate_multiple_covariates creates wide format", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_multiple_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_names = c("diabetes", "hypertension"),
    show_quality_report = FALSE
  )

  # Should have columns for both covariates
  expect_true("diabetes_covariate_flag" %in% names(result))
  expect_true("hypertension_covariate_flag" %in% names(result))
  expect_true("diabetes_covariate_date" %in% names(result))
  expect_true("hypertension_covariate_date" %in% names(result))
})

test_that("generate_multiple_covariates maintains cohort size", {
  disease_asset <- create_test_disease_asset()
  cohort <- create_test_cohort()
  lookup <- create_test_lookup()

  result <- generate_multiple_covariates(
    disease_treatment_asset = disease_asset,
    cohort = cohort,
    lookup_table = lookup,
    covariate_names = c("diabetes", "hypertension")
  )

  expect_equal(nrow(result), nrow(cohort))
})
