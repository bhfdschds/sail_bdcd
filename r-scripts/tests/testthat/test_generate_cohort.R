# Tests for generate_cohort.R

library(testthat)
library(dplyr)
library(lubridate)

source("../../pipeline_code/generate_cohort.R")

# ============================================================================
# Test Data Setup
# ============================================================================

create_test_demographics <- function() {
  data.frame(
    patient_id = 1:10,
    date_of_birth = as.Date(c(
      "1950-01-01", "1960-05-15", "1970-08-20", "1980-12-10", "1990-03-25",
      "2000-07-30", "1945-11-05", "1985-02-14", "1995-09-08", "2005-04-12"
    )),
    sex_code = c("M", "F", "M", "F", NA, "M", "F", "M", "F", "M"),
    ethnicity_code = c("W", "W", "A", NA, "B", "W", NA, "A", "W", "B"),
    lsoa_code = c("W01000001", "W01000002", NA, "W01000003", "W01000004",
                  "W01000005", "W01000006", "W01000007", NA, "W01000008")
  )
}

# ============================================================================
# Tests for combine_demographics
# ============================================================================

test_that("combine_demographics joins all assets correctly", {
  dob_asset <- data.frame(
    patient_id = 1:5,
    date_of_birth = as.Date("2000-01-01") + 0:4
  )

  sex_asset <- data.frame(
    patient_id = 1:5,
    sex_code = c("M", "F", "M", "F", "M")
  )

  result <- combine_demographics(dob_asset, sex_asset)

  expect_equal(nrow(result), 5)
  expect_equal(ncol(result), 3)
  expect_true("patient_id" %in% names(result))
  expect_true("date_of_birth" %in% names(result))
  expect_true("sex_code" %in% names(result))
})

test_that("combine_demographics handles optional assets", {
  dob_asset <- data.frame(
    patient_id = 1:3,
    date_of_birth = as.Date("2000-01-01") + 0:2
  )

  sex_asset <- data.frame(
    patient_id = 1:3,
    sex_code = c("M", "F", "M")
  )

  ethnicity_asset <- data.frame(
    patient_id = 1:3,
    ethnicity_code = c("W", "A", "B")
  )

  result <- combine_demographics(dob_asset, sex_asset, ethnicity_asset)

  expect_equal(ncol(result), 4)
  expect_true("ethnicity_code" %in% names(result))
})

# ============================================================================
# Tests for generate_cohort
# ============================================================================

test_that("generate_cohort calculates age correctly", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = FALSE,
    require_known_ethnicity = FALSE,
    require_lsoa = FALSE
  )

  expect_true("age_at_index" %in% names(cohort))
  expect_true(all(cohort$age_at_index >= 0))
  expect_true(all(cohort$age_at_index < 150))
})

test_that("generate_cohort applies age restrictions", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    min_age = 25,
    max_age = 65,
    require_known_sex = FALSE,
    require_known_ethnicity = FALSE,
    require_lsoa = FALSE
  )

  expect_true(all(cohort$age_at_index >= 25))
  expect_true(all(cohort$age_at_index <= 65))
})

test_that("generate_cohort applies sex restriction", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = TRUE,
    require_known_ethnicity = FALSE,
    require_lsoa = FALSE
  )

  expect_true(all(!is.na(cohort$sex_code)))
  expect_true(all(cohort$sex_code != ""))
})

test_that("generate_cohort applies ethnicity restriction", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = FALSE,
    require_known_ethnicity = TRUE,
    require_lsoa = FALSE
  )

  expect_true(all(!is.na(cohort$ethnicity_code)))
})

test_that("generate_cohort applies LSOA restriction", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = FALSE,
    require_known_ethnicity = FALSE,
    require_lsoa = TRUE
  )

  expect_true(all(!is.na(cohort$lsoa_code)))
})

test_that("generate_cohort handles single index date", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = FALSE,
    require_known_ethnicity = FALSE,
    require_lsoa = FALSE
  )

  expect_true(all(cohort$index_date == index_date))
})

test_that("generate_cohort includes all required columns", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    require_known_sex = FALSE,
    require_known_ethnicity = FALSE,
    require_lsoa = FALSE
  )

  expect_true("patient_id" %in% names(cohort))
  expect_true("index_date" %in% names(cohort))
  expect_true("age_at_index" %in% names(cohort))
  expect_true("date_of_birth" %in% names(cohort))
})

test_that("generate_cohort excludes patients correctly", {
  demographics <- create_test_demographics()
  index_date <- as.Date("2024-01-01")

  # All restrictions
  cohort <- generate_cohort(
    demographics_asset = demographics,
    index_date = index_date,
    min_age = 25,
    max_age = 50,
    require_known_sex = TRUE,
    require_known_ethnicity = TRUE,
    require_lsoa = TRUE
  )

  # Should have fewer patients after all restrictions
  expect_true(nrow(cohort) < nrow(demographics))

  # All remaining should meet criteria
  expect_true(all(cohort$age_at_index >= 25))
  expect_true(all(cohort$age_at_index <= 50))
  expect_true(all(!is.na(cohort$sex_code)))
  expect_true(all(!is.na(cohort$ethnicity_code)))
  expect_true(all(!is.na(cohort$lsoa_code)))
})
