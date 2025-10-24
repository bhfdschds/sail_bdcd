# Tests for generate_covariates.R
#
# Tests for:
# - filter_long_by_lookup()
# - generate_covariates_before_index()
# - generate_covariates_time_window()
# - generate_multiple_time_windows()

library(testthat)
library(dplyr)
library(lubridate)

# Source the functions
source(file.path(RSCRIPTS_PATH, "pipeline_code", "generate_covariates.R"))

context("Generate Covariates Tests")

# ============================================================================
# Tests for filter_long_by_lookup()
# ============================================================================

test_that("filter_long_by_lookup filters correctly by code", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Create long format data for sex from database
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Create a lookup table for specific sex codes
  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male gender", "Female gender"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  # Filter using the lookup table
  result <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = FALSE
  )

  # Should only have M and F codes
  expect_true(all(result$sex_code %in% c("M", "F")))
  expect_true(nrow(result) > 0)

  cleanup_connection(conn)
})


test_that("filter_long_by_lookup adds lookup table info when requested", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Create long format data
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Create lookup table
  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male gender", "Female gender"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  # Filter with add_lookup_info = TRUE
  result <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  # Should have additional columns from lookup table
  expect_true("name" %in% names(result))
  expect_true("description" %in% names(result))
  expect_true("terminology" %in% names(result))

  # Check that lookup info is correct for Male
  male_rows <- result %>% filter(sex_code == "M")
  if (nrow(male_rows) > 0) {
    expect_true(all(male_rows$name == "Male"))
  }

  cleanup_connection(conn)
})


test_that("filter_long_by_lookup filters by date range", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Create long format data
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get date range from data
  if (nrow(long_data) > 0 && "record_date" %in% names(long_data)) {
    all_dates <- as.Date(long_data$record_date)
    all_dates <- all_dates[!is.na(all_dates)]

    if (length(all_dates) > 0) {
      # Use middle 50% of dates
      date_range <- quantile(all_dates, c(0.25, 0.75))

      lookup_table <- data.frame(
        code = c("M", "F"),
        name = c("Male", "Female"),
        description = c("Male", "Female"),
        terminology = c("GNDR", "GNDR"),
        stringsAsFactors = FALSE
      )

      result <- filter_long_by_lookup(
        long_data = long_data,
        lookup_table = lookup_table,
        code_col_in_data = "sex_code",
        event_date_col = "record_date",
        event_date_range = as.Date(date_range),
        add_lookup_info = FALSE
      )

      # All dates should be within range
      result_dates <- as.Date(result$record_date)
      result_dates <- result_dates[!is.na(result_dates)]

      if (length(result_dates) > 0) {
        expect_true(all(result_dates >= date_range[1]))
        expect_true(all(result_dates <= date_range[2]))
      }
    }
  }

  cleanup_connection(conn)
})


test_that("filter_long_by_lookup handles empty lookup table", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Empty lookup table
  empty_lookup <- data.frame(
    code = character(),
    name = character(),
    description = character(),
    terminology = character(),
    stringsAsFactors = FALSE
  )

  expect_warning(
    result <- filter_long_by_lookup(
      long_data = long_data,
      lookup_table = empty_lookup,
      code_col_in_data = "sex_code"
    ),
    "lookup_table is empty"
  )

  expect_equal(nrow(result), 0)

  cleanup_connection(conn)
})


test_that("filter_long_by_lookup validates inputs", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  # Missing code column in data
  expect_error(
    filter_long_by_lookup(
      long_data = long_data,
      lookup_table = lookup_table,
      code_col_in_data = "nonexistent_col"
    ),
    "not found in long_data"
  )

  # Missing required columns in lookup table
  bad_lookup <- data.frame(code = c("M", "F"))
  expect_error(
    filter_long_by_lookup(
      long_data = long_data,
      lookup_table = bad_lookup,
      code_col_in_data = "sex_code"
    ),
    "lookup_table must have columns"
  )

  cleanup_connection(conn)
})


# ============================================================================
# Tests for generate_covariates_before_index()
# ============================================================================

test_that("generate_covariates_before_index creates default covariates", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Get long format data
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get unique patients from long_data
  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 100)  # Use first 100 patients

  # Create index dates (set to current date)
  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate covariates
  result <- generate_covariates_before_index(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date"
  )

  # Should have all patients
  expect_equal(nrow(result), nrow(index_dates))
  expect_true(all(c("patient_id", "index_date", "n_events", "has_event") %in% names(result)))

  # All n_events should be numeric and >= 0
  expect_true(all(result$n_events >= 0))

  # has_event should be TRUE when n_events > 0
  expect_equal(result$has_event, result$n_events > 0)

  cleanup_connection(conn)
})


test_that("generate_covariates_before_index handles custom aggregation functions", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Get long format data with lookup info
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get patients
  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 50)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Custom aggregation functions
  result <- generate_covariates_before_index(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    aggregation_functions = list(
      n_events = ~ n(),
      n_sources = ~ n_distinct(source_table),
      earliest_date = ~ min(record_date, na.rm = TRUE)
    )
  )

  # Check result structure
  expect_true("n_events" %in% names(result))
  expect_true("n_sources" %in% names(result))
  expect_true("earliest_date" %in% names(result))

  # Check values make sense
  expect_true(all(result$n_events >= 0))
  expect_true(all(result$n_sources >= 0))

  cleanup_connection(conn)
})


test_that("generate_covariates_before_index respects include_before_only parameter", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get a patient with data
  patient_with_data <- long_data %>%
    group_by(patient_id) %>%
    summarise(n = n(), .groups = "drop") %>%
    filter(n > 0) %>%
    slice_head(n = 1) %>%
    pull(patient_id)

  if (length(patient_with_data) > 0) {
    # Get their event dates
    patient_events <- long_data %>%
      filter(patient_id == patient_with_data) %>%
      arrange(record_date)

    # Set index date to a date in the middle of their records
    if (nrow(patient_events) > 0) {
      all_dates <- as.Date(patient_events$record_date)
      all_dates <- all_dates[!is.na(all_dates)]

      if (length(all_dates) > 1) {
        index_date <- all_dates[ceiling(length(all_dates) / 2)]

        index_dates <- data.frame(
          patient_id = patient_with_data,
          index_date = index_date
        )

        # Test with include_before_only = TRUE
        result_before <- generate_covariates_before_index(
          long_data = long_data,
          index_dates = index_dates,
          event_date_col = "record_date",
          include_before_only = TRUE
        )

        # Test with include_before_only = FALSE
        result_on_or_before <- generate_covariates_before_index(
          long_data = long_data,
          index_dates = index_dates,
          event_date_col = "record_date",
          include_before_only = FALSE
        )

        # Should have >= events when including the index date
        expect_true(result_on_or_before$n_events >= result_before$n_events)
      }
    }
  }

  cleanup_connection(conn)
})


test_that("generate_covariates_before_index applies min_days_before filter", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get patients
  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 10)

  # Set index date to current date
  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate without min_days_before
  result_all <- generate_covariates_before_index(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    min_days_before = NULL
  )

  # Generate with min_days_before = 365 (only events > 1 year ago)
  result_filtered <- generate_covariates_before_index(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    min_days_before = 365
  )

  # Filtered should have <= events
  expect_true(sum(result_filtered$n_events) <= sum(result_all$n_events))

  cleanup_connection(conn)
})


# ============================================================================
# Tests for generate_covariates_time_window()
# ============================================================================

test_that("generate_covariates_time_window filters by time window correctly", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Get patients
  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 50)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate covariates for last year (1-365 days before)
  result <- generate_covariates_time_window(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    days_before_start = 365,
    days_before_end = 1,
    window_label = "last_year"
  )

  # Check structure
  expect_equal(nrow(result), nrow(index_dates))
  expect_true("last_year_n_events" %in% names(result))
  expect_true("last_year_has_event" %in% names(result))

  # All values should be >= 0
  expect_true(all(result$last_year_n_events >= 0))

  cleanup_connection(conn)
})


test_that("generate_covariates_time_window adds window label to column names", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 20)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  result <- generate_covariates_time_window(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    days_before_start = 90,
    days_before_end = 1,
    window_label = "last_90d"
  )

  expect_true("last_90d_n_events" %in% names(result))
  expect_true("last_90d_has_event" %in% names(result))

  cleanup_connection(conn)
})


test_that("generate_covariates_time_window validates time window parameters", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  index_dates <- data.frame(
    patient_id = long_data$patient_id[1],
    index_date = as.Date(Sys.Date())
  )

  # days_before_end >= days_before_start should error
  expect_error(
    generate_covariates_time_window(
      long_data = long_data,
      index_dates = index_dates,
      event_date_col = "record_date",
      days_before_start = 30,
      days_before_end = 90
    ),
    "days_before_end must be < days_before_start"
  )

  # Negative values should error
  expect_error(
    generate_covariates_time_window(
      long_data = long_data,
      index_dates = index_dates,
      event_date_col = "record_date",
      days_before_start = -30,
      days_before_end = 1
    ),
    "must be a non-negative number"
  )

  cleanup_connection(conn)
})


# ============================================================================
# Tests for generate_multiple_time_windows()
# ============================================================================

test_that("generate_multiple_time_windows creates covariates for all windows", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  unique_patients <- long_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 30)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Define multiple windows
  windows <- list(
    list(start = 90, end = 1, label = "last_90d"),
    list(start = 180, end = 91, label = "90to180d"),
    list(start = 365, end = 181, label = "180dto1y")
  )

  result <- generate_multiple_time_windows(
    long_data = long_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    time_windows = windows
  )

  # Should have columns for all three windows
  expect_true("last_90d_n_events" %in% names(result))
  expect_true("90to180d_n_events" %in% names(result))
  expect_true("180dto1y_n_events" %in% names(result))

  # Should have all patients
  expect_equal(nrow(result), nrow(index_dates))

  cleanup_connection(conn)
})


test_that("generate_multiple_time_windows validates window structure", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  index_dates <- data.frame(
    patient_id = long_data$patient_id[1],
    index_date = as.Date(Sys.Date())
  )

  # Missing 'label' in one window
  bad_windows <- list(
    list(start = 90, end = 1),  # Missing label
    list(start = 180, end = 91, label = "90to180d")
  )

  expect_error(
    generate_multiple_time_windows(
      long_data = long_data,
      index_dates = index_dates,
      event_date_col = "record_date",
      time_windows = bad_windows
    ),
    "must have 'start', 'end', and 'label'"
  )

  cleanup_connection(conn)
})


# ============================================================================
# Integration Tests
# ============================================================================

test_that("Full workflow: lookup -> before_index covariates", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Step 1: Get long format data
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Step 2: Filter by lookup table
  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male gender", "Female gender"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  expect_true(nrow(filtered_data) > 0)

  # Step 3: Generate covariates before index
  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 20)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  covariates <- generate_covariates_before_index(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    aggregation_functions = list(
      n_events = ~ n(),
      has_event = ~ n() > 0,
      n_sources = ~ n_distinct(source_table)
    )
  )

  # Verify results
  expect_equal(nrow(covariates), nrow(index_dates))
  expect_true(all(c("patient_id", "index_date", "n_events") %in% names(covariates)))

  cleanup_connection(conn)
})


test_that("Full workflow: lookup -> multiple time window covariates", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Step 1: Get long format data
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Step 2: Filter by lookup table
  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = FALSE
  )

  # Step 3: Generate multiple time windows
  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 15)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  windows <- list(
    list(start = 365, end = 1, label = "last_year"),
    list(start = 730, end = 366, label = "year_before_last")
  )

  covariates <- generate_multiple_time_windows(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    time_windows = windows
  )

  # Verify results
  expect_equal(nrow(covariates), nrow(index_dates))
  expect_true("last_year_n_events" %in% names(covariates))
  expect_true("year_before_last_n_events" %in% names(covariates))

  cleanup_connection(conn)
})


# ============================================================================
# Tests for generate_flag_covariates_by_name()
# ============================================================================

test_that("generate_flag_covariates_by_name creates binary flags", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  # Get long format data with lookup info
  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Create lookup table
  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  # Filter with lookup info
  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  # Get patients
  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 50)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate flags
  flags <- generate_flag_covariates_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date"
  )

  # Check structure
  expect_equal(nrow(flags), nrow(index_dates))
  expect_true("Male_flag" %in% names(flags) || "Female_flag" %in% names(flags))

  # Flags should be 0 or 1
  flag_cols <- grep("_flag$", names(flags), value = TRUE)
  for (col in flag_cols) {
    expect_true(all(flags[[col]] %in% c(0, 1)))
  }

  cleanup_connection(conn)
})


test_that("generate_flag_covariates_by_name includes dates when requested", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 20)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate with dates
  flags_with_dates <- generate_flag_covariates_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    include_date = "both"
  )

  # Should have earliest and latest date columns
  expect_true(any(grepl("_earliest_date$", names(flags_with_dates))))
  expect_true(any(grepl("_latest_date$", names(flags_with_dates))))

  cleanup_connection(conn)
})


test_that("generate_flag_covariates_by_name includes days_between selectively", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 20)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate with days_between for specific name
  flags_with_days <- generate_flag_covariates_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    include_days_between = c("Male")
  )

  # Should have days_between for Male only
  expect_true("Male_days_between" %in% names(flags_with_days))

  cleanup_connection(conn)
})


test_that("generate_flag_covariates_by_name respects time_window", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 20)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate with time window
  flags_recent <- generate_flag_covariates_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    time_window = list(start = 365, end = 1)
  )

  # Should return valid flags
  expect_equal(nrow(flags_recent), nrow(index_dates))
  flag_cols <- grep("_flag$", names(flags_recent), value = TRUE)
  expect_true(length(flag_cols) > 0)

  cleanup_connection(conn)
})


# ============================================================================
# Tests for extract_value_by_name()
# ============================================================================

test_that("extract_value_by_name extracts min values correctly", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 30)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Extract earliest dates
  earliest_dates <- extract_value_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    extract_function = "min",
    time_window = list(start = 365, end = 1),
    window_label = "last_year"
  )

  # Check structure
  expect_equal(nrow(earliest_dates), nrow(index_dates))
  min_cols <- grep("_min_", names(earliest_dates), value = TRUE)
  expect_true(length(min_cols) > 0)

  cleanup_connection(conn)
})


test_that("extract_value_by_name extracts max values correctly", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 30)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Extract latest dates
  latest_dates <- extract_value_by_name(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    extract_function = "max",
    time_window = list(start = 365, end = 1),
    window_label = "last_year"
  )

  # Check structure
  expect_equal(nrow(latest_dates), nrow(index_dates))
  max_cols <- grep("_max_", names(latest_dates), value = TRUE)
  expect_true(length(max_cols) > 0)

  cleanup_connection(conn)
})


# ============================================================================
# Tests for generate_covariates_by_name_window()
# ============================================================================

test_that("generate_covariates_by_name_window separates by name", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 30)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate with separate columns per name
  covariates <- generate_covariates_by_name_window(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    time_window = list(start = 365, end = 1),
    window_label = "last_year",
    separate_by_name = TRUE
  )

  # Should have separate columns for each name
  expect_equal(nrow(covariates), nrow(index_dates))
  expect_true(any(grepl("Male_", names(covariates))) || any(grepl("Female_", names(covariates))))

  cleanup_connection(conn)
})


test_that("generate_covariates_by_name_window aggregates across names", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db2_config(CONFIG_PATH)

  long_data <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  lookup_table <- data.frame(
    code = c("M", "F"),
    name = c("Male", "Female"),
    description = c("Male", "Female"),
    terminology = c("GNDR", "GNDR"),
    stringsAsFactors = FALSE
  )

  filtered_data <- filter_long_by_lookup(
    long_data = long_data,
    lookup_table = lookup_table,
    code_col_in_data = "sex_code",
    add_lookup_info = TRUE
  )

  unique_patients <- filtered_data %>%
    select(patient_id) %>%
    distinct() %>%
    slice_head(n = 30)

  index_dates <- unique_patients %>%
    mutate(index_date = as.Date(Sys.Date()))

  # Generate with aggregation across all names
  covariates_agg <- generate_covariates_by_name_window(
    long_data = filtered_data,
    index_dates = index_dates,
    event_date_col = "record_date",
    time_window = list(start = 365, end = 1),
    window_label = "last_year",
    separate_by_name = FALSE
  )

  # Should have aggregated columns without name prefix
  expect_equal(nrow(covariates_agg), nrow(index_dates))
  expect_true("last_year_n_events" %in% names(covariates_agg))

  cleanup_connection(conn)
})
