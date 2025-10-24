# Generate Outcomes from Long Format Data
#
# This module provides functions to:
# 1. Filter long format data using lookup tables for specific codes (same as covariates)
# 2. Generate outcomes based on events AFTER an index date
# 3. Generate outcomes for events within specific time windows AFTER index date
#
# This mirrors generate_covariates.R but for outcome analysis (events after index)
#
# @author SAIL BDCD Team
# @date 2025-01-24

library(dplyr)
library(tidyr)
library(lubridate)

# NOTE: filter_long_by_lookup() is in generate_covariates.R
# Use that function first, then pass filtered data to outcome functions below


#' Generate Outcomes After Index Date
#'
#' Creates patient-level outcomes based on events occurring AFTER an index date.
#' This is the mirror of generate_covariates_before_index() for outcome analysis.
#'
#' @param long_data Data frame. Long format data with patient_id, event date,
#'   and other event-level data columns.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#'   One row per patient.
#' @param event_date_col Character. Name of the event date column in long_data.
#'   Default is "record_date".
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param aggregation_functions Named list. Functions to apply for each outcome.
#'   Names are output column names, values are expressions or functions.
#'   Default creates count and indicator outcomes.
#' @param include_after_only Logical. If TRUE, only includes events strictly after
#'   index_date (exclusive). If FALSE, includes events on or after index_date
#'   (inclusive). Default is TRUE.
#' @param max_days_after Numeric. Optional maximum number of days after index_date
#'   to include events. Default is NULL (no maximum).
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events
#'   (fills with 0/FALSE). Default is TRUE.
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - Aggregated outcomes as specified in aggregation_functions
#'
#' @examples
#' # Create index dates
#' index_dates <- data.frame(
#'   patient_id = c(1, 2, 3),
#'   index_date = as.Date(c("2023-01-01", "2023-06-01", "2023-03-15"))
#' )
#'
#' # Generate outcomes: count events after index
#' outcomes <- generate_outcomes_after_index(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date"
#' )
#'
#' # Custom aggregation functions
#' outcomes <- generate_outcomes_after_index(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   aggregation_functions = list(
#'     n_events = ~ n(),
#'     has_outcome = ~ n() > 0,
#'     first_event_date = ~ min(event_date, na.rm = TRUE),
#'     latest_event = ~ max(event_date, na.rm = TRUE),
#'     n_sources = ~ n_distinct(source_table),
#'     n_unique_codes = ~ n_distinct(icd10_code),
#'     days_to_first_event = ~ as.numeric(min(event_date) - first(index_date))
#'   )
#' )
#'
#' @export
generate_outcomes_after_index <- function(long_data,
                                           index_dates,
                                           event_date_col = "record_date",
                                           patient_id_col = "patient_id",
                                           aggregation_functions = NULL,
                                           include_after_only = TRUE,
                                           max_days_after = NULL,
                                           fill_missing_patients = TRUE) {

  # Input validation
  if (!is.data.frame(long_data)) {
    stop("long_data must be a data frame")
  }

  if (!is.data.frame(index_dates)) {
    stop("index_dates must be a data frame")
  }

  if (!patient_id_col %in% names(long_data)) {
    stop("patient_id_col '", patient_id_col, "' not found in long_data")
  }

  if (!patient_id_col %in% names(index_dates)) {
    stop("patient_id_col '", patient_id_col, "' not found in index_dates")
  }

  if (!"index_date" %in% names(index_dates)) {
    stop("index_dates must contain a column named 'index_date'")
  }

  if (!event_date_col %in% names(long_data)) {
    stop("event_date_col '", event_date_col, "' not found in long_data")
  }

  # Default aggregation functions
  if (is.null(aggregation_functions)) {
    aggregation_functions <- list(
      n_events = ~ n(),
      has_outcome = ~ n() > 0
    )
  }

  # Join long_data with index_dates to get index_date for each event
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Filter events based on index_date (AFTER instead of BEFORE)
  if (include_after_only) {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] > .data[["index_date"]])
  } else {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] >= .data[["index_date"]])
  }

  # Apply maximum days after filter if specified
  if (!is.null(max_days_after)) {
    if (!is.numeric(max_days_after) || max_days_after < 0) {
      stop("max_days_after must be a non-negative number")
    }

    data_filtered <- data_filtered %>%
      mutate(days_after = as.numeric(.data[[event_date_col]] - .data[["index_date"]])) %>%
      filter(days_after <= max_days_after) %>%
      select(-days_after)
  }

  # Check if there's any data left after filtering
  if (nrow(data_filtered) == 0 && fill_missing_patients) {
    # No events for any patient - return index_dates with default values
    outcomes <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date")))

    # Add outcome columns with default values
    for (agg_name in names(aggregation_functions)) {
      if (grepl("^n_", agg_name) || grepl("count", agg_name, ignore.case = TRUE)) {
        outcomes[[agg_name]] <- 0
      } else if (grepl("^has_", agg_name)) {
        outcomes[[agg_name]] <- FALSE
      } else {
        outcomes[[agg_name]] <- NA
      }
    }

    return(outcomes)
  }

  # Aggregate by patient
  outcomes <- data_filtered %>%
    group_by(.data[[patient_id_col]]) %>%
    summarise(
      !!!lapply(aggregation_functions, function(f) {
        if (is.function(f)) return(f)
        if (rlang::is_formula(f)) return(rlang::as_function(f))
        return(f)
      }),
      .groups = "drop"
    )

  # Join back with index_dates to get all patients and index_date
  if (fill_missing_patients) {
    outcomes <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date"))) %>%
      left_join(outcomes, by = patient_id_col)

    # Fill missing values
    for (col in names(outcomes)) {
      if (col %in% c(patient_id_col, "index_date")) next

      if (grepl("^n_", col) || grepl("count", col, ignore.case = TRUE)) {
        outcomes[[col]][is.na(outcomes[[col]])] <- 0
      } else if (grepl("^has_", col) || grepl("outcome", col, ignore.case = TRUE)) {
        outcomes[[col]][is.na(outcomes[[col]])] <- FALSE
      }
    }
  } else {
    outcomes <- outcomes %>%
      left_join(
        index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
        by = patient_id_col
      )
  }

  return(outcomes)
}


#' Generate Outcomes Within Time Window After Index Date
#'
#' Creates patient-level outcomes based on events occurring within a specific
#' time window AFTER an index date (e.g., 30-90 days after, 1-2 years after).
#' Mirror of generate_covariates_time_window() but for events AFTER index.
#'
#' @param long_data Data frame. Long format data with patient_id, event date,
#'   and other event-level data columns.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column in long_data.
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param days_after_start Numeric. Start of the time window (days AFTER index_date).
#'   Must be >= 0. For example, 0 means "starting at index date".
#' @param days_after_end Numeric. End of the time window (days AFTER index_date).
#'   Must be >= 0 and > days_after_start. For example, 30 means "up to 30 days after".
#' @param aggregation_functions Named list. Functions to apply for each outcome.
#'   Default creates count and indicator outcomes.
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events
#'   in the window. Default is TRUE.
#' @param window_label Character. Optional label for the time window to prepend to
#'   outcome names (e.g., "30to90d"). If NULL, uses "window".
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - Aggregated outcomes with window_label prefix
#'
#' @examples
#' # Create index dates
#' index_dates <- data.frame(
#'   patient_id = c(1, 2, 3),
#'   index_date = as.Date(c("2023-01-01", "2023-06-01", "2023-03-15"))
#' )
#'
#' # Events in first 30 days after index (0-30 days)
#' outcomes_30d <- generate_outcomes_time_window(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   days_after_start = 0,
#'   days_after_end = 30,
#'   window_label = "first_30d"
#' )
#'
#' # Events in 30-90 days after index
#' outcomes_30to90 <- generate_outcomes_time_window(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   days_after_start = 30,
#'   days_after_end = 90,
#'   window_label = "30to90d"
#' )
#'
#' # Events in 1-2 years after index
#' outcomes_1to2y <- generate_outcomes_time_window(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   days_after_start = 365,
#'   days_after_end = 730,
#'   window_label = "1to2y"
#' )
#'
#' @export
generate_outcomes_time_window <- function(long_data,
                                           index_dates,
                                           event_date_col = "record_date",
                                           patient_id_col = "patient_id",
                                           days_after_start,
                                           days_after_end,
                                           aggregation_functions = NULL,
                                           fill_missing_patients = TRUE,
                                           window_label = NULL) {

  # Input validation
  if (!is.data.frame(long_data)) {
    stop("long_data must be a data frame")
  }

  if (!is.data.frame(index_dates)) {
    stop("index_dates must be a data frame")
  }

  if (!patient_id_col %in% names(long_data)) {
    stop("patient_id_col '", patient_id_col, "' not found in long_data")
  }

  if (!patient_id_col %in% names(index_dates)) {
    stop("patient_id_col '", patient_id_col, "' not found in index_dates")
  }

  if (!"index_date" %in% names(index_dates)) {
    stop("index_dates must contain a column named 'index_date'")
  }

  if (!event_date_col %in% names(long_data)) {
    stop("event_date_col '", event_date_col, "' not found in long_data")
  }

  # Validate time window parameters (REVERSED from covariates - end must be > start)
  if (!is.numeric(days_after_start) || days_after_start < 0) {
    stop("days_after_start must be a non-negative number")
  }

  if (!is.numeric(days_after_end) || days_after_end < 0) {
    stop("days_after_end must be a non-negative number")
  }

  if (days_after_end <= days_after_start) {
    stop("days_after_end must be > days_after_start. ",
         "For example, days_after_start=30, days_after_end=90 means ",
         "'between 30 and 90 days AFTER index'")
  }

  # Default aggregation functions
  if (is.null(aggregation_functions)) {
    aggregation_functions <- list(
      n_events = ~ n(),
      has_outcome = ~ n() > 0
    )
  }

  # Default window label
  if (is.null(window_label)) {
    window_label <- "window"
  }

  # Join long_data with index_dates
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Calculate days_after_index for each event
  data_with_window <- data_with_index %>%
    mutate(
      days_after_index = as.numeric(.data[[event_date_col]] - .data[["index_date"]])
    )

  # Filter to events within the time window (AFTER index)
  data_filtered <- data_with_window %>%
    filter(
      days_after_index >= days_after_start,
      days_after_index <= days_after_end
    ) %>%
    select(-days_after_index)

  # Check if there's any data left after filtering
  if (nrow(data_filtered) == 0 && fill_missing_patients) {
    # No events for any patient in this window - return index_dates with default values
    outcomes <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date")))

    # Add outcome columns with default values and window prefix
    for (agg_name in names(aggregation_functions)) {
      col_name <- paste0(window_label, "_", agg_name)
      if (grepl("^n_", agg_name) || grepl("count", agg_name, ignore.case = TRUE)) {
        outcomes[[col_name]] <- 0
      } else if (grepl("^has_", agg_name)) {
        outcomes[[col_name]] <- FALSE
      } else {
        outcomes[[col_name]] <- NA
      }
    }

    return(outcomes)
  }

  # Aggregate by patient
  outcomes <- data_filtered %>%
    group_by(.data[[patient_id_col]]) %>%
    summarise(
      !!!lapply(aggregation_functions, function(f) {
        if (is.function(f)) return(f)
        if (rlang::is_formula(f)) return(rlang::as_function(f))
        return(f)
      }),
      .groups = "drop"
    )

  # Prepend window_label to outcome names (except patient_id)
  outcome_cols <- setdiff(names(outcomes), patient_id_col)
  for (col in outcome_cols) {
    new_name <- paste0(window_label, "_", col)
    outcomes <- outcomes %>%
      rename(!!new_name := !!col)
  }

  # Join back with index_dates to get all patients
  if (fill_missing_patients) {
    outcomes <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date"))) %>%
      left_join(outcomes, by = patient_id_col)

    # Fill missing values
    for (col in names(outcomes)) {
      if (col %in% c(patient_id_col, "index_date")) next

      if (grepl("_n_", col) || grepl("count", col, ignore.case = TRUE)) {
        outcomes[[col]][is.na(outcomes[[col]])] <- 0
      } else if (grepl("_has_", col) || grepl("outcome", col, ignore.case = TRUE)) {
        outcomes[[col]][is.na(outcomes[[col]])] <- FALSE
      }
    }
  } else {
    outcomes <- outcomes %>%
      left_join(
        index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
        by = patient_id_col
      )
  }

  return(outcomes)
}


#' Generate Multiple Time Window Outcomes
#'
#' Convenience function to generate outcomes for multiple time windows in one call.
#' Mirror of generate_multiple_time_windows() but for events AFTER index.
#'
#' @param long_data Data frame. Long format data with patient_id and event date.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column.
#' @param patient_id_col Character. Name of the patient ID column.
#' @param time_windows List of lists. Each element specifies a time window:
#'   list(start = days_after_start, end = days_after_end, label = "label")
#' @param aggregation_functions Named list. Functions to apply for each outcome.
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events.
#'
#' @return Data frame with patient_id, index_date, and outcomes from all time windows.
#'
#' @examples
#' # Define multiple time windows AFTER index
#' windows <- list(
#'   list(start = 0, end = 30, label = "first_30d"),
#'   list(start = 31, end = 90, label = "30to90d"),
#'   list(start = 91, end = 365, label = "90d_to_1y"),
#'   list(start = 366, end = 730, label = "1to2y")
#' )
#'
#' # Generate outcomes for all windows
#' all_outcomes <- generate_multiple_time_windows_outcomes(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   time_windows = windows
#' )
#'
#' @export
generate_multiple_time_windows_outcomes <- function(long_data,
                                                     index_dates,
                                                     event_date_col = "record_date",
                                                     patient_id_col = "patient_id",
                                                     time_windows,
                                                     aggregation_functions = NULL,
                                                     fill_missing_patients = TRUE) {

  # Input validation
  if (!is.list(time_windows) || length(time_windows) == 0) {
    stop("time_windows must be a non-empty list")
  }

  # Start with index_dates
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  # Generate outcomes for each time window
  for (i in seq_along(time_windows)) {
    window <- time_windows[[i]]

    # Validate window structure
    if (!all(c("start", "end", "label") %in% names(window))) {
      stop("Each time window must have 'start', 'end', and 'label' elements. ",
           "Window ", i, " is missing some of these.")
    }

    # Generate outcomes for this window
    window_outcomes <- generate_outcomes_time_window(
      long_data = long_data,
      index_dates = index_dates,
      event_date_col = event_date_col,
      patient_id_col = patient_id_col,
      days_after_start = window$start,
      days_after_end = window$end,
      aggregation_functions = aggregation_functions,
      fill_missing_patients = fill_missing_patients,
      window_label = window$label
    )

    # Join with result
    result <- result %>%
      left_join(
        window_outcomes %>% select(-index_date),
        by = patient_id_col
      )
  }

  return(result)
}


#' Generate Outcome Flags by Name
#'
#' Creates patient-level binary flags (0/1) for each unique 'name' in the lookup table.
#' Optionally includes the date of first event and days from index to event.
#' Mirror of generate_flag_covariates_by_name() but for outcomes AFTER index.
#'
#' @param long_data Data frame. Long format data that has been filtered by
#'   filter_long_by_lookup() and includes the 'name' column.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column in long_data.
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param names_to_flag Character vector. Specific 'name' values to create flags for.
#'   If NULL, creates flags for all unique names in long_data.
#' @param include_date Character. Whether to include event date: "none", "first",
#'   "last", or "both". Default is "none".
#' @param include_days_to_event Logical or character vector. If TRUE, includes
#'   days_to_event for all names. If character vector, only includes for specified names.
#'   Default is FALSE.
#' @param date_filter Character. Which events to consider: "after_index" (default),
#'   "after_or_on_index", or "all".
#' @param time_window List with 'start' and 'end' (days AFTER index). Optional.
#'   If provided, only considers events within this window.
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - {name}_outcome: Binary outcome flag (0/1) for each name
#'   - {name}_first_date: (if include_date = "first" or "both")
#'   - {name}_last_date: (if include_date = "last" or "both")
#'   - {name}_days_to_event: (if include_days_to_event includes this name)
#'
#' @examples
#' # Filter and add lookup info first
#' adverse_events <- filter_long_by_lookup(
#'   long_data = hospital_admissions,
#'   lookup_table = adverse_event_codes,
#'   code_col_in_data = "diagnosis_code",
#'   add_lookup_info = TRUE  # MUST be TRUE to have 'name' column
#' )
#'
#' # Create outcome flags for each adverse event type
#' outcomes <- generate_outcome_flags_by_name(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date"
#' )
#'
#' # With dates and days_to_event
#' outcomes_detailed <- generate_outcome_flags_by_name(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   include_date = "both",
#'   include_days_to_event = c("MI", "Stroke")
#' )
#'
#' # Within specific time window (first year after index)
#' outcomes_1year <- generate_outcome_flags_by_name(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   time_window = list(start = 0, end = 365)
#' )
#'
#' @export
generate_outcome_flags_by_name <- function(long_data,
                                            index_dates,
                                            event_date_col = "record_date",
                                            patient_id_col = "patient_id",
                                            names_to_flag = NULL,
                                            include_date = c("none", "first", "last", "both"),
                                            include_days_to_event = FALSE,
                                            date_filter = c("after_index", "after_or_on_index", "all"),
                                            time_window = NULL) {

  # Input validation
  if (!is.data.frame(long_data)) {
    stop("long_data must be a data frame")
  }

  if (!"name" %in% names(long_data)) {
    stop("long_data must contain a 'name' column. ",
         "Use filter_long_by_lookup() with add_lookup_info=TRUE first.")
  }

  if (!is.data.frame(index_dates)) {
    stop("index_dates must be a data frame")
  }

  if (!patient_id_col %in% names(long_data)) {
    stop("patient_id_col '", patient_id_col, "' not found in long_data")
  }

  if (!patient_id_col %in% names(index_dates)) {
    stop("patient_id_col '", patient_id_col, "' not found in index_dates")
  }

  if (!"index_date" %in% names(index_dates)) {
    stop("index_dates must contain a column named 'index_date'")
  }

  if (!event_date_col %in% names(long_data)) {
    stop("event_date_col '", event_date_col, "' not found in long_data")
  }

  include_date <- match.arg(include_date)
  date_filter <- match.arg(date_filter)

  # Determine which names to create flags for
  if (is.null(names_to_flag)) {
    names_to_flag <- unique(long_data$name)
    names_to_flag <- names_to_flag[!is.na(names_to_flag)]
  }

  if (length(names_to_flag) == 0) {
    stop("No names found to create outcome flags for")
  }

  # Join with index dates
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Apply date filter (AFTER index instead of BEFORE)
  if (date_filter == "after_index") {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] > .data[["index_date"]])
  } else if (date_filter == "after_or_on_index") {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] >= .data[["index_date"]])
  } else {
    data_filtered <- data_with_index
  }

  # Apply time window filter if specified
  if (!is.null(time_window)) {
    if (!all(c("start", "end") %in% names(time_window))) {
      stop("time_window must have 'start' and 'end' elements")
    }

    data_filtered <- data_filtered %>%
      mutate(days_after = as.numeric(.data[[event_date_col]] - .data[["index_date"]])) %>%
      filter(
        days_after >= time_window$start,
        days_after <= time_window$end
      ) %>%
      select(-days_after)
  }

  # Start with index_dates
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  # Create outcome flags for each name
  for (name_val in names_to_flag) {
    # Filter for this specific name
    name_data <- data_filtered %>%
      filter(name == name_val)

    # Create safe column name (replace spaces and special characters)
    safe_name <- gsub("[^A-Za-z0-9]", "_", name_val)
    outcome_col <- paste0(safe_name, "_outcome")

    # Create outcome flag (1 if patient had event, 0 otherwise)
    name_outcomes <- name_data %>%
      group_by(.data[[patient_id_col]]) %>%
      summarise(
        !!outcome_col := 1L,
        .groups = "drop"
      )

    # Add first date if requested
    if (include_date %in% c("first", "both")) {
      first_col <- paste0(safe_name, "_first_date")
      name_first <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!first_col := min(.data[[event_date_col]], na.rm = TRUE),
          .groups = "drop"
        )
      name_outcomes <- name_outcomes %>%
        left_join(name_first, by = patient_id_col)
    }

    # Add last date if requested
    if (include_date %in% c("last", "both")) {
      last_col <- paste0(safe_name, "_last_date")
      name_last <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!last_col := max(.data[[event_date_col]], na.rm = TRUE),
          .groups = "drop"
        )
      name_outcomes <- name_outcomes %>%
        left_join(name_last, by = patient_id_col)
    }

    # Add days_to_event if requested (days FROM index TO event)
    should_include_days <- FALSE
    if (is.logical(include_days_to_event) && include_days_to_event) {
      should_include_days <- TRUE
    } else if (is.character(include_days_to_event) && name_val %in% include_days_to_event) {
      should_include_days <- TRUE
    }

    if (should_include_days) {
      days_col <- paste0(safe_name, "_days_to_event")
      name_days <- name_data %>%
        left_join(
          index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
          by = patient_id_col
        ) %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!days_col := as.numeric(min(.data[[event_date_col]]) - first(.data[["index_date"]])),
          .groups = "drop"
        )
      name_outcomes <- name_outcomes %>%
        left_join(name_days, by = patient_id_col)
    }

    # Join with result
    result <- result %>%
      left_join(name_outcomes, by = patient_id_col)

    # Fill NA outcome flags with 0
    result[[outcome_col]][is.na(result[[outcome_col]])] <- 0L
  }

  return(result)
}


#' Extract Min or Max Value from Time Window by Name (Outcomes)
#'
#' For patients with multiple events AFTER index, extracts either the minimum or
#' maximum value of a specified column. Mirror of extract_value_by_name() but for
#' outcomes AFTER index.
#'
#' @param long_data Data frame. Long format data filtered by filter_long_by_lookup()
#'   with 'name' column included.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column.
#' @param value_col Character. Name of the column to extract min/max from.
#'   If NULL, uses the event_date_col itself. Default is NULL.
#' @param patient_id_col Character. Name of the patient ID column.
#' @param names_to_extract Character vector. Specific 'name' values to extract for.
#'   If NULL, extracts for all unique names.
#' @param extract_function Character. Either "min" or "max". Default is "min".
#' @param time_window List with 'start' and 'end' (days AFTER index). Required.
#' @param window_label Character. Label for the time window to use in column names.
#'
#' @return Data frame with one row per patient containing extracted values.
#'
#' @examples
#' # Get first (earliest) adverse event date for each type in first year
#' first_events <- extract_outcome_value_by_name(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   value_col = "event_date",
#'   names_to_extract = c("MI", "Stroke"),
#'   extract_function = "min",
#'   time_window = list(start = 0, end = 365),
#'   window_label = "first_year"
#' )
#'
#' # Get maximum lab value for each test in first 90 days
#' max_labs <- extract_outcome_value_by_name(
#'   long_data = lab_results,
#'   index_dates = index_dates,
#'   event_date_col = "test_date",
#'   value_col = "test_value",
#'   extract_function = "max",
#'   time_window = list(start = 0, end = 90),
#'   window_label = "first_90d"
#' )
#'
#' @export
extract_outcome_value_by_name <- function(long_data,
                                           index_dates,
                                           event_date_col = "record_date",
                                           value_col = NULL,
                                           patient_id_col = "patient_id",
                                           names_to_extract = NULL,
                                           extract_function = c("min", "max"),
                                           time_window,
                                           window_label = "window") {

  # Input validation
  if (!is.data.frame(long_data)) {
    stop("long_data must be a data frame")
  }

  if (!"name" %in% names(long_data)) {
    stop("long_data must contain a 'name' column. ",
         "Use filter_long_by_lookup() with add_lookup_info=TRUE first.")
  }

  if (!is.data.frame(index_dates)) {
    stop("index_dates must be a data frame")
  }

  if (!patient_id_col %in% names(long_data)) {
    stop("patient_id_col '", patient_id_col, "' not found in long_data")
  }

  if (!patient_id_col %in% names(index_dates)) {
    stop("patient_id_col '", patient_id_col, "' not found in index_dates")
  }

  if (!"index_date" %in% names(index_dates)) {
    stop("index_dates must contain a column named 'index_date'")
  }

  if (!event_date_col %in% names(long_data)) {
    stop("event_date_col '", event_date_col, "' not found in long_data")
  }

  # If value_col not specified, use event_date_col
  if (is.null(value_col)) {
    value_col <- event_date_col
  }

  if (!value_col %in% names(long_data)) {
    stop("value_col '", value_col, "' not found in long_data")
  }

  extract_function <- match.arg(extract_function)

  if (is.null(time_window) || !all(c("start", "end") %in% names(time_window))) {
    stop("time_window is required and must have 'start' and 'end' elements")
  }

  # Determine which names to extract
  if (is.null(names_to_extract)) {
    names_to_extract <- unique(long_data$name)
    names_to_extract <- names_to_extract[!is.na(names_to_extract)]
  }

  # Join with index dates
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Apply time window filter (AFTER index)
  data_filtered <- data_with_index %>%
    mutate(days_after = as.numeric(.data[[event_date_col]] - .data[["index_date"]])) %>%
    filter(
      days_after >= time_window$start,
      days_after <= time_window$end
    ) %>%
    select(-days_after)

  # Start with index_dates
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  # Extract value for each name
  for (name_val in names_to_extract) {
    # Filter for this specific name
    name_data <- data_filtered %>%
      filter(name == name_val)

    # Create safe column name
    safe_name <- gsub("[^A-Za-z0-9]", "_", name_val)
    func_label <- tolower(extract_function)
    value_col_name <- paste0(window_label, "_", safe_name, "_", func_label, "_", gsub("[^A-Za-z0-9]", "_", value_col))

    # Extract min or max value
    if (extract_function == "min") {
      name_values <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!value_col_name := min(.data[[value_col]], na.rm = TRUE),
          .groups = "drop"
        )
    } else {
      name_values <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!value_col_name := max(.data[[value_col]], na.rm = TRUE),
          .groups = "drop"
        )
    }

    # Join with result
    result <- result %>%
      left_join(name_values, by = patient_id_col)
  }

  return(result)
}


#' Generate Outcomes by Name with Time Window
#'
#' Convenience function that combines filtering by name and generating outcomes
#' within a time window AFTER index. Creates separate outcomes for each unique 'name' value.
#' Mirror of generate_covariates_by_name_window() but for outcomes AFTER index.
#'
#' @param long_data Data frame. Long format data with 'name' column from lookup table.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column.
#' @param patient_id_col Character. Name of the patient ID column.
#' @param names_to_analyze Character vector. Specific names to analyze.
#'   If NULL, analyzes all unique names.
#' @param time_window List with 'start' and 'end' (days AFTER index). Optional.
#' @param window_label Character. Label for the time window.
#' @param aggregation_functions Named list. Functions to apply for each name.
#' @param separate_by_name Logical. If TRUE, creates separate columns for each name.
#'   If FALSE, aggregates across all specified names. Default is TRUE.
#'
#' @return Data frame with outcomes, either separated by name or aggregated.
#'
#' @examples
#' # Count events for each adverse event type in first year
#' adverse_counts <- generate_outcomes_by_name_window(
#'   long_data = adverse_events,
#'   index_dates = index_dates,
#'   event_date_col = "event_date",
#'   names_to_analyze = c("MI", "Stroke"),
#'   time_window = list(start = 0, end = 365),
#'   window_label = "first_year"
#' )
#'
#' @export
generate_outcomes_by_name_window <- function(long_data,
                                              index_dates,
                                              event_date_col = "record_date",
                                              patient_id_col = "patient_id",
                                              names_to_analyze = NULL,
                                              time_window = NULL,
                                              window_label = "window",
                                              aggregation_functions = NULL,
                                              separate_by_name = TRUE) {

  # Input validation
  if (!"name" %in% names(long_data)) {
    stop("long_data must contain a 'name' column. ",
         "Use filter_long_by_lookup() with add_lookup_info=TRUE first.")
  }

  # Default aggregation functions
  if (is.null(aggregation_functions)) {
    aggregation_functions <- list(
      n_events = ~ n(),
      has_outcome = ~ n() > 0
    )
  }

  # Determine which names to analyze
  if (is.null(names_to_analyze)) {
    names_to_analyze <- unique(long_data$name)
    names_to_analyze <- names_to_analyze[!is.na(names_to_analyze)]
  }

  if (!separate_by_name) {
    # Filter to specified names and generate outcomes
    filtered_data <- long_data %>%
      filter(name %in% names_to_analyze)

    if (!is.null(time_window)) {
      return(generate_outcomes_time_window(
        long_data = filtered_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        days_after_start = time_window$start,
        days_after_end = time_window$end,
        aggregation_functions = aggregation_functions,
        window_label = window_label
      ))
    } else {
      return(generate_outcomes_after_index(
        long_data = filtered_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        aggregation_functions = aggregation_functions
      ))
    }
  }

  # Separate outcomes by name
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  for (name_val in names_to_analyze) {
    # Filter for this name
    name_data <- long_data %>%
      filter(name == name_val)

    # Generate outcomes
    if (!is.null(time_window)) {
      name_outcomes <- generate_outcomes_time_window(
        long_data = name_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        days_after_start = time_window$start,
        days_after_end = time_window$end,
        aggregation_functions = aggregation_functions,
        window_label = window_label,
        fill_missing_patients = TRUE
      )
    } else {
      name_outcomes <- generate_outcomes_after_index(
        long_data = name_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        aggregation_functions = aggregation_functions,
        fill_missing_patients = TRUE
      )
    }

    # Add name to column names (except patient_id and index_date)
    safe_name <- gsub("[^A-Za-z0-9]", "_", name_val)
    outcome_cols <- setdiff(names(name_outcomes), c(patient_id_col, "index_date"))

    for (col in outcome_cols) {
      new_name <- paste0(safe_name, "_", col)
      name_outcomes <- name_outcomes %>%
        rename(!!new_name := !!col)
    }

    # Join with result
    result <- result %>%
      left_join(
        name_outcomes %>% select(-index_date),
        by = patient_id_col
      )
  }

  return(result)
}
