# Generate Covariates from Long Format Data
#
# This module provides functions to:
# 1. Filter long format data using lookup tables for specific codes
# 2. Generate covariates based on events before an index date
# 3. Generate covariates for events within specific time windows before index date
#
# @author SAIL BDCD Team
# @date 2025-01-24

library(dplyr)
library(tidyr)
library(lubridate)

#' Filter Long Format Data by Code Lookup Table
#'
#' Filters long format data to retain only rows where a specified code column
#' matches codes in a lookup table. The lookup table should have columns:
#' 'code', 'name', 'description', 'terminology'.
#'
#' @param long_data Data frame. Long format data with patient_id, source metadata,
#'   and data columns including a code column and event date.
#' @param lookup_table Data frame. Lookup table with columns 'code', 'name',
#'   'description', 'terminology'. The 'code' column contains the codes to filter for.
#' @param code_col_in_data Character. Name of the code column in long_data to match
#'   against lookup_table$code (e.g., "icd10_code", "read_code", "opcs_code").
#' @param event_date_col Character. Name of the event date column in long_data
#'   (e.g., "record_date", "admission_date"). Default is "record_date".
#' @param event_date_range Date vector of length 2. Optional date range to filter
#'   events: c(start_date, end_date). If NULL, no date filtering applied.
#' @param terminology_filter Character vector. Optional filter for specific
#'   terminology types (e.g., c("ICD10", "Read")). If NULL, all terminologies included.
#' @param add_lookup_info Logical. If TRUE, adds 'name', 'description', 'terminology'
#'   columns from lookup_table to the result. Default is TRUE.
#'
#' @return Data frame. Filtered long format data with only rows matching the
#'   lookup table codes and optional date range. If add_lookup_info=TRUE, includes
#'   additional columns from the lookup table.
#'
#' @examples
#' # Create a lookup table for specific diagnosis codes
#' diabetes_codes <- data.frame(
#'   code = c("E10", "E11", "E13", "E14"),
#'   name = c("Type 1 DM", "Type 2 DM", "Other DM", "Unspecified DM"),
#'   description = c("Type 1 diabetes mellitus", "Type 2 diabetes mellitus",
#'                   "Other specified diabetes", "Unspecified diabetes"),
#'   terminology = c("ICD10", "ICD10", "ICD10", "ICD10")
#' )
#'
#' # Filter long format data for these codes
#' diabetes_events <- filter_long_by_lookup(
#'   long_data = hospital_long,
#'   lookup_table = diabetes_codes,
#'   code_col_in_data = "icd10_code",
#'   event_date_col = "admission_date"
#' )
#'
#' # Filter with date range and terminology
#' recent_diabetes <- filter_long_by_lookup(
#'   long_data = hospital_long,
#'   lookup_table = diabetes_codes,
#'   code_col_in_data = "icd10_code",
#'   event_date_col = "admission_date",
#'   event_date_range = c(as.Date("2020-01-01"), as.Date("2023-12-31")),
#'   terminology_filter = "ICD10"
#' )
#'
#' @export
filter_long_by_lookup <- function(long_data,
                                   lookup_table,
                                   code_col_in_data,
                                   event_date_col = "record_date",
                                   event_date_range = NULL,
                                   terminology_filter = NULL,
                                   add_lookup_info = TRUE) {

  # Input validation
  if (!is.data.frame(long_data)) {
    stop("long_data must be a data frame")
  }

  if (!is.data.frame(lookup_table)) {
    stop("lookup_table must be a data frame")
  }

  if (nrow(long_data) == 0) {
    warning("long_data is empty, returning empty data frame")
    return(long_data)
  }

  if (nrow(lookup_table) == 0) {
    warning("lookup_table is empty, returning empty data frame")
    return(long_data[0, ])
  }

  # Validate lookup_table has required columns
  required_lookup_cols <- c("code", "name", "description", "terminology")
  missing_cols <- setdiff(required_lookup_cols, names(lookup_table))
  if (length(missing_cols) > 0) {
    stop("lookup_table must have columns: ", paste(required_lookup_cols, collapse = ", "),
         "\nMissing: ", paste(missing_cols, collapse = ", "))
  }

  # Validate code_col_in_data exists in long_data
  if (!code_col_in_data %in% names(long_data)) {
    stop("code_col_in_data '", code_col_in_data, "' not found in long_data")
  }

  # Validate event_date_col exists if specified
  if (!is.null(event_date_col) && !event_date_col %in% names(long_data)) {
    stop("event_date_col '", event_date_col, "' not found in long_data")
  }

  # Filter lookup_table by terminology if specified
  lookup_filtered <- lookup_table
  if (!is.null(terminology_filter)) {
    lookup_filtered <- lookup_filtered %>%
      filter(terminology %in% terminology_filter)

    if (nrow(lookup_filtered) == 0) {
      warning("No codes found for terminology: ", paste(terminology_filter, collapse = ", "),
              "\nReturning empty data frame")
      return(long_data[0, ])
    }
  }

  # Perform the join based on whether we want to add lookup info
  if (add_lookup_info) {
    # Inner join to add lookup table columns
    filtered_data <- long_data %>%
      inner_join(
        lookup_filtered,
        by = setNames("code", code_col_in_data),
        relationship = "many-to-many"
      )
  } else {
    # Semi join to just filter without adding columns
    filtered_data <- long_data %>%
      semi_join(
        lookup_filtered %>% select(code),
        by = setNames("code", code_col_in_data)
      )
  }

  # Apply date range filter if specified
  if (!is.null(event_date_range)) {
    if (!is.null(event_date_col)) {
      if (length(event_date_range) != 2) {
        stop("event_date_range must be a vector of length 2: c(start_date, end_date)")
      }

      start_date <- as.Date(event_date_range[1])
      end_date <- as.Date(event_date_range[2])

      if (start_date > end_date) {
        stop("event_date_range start_date must be <= end_date")
      }

      filtered_data <- filtered_data %>%
        filter(
          .data[[event_date_col]] >= start_date,
          .data[[event_date_col]] <= end_date
        )
    } else {
      warning("event_date_range specified but event_date_col is NULL. Ignoring date filter.")
    }
  }

  return(filtered_data)
}


#' Generate Covariates from Events Before Index Date
#'
#' Creates patient-level covariates based on events occurring before an index date.
#' This function aggregates event-level data (long format) into patient-level
#' summary statistics.
#'
#' @param long_data Data frame. Long format data with patient_id, event date,
#'   and other event-level data columns.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#'   One row per patient.
#' @param event_date_col Character. Name of the event date column in long_data.
#'   Default is "record_date".
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param aggregation_functions Named list. Functions to apply for each covariate.
#'   Names are output column names, values are expressions or functions.
#'   Default creates count and indicator covariates.
#' @param include_before_only Logical. If TRUE, only includes events strictly before
#'   index_date (exclusive). If FALSE, includes events on or before index_date
#'   (inclusive). Default is TRUE.
#' @param min_days_before Numeric. Optional minimum number of days before index_date
#'   to include events. Default is NULL (no minimum).
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events
#'   (fills with 0/FALSE). Default is TRUE.
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - Aggregated covariates as specified in aggregation_functions
#'
#' @examples
#' # Create index dates
#' index_dates <- data.frame(
#'   patient_id = c(1, 2, 3),
#'   index_date = as.Date(c("2023-01-01", "2023-06-01", "2023-03-15"))
#' )
#'
#' # Generate covariates: count events before index
#' covariates <- generate_covariates_before_index(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date"
#' )
#'
#' # Custom aggregation functions
#' covariates <- generate_covariates_before_index(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   aggregation_functions = list(
#'     n_events = ~ n(),
#'     has_event = ~ n() > 0,
#'     earliest_event = ~ min(admission_date, na.rm = TRUE),
#'     latest_event = ~ max(admission_date, na.rm = TRUE),
#'     n_sources = ~ n_distinct(source_table),
#'     n_unique_codes = ~ n_distinct(icd10_code),
#'     most_common_code = ~ names(sort(table(icd10_code), decreasing = TRUE))[1]
#'   )
#' )
#'
#' @export
generate_covariates_before_index <- function(long_data,
                                              index_dates,
                                              event_date_col = "record_date",
                                              patient_id_col = "patient_id",
                                              aggregation_functions = NULL,
                                              include_before_only = TRUE,
                                              min_days_before = NULL,
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
      has_event = ~ n() > 0
    )
  }

  # Join long_data with index_dates to get index_date for each event
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Filter events based on index_date
  if (include_before_only) {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] < .data[["index_date"]])
  } else {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] <= .data[["index_date"]])
  }

  # Apply minimum days before filter if specified
  if (!is.null(min_days_before)) {
    if (!is.numeric(min_days_before) || min_days_before < 0) {
      stop("min_days_before must be a non-negative number")
    }

    data_filtered <- data_filtered %>%
      mutate(days_before = as.numeric(.data[["index_date"]] - .data[[event_date_col]])) %>%
      filter(days_before >= min_days_before) %>%
      select(-days_before)
  }

  # Check if there's any data left after filtering
  if (nrow(data_filtered) == 0 && fill_missing_patients) {
    # No events for any patient - return index_dates with default values
    covariates <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date")))

    # Add covariate columns with default values
    for (agg_name in names(aggregation_functions)) {
      if (grepl("^n_", agg_name) || grepl("count", agg_name, ignore.case = TRUE)) {
        covariates[[agg_name]] <- 0
      } else if (grepl("^has_", agg_name)) {
        covariates[[agg_name]] <- FALSE
      } else {
        covariates[[agg_name]] <- NA
      }
    }

    return(covariates)
  }

  # Aggregate by patient
  covariates <- data_filtered %>%
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
    covariates <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date"))) %>%
      left_join(covariates, by = patient_id_col)

    # Fill missing values
    for (col in names(covariates)) {
      if (col %in% c(patient_id_col, "index_date")) next

      if (grepl("^n_", col) || grepl("count", col, ignore.case = TRUE)) {
        covariates[[col]][is.na(covariates[[col]])] <- 0
      } else if (grepl("^has_", col) || is.logical(covariates[[col]])) {
        covariates[[col]][is.na(covariates[[col]])] <- FALSE
      }
    }
  } else {
    covariates <- covariates %>%
      left_join(
        index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
        by = patient_id_col
      )
  }

  return(covariates)
}


#' Generate Covariates from Events Within Time Window Before Index Date
#'
#' Creates patient-level covariates based on events occurring within a specific
#' time window before an index date (e.g., 30-90 days before, 1-2 years before).
#'
#' @param long_data Data frame. Long format data with patient_id, event date,
#'   and other event-level data columns.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column in long_data.
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param days_before_start Numeric. Start of the time window (days before index_date).
#'   Must be >= 0. For example, 30 means "starting 30 days before index".
#' @param days_before_end Numeric. End of the time window (days before index_date).
#'   Must be >= 0 and < days_before_start. For example, 0 means "up to index date".
#'   Use 1 to exclude the index date itself.
#' @param aggregation_functions Named list. Functions to apply for each covariate.
#'   Default creates count and indicator covariates.
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events
#'   in the window. Default is TRUE.
#' @param window_label Character. Optional label for the time window to prepend to
#'   covariate names (e.g., "30to90d"). If NULL, uses "window".
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - Aggregated covariates with window_label prefix
#'
#' @examples
#' # Create index dates
#' index_dates <- data.frame(
#'   patient_id = c(1, 2, 3),
#'   index_date = as.Date(c("2023-01-01", "2023-06-01", "2023-03-15"))
#' )
#'
#' # Events in last 30 days before index (1-30 days to exclude index date itself)
#' covariates_30d <- generate_covariates_time_window(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   days_before_start = 30,
#'   days_before_end = 1,
#'   window_label = "last_30d"
#' )
#'
#' # Events in 30-90 days before index
#' covariates_30to90 <- generate_covariates_time_window(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   days_before_start = 90,
#'   days_before_end = 30,
#'   window_label = "30to90d"
#' )
#'
#' # Events in 1-2 years before index
#' covariates_1to2y <- generate_covariates_time_window(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   days_before_start = 730,  # 2 years
#'   days_before_end = 365,    # 1 year
#'   window_label = "1to2y"
#' )
#'
#' @export
generate_covariates_time_window <- function(long_data,
                                             index_dates,
                                             event_date_col = "record_date",
                                             patient_id_col = "patient_id",
                                             days_before_start,
                                             days_before_end,
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

  # Validate time window parameters
  if (!is.numeric(days_before_start) || days_before_start < 0) {
    stop("days_before_start must be a non-negative number")
  }

  if (!is.numeric(days_before_end) || days_before_end < 0) {
    stop("days_before_end must be a non-negative number")
  }

  if (days_before_end >= days_before_start) {
    stop("days_before_end must be < days_before_start. ",
         "For example, days_before_start=90, days_before_end=30 means ",
         "'between 90 and 30 days before index'")
  }

  # Default aggregation functions
  if (is.null(aggregation_functions)) {
    aggregation_functions <- list(
      n_events = ~ n(),
      has_event = ~ n() > 0
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

  # Calculate days_before_index for each event
  data_with_window <- data_with_index %>%
    mutate(
      days_before_index = as.numeric(.data[["index_date"]] - .data[[event_date_col]])
    )

  # Filter to events within the time window
  data_filtered <- data_with_window %>%
    filter(
      days_before_index >= days_before_end,
      days_before_index <= days_before_start
    ) %>%
    select(-days_before_index)

  # Check if there's any data left after filtering
  if (nrow(data_filtered) == 0 && fill_missing_patients) {
    # No events for any patient in this window - return index_dates with default values
    covariates <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date")))

    # Add covariate columns with default values and window prefix
    for (agg_name in names(aggregation_functions)) {
      col_name <- paste0(window_label, "_", agg_name)
      if (grepl("^n_", agg_name) || grepl("count", agg_name, ignore.case = TRUE)) {
        covariates[[col_name]] <- 0
      } else if (grepl("^has_", agg_name)) {
        covariates[[col_name]] <- FALSE
      } else {
        covariates[[col_name]] <- NA
      }
    }

    return(covariates)
  }

  # Aggregate by patient
  covariates <- data_filtered %>%
    group_by(.data[[patient_id_col]]) %>%
    summarise(
      !!!lapply(aggregation_functions, function(f) {
        if (is.function(f)) return(f)
        if (rlang::is_formula(f)) return(rlang::as_function(f))
        return(f)
      }),
      .groups = "drop"
    )

  # Prepend window_label to covariate names (except patient_id)
  covariate_cols <- setdiff(names(covariates), patient_id_col)
  for (col in covariate_cols) {
    new_name <- paste0(window_label, "_", col)
    covariates <- covariates %>%
      rename(!!new_name := !!col)
  }

  # Join back with index_dates to get all patients
  if (fill_missing_patients) {
    covariates <- index_dates %>%
      select(all_of(c(patient_id_col, "index_date"))) %>%
      left_join(covariates, by = patient_id_col)

    # Fill missing values
    for (col in names(covariates)) {
      if (col %in% c(patient_id_col, "index_date")) next

      if (grepl("_n_", col) || grepl("count", col, ignore.case = TRUE)) {
        covariates[[col]][is.na(covariates[[col]])] <- 0
      } else if (grepl("_has_", col) || is.logical(covariates[[col]])) {
        covariates[[col]][is.na(covariates[[col]])] <- FALSE
      }
    }
  } else {
    covariates <- covariates %>%
      left_join(
        index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
        by = patient_id_col
      )
  }

  return(covariates)
}


#' Generate Multiple Time Window Covariates
#'
#' Convenience function to generate covariates for multiple time windows in one call.
#' This is useful for creating a comprehensive set of temporal covariates.
#'
#' @param long_data Data frame. Long format data with patient_id and event date.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column.
#' @param patient_id_col Character. Name of the patient ID column.
#' @param time_windows List of lists. Each element specifies a time window:
#'   list(start = days_before_start, end = days_before_end, label = "label")
#' @param aggregation_functions Named list. Functions to apply for each covariate.
#' @param fill_missing_patients Logical. If TRUE, includes patients with no events.
#'
#' @return Data frame with patient_id, index_date, and covariates from all time windows.
#'
#' @examples
#' # Define multiple time windows
#' windows <- list(
#'   list(start = 30, end = 1, label = "last_30d"),
#'   list(start = 90, end = 31, label = "30to90d"),
#'   list(start = 365, end = 91, label = "90d_to_1y"),
#'   list(start = 730, end = 366, label = "1to2y")
#' )
#'
#' # Generate covariates for all windows
#' all_covariates <- generate_multiple_time_windows(
#'   long_data = hospital_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   time_windows = windows
#' )
#'
#' @export
generate_multiple_time_windows <- function(long_data,
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

  # Generate covariates for each time window
  for (i in seq_along(time_windows)) {
    window <- time_windows[[i]]

    # Validate window structure
    if (!all(c("start", "end", "label") %in% names(window))) {
      stop("Each time window must have 'start', 'end', and 'label' elements. ",
           "Window ", i, " is missing some of these.")
    }

    # Generate covariates for this window
    window_covariates <- generate_covariates_time_window(
      long_data = long_data,
      index_dates = index_dates,
      event_date_col = event_date_col,
      patient_id_col = patient_id_col,
      days_before_start = window$start,
      days_before_end = window$end,
      aggregation_functions = aggregation_functions,
      fill_missing_patients = fill_missing_patients,
      window_label = window$label
    )

    # Join with result
    result <- result %>%
      left_join(
        window_covariates %>% select(-index_date),
        by = patient_id_col
      )
  }

  return(result)
}


#' Generate Flag-Based Covariates by Name
#'
#' Creates patient-level binary flags (0/1) for each unique 'name' in the lookup table.
#' Optionally includes the date of first/last event and days between index and event.
#' This function filters by name and creates one flag column per name.
#'
#' @param long_data Data frame. Long format data that has been filtered by
#'   filter_long_by_lookup() and includes the 'name' column.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column in long_data.
#' @param patient_id_col Character. Name of the patient ID column. Default is "patient_id".
#' @param names_to_flag Character vector. Specific 'name' values to create flags for.
#'   If NULL, creates flags for all unique names in long_data.
#' @param include_date Character. Whether to include event date: "none", "earliest",
#'   "latest", or "both". Default is "none".
#' @param include_days_between Logical or character vector. If TRUE, includes
#'   days_between for all names. If character vector, only includes for specified names.
#'   Default is FALSE.
#' @param date_filter Character. Which events to consider: "before_index" (default),
#'   "before_or_on_index", or "all".
#' @param time_window List with 'start' and 'end' (days before index). Optional.
#'   If provided, only considers events within this window.
#'
#' @return Data frame with one row per patient containing:
#'   - patient_id
#'   - index_date
#'   - {name}_flag: Binary flag (0/1) for each name
#'   - {name}_earliest_date: (if include_date = "earliest" or "both")
#'   - {name}_latest_date: (if include_date = "latest" or "both")
#'   - {name}_days_between: (if include_days_between includes this name)
#'
#' @examples
#' # Filter and add lookup info first
#' diabetes_events <- filter_long_by_lookup(
#'   long_data = hospital_admissions,
#'   lookup_table = diabetes_codes,
#'   code_col_in_data = "icd10_code",
#'   add_lookup_info = TRUE  # MUST be TRUE to have 'name' column
#' )
#'
#' # Create flags for each diabetes type
#' flags <- generate_flag_covariates_by_name(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date"
#' )
#'
#' # With dates and days_between
#' flags_detailed <- generate_flag_covariates_by_name(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   include_date = "both",
#'   include_days_between = c("Type 1 DM", "Type 2 DM")
#' )
#'
#' # Within specific time window
#' flags_recent <- generate_flag_covariates_by_name(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   time_window = list(start = 365, end = 1)
#' )
#'
#' @export
generate_flag_covariates_by_name <- function(long_data,
                                              index_dates,
                                              event_date_col = "record_date",
                                              patient_id_col = "patient_id",
                                              names_to_flag = NULL,
                                              include_date = c("none", "earliest", "latest", "both"),
                                              include_days_between = FALSE,
                                              date_filter = c("before_index", "before_or_on_index", "all"),
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
    stop("No names found to create flags for")
  }

  # Join with index dates
  data_with_index <- long_data %>%
    inner_join(
      index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
      by = patient_id_col
    )

  # Apply date filter
  if (date_filter == "before_index") {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] < .data[["index_date"]])
  } else if (date_filter == "before_or_on_index") {
    data_filtered <- data_with_index %>%
      filter(.data[[event_date_col]] <= .data[["index_date"]])
  } else {
    data_filtered <- data_with_index
  }

  # Apply time window filter if specified
  if (!is.null(time_window)) {
    if (!all(c("start", "end") %in% names(time_window))) {
      stop("time_window must have 'start' and 'end' elements")
    }

    data_filtered <- data_filtered %>%
      mutate(days_before = as.numeric(.data[["index_date"]] - .data[[event_date_col]])) %>%
      filter(
        days_before >= time_window$end,
        days_before <= time_window$start
      ) %>%
      select(-days_before)
  }

  # Start with index_dates
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  # Create flags for each name
  for (name_val in names_to_flag) {
    # Filter for this specific name
    name_data <- data_filtered %>%
      filter(name == name_val)

    # Create safe column name (replace spaces and special characters)
    safe_name <- gsub("[^A-Za-z0-9]", "_", name_val)
    flag_col <- paste0(safe_name, "_flag")

    # Create flag (1 if patient has any event with this name, 0 otherwise)
    name_flags <- name_data %>%
      group_by(.data[[patient_id_col]]) %>%
      summarise(
        !!flag_col := 1L,
        .groups = "drop"
      )

    # Add earliest date if requested
    if (include_date %in% c("earliest", "both")) {
      earliest_col <- paste0(safe_name, "_earliest_date")
      name_earliest <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!earliest_col := min(.data[[event_date_col]], na.rm = TRUE),
          .groups = "drop"
        )
      name_flags <- name_flags %>%
        left_join(name_earliest, by = patient_id_col)
    }

    # Add latest date if requested
    if (include_date %in% c("latest", "both")) {
      latest_col <- paste0(safe_name, "_latest_date")
      name_latest <- name_data %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!latest_col := max(.data[[event_date_col]], na.rm = TRUE),
          .groups = "drop"
        )
      name_flags <- name_flags %>%
        left_join(name_latest, by = patient_id_col)
    }

    # Add days_between if requested
    should_include_days <- FALSE
    if (is.logical(include_days_between) && include_days_between) {
      should_include_days <- TRUE
    } else if (is.character(include_days_between) && name_val %in% include_days_between) {
      should_include_days <- TRUE
    }

    if (should_include_days) {
      days_col <- paste0(safe_name, "_days_between")
      name_days <- name_data %>%
        left_join(
          index_dates %>% select(all_of(c(patient_id_col, "index_date"))),
          by = patient_id_col
        ) %>%
        group_by(.data[[patient_id_col]]) %>%
        summarise(
          !!days_col := as.numeric(first(.data[["index_date"]]) - max(.data[[event_date_col]])),
          .groups = "drop"
        )
      name_flags <- name_flags %>%
        left_join(name_days, by = patient_id_col)
    }

    # Join with result
    result <- result %>%
      left_join(name_flags, by = patient_id_col)

    # Fill NA flags with 0
    result[[flag_col]][is.na(result[[flag_col]])] <- 0L
  }

  return(result)
}


#' Extract Min or Max Value from Time Window by Name
#'
#' For patients with multiple events within a time window, extracts either the
#' minimum or maximum value of a specified column. Useful for getting earliest/latest
#' dates or min/max clinical measurements.
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
#' @param time_window List with 'start' and 'end' (days before index). Required.
#' @param window_label Character. Label for the time window to use in column names.
#'
#' @return Data frame with one row per patient containing extracted values.
#'
#' @examples
#' # Get earliest admission date for each diabetes type in last year
#' earliest_admissions <- extract_value_by_name(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   value_col = "admission_date",
#'   names_to_extract = c("Type 1 DM", "Type 2 DM"),
#'   extract_function = "min",
#'   time_window = list(start = 365, end = 1),
#'   window_label = "last_year"
#' )
#'
#' # Get maximum lab value for each test in last 90 days
#' max_labs <- extract_value_by_name(
#'   long_data = lab_results,
#'   index_dates = index_dates,
#'   event_date_col = "test_date",
#'   value_col = "test_value",
#'   extract_function = "max",
#'   time_window = list(start = 90, end = 1),
#'   window_label = "last_90d"
#' )
#'
#' @export
extract_value_by_name <- function(long_data,
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

  # Apply time window filter
  data_filtered <- data_with_index %>%
    mutate(days_before = as.numeric(.data[["index_date"]] - .data[[event_date_col]])) %>%
    filter(
      days_before >= time_window$end,
      days_before <= time_window$start
    ) %>%
    select(-days_before)

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


#' Generate Covariates by Name with Time Window
#'
#' Convenience function that combines filtering by name and generating covariates
#' within a time window. Creates separate covariates for each unique 'name' value.
#'
#' @param long_data Data frame. Long format data with 'name' column from lookup table.
#' @param index_dates Data frame. Must contain patient_id and index_date columns.
#' @param event_date_col Character. Name of the event date column.
#' @param patient_id_col Character. Name of the patient ID column.
#' @param names_to_analyze Character vector. Specific names to analyze.
#'   If NULL, analyzes all unique names.
#' @param time_window List with 'start' and 'end' (days before index). Optional.
#' @param window_label Character. Label for the time window.
#' @param aggregation_functions Named list. Functions to apply for each name.
#' @param separate_by_name Logical. If TRUE, creates separate columns for each name.
#'   If FALSE, aggregates across all specified names. Default is TRUE.
#'
#' @return Data frame with covariates, either separated by name or aggregated.
#'
#' @examples
#' # Count events for each diabetes type in last year
#' diabetes_counts <- generate_covariates_by_name_window(
#'   long_data = diabetes_events,
#'   index_dates = index_dates,
#'   event_date_col = "admission_date",
#'   names_to_analyze = c("Type 1 DM", "Type 2 DM"),
#'   time_window = list(start = 365, end = 1),
#'   window_label = "last_year"
#' )
#'
#' @export
generate_covariates_by_name_window <- function(long_data,
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
      has_event = ~ n() > 0
    )
  }

  # Determine which names to analyze
  if (is.null(names_to_analyze)) {
    names_to_analyze <- unique(long_data$name)
    names_to_analyze <- names_to_analyze[!is.na(names_to_analyze)]
  }

  if (!separate_by_name) {
    # Filter to specified names and generate covariates
    filtered_data <- long_data %>%
      filter(name %in% names_to_analyze)

    if (!is.null(time_window)) {
      return(generate_covariates_time_window(
        long_data = filtered_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        days_before_start = time_window$start,
        days_before_end = time_window$end,
        aggregation_functions = aggregation_functions,
        window_label = window_label
      ))
    } else {
      return(generate_covariates_before_index(
        long_data = filtered_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        aggregation_functions = aggregation_functions
      ))
    }
  }

  # Separate covariates by name
  result <- index_dates %>%
    select(all_of(c(patient_id_col, "index_date")))

  for (name_val in names_to_analyze) {
    # Filter for this name
    name_data <- long_data %>%
      filter(name == name_val)

    # Generate covariates
    if (!is.null(time_window)) {
      name_covariates <- generate_covariates_time_window(
        long_data = name_data,
        index_dates = index_dates,
        event_date_col = event_date_col,
        patient_id_col = patient_id_col,
        days_before_start = time_window$start,
        days_before_end = time_window$end,
        aggregation_functions = aggregation_functions,
        window_label = window_label,
        fill_missing_patients = TRUE
      )
    } else {
      name_covariates <- generate_covariates_before_index(
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
    covariate_cols <- setdiff(names(name_covariates), c(patient_id_col, "index_date"))

    for (col in covariate_cols) {
      new_name <- paste0(safe_name, "_", col)
      name_covariates <- name_covariates %>%
        rename(!!new_name := !!col)
    }

    # Join with result
    result <- result %>%
      left_join(
        name_covariates %>% select(-index_date),
        by = patient_id_col
      )
  }

  return(result)
}
