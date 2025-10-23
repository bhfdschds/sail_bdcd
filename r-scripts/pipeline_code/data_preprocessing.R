# Data Preprocessing Module for Asset Creation Pipeline
# This module provides functions for cleaning and preprocessing data before asset creation
#
# Features:
# - Code matching from lookup tables
# - Value validation and range checking
# - Covariate flagging (events before baseline)
# - Outcome flagging (events after baseline)
# - Parameterized configuration via YAML

library(dplyr)
library(tidyr)
library(lubridate)

#' Apply All Preprocessing Steps to a Dataset
#'
#' Main entry point that orchestrates all preprocessing steps defined in configuration
#'
#' @param data Data frame to preprocess
#' @param preprocessing_config List containing preprocessing configuration from YAML
#' @param conn Database connection for lookup tables
#' @param cohort_data Optional data frame with patient baseline dates (patient_id, baseline_date)
#' @return Preprocessed data frame
#' @export
apply_preprocessing <- function(data, preprocessing_config, conn = NULL, cohort_data = NULL) {

  if (is.null(preprocessing_config) || length(preprocessing_config) == 0) {
    message("No preprocessing configuration found. Returning original data.")
    return(data)
  }

  preprocessed_data <- data

  # Apply each preprocessing step in order
  for (step_name in names(preprocessing_config)) {
    step_config <- preprocessing_config[[step_name]]
    step_type <- step_config$type

    message(sprintf("Applying preprocessing step: %s (type: %s)", step_name, step_type))

    preprocessed_data <- tryCatch({
      switch(step_type,
             "code_match" = apply_code_matching(preprocessed_data, step_config, conn),
             "value_validation" = apply_value_validation(preprocessed_data, step_config),
             "covariate_flag" = apply_covariate_flagging(preprocessed_data, step_config, cohort_data, conn),
             "outcome_flag" = apply_outcome_flagging(preprocessed_data, step_config, cohort_data, conn),
             "data_transformation" = apply_data_transformation(preprocessed_data, step_config),
             {
               warning(sprintf("Unknown preprocessing type: %s", step_type))
               preprocessed_data
             }
      )
    }, error = function(e) {
      warning(sprintf("Error in preprocessing step '%s': %s", step_name, e$message))
      preprocessed_data
    })
  }

  return(preprocessed_data)
}


#' Apply Code Matching from Lookup Tables
#'
#' Match codes in data against lookup tables to get standardized values or descriptions
#'
#' @param data Data frame to process
#' @param config Configuration for this code matching step
#' @param conn Database connection
#' @return Data frame with matched codes
#' @export
apply_code_matching <- function(data, config, conn) {

  lookup_source <- config$lookup_source
  code_column <- config$code_column
  match_column <- config$match_column
  output_columns <- config$output_columns
  join_type <- config$join_type %||% "left"

  # Get lookup table
  lookup_table <- get_lookup_table(lookup_source, conn, config)

  if (is.null(lookup_table) || nrow(lookup_table) == 0) {
    warning(sprintf("Lookup table is empty or not found: %s", lookup_source))
    return(data)
  }

  # Perform join
  joined_data <- switch(join_type,
                        "left" = left_join(data, lookup_table, by = setNames(match_column, code_column)),
                        "inner" = inner_join(data, lookup_table, by = setNames(match_column, code_column)),
                        "semi" = semi_join(data, lookup_table, by = setNames(match_column, code_column)),
                        left_join(data, lookup_table, by = setNames(match_column, code_column))
  )

  # Select output columns if specified
  if (!is.null(output_columns)) {
    # Keep original columns plus specified output columns
    original_cols <- names(data)
    joined_data <- joined_data %>% select(all_of(c(original_cols, output_columns)))
  }

  n_matches <- sum(!is.na(joined_data[[output_columns[1]]]))
  message(sprintf("Code matching: %d/%d records matched", n_matches, nrow(data)))

  return(joined_data)
}


#' Get Lookup Table from Various Sources
#'
#' @param source Source identifier (table name, file path, or inline data)
#' @param conn Database connection
#' @param config Configuration containing additional source details
#' @return Data frame with lookup values
#' @keywords internal
get_lookup_table <- function(source, conn, config) {

  source_type <- config$source_type %||% "database"

  lookup_table <- switch(source_type,
                         "database" = get_lookup_from_database(source, conn, config),
                         "csv" = read.csv(source, stringsAsFactors = FALSE),
                         "inline" = config$lookup_values,
                         stop(sprintf("Unknown lookup source type: %s", source_type))
  )

  return(lookup_table)
}


#' Get Lookup Table from Database
#'
#' @param table_name Name of the lookup table in database
#' @param conn Database connection
#' @param config Configuration with schema and column info
#' @return Data frame with lookup values
#' @keywords internal
get_lookup_from_database <- function(table_name, conn, config) {

  schema <- config$schema %||% "SAIL"
  columns <- config$lookup_columns

  if (is.null(columns)) {
    query <- sprintf("SELECT * FROM %s.%s", schema, table_name)
  } else {
    col_list <- paste(columns, collapse = ", ")
    query <- sprintf("SELECT %s FROM %s.%s", col_list, schema, table_name)
  }

  # Add filters if specified
  if (!is.null(config$lookup_filter)) {
    query <- sprintf("%s WHERE %s", query, config$lookup_filter)
  }

  message(sprintf("Loading lookup table: %s", query))
  lookup_data <- DBI::dbGetQuery(conn, query)

  return(lookup_data)
}


#' Apply Value Validation and Range Checking
#'
#' Validate that values fall within specified ranges and flag or filter invalid values
#'
#' @param data Data frame to validate
#' @param config Configuration for validation rules
#' @return Data frame with validation applied
#' @export
apply_value_validation <- function(data, config) {

  column <- config$column
  min_value <- config$min_value
  max_value <- config$max_value
  allowed_values <- config$allowed_values
  action <- config$action %||% "flag"  # "flag", "filter", or "transform"
  flag_column <- config$flag_column %||% paste0(column, "_valid")

  if (!column %in% names(data)) {
    warning(sprintf("Column not found for validation: %s", column))
    return(data)
  }

  # Create validity check
  is_valid <- rep(TRUE, nrow(data))

  # Range validation
  if (!is.null(min_value)) {
    is_valid <- is_valid & (data[[column]] >= min_value | is.na(data[[column]]))
  }
  if (!is.null(max_value)) {
    is_valid <- is_valid & (data[[column]] <= max_value | is.na(data[[column]]))
  }

  # Allowed values validation
  if (!is.null(allowed_values)) {
    is_valid <- is_valid & (data[[column]] %in% allowed_values | is.na(data[[column]]))
  }

  n_invalid <- sum(!is_valid)
  if (n_invalid > 0) {
    message(sprintf("Value validation: %d/%d records invalid for column '%s'", n_invalid, nrow(data), column))
  }

  # Apply action
  result <- switch(action,
                   "flag" = {
                     data[[flag_column]] <- is_valid
                     data
                   },
                   "filter" = {
                     data %>% filter(is_valid)
                   },
                   "transform" = {
                     transform_value <- config$transform_value %||% NA
                     data[[column]] <- ifelse(is_valid, data[[column]], transform_value)
                     data
                   },
                   data
  )

  return(result)
}


#' Apply Covariate Flagging Based on Event Timing
#'
#' Flag covariates where events occur BEFORE the baseline date
#'
#' @param data Data frame with event data
#' @param config Configuration for covariate flagging
#' @param cohort_data Data frame with patient baseline dates
#' @param conn Database connection for code lookups
#' @return Data frame with covariate flags added
#' @export
apply_covariate_flagging <- function(data, config, cohort_data, conn = NULL) {

  if (is.null(cohort_data)) {
    warning("Cohort data with baseline dates required for covariate flagging")
    return(data)
  }

  covariate_name <- config$covariate_name
  event_date_column <- config$event_date_column
  code_column <- config$code_column
  flag_column <- config$flag_column %||% paste0("has_", covariate_name)

  # Get event codes to match
  event_codes <- get_event_codes(config, conn)

  # Merge with cohort baseline dates
  data_with_baseline <- data %>%
    left_join(cohort_data %>% select(patient_id, baseline_date), by = "patient_id")

  # Flag covariates (events before baseline)
  data_with_baseline <- data_with_baseline %>%
    mutate(
      is_covariate_event = (!!sym(code_column) %in% event_codes) &
        (!!sym(event_date_column) < baseline_date)
    )

  # Aggregate to patient level if requested
  if (config$aggregate_to_patient %||% FALSE) {
    patient_flags <- data_with_baseline %>%
      group_by(patient_id) %>%
      summarise(
        !!flag_column := any(is_covariate_event, na.rm = TRUE),
        .groups = "drop"
      )

    # If aggregating, return just the flags
    n_flagged <- sum(patient_flags[[flag_column]])
    message(sprintf("Covariate flagging '%s': %d patients flagged", covariate_name, n_flagged))

    return(patient_flags)
  } else {
    # Add flag to each record
    data_with_baseline[[flag_column]] <- data_with_baseline$is_covariate_event
    result <- data_with_baseline %>% select(-baseline_date, -is_covariate_event)

    n_flagged <- sum(result[[flag_column]], na.rm = TRUE)
    message(sprintf("Covariate flagging '%s': %d records flagged", covariate_name, n_flagged))

    return(result)
  }
}


#' Apply Outcome Flagging Based on Event Timing
#'
#' Flag outcomes where events occur AFTER the baseline date
#'
#' @param data Data frame with event data
#' @param config Configuration for outcome flagging
#' @param cohort_data Data frame with patient baseline dates
#' @param conn Database connection for code lookups
#' @return Data frame with outcome flags added
#' @export
apply_outcome_flagging <- function(data, config, cohort_data, conn = NULL) {

  if (is.null(cohort_data)) {
    warning("Cohort data with baseline dates required for outcome flagging")
    return(data)
  }

  outcome_name <- config$outcome_name
  event_date_column <- config$event_date_column
  code_column <- config$code_column
  flag_column <- config$flag_column %||% paste0("has_", outcome_name)
  date_column <- config$date_column %||% paste0(outcome_name, "_date")

  # Get event codes to match
  event_codes <- get_event_codes(config, conn)

  # Merge with cohort baseline dates
  data_with_baseline <- data %>%
    left_join(cohort_data %>% select(patient_id, baseline_date), by = "patient_id")

  # Flag outcomes (events after baseline)
  data_with_baseline <- data_with_baseline %>%
    mutate(
      is_outcome_event = (!!sym(code_column) %in% event_codes) &
        (!!sym(event_date_column) >= baseline_date)
    )

  # Aggregate to patient level if requested
  if (config$aggregate_to_patient %||% FALSE) {
    # Get first occurrence per patient
    patient_outcomes <- data_with_baseline %>%
      filter(is_outcome_event) %>%
      group_by(patient_id) %>%
      arrange(!!sym(event_date_column)) %>%
      slice(1) %>%
      ungroup() %>%
      select(patient_id, outcome_date = !!sym(event_date_column))

    # Create flags for all patients in cohort
    patient_flags <- cohort_data %>%
      select(patient_id) %>%
      left_join(patient_outcomes, by = "patient_id") %>%
      mutate(!!flag_column := !is.na(outcome_date))

    # Optionally include outcome date
    if (config$include_date %||% TRUE) {
      patient_flags[[date_column]] <- patient_flags$outcome_date
      patient_flags <- patient_flags %>% select(-outcome_date)
    }

    n_outcomes <- sum(patient_flags[[flag_column]])
    message(sprintf("Outcome flagging '%s': %d patients with outcome", outcome_name, n_outcomes))

    return(patient_flags)
  } else {
    # Add flag to each record
    data_with_baseline[[flag_column]] <- data_with_baseline$is_outcome_event
    result <- data_with_baseline %>% select(-baseline_date, -is_outcome_event)

    n_flagged <- sum(result[[flag_column]], na.rm = TRUE)
    message(sprintf("Outcome flagging '%s': %d records flagged", outcome_name, n_flagged))

    return(result)
  }
}


#' Get Event Codes from Configuration
#'
#' Extract event codes from config, either directly or from database lookup
#'
#' @param config Configuration containing event codes
#' @param conn Database connection
#' @return Vector of event codes
#' @keywords internal
get_event_codes <- function(config, conn) {

  if (!is.null(config$event_codes)) {
    # Direct specification in config
    return(config$event_codes)
  } else if (!is.null(config$code_lookup_table)) {
    # Load from database lookup table
    lookup_config <- list(
      schema = config$schema %||% "SAIL",
      lookup_columns = c(config$code_column),
      lookup_filter = config$code_filter
    )

    lookup_data <- get_lookup_from_database(config$code_lookup_table, conn, lookup_config)
    return(lookup_data[[config$code_column]])
  } else if (!is.null(config$code_file)) {
    # Load from CSV file
    code_data <- read.csv(config$code_file, stringsAsFactors = FALSE)
    return(code_data[[config$code_column]])
  } else {
    stop("No event codes specified in configuration")
  }
}


#' Apply Data Transformations
#'
#' Apply custom transformations like type conversion, string manipulation, etc.
#'
#' @param data Data frame to transform
#' @param config Configuration for transformation
#' @return Transformed data frame
#' @export
apply_data_transformation <- function(data, config) {

  transform_type <- config$transform_type
  column <- config$column

  if (!column %in% names(data)) {
    warning(sprintf("Column not found for transformation: %s", column))
    return(data)
  }

  result <- switch(transform_type,
                   "date_conversion" = {
                     data[[column]] <- as.Date(data[[column]], format = config$date_format %||% "%Y-%m-%d")
                     data
                   },
                   "numeric_conversion" = {
                     data[[column]] <- as.numeric(data[[column]])
                     data
                   },
                   "string_cleaning" = {
                     data[[column]] <- trimws(toupper(data[[column]]))
                     data
                   },
                   "categorical_mapping" = {
                     mapping <- config$mapping
                     data[[column]] <- mapping[data[[column]]]
                     data
                   },
                   {
                     warning(sprintf("Unknown transformation type: %s", transform_type))
                     data
                   }
  )

  return(result)
}


#' Create Cohort with Baseline Dates
#'
#' Helper function to create or load cohort data with baseline dates
#'
#' @param patient_ids Vector of patient IDs
#' @param baseline_date_source Source for baseline dates ("fixed", "database", or "event")
#' @param config Configuration for baseline date calculation
#' @param conn Database connection
#' @return Data frame with patient_id and baseline_date
#' @export
create_cohort_baseline <- function(patient_ids, baseline_date_source, config, conn = NULL) {

  cohort <- data.frame(patient_id = patient_ids, stringsAsFactors = FALSE)

  baseline_dates <- switch(baseline_date_source,
                           "fixed" = {
                             rep(as.Date(config$fixed_date), length(patient_ids))
                           },
                           "database" = {
                             query <- sprintf(
                               "SELECT %s AS patient_id, %s AS baseline_date FROM %s.%s WHERE %s IN (%s)",
                               config$patient_id_column,
                               config$baseline_date_column,
                               config$schema %||% "SAIL",
                               config$table,
                               config$patient_id_column,
                               paste(sprintf("'%s'", patient_ids), collapse = ", ")
                             )
                             result <- DBI::dbGetQuery(conn, query)
                             result$baseline_date
                           },
                           "event" = {
                             # Calculate baseline from first event in a dataset
                             stop("Event-based baseline calculation not yet implemented")
                           },
                           stop(sprintf("Unknown baseline date source: %s", baseline_date_source))
  )

  cohort$baseline_date <- as.Date(baseline_dates)
  return(cohort)
}


# Utility function for null coalescing
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
