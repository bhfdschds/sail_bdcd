# Integration Functions for Data Preprocessing Module
# These functions integrate preprocessing into the existing asset creation pipeline

library(yaml)
source("data_preprocessing.R")

#' Read Preprocessing Configuration from YAML
#'
#' @param config_file Path to preprocessing configuration YAML file
#' @return List containing preprocessing configuration
#' @export
read_preprocessing_config <- function(config_file) {

  if (!file.exists(config_file)) {
    stop(sprintf("Preprocessing config file not found: %s", config_file))
  }

  config <- yaml::read_yaml(config_file)

  message(sprintf("Loaded preprocessing configuration from: %s", config_file))
  message(sprintf("  - %d preprocessing configurations defined", length(config$preprocessing)))
  message(sprintf("  - %d dataset mappings defined", length(config$dataset_preprocessing_map)))

  return(config)
}


#' Get Preprocessing Config for a Specific Dataset
#'
#' @param table_name Name of the database table
#' @param preprocessing_config Full preprocessing configuration
#' @return Preprocessing steps for this dataset, or NULL if none defined
#' @export
get_dataset_preprocessing_config <- function(table_name, preprocessing_config) {

  dataset_map <- preprocessing_config$dataset_preprocessing_map

  if (is.null(dataset_map) || !table_name %in% names(dataset_map)) {
    return(NULL)
  }

  dataset_config <- dataset_map[[table_name]]

  # Check if preprocessing is enabled
  if (!is.null(dataset_config$enabled) && !dataset_config$enabled) {
    return(NULL)
  }

  preprocessing_name <- dataset_config$preprocessing

  if (is.null(preprocessing_name)) {
    return(NULL)
  }

  # Get the actual preprocessing steps
  preprocessing_steps <- preprocessing_config$preprocessing[[preprocessing_name]]

  return(preprocessing_steps)
}


#' Apply Preprocessing to Asset Data (Integration Function)
#'
#' Wrapper function that integrates with existing asset creation pipeline
#'
#' @param data Data frame retrieved from database
#' @param table_name Name of the source table
#' @param preprocessing_config Full preprocessing configuration from YAML
#' @param conn Database connection
#' @param cohort_data Optional cohort data with baseline dates
#' @return Preprocessed data frame
#' @export
apply_preprocessing_to_asset_data <- function(data, table_name, preprocessing_config,
                                               conn = NULL, cohort_data = NULL) {

  # Get preprocessing config for this specific dataset
  dataset_preprocessing <- get_dataset_preprocessing_config(table_name, preprocessing_config)

  if (is.null(dataset_preprocessing)) {
    message(sprintf("No preprocessing configured for table: %s", table_name))
    return(data)
  }

  message(sprintf("\n=== Starting preprocessing for table: %s ===", table_name))
  message(sprintf("Original data: %d rows, %d columns", nrow(data), ncol(data)))

  # Apply preprocessing
  preprocessed_data <- apply_preprocessing(
    data = data,
    preprocessing_config = dataset_preprocessing,
    conn = conn,
    cohort_data = cohort_data
  )

  message(sprintf("Preprocessed data: %d rows, %d columns", nrow(preprocessed_data), ncol(preprocessed_data)))
  message(sprintf("=== Completed preprocessing for table: %s ===\n", table_name))

  return(preprocessed_data)
}


#' Create or Load Cohort Data with Baseline Dates
#'
#' Helper function to prepare cohort data based on configuration
#'
#' @param preprocessing_config Preprocessing configuration containing cohort setup
#' @param conn Database connection
#' @param patient_ids Optional vector of patient IDs to filter cohort
#' @return Data frame with patient_id and baseline_date columns
#' @export
prepare_cohort_data <- function(preprocessing_config, conn, patient_ids = NULL) {

  cohort_config <- preprocessing_config$cohort

  if (is.null(cohort_config)) {
    message("No cohort configuration found. Covariate/outcome flagging will not be available.")
    return(NULL)
  }

  baseline_strategy <- cohort_config$baseline_strategy

  if (baseline_strategy == "fixed") {
    # Fixed baseline date for all patients
    if (is.null(patient_ids)) {
      warning("Patient IDs required for fixed baseline cohort creation")
      return(NULL)
    }

    cohort_data <- data.frame(
      patient_id = patient_ids,
      baseline_date = as.Date(cohort_config$fixed_date),
      stringsAsFactors = FALSE
    )

    message(sprintf("Created cohort with fixed baseline date: %s (%d patients)",
                    cohort_config$fixed_date, nrow(cohort_data)))

  } else if (baseline_strategy == "database") {
    # Load baseline dates from database table
    query <- sprintf(
      "SELECT %s AS patient_id, %s AS baseline_date FROM %s.%s",
      cohort_config$patient_id_column,
      cohort_config$baseline_date_column,
      cohort_config$baseline_schema %||% "SAIL",
      cohort_config$baseline_table
    )

    if (!is.null(patient_ids)) {
      query <- sprintf("%s WHERE %s IN (%s)",
                       query,
                       cohort_config$patient_id_column,
                       paste(sprintf("'%s'", patient_ids), collapse = ", "))
    }

    message(sprintf("Loading cohort from database: %s.%s",
                    cohort_config$baseline_schema %||% "SAIL",
                    cohort_config$baseline_table))

    cohort_data <- DBI::dbGetQuery(conn, query)
    cohort_data$baseline_date <- as.Date(cohort_data$baseline_date)

    message(sprintf("Loaded cohort with %d patients", nrow(cohort_data)))

  } else {
    stop(sprintf("Unknown baseline strategy: %s", baseline_strategy))
  }

  return(cohort_data)
}


#' Enhanced Asset Data Retrieval with Preprocessing
#'
#' Modified version of get_asset_data that includes preprocessing
#'
#' @param config Database configuration
#' @param preprocessing_config Preprocessing configuration
#' @param asset_name Name of the asset to retrieve
#' @param source_name Name of the source to use (optional)
#' @param conn Database connection (optional, will create if NULL)
#' @param apply_preprocessing Whether to apply preprocessing (default TRUE)
#' @param cohort_data Cohort data for covariate/outcome flagging (optional)
#' @return Data frame with asset data (preprocessed if configured)
#' @export
get_asset_data_with_preprocessing <- function(config, preprocessing_config, asset_name,
                                               source_name = NULL, conn = NULL,
                                               apply_preprocessing = TRUE,
                                               cohort_data = NULL) {

  # Source the required functions if not already loaded
  if (!exists("get_asset_data")) {
    source("read_db2_config_multi_source.R")
  }
  if (!exists("create_db2_connection")) {
    source("../utility_code/db2_connection.R")
  }

  # Create connection if not provided
  if (is.null(conn)) {
    conn <- create_db2_connection(config)
    on.exit(DBI::dbDisconnect(conn))
  }

  # Get raw data from database
  if (is.null(source_name)) {
    # Use default source
    asset_config <- config$assets[[asset_name]]
    source_name <- asset_config$default_source
  }

  message(sprintf("Retrieving asset '%s' from source '%s'", asset_name, source_name))

  raw_data <- get_asset_data(config, asset_name, source_name, conn)

  # Apply preprocessing if configured
  if (apply_preprocessing && !is.null(preprocessing_config)) {
    # Get the table name from config
    source_config <- config$assets[[asset_name]]$sources[[source_name]]
    table_name <- source_config$table_name

    preprocessed_data <- apply_preprocessing_to_asset_data(
      data = raw_data,
      table_name = table_name,
      preprocessing_config = preprocessing_config,
      conn = conn,
      cohort_data = cohort_data
    )

    return(preprocessed_data)
  } else {
    return(raw_data)
  }
}


#' Create Long Format Asset with Preprocessing
#'
#' Enhanced version of create_long_format_asset that includes preprocessing
#'
#' @param config Database configuration
#' @param preprocessing_config Preprocessing configuration
#' @param asset_name Name of the asset to create
#' @param conn Database connection
#' @param include_all_sources Whether to include all sources (default TRUE)
#' @param apply_preprocessing Whether to apply preprocessing (default TRUE)
#' @param cohort_data Cohort data for covariate/outcome flagging (optional)
#' @return Long format data frame with all sources
#' @export
create_long_format_asset_with_preprocessing <- function(config, preprocessing_config,
                                                         asset_name, conn,
                                                         include_all_sources = TRUE,
                                                         apply_preprocessing = TRUE,
                                                         cohort_data = NULL) {

  # Source the required functions if not already loaded
  if (!exists("create_long_format_asset")) {
    source("create_long_format_assets.R")
  }

  asset_config <- config$assets[[asset_name]]

  if (is.null(asset_config)) {
    stop(sprintf("Asset not found in config: %s", asset_name))
  }

  message(sprintf("\n=== Creating long format asset: %s ===", asset_name))

  all_source_data <- list()

  # Get data from each source
  for (source_name in names(asset_config$sources)) {
    source_config <- asset_config$sources[[source_name]]

    message(sprintf("\nProcessing source: %s (priority: %d)",
                    source_name, source_config$priority))

    # Get raw data
    raw_data <- get_asset_data(config, asset_name, source_name, conn)

    if (nrow(raw_data) == 0) {
      message(sprintf("  Warning: No data retrieved from source '%s'", source_name))
      next
    }

    # Apply preprocessing if enabled
    if (apply_preprocessing && !is.null(preprocessing_config)) {
      table_name <- source_config$table_name

      processed_data <- apply_preprocessing_to_asset_data(
        data = raw_data,
        table_name = table_name,
        preprocessing_config = preprocessing_config,
        conn = conn,
        cohort_data = cohort_data
      )
    } else {
      processed_data <- raw_data
    }

    # Add source metadata
    processed_data$source_table <- source_name
    processed_data$source_db_table <- source_config$table_name
    processed_data$source_priority <- source_config$priority

    all_source_data[[source_name]] <- processed_data
  }

  # Combine all sources
  if (length(all_source_data) == 0) {
    stop(sprintf("No data retrieved for asset: %s", asset_name))
  }

  long_format_data <- dplyr::bind_rows(all_source_data)

  message(sprintf("\n=== Long format asset created ==="))
  message(sprintf("  Total rows: %d", nrow(long_format_data)))
  message(sprintf("  Total sources: %d", length(all_source_data)))
  message(sprintf("  Unique patients: %d", length(unique(long_format_data$patient_id))))

  return(long_format_data)
}


#' Complete Preprocessing Pipeline Workflow
#'
#' End-to-end function that runs the full preprocessing pipeline
#'
#' @param db_config_file Path to database configuration YAML
#' @param preprocessing_config_file Path to preprocessing configuration YAML
#' @param asset_names Vector of asset names to process
#' @param output_dir Directory for output files
#' @return List of preprocessed asset data frames
#' @export
run_preprocessing_pipeline <- function(db_config_file, preprocessing_config_file,
                                        asset_names, output_dir = NULL) {

  message("\n========================================")
  message("Starting Preprocessing Pipeline")
  message("========================================\n")

  # Load configurations
  message("1. Loading configurations...")
  db_config <- read_db_config(db_config_file)
  preprocessing_config <- read_preprocessing_config(preprocessing_config_file)

  # Create database connection
  message("\n2. Creating database connection...")
  conn <- create_db2_connection(db_config)
  on.exit(DBI::dbDisconnect(conn))

  # Prepare cohort data if needed
  message("\n3. Preparing cohort data...")
  cohort_data <- prepare_cohort_data(preprocessing_config, conn)

  # Process each asset
  message("\n4. Processing assets...")
  results <- list()

  for (asset_name in asset_names) {
    message(sprintf("\n--- Processing asset: %s ---", asset_name))

    asset_data <- tryCatch({
      create_long_format_asset_with_preprocessing(
        config = db_config,
        preprocessing_config = preprocessing_config,
        asset_name = asset_name,
        conn = conn,
        apply_preprocessing = TRUE,
        cohort_data = cohort_data
      )
    }, error = function(e) {
      warning(sprintf("Error processing asset '%s': %s", asset_name, e$message))
      NULL
    })

    if (!is.null(asset_data)) {
      results[[asset_name]] <- asset_data

      # Save to file if output directory specified
      if (!is.null(output_dir)) {
        output_file <- file.path(output_dir, sprintf("%s_preprocessed.rds", asset_name))
        saveRDS(asset_data, output_file)
        message(sprintf("Saved preprocessed data to: %s", output_file))
      }
    }
  }

  message("\n========================================")
  message("Preprocessing Pipeline Complete")
  message(sprintf("Successfully processed %d/%d assets", length(results), length(asset_names)))
  message("========================================\n")

  return(results)
}


# Utility function for null coalescing
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
