# Example 06: Exporting Asset Tables
# This example demonstrates: export_asset_table() and export_all_asset_tables()
# Using the 'sex' asset as an example

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source required files
source("/workspaces/sail_bdcd/r-scripts/utility_code/db2_connection.R")
source("/workspaces/sail_bdcd/r-scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# EXAMPLE 1: Basic Export to CSV
# ============================================================================

example_basic_csv_export <- function() {
  cat("\n=== EXAMPLE 1: Basic CSV Export ===\n\n")

  # 1. Create the long format table
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1200)
  )

  # 2. Export to CSV
  cat("Exporting sex asset table to CSV...\n")

  filename <- export_asset_table(
    long_table = sex_long,
    asset_name = "sex",
    output_dir = "/mnt/user-data/outputs",
    format = "csv"
  )

  cat(glue("\n✓ File exported: {filename}\n"))
  cat("\nThe file can be opened in:\n")
  cat("  - Excel\n")
  cat("  - R (read.csv)\n")
  cat("  - Python (pandas.read_csv)\n")
  cat("  - Any text editor\n")

  DBI::dbDisconnect(conn)

  return(filename)
}

# ============================================================================
# EXAMPLE 2: Export to RDS Format
# ============================================================================

example_rds_export <- function() {
  cat("\n=== EXAMPLE 2: Export to RDS Format ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1200)
  )

  # Export to RDS
  cat("Exporting sex asset table to RDS...\n")

  filename <- export_asset_table(
    long_table = sex_long,
    asset_name = "sex",
    output_dir = "/mnt/user-data/outputs",
    format = "rds"
  )

  cat(glue("\n✓ File exported: {filename}\n"))

  cat("\n--- RDS vs CSV ---\n")
  cat("RDS advantages:\n")
  cat("  ✓ Preserves R data types exactly\n")
  cat("  ✓ Smaller file size (compressed)\n")
  cat("  ✓ Faster to read/write\n")
  cat("  ✓ Preserves factor levels, dates, etc.\n")
  cat("\nRDS disadvantages:\n")
  cat("  ✗ Only readable by R\n")
  cat("  ✗ Not human-readable\n")

  cat("\n--- How to Read Back ---\n")
  cat("In R: sex_data <- readRDS('", filename, "')\n", sep = "")

  DBI::dbDisconnect(conn)

  return(filename)
}

# ============================================================================
# EXAMPLE 3: Export Multiple Formats
# ============================================================================

example_export_both_formats <- function() {
  cat("\n=== EXAMPLE 3: Export in Both CSV and RDS ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  cat("Exporting in both formats...\n\n")

  # Export as CSV
  csv_file <- export_asset_table(sex_long, "sex", format = "csv")

  # Export as RDS
  rds_file <- export_asset_table(sex_long, "sex", format = "rds")

  # Compare file sizes
  csv_size <- file.info(csv_file)$size
  rds_size <- file.info(rds_file)$size

  cat("\n--- File Size Comparison ---\n")
  cat(glue("CSV: {round(csv_size / 1024, 1)} KB\n"))
  cat(glue("RDS: {round(rds_size / 1024, 1)} KB\n"))

  compression_ratio <- round(100 * (1 - rds_size / csv_size), 1)
  cat(glue("RDS is {compression_ratio}% smaller\n"))

  DBI::dbDisconnect(conn)

  return(list(csv = csv_file, rds = rds_file))
}

# ============================================================================
# EXAMPLE 4: Export All Asset Tables
# ============================================================================

example_export_all_assets <- function() {
  cat("\n=== EXAMPLE 4: Export All Asset Tables ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  patient_ids <- c(1001:1500)

  cat("Creating all asset tables...\n\n")

  # Create all asset tables
  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = patient_ids
  )

  cat("\n--- Exporting All Tables ---\n\n")

  # Export all as CSV
  exported_files <- export_all_asset_tables(
    asset_tables = asset_tables,
    output_dir = "/mnt/user-data/outputs",
    format = "csv"
  )

  cat("\n--- Summary of Exported Files ---\n\n")

  for (asset_name in names(exported_files)) {
    filename <- exported_files[[asset_name]]
    n_rows <- nrow(asset_tables[[asset_name]])
    n_cols <- ncol(asset_tables[[asset_name]])

    cat(glue("{asset_name}:\n"))
    cat(glue("  File: {filename}\n"))
    cat(glue("  Rows: {n_rows}, Columns: {n_cols}\n\n"))
  }

  DBI::dbDisconnect(conn)

  return(exported_files)
}

# ============================================================================
# EXAMPLE 5: Export Different Versions of Same Asset
# ============================================================================

example_export_different_versions <- function() {
  cat("\n=== EXAMPLE 5: Export Different Versions of Same Asset ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create the base long format table
  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:2000)
  )

  cat("Creating and exporting different versions...\n\n")

  # 1. Export full long format
  cat("1. Exporting long format (all sources)...\n")
  export_asset_table(sex_long, "sex_long_format", format = "csv")

  # 2. Export priority-selected version
  cat("2. Exporting priority-selected version...\n")
  sex_priority <- get_highest_priority_per_patient(sex_long)
  export_asset_table(sex_priority, "sex_priority", format = "csv")

  # 3. Export wide format for comparison
  cat("3. Exporting wide format (source comparison)...\n")
  sex_wide <- pivot_to_wide_by_source(sex_long, c("sex_code", "sex_description"))

  # Note: export_asset_table expects specific structure, so use direct export
  wide_file <- "/mnt/user-data/outputs/sex_wide_format.csv"
  write.csv(sex_wide, wide_file, row.names = FALSE)
  cat(glue("✓ Exported sex_wide_format to: {wide_file}\n"))

  # 4. Export conflicts only
  cat("4. Exporting conflicts...\n")
  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (!is.null(conflicts)) {
    conflict_file <- "/mnt/user-data/outputs/sex_conflicts.csv"
    write.csv(conflicts, conflict_file, row.names = FALSE)
    cat(glue("✓ Exported conflicts to: {conflict_file}\n"))

    # Also export detailed conflict data
    conflict_details <- sex_long %>%
      filter(patient_id %in% conflicts$patient_id)

    detail_file <- "/mnt/user-data/outputs/sex_conflict_details.csv"
    write.csv(conflict_details, detail_file, row.names = FALSE)
    cat(glue("✓ Exported conflict details to: {detail_file}\n"))
  }

  cat("\n--- Summary ---\n")
  cat("Created multiple versions for different use cases:\n")
  cat("  1. sex_long_format.csv - Full data with all sources\n")
  cat("  2. sex_priority.csv - One row per patient (highest priority)\n")
  cat("  3. sex_wide_format.csv - Source comparison view\n")
  cat("  4. sex_conflicts.csv - Summary of conflicting patients\n")
  cat("  5. sex_conflict_details.csv - Full details of conflicts\n")

  DBI::dbDisconnect(conn)
}

# ============================================================================
# EXAMPLE 6: Custom Output Directory
# ============================================================================

example_custom_output_dir <- function() {
  cat("\n=== EXAMPLE 6: Custom Output Directory ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)
  )

  # Create custom directory with timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  custom_dir <- glue("/mnt/user-data/outputs/sex_export_{timestamp}")

  # Create directory if it doesn't exist
  if (!dir.exists(custom_dir)) {
    dir.create(custom_dir, recursive = TRUE)
    cat(glue("Created directory: {custom_dir}\n\n"))
  }

  # Export to custom directory
  filename <- export_asset_table(
    sex_long,
    "sex",
    output_dir = custom_dir,
    format = "csv"
  )

  cat(glue("\n✓ Exported to custom directory: {filename}\n"))

  cat("\nThis is useful for:\n")
  cat("  - Organizing exports by date/time\n")
  cat("  - Separating different analysis runs\n")
  cat("  - Creating versioned outputs\n")

  DBI::dbDisconnect(conn)

  return(filename)
}

# ============================================================================
# EXAMPLE 7: Reading Exported Files Back
# ============================================================================

example_read_exported_files <- function() {
  cat("\n=== EXAMPLE 7: Reading Exported Files Back ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1100)
  )

  # Export both formats
  cat("Exporting files...\n")
  csv_file <- export_asset_table(sex_long, "sex", format = "csv")
  rds_file <- export_asset_table(sex_long, "sex", format = "rds")

  cat("\n--- Reading Files Back ---\n\n")

  # Read CSV
  cat("1. Reading CSV file...\n")
  sex_from_csv <- read.csv(csv_file)
  cat(glue("   Loaded {nrow(sex_from_csv)} rows, {ncol(sex_from_csv)} columns\n"))
  cat("   Data types:\n")
  str(sex_from_csv, give.attr = FALSE, vec.len = 1)

  # Read RDS
  cat("\n2. Reading RDS file...\n")
  sex_from_rds <- readRDS(rds_file)
  cat(glue("   Loaded {nrow(sex_from_rds)} rows, {ncol(sex_from_rds)} columns\n"))
  cat("   Data types:\n")
  str(sex_from_rds, give.attr = FALSE, vec.len = 1)

  # Compare
  cat("\n--- Comparison ---\n")
  cat("Both should have the same data, but data types may differ:\n")
  cat("  - CSV: All columns read as character/numeric\n")
  cat("  - RDS: Original R data types preserved\n")

  # Verify they contain the same data
  identical_data <- all.equal(
    sex_from_rds,
    sex_from_csv,
    check.attributes = FALSE
  )

  if (isTRUE(identical_data)) {
    cat("\n✓ Data is identical!\n")
  } else {
    cat("\n⚠ Data differs (likely just data types):\n")
    print(identical_data)
  }

  DBI::dbDisconnect(conn)

  return(list(csv = sex_from_csv, rds = sex_from_rds))
}

# ============================================================================
# EXAMPLE 8: Export with Metadata File
# ============================================================================

example_export_with_metadata <- function() {
  cat("\n=== EXAMPLE 8: Export with Metadata File ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex"
  )

  # Export the data
  cat("Exporting data...\n")
  data_file <- export_asset_table(sex_long, "sex", format = "csv")

  # Create metadata file
  metadata <- list(
    asset_name = "sex",
    export_date = as.character(Sys.time()),
    n_rows = nrow(sex_long),
    n_columns = ncol(sex_long),
    n_patients = n_distinct(sex_long$patient_id),
    sources = unique(sex_long$source_table),
    column_names = names(sex_long),
    data_file = data_file
  )

  # Save metadata as JSON
  metadata_file <- "/mnt/user-data/outputs/sex_metadata.json"
  jsonlite::write_json(metadata, metadata_file, pretty = TRUE, auto_unbox = TRUE)

  cat(glue("\n✓ Data exported to: {data_file}\n"))
  cat(glue("✓ Metadata exported to: {metadata_file}\n"))

  cat("\n--- Metadata Contents ---\n")
  print(metadata)

  cat("\nMetadata is useful for:\n")
  cat("  - Tracking when data was exported\n")
  cat("  - Documenting data structure\n")
  cat("  - Data provenance\n")
  cat("  - Quality assurance\n")

  DBI::dbDisconnect(conn)

  return(list(data = data_file, metadata = metadata_file))
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic CSV export
# csv_file <- example_basic_csv_export()

# RDS export
# rds_file <- example_rds_export()

# Both formats
# both_files <- example_export_both_formats()

# Export all assets
# all_files <- example_export_all_assets()

# Export different versions
# example_export_different_versions()

# Custom output directory
# custom_file <- example_custom_output_dir()

# Read exported files back
# read_back <- example_read_exported_files()

# Export with metadata
# with_metadata <- example_export_with_metadata()
