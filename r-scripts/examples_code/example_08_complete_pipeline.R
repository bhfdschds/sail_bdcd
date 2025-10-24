# Example 08: Complete Asset Pipeline
# This example demonstrates: create_asset_pipeline()
# The complete end-to-end workflow for creating, analyzing, and exporting asset tables

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
# EXAMPLE 1: Basic Pipeline - All Default Settings
# ============================================================================

example_basic_pipeline <- function() {
  cat("\n=== EXAMPLE 1: Basic Pipeline with Default Settings ===\n\n")

  # Run the complete pipeline with default settings
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:1500),  # Subset for testing
    assets = c("date_of_birth", "sex", "ethnicity", "lsoa"),  # All default assets
    output_dir = "/mnt/user-data/outputs"
  )

  cat("\n--- Pipeline Results ---\n\n")

  cat("The pipeline returns a list with four components:\n")
  cat("  1. asset_tables: The long format tables\n")
  cat("  2. summaries: Summary statistics for each asset\n")
  cat("  3. conflicts: Conflict analysis for each asset\n")
  cat("  4. exported_files: Paths to exported CSV files\n\n")

  cat("--- Asset Tables ---\n")
  for (asset_name in names(results$asset_tables)) {
    table <- results$asset_tables[[asset_name]]
    cat(glue("{asset_name}: {nrow(table)} rows\n"))
  }

  cat("\n--- Exported Files ---\n")
  for (asset_name in names(results$exported_files)) {
    cat(glue("{asset_name}: {results$exported_files[[asset_name]]}\n"))
  }

  return(results)
}

# ============================================================================
# EXAMPLE 2: Pipeline for All Patients
# ============================================================================

example_full_pipeline <- function() {
  cat("\n=== EXAMPLE 2: Pipeline for All Patients ===\n\n")

  # Run pipeline without patient_ids filter (all patients)
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = NULL,  # NULL = all patients
    assets = c("sex", "date_of_birth", "ethnicity", "lsoa"),
    output_dir = "/mnt/user-data/outputs"
  )

  cat("\n--- Full Dataset Statistics ---\n\n")

  for (asset_name in names(results$asset_tables)) {
    table <- results$asset_tables[[asset_name]]

    cat(glue("{asset_name}:\n"))
    cat(glue("  Rows: {nrow(table)}\n"))
    cat(glue("  Patients: {n_distinct(table$patient_id)}\n"))
    cat(glue("  Sources: {n_distinct(table$source_table)}\n\n"))
  }

  return(results)
}

# ============================================================================
# EXAMPLE 3: Pipeline for Specific Assets Only
# ============================================================================

example_specific_assets_pipeline <- function() {
  cat("\n=== EXAMPLE 3: Pipeline for Specific Assets ===\n\n")

  # Run pipeline for only sex and ethnicity
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:2000),
    assets = c("sex", "ethnicity"),  # Only these two
    output_dir = "/mnt/user-data/outputs"
  )

  cat("\n--- Selected Assets Only ---\n\n")

  cat(glue("Created {length(results$asset_tables)} asset tables:\n"))
  for (asset_name in names(results$asset_tables)) {
    cat(glue("  - {asset_name}\n"))
  }

  return(results)
}

# ============================================================================
# EXAMPLE 4: Exploring Pipeline Results
# ============================================================================

example_explore_pipeline_results <- function() {
  cat("\n=== EXAMPLE 4: Exploring Pipeline Results ===\n\n")

  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:1500),
    assets = c("sex", "ethnicity"),
    output_dir = "/mnt/user-data/outputs"
  )

  cat("\n--- 1. Accessing Asset Tables ---\n\n")

  sex_long <- results$asset_tables$sex
  cat(glue("Sex table: {nrow(sex_long)} rows\n"))
  cat("First few rows:\n")
  print(head(sex_long, 3))

  cat("\n--- 2. Accessing Summaries ---\n\n")

  sex_summary <- results$summaries$sex
  cat("Sex summary components:\n")
  cat("  - source_summary:\n")
  print(sex_summary$source_summary)
  cat("\n  - coverage:\n")
  print(sex_summary$coverage)

  cat("\n--- 3. Accessing Conflicts ---\n\n")

  sex_conflicts <- results$conflicts$sex
  if (is.null(sex_conflicts)) {
    cat("✓ No conflicts in sex data\n")
  } else {
    cat(glue("⚠ {nrow(sex_conflicts)} patients with conflicts\n"))
    print(head(sex_conflicts, 3))
  }

  cat("\n--- 4. Accessing Exported Files ---\n\n")

  sex_file <- results$exported_files$sex
  cat(glue("Sex data exported to: {sex_file}\n"))

  # You can read the file back
  sex_from_file <- read.csv(sex_file)
  cat(glue("File contains {nrow(sex_from_file)} rows\n"))

  return(results)
}

# ============================================================================
# EXAMPLE 5: Post-Pipeline Analysis
# ============================================================================

example_post_pipeline_analysis <- function() {
  cat("\n=== EXAMPLE 5: Post-Pipeline Analysis ===\n\n")

  # Run the pipeline
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:2000),
    assets = c("sex", "date_of_birth", "ethnicity"),
    output_dir = "/mnt/user-data/outputs"
  )

  cat("\n--- Additional Analysis After Pipeline ---\n\n")

  # 1. Create priority-selected versions
  cat("1. Creating priority-selected versions...\n")

  priority_tables <- list()
  for (asset_name in names(results$asset_tables)) {
    priority_tables[[asset_name]] <- get_highest_priority_per_patient(
      results$asset_tables[[asset_name]]
    )
    cat(glue("   {asset_name}: {nrow(priority_tables[[asset_name]])} patients\n"))
  }

  # 2. Create master demographics table
  cat("\n2. Creating master demographics table...\n")

  demographics <- priority_tables$sex %>%
    select(patient_id, sex_code, sex_description) %>%
    left_join(
      priority_tables$date_of_birth %>%
        select(patient_id, date_of_birth),
      by = "patient_id"
    ) %>%
    left_join(
      priority_tables$ethnicity %>%
        select(patient_id, ethnicity_code, ethnicity_category),
      by = "patient_id"
    )

  cat(glue("   Master table: {nrow(demographics)} patients\n"))

  # 3. Create wide format comparison tables
  cat("\n3. Creating wide format comparison tables...\n")

  wide_tables <- list()
  for (asset_name in names(results$asset_tables)) {
    # Get value columns (exclude metadata columns)
    all_cols <- names(results$asset_tables[[asset_name]])
    meta_cols <- c("patient_id", "source_table", "source_db_table",
                   "source_priority", "source_quality", "source_coverage",
                   "source_last_updated")
    value_cols <- setdiff(all_cols, meta_cols)

    wide_tables[[asset_name]] <- pivot_to_wide_by_source(
      results$asset_tables[[asset_name]],
      value_cols
    )

    cat(glue("   {asset_name} wide: {ncol(wide_tables[[asset_name]])} columns\n"))
  }

  # 4. Export additional tables
  cat("\n4. Exporting additional analysis tables...\n")

  write.csv(demographics,
            "/mnt/user-data/outputs/master_demographics.csv",
            row.names = FALSE)
  cat("   ✓ master_demographics.csv\n")

  for (asset_name in names(wide_tables)) {
    filename <- glue("/mnt/user-data/outputs/{asset_name}_wide_comparison.csv")
    write.csv(wide_tables[[asset_name]], filename, row.names = FALSE)
    cat(glue("   ✓ {asset_name}_wide_comparison.csv\n"))
  }

  return(list(
    pipeline_results = results,
    priority_tables = priority_tables,
    demographics = demographics,
    wide_tables = wide_tables
  ))
}

# ============================================================================
# EXAMPLE 6: Custom Output Directory with Timestamp
# ============================================================================

example_timestamped_pipeline <- function() {
  cat("\n=== EXAMPLE 6: Pipeline with Timestamped Output ===\n\n")

  # Create timestamped directory
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  custom_output <- glue("/mnt/user-data/outputs/pipeline_run_{timestamp}")

  cat(glue("Creating output directory: {custom_output}\n\n"))

  # Create directory
  if (!dir.exists(custom_output)) {
    dir.create(custom_output, recursive = TRUE)
  }

  # Run pipeline
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:1500),
    assets = c("sex", "ethnicity"),
    output_dir = custom_output
  )

  cat("\n--- Files Created in Timestamped Directory ---\n\n")

  files <- list.files(custom_output, full.names = TRUE)
  for (file in files) {
    size_kb <- round(file.info(file)$size / 1024, 1)
    cat(glue("{basename(file)}: {size_kb} KB\n"))
  }

  cat("\nThis approach is useful for:\n")
  cat("  - Keeping different pipeline runs separate\n")
  cat("  - Version control of outputs\n")
  cat("  - Comparing results across time\n")

  return(results)
}

# ============================================================================
# EXAMPLE 7: Pipeline Error Handling
# ============================================================================

example_pipeline_error_handling <- function() {
  cat("\n=== EXAMPLE 7: Pipeline with Error Handling ===\n\n")

  # Wrap pipeline in error handling
  results <- tryCatch({
    create_asset_pipeline(
      config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
      patient_ids = c(1001:1500),
      assets = c("sex", "date_of_birth"),
      output_dir = "/mnt/user-data/outputs"
    )
  }, error = function(e) {
    cat("ERROR: Pipeline failed!\n")
    cat(glue("Error message: {e$message}\n"))
    return(NULL)
  })

  if (is.null(results)) {
    cat("\nPipeline failed. Check:\n")
    cat("  1. Database connection settings\n")
    cat("  2. Configuration file path\n")
    cat("  3. Asset names in config\n")
    cat("  4. Output directory permissions\n")
    return(NULL)
  } else {
    cat("\n✓ Pipeline completed successfully!\n")
    return(results)
  }
}

# ============================================================================
# EXAMPLE 8: Complete Workflow with All Post-Processing
# ============================================================================

example_complete_workflow <- function() {
  cat("\n")
  cat("==========================================================\n")
  cat("COMPLETE ASSET PIPELINE WORKFLOW\n")
  cat("==========================================================\n\n")

  # Setup
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  output_dir <- glue("/mnt/user-data/outputs/complete_workflow_{timestamp}")

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  cat(glue("Output directory: {output_dir}\n\n"))

  # STEP 1: Run main pipeline
  cat("STEP 1: Running main pipeline...\n")
  cat("=" * 60, "\n")

  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = c(1001:3000),
    assets = c("sex", "date_of_birth", "ethnicity", "lsoa"),
    output_dir = output_dir
  )

  # STEP 2: Create priority-selected master tables
  cat("\n\nSTEP 2: Creating priority-selected master tables...\n")
  cat("=" * 60, "\n\n")

  master_tables <- list()
  for (asset_name in names(results$asset_tables)) {
    master_tables[[asset_name]] <- get_highest_priority_per_patient(
      results$asset_tables[[asset_name]]
    )

    # Export each master table
    filename <- glue("{output_dir}/{asset_name}_master.csv")
    write.csv(master_tables[[asset_name]], filename, row.names = FALSE)
    cat(glue("✓ {asset_name}_master.csv\n"))
  }

  # STEP 3: Create comprehensive demographics table
  cat("\n\nSTEP 3: Creating comprehensive demographics table...\n")
  cat("=" * 60, "\n\n")

  demographics <- master_tables$sex %>%
    select(patient_id, sex_code, sex_description, sex_source = source_table) %>%
    full_join(
      master_tables$date_of_birth %>%
        select(patient_id, date_of_birth, dob_source = source_table),
      by = "patient_id"
    ) %>%
    full_join(
      master_tables$ethnicity %>%
        select(patient_id, ethnicity_code, ethnicity_category,
               ethnicity_source = source_table),
      by = "patient_id"
    ) %>%
    full_join(
      master_tables$lsoa %>%
        select(patient_id, lsoa_code, lsoa_source = source_table),
      by = "patient_id"
    )

  demographics_file <- glue("{output_dir}/comprehensive_demographics.csv")
  write.csv(demographics, demographics_file, row.names = FALSE)
  cat(glue("✓ comprehensive_demographics.csv ({nrow(demographics)} patients)\n"))

  # STEP 4: Create wide format comparison tables
  cat("\n\nSTEP 4: Creating wide format comparison tables...\n")
  cat("=" * 60, "\n\n")

  for (asset_name in names(results$asset_tables)) {
    # Get value columns
    all_cols <- names(results$asset_tables[[asset_name]])
    meta_cols <- c("patient_id", "source_table", "source_db_table",
                   "source_priority", "source_quality", "source_coverage",
                   "source_last_updated")
    value_cols <- setdiff(all_cols, meta_cols)

    wide_table <- pivot_to_wide_by_source(
      results$asset_tables[[asset_name]],
      value_cols
    )

    filename <- glue("{output_dir}/{asset_name}_wide_comparison.csv")
    write.csv(wide_table, filename, row.names = FALSE)
    cat(glue("✓ {asset_name}_wide_comparison.csv\n"))
  }

  # STEP 5: Export conflict details
  cat("\n\nSTEP 5: Exporting detailed conflict reports...\n")
  cat("=" * 60, "\n\n")

  for (asset_name in names(results$conflicts)) {
    conflicts <- results$conflicts[[asset_name]]

    if (!is.null(conflicts) && nrow(conflicts) > 0) {
      # Export conflict summary
      summary_file <- glue("{output_dir}/{asset_name}_conflicts_summary.csv")
      write.csv(conflicts, summary_file, row.names = FALSE)

      # Export detailed conflict data
      conflict_details <- results$asset_tables[[asset_name]] %>%
        filter(patient_id %in% conflicts$patient_id)

      detail_file <- glue("{output_dir}/{asset_name}_conflicts_details.csv")
      write.csv(conflict_details, detail_file, row.names = FALSE)

      cat(glue("✓ {asset_name} conflicts: {nrow(conflicts)} patients\n"))
    } else {
      cat(glue("✓ {asset_name}: No conflicts\n"))
    }
  }

  # STEP 6: Create summary report
  cat("\n\nSTEP 6: Creating summary report...\n")
  cat("=" * 60, "\n\n")

  report_file <- glue("{output_dir}/SUMMARY_REPORT.txt")
  sink(report_file)

  cat("==========================================================\n")
  cat("ASSET PIPELINE SUMMARY REPORT\n")
  cat("==========================================================\n\n")
  cat(glue("Generated: {Sys.time()}\n"))
  cat(glue("Patient IDs: 1001-3000\n\n"))

  for (asset_name in names(results$asset_tables)) {
    cat(glue("\n{toupper(asset_name)}\n"))
    cat(glue("{paste(rep('-', nchar(asset_name)), collapse='')}\n"))

    table <- results$asset_tables[[asset_name]]
    summary <- results$summaries[[asset_name]]
    conflicts <- results$conflicts[[asset_name]]

    cat(glue("Total rows: {nrow(table)}\n"))
    cat(glue("Unique patients: {n_distinct(table$patient_id)}\n"))
    cat(glue("Number of sources: {n_distinct(table$source_table)}\n"))

    if (!is.null(conflicts)) {
      cat(glue("Conflicts: {nrow(conflicts)} patients\n"))
    } else {
      cat("Conflicts: None\n")
    }
  }

  sink()

  cat(glue("✓ SUMMARY_REPORT.txt\n"))

  # Final summary
  cat("\n\n")
  cat("==========================================================\n")
  cat("WORKFLOW COMPLETE!\n")
  cat("==========================================================\n\n")

  cat(glue("All outputs saved to: {output_dir}\n\n"))

  cat("Files created:\n")
  all_files <- list.files(output_dir)
  for (file in all_files) {
    cat(glue("  - {file}\n"))
  }

  return(list(
    pipeline_results = results,
    master_tables = master_tables,
    demographics = demographics,
    output_dir = output_dir
  ))
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic pipeline
# results1 <- example_basic_pipeline()

# Full pipeline (all patients)
# results2 <- example_full_pipeline()

# Specific assets
# results3 <- example_specific_assets_pipeline()

# Explore results
# results4 <- example_explore_pipeline_results()

# Post-pipeline analysis
# analysis <- example_post_pipeline_analysis()

# Timestamped output
# results5 <- example_timestamped_pipeline()

# Error handling
# results6 <- example_pipeline_error_handling()

# Complete workflow
# complete <- example_complete_workflow()
