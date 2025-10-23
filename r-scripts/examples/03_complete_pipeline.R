# ===============================================================================
# Example 3: Complete Pipeline - All Assets End-to-End
# ===============================================================================
# This example demonstrates how to run the complete end-to-end pipeline to
# create curated demographic datasets for all assets.
#
# What you'll learn:
# - How to use create_asset_pipeline() for automated processing
# - How to process all assets in one go
# - How to work with pipeline results
# - How to combine assets into a master demographic table
# - How to export production-ready datasets
# ===============================================================================

# Load required libraries
library(dplyr)
library(tidyr)
library(DBI)
library(yaml)

# Source required scripts
source("utility_code/db2_connection.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")

# ===============================================================================
# Configuration
# ===============================================================================

# Define which assets to process
ASSETS_TO_PROCESS <- c("date_of_birth", "sex", "ethnicity", "lsoa")

# Define patient cohort (NULL = all patients)
# For testing, you might want to use a subset:
# PATIENT_COHORT <- c(1001:5000)
PATIENT_COHORT <- NULL  # All patients

# Define output directory
OUTPUT_DIR <- "/mnt/user-data/curated_demographics"

# Ensure output directory exists
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ===============================================================================
# Step 1: Run Complete Pipeline
# ===============================================================================

print("===== RUNNING COMPLETE PIPELINE =====\n")

print(paste("Processing assets:", paste(ASSETS_TO_PROCESS, collapse = ", ")))
print(paste("Patient cohort:", ifelse(is.null(PATIENT_COHORT), "All patients",
                                      paste(length(PATIENT_COHORT), "patients"))))
print(paste("Output directory:", OUTPUT_DIR))

# Run the complete pipeline
# This function will:
# 1. Load configuration
# 2. Connect to database
# 3. Create long format tables for all assets
# 4. Generate summary statistics
# 5. Check for conflicts
# 6. Export files (if output_dir provided)
# 7. Disconnect from database

start_time <- Sys.time()

results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  patient_ids = PATIENT_COHORT,
  assets = ASSETS_TO_PROCESS,
  output_dir = OUTPUT_DIR,
  export_format = "csv"  # or "rds"
)

end_time <- Sys.time()
print(paste("\nPipeline completed in:", round(difftime(end_time, start_time, units = "secs"), 2), "seconds"))

# ===============================================================================
# Step 2: Review Pipeline Results
# ===============================================================================

print("\n===== PIPELINE RESULTS =====\n")

# The results object contains:
# - asset_tables: List of long format tables
# - summaries: Summary statistics for each asset
# - conflicts: Conflict analysis for each asset
# - exported_files: Paths to exported files

print("Available components:")
print(names(results))

print("\nAvailable assets:")
print(names(results$asset_tables))

# ===============================================================================
# Step 3: Review Summary Statistics
# ===============================================================================

print("\n===== SUMMARY STATISTICS =====\n")

# Print summaries for all assets
for (asset_name in names(results$summaries)) {
  print(paste("\n----- Summary for", asset_name, "-----"))

  summary <- results$summaries[[asset_name]]

  print("Overall:")
  print(summary$overall)

  print("\nBy Source:")
  print(summary$by_source)

  print("\nPatient Coverage:")
  print(summary$patient_coverage)
}

# ===============================================================================
# Step 4: Review Conflicts
# ===============================================================================

print("\n===== CONFLICT ANALYSIS =====\n")

# Check for conflicts in each asset
conflict_counts <- data.frame(
  asset = character(),
  n_conflicts = integer(),
  pct_of_patients = numeric(),
  stringsAsFactors = FALSE
)

for (asset_name in names(results$conflicts)) {
  conflicts <- results$conflicts[[asset_name]]
  total_patients <- n_distinct(results$asset_tables[[asset_name]]$patient_id)

  conflict_counts <- rbind(
    conflict_counts,
    data.frame(
      asset = asset_name,
      n_conflicts = nrow(conflicts),
      pct_of_patients = round(100 * nrow(conflicts) / total_patients, 2)
    )
  )

  if (nrow(conflicts) > 0) {
    print(paste("\n", asset_name, "- First 5 conflicts:"))
    print(head(conflicts, 5))
  } else {
    print(paste("\n", asset_name, "- No conflicts detected"))
  }
}

print("\n----- Conflict Summary Across All Assets -----")
print(conflict_counts)

# ===============================================================================
# Step 5: Access Individual Asset Tables
# ===============================================================================

print("\n===== ACCESSING ASSET TABLES =====\n")

# Extract individual tables from results
dob_long <- results$asset_tables$date_of_birth
sex_long <- results$asset_tables$sex
ethnicity_long <- results$asset_tables$ethnicity
lsoa_long <- results$asset_tables$lsoa

# View structure
print("Date of Birth structure:")
print(str(dob_long))

print("\nSex structure:")
print(str(sex_long))

print("\nEthnicity structure:")
print(str(ethnicity_long))

print("\nLSOA structure:")
print(str(lsoa_long))

# ===============================================================================
# Step 6: Resolve Each Asset to One Row Per Patient
# ===============================================================================

print("\n===== RESOLVING TO ONE ROW PER PATIENT =====\n")

# Apply priority-based resolution to each asset
dob_resolved <- get_highest_priority_per_patient(dob_long)
sex_resolved <- get_highest_priority_per_patient(sex_long)
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)
lsoa_resolved <- get_highest_priority_per_patient(lsoa_long)

print(paste("Date of Birth: Resolved", n_distinct(dob_long$patient_id),
            "patients from", nrow(dob_long), "rows"))
print(paste("Sex: Resolved", n_distinct(sex_long$patient_id),
            "patients from", nrow(sex_long), "rows"))
print(paste("Ethnicity: Resolved", n_distinct(ethnicity_long$patient_id),
            "patients from", nrow(ethnicity_long), "rows"))
print(paste("LSOA: Resolved", n_distinct(lsoa_long$patient_id),
            "patients from", nrow(lsoa_long), "rows"))

# ===============================================================================
# Step 7: Create Master Demographics Table
# ===============================================================================

print("\n===== CREATING MASTER DEMOGRAPHICS TABLE =====\n")

# Combine all resolved assets into a single master table
# This creates one row per patient with all demographic attributes

demographics_master <- dob_resolved %>%
  select(
    patient_id,
    date_of_birth,
    dob_source = source_table,
    dob_priority = source_priority,
    dob_quality = source_quality
  ) %>%
  full_join(
    sex_resolved %>% select(
      patient_id,
      sex,
      sex_source = source_table,
      sex_priority = source_priority,
      sex_quality = source_quality
    ),
    by = "patient_id"
  ) %>%
  full_join(
    ethnicity_resolved %>% select(
      patient_id,
      ethnicity_code,
      ethnicity_category,
      ethnicity_source = source_table,
      ethnicity_priority = source_priority,
      ethnicity_quality = source_quality
    ),
    by = "patient_id"
  ) %>%
  full_join(
    lsoa_resolved %>% select(
      patient_id,
      lsoa_code,
      lsoa_name,
      lsoa_source = source_table,
      lsoa_priority = source_priority,
      lsoa_quality = source_quality
    ),
    by = "patient_id"
  )

print(paste("Master demographics table created with", nrow(demographics_master), "patients"))
print("\nStructure:")
print(str(demographics_master))

print("\nFirst 10 rows:")
print(head(demographics_master, 10))

# ===============================================================================
# Step 8: Data Completeness Analysis
# ===============================================================================

print("\n===== DATA COMPLETENESS ANALYSIS =====\n")

# Analyze completeness for each variable
completeness <- data.frame(
  variable = c("date_of_birth", "sex", "ethnicity_code", "lsoa_code"),
  n_non_missing = c(
    sum(!is.na(demographics_master$date_of_birth)),
    sum(!is.na(demographics_master$sex)),
    sum(!is.na(demographics_master$ethnicity_code)),
    sum(!is.na(demographics_master$lsoa_code))
  )
)

completeness <- completeness %>%
  mutate(
    n_missing = nrow(demographics_master) - n_non_missing,
    pct_complete = round(100 * n_non_missing / nrow(demographics_master), 2)
  )

print("Completeness Summary:")
print(completeness)

# Identify patients with complete data
complete_patients <- demographics_master %>%
  filter(
    !is.na(date_of_birth) &
    !is.na(sex) &
    !is.na(ethnicity_code) &
    !is.na(lsoa_code)
  )

print(paste("\nPatients with complete data:", nrow(complete_patients),
            "of", nrow(demographics_master),
            paste0("(", round(100 * nrow(complete_patients) / nrow(demographics_master), 2), "%)")))

# ===============================================================================
# Step 9: Quality Analysis
# ===============================================================================

print("\n===== QUALITY ANALYSIS =====\n")

# Analyze source quality distribution
quality_summary <- demographics_master %>%
  summarise(
    high_quality_dob = sum(dob_quality == "high", na.rm = TRUE),
    high_quality_sex = sum(sex_quality == "high", na.rm = TRUE),
    high_quality_ethnicity = sum(ethnicity_quality == "high", na.rm = TRUE),
    high_quality_lsoa = sum(lsoa_quality == "high", na.rm = TRUE),
    all_high_quality = sum(
      dob_quality == "high" &
      sex_quality == "high" &
      ethnicity_quality == "high" &
      lsoa_quality == "high",
      na.rm = TRUE
    )
  )

print("High Quality Data Counts:")
print(quality_summary)

# Patients with all high-quality data
all_high_quality_patients <- demographics_master %>%
  filter(
    dob_quality == "high" &
    sex_quality == "high" &
    ethnicity_quality == "high" &
    lsoa_quality == "high"
  )

print(paste("\nPatients with ALL high-quality data:", nrow(all_high_quality_patients),
            paste0("(", round(100 * nrow(all_high_quality_patients) / nrow(demographics_master), 2), "%)")))

# ===============================================================================
# Step 10: Export Production Datasets
# ===============================================================================

print("\n===== EXPORTING PRODUCTION DATASETS =====\n")

# Create timestamped output subdirectory
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
versioned_output_dir <- file.path(OUTPUT_DIR, paste0("run_", timestamp))
dir.create(versioned_output_dir, showWarnings = FALSE, recursive = TRUE)

# Export master demographics table
master_file <- file.path(versioned_output_dir, "demographics_master.csv")
write.csv(demographics_master, master_file, row.names = FALSE)
print(paste("Exported:", master_file))

# Export complete patients only
complete_file <- file.path(versioned_output_dir, "demographics_complete.csv")
write.csv(complete_patients, complete_file, row.names = FALSE)
print(paste("Exported:", complete_file))

# Export high quality patients only
hq_file <- file.path(versioned_output_dir, "demographics_high_quality.csv")
write.csv(all_high_quality_patients, hq_file, row.names = FALSE)
print(paste("Exported:", hq_file))

# Export completeness summary
completeness_file <- file.path(versioned_output_dir, "completeness_summary.csv")
write.csv(completeness, completeness_file, row.names = FALSE)
print(paste("Exported:", completeness_file))

# Export conflict summary
conflict_file <- file.path(versioned_output_dir, "conflict_summary.csv")
write.csv(conflict_counts, conflict_file, row.names = FALSE)
print(paste("Exported:", conflict_file))

# Export individual conflict reports
for (asset_name in names(results$conflicts)) {
  conflicts <- results$conflicts[[asset_name]]
  if (nrow(conflicts) > 0) {
    conflict_detail_file <- file.path(versioned_output_dir,
                                     paste0(asset_name, "_conflicts.csv"))
    write.csv(conflicts, conflict_detail_file, row.names = FALSE)
    print(paste("Exported:", conflict_detail_file))
  }
}

# Export RDS versions for fast loading in R
saveRDS(demographics_master,
        file.path(versioned_output_dir, "demographics_master.rds"))
saveRDS(results,
        file.path(versioned_output_dir, "pipeline_results_full.rds"))

print("\nRDS files exported for fast R loading")

# ===============================================================================
# Step 11: Create Data Dictionary
# ===============================================================================

print("\n===== CREATING DATA DICTIONARY =====\n")

data_dictionary <- data.frame(
  column_name = names(demographics_master),
  data_type = sapply(demographics_master, class),
  description = c(
    "Unique patient identifier",
    "Patient date of birth",
    "Source table for date of birth",
    "Priority of DOB source (1=highest)",
    "Quality rating of DOB source",
    "Patient biological sex",
    "Source table for sex",
    "Priority of sex source (1=highest)",
    "Quality rating of sex source",
    "Ethnicity code",
    "Ethnicity category description",
    "Source table for ethnicity",
    "Priority of ethnicity source (1=highest)",
    "Quality rating of ethnicity source",
    "LSOA code (geographical)",
    "LSOA name (geographical)",
    "Source table for LSOA",
    "Priority of LSOA source (1=highest)",
    "Quality rating of LSOA source"
  ),
  stringsAsFactors = FALSE
)

dict_file <- file.path(versioned_output_dir, "data_dictionary.csv")
write.csv(data_dictionary, dict_file, row.names = FALSE)
print(paste("Exported:", dict_file))

# ===============================================================================
# Step 12: Generate Summary Report
# ===============================================================================

print("\n===== GENERATING SUMMARY REPORT =====\n")

# Create a text report summarizing the pipeline run
report_file <- file.path(versioned_output_dir, "pipeline_report.txt")

sink(report_file)

cat("=================================================================\n")
cat("CURATED DEMOGRAPHICS PIPELINE REPORT\n")
cat("=================================================================\n\n")

cat("Run Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("Processing Time:", round(difftime(end_time, start_time, units = "secs"), 2), "seconds\n\n")

cat("Configuration:\n")
cat("  - Assets processed:", paste(ASSETS_TO_PROCESS, collapse = ", "), "\n")
cat("  - Patient cohort:", ifelse(is.null(PATIENT_COHORT), "All patients",
                                  paste(length(PATIENT_COHORT), "patients")), "\n")
cat("  - Output directory:", OUTPUT_DIR, "\n\n")

cat("Results Summary:\n")
cat("  - Total patients:", nrow(demographics_master), "\n")
cat("  - Patients with complete data:", nrow(complete_patients),
    paste0("(", round(100 * nrow(complete_patients) / nrow(demographics_master), 2), "%)"), "\n")
cat("  - Patients with all high-quality data:", nrow(all_high_quality_patients),
    paste0("(", round(100 * nrow(all_high_quality_patients) / nrow(demographics_master), 2), "%)"), "\n\n")

cat("Completeness by Variable:\n")
for (i in 1:nrow(completeness)) {
  cat("  -", completeness$variable[i], ":",
      completeness$pct_complete[i], "% complete\n")
}

cat("\nConflict Summary:\n")
for (i in 1:nrow(conflict_counts)) {
  cat("  -", conflict_counts$asset[i], ":",
      conflict_counts$n_conflicts[i], "conflicts",
      paste0("(", conflict_counts$pct_of_patients[i], "%)"), "\n")
}

cat("\n=================================================================\n")
cat("Files exported to:", versioned_output_dir, "\n")
cat("=================================================================\n")

sink()

print(paste("Report saved to:", report_file))

# ===============================================================================
# Summary
# ===============================================================================

print("\n===== PIPELINE COMPLETE =====\n")

print("What we accomplished:")
print("  1. Ran complete pipeline for all demographic assets")
print("  2. Generated summary statistics and conflict reports")
print("  3. Resolved each asset to one row per patient")
print("  4. Created master demographics table")
print("  5. Analyzed data completeness and quality")
print("  6. Exported production-ready datasets")
print("  7. Created data dictionary and summary report")

print("\nOutput files available in:")
print(paste("  ", versioned_output_dir))

print("\nKey files:")
print("  - demographics_master.csv - All patients, all variables")
print("  - demographics_complete.csv - Patients with complete data")
print("  - demographics_high_quality.csv - High quality data only")
print("  - completeness_summary.csv - Completeness metrics")
print("  - conflict_summary.csv - Conflict counts")
print("  - data_dictionary.csv - Column descriptions")
print("  - pipeline_report.txt - Human-readable summary")

# ===============================================================================
# Next Steps
# ===============================================================================

# The curated datasets are now ready for analysis!

# You can reload the results later with:
# results <- readRDS(file.path(versioned_output_dir, "pipeline_results_full.rds"))
# demographics <- readRDS(file.path(versioned_output_dir, "demographics_master.rds"))

# Suggested next steps:
# 1. Review conflict reports and investigate high-conflict patients
# 2. Use demographics_complete.csv for analyses requiring complete data
# 3. Use demographics_high_quality.csv for high-quality analyses
# 4. Join demographics_master with other clinical datasets using patient_id
# 5. Implement quality checks or validation rules on the curated data
# 6. Archive or version the output directory for reproducibility

print("\nPipeline execution complete!")
