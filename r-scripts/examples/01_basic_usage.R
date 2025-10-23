# ===============================================================================
# Example 1: Basic Usage of Long Format Code
# ===============================================================================
# This example demonstrates the fundamental workflow for creating a long format
# asset table from multiple database sources.
#
# What you'll learn:
# - How to load configuration
# - How to connect to the database
# - How to create a long format table for a single asset
# - How to view and understand the output structure
# ===============================================================================

# Load required libraries
library(dplyr)
library(DBI)
library(yaml)

# Source required scripts
source("utility_code/db2_connection.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")

# ===============================================================================
# Step 1: Load Configuration
# ===============================================================================

# The configuration file contains:
# - Database connection details
# - Asset definitions (date_of_birth, sex, ethnicity, lsoa)
# - Source tables and their priorities
# - Column mappings from database to R

config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")

# View available assets
print("Available assets:")
print(names(config$assets))

# ===============================================================================
# Step 2: Connect to Database
# ===============================================================================

# Create connection using credentials from environment variables
conn <- create_db2_connection(config)

# Verify connection
print(paste("Connected to database:", config$database$schema))

# ===============================================================================
# Step 3: Create Long Format Table
# ===============================================================================

# Create long format table for ethnicity
# This pulls data from ALL ethnicity sources defined in the config
ethnicity_long <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "ethnicity",
  patient_ids = NULL  # NULL = all patients; or provide vector of IDs
)

# ===============================================================================
# Step 4: Examine the Output
# ===============================================================================

# View structure
print("Structure of long format table:")
str(ethnicity_long)

# View first few rows
print("First 10 rows:")
print(head(ethnicity_long, 10))

# Understand the columns:
# - patient_id: Patient identifier
# - source_table: Which source this row came from
# - source_priority: Priority ranking (1 = highest priority)
# - source_quality: Quality rating (high, medium, low)
# - source_coverage: Coverage proportion (0.0 to 1.0)
# - source_last_updated: When the source was last updated
# - [asset columns]: The actual data (e.g., ethnicity_code, ethnicity_category)

# ===============================================================================
# Step 5: Basic Analysis
# ===============================================================================

# Count total rows
print(paste("Total rows:", nrow(ethnicity_long)))

# Count unique patients
print(paste("Unique patients:", n_distinct(ethnicity_long$patient_id)))

# View which sources are included
print("Sources included:")
print(unique(ethnicity_long$source_table))

# Count rows per source
source_counts <- ethnicity_long %>%
  group_by(source_table, source_priority) %>%
  summarise(
    n_patients = n_distinct(patient_id),
    n_rows = n()
  ) %>%
  arrange(source_priority)

print("Rows per source:")
print(source_counts)

# ===============================================================================
# Step 6: Understand Long Format
# ===============================================================================

# Example: View all sources for a single patient
example_patient <- ethnicity_long$patient_id[1]

patient_data <- ethnicity_long %>%
  filter(patient_id == example_patient)

print(paste("Data for patient", example_patient, "from all sources:"))
print(patient_data)

# This shows how one patient can have multiple rows (one per source)
# Each row represents the same patient's data from a different source table

# ===============================================================================
# Step 7: Convert to One Row Per Patient
# ===============================================================================

# Use priority-based resolution to get one row per patient
# This keeps the row from the highest priority source (lowest priority number)
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)

print("After resolution:")
print(paste("Total rows:", nrow(ethnicity_resolved)))
print(paste("Unique patients:", n_distinct(ethnicity_resolved$patient_id)))

# Now each patient appears exactly once
print("First 10 resolved rows:")
print(head(ethnicity_resolved, 10))

# ===============================================================================
# Step 8: Save the Results
# ===============================================================================

# Option 1: Save as CSV (human-readable)
write.csv(
  ethnicity_long,
  "ethnicity_long_format.csv",
  row.names = FALSE
)

write.csv(
  ethnicity_resolved,
  "ethnicity_resolved.csv",
  row.names = FALSE
)

# Option 2: Save as RDS (R binary format, faster and preserves data types)
saveRDS(ethnicity_long, "ethnicity_long_format.rds")
saveRDS(ethnicity_resolved, "ethnicity_resolved.rds")

print("Results saved to:")
print("  - ethnicity_long_format.csv")
print("  - ethnicity_resolved.csv")
print("  - ethnicity_long_format.rds")
print("  - ethnicity_resolved.rds")

# ===============================================================================
# Step 9: Clean Up
# ===============================================================================

# Always disconnect from database when done
DBI::dbDisconnect(conn)

print("Database connection closed.")

# ===============================================================================
# Summary
# ===============================================================================

# What we did:
# 1. Loaded configuration from YAML file
# 2. Connected to DB2 database
# 3. Created a long format table for ethnicity from multiple sources
# 4. Examined the structure and content
# 5. Analyzed sources and patient counts
# 6. Resolved to one row per patient using priority
# 7. Saved results to files
# 8. Disconnected from database

# Key takeaways:
# - Long format preserves ALL source data (multiple rows per patient)
# - Each row includes metadata about its source (priority, quality, coverage)
# - Use get_highest_priority_per_patient() to resolve to one row per patient
# - The source with priority=1 is preferred when conflicts exist

# ===============================================================================
# Next Steps
# ===============================================================================

# Try modifying this script to:
# 1. Create long format tables for other assets (date_of_birth, sex, lsoa)
# 2. Filter to specific patients using patient_ids parameter
# 3. Analyze data quality by source
# 4. Investigate patients appearing in multiple sources

# See other examples:
# - 02_conflict_detection.R - Detecting and analyzing conflicts
# - 03_quality_assessment.R - Assessing data quality
# - 04_complete_pipeline.R - Running the full pipeline for all assets
