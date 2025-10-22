# Data Scientist Work Description
## Healthcare Data Curation Pipeline Development

### Project Overview
Develop a scalable, researcher-facing data curation product that ingests population-wide healthcare data from multiple IBM DB2 sources, standardizes it into long format, and produces analysis-ready datasets for healthcare research projects.

---

### Core Responsibilities

#### 1. Data Ingestion Framework
- Design and implement an R-based ingestion system that connects to multiple IBM DB2 databases
- Create a flexible configuration system to add/remove data sources without code changes
- Develop automated data profiling routines to assess incoming data quality and structure
- Build logging and error-handling mechanisms to track data provenance and ingestion issues
- Ensure the system can handle varying data volumes and table structures across sources

#### 2. Priority-Based Variable Resolution
- Develop a priority ranking system that allows users to specify data source hierarchy
- Implement logic to resolve conflicts when the same variable (DOB, sex, ethnicity, LSOA) appears in multiple sources
- Create a transparent audit trail showing which source was selected for each variable and why
- Design a user-configurable interface (e.g., configuration file or parameter list) for priority settings
- Handle edge cases such as:
  - Missing values in high-priority sources
  - Data quality differences across sources
  - Date conflicts or versioning issues

#### 3. Data Transformation Pipeline
- Transform raw data into **long format** with the following structure:
  - Each row represents a single measurement or observation
  - Core demographic variables: `patient_id`, `date_of_birth`, `sex`, `ethnicity`, `lsoa`
  - Measurement variables: `measurement_date`, `measurement_type`, `measurement_value`, `measurement_unit`
- Develop data quality checks and validation rules:
  - Date range validation
  - Sex/ethnicity coding standardization
  - LSOA code validation
  - Clinical measurement plausibility checks (e.g., BMI ranges, negative values)
- Implement data type standardization and missing value handling strategies
- Create data lineage metadata to track transformations applied

#### 4. Measurements Table Integration
- Design linkage methodology between demographic data and clinical measurements table
- Handle multiple measurements per patient over time (BMI, HEIGHT, WEIGHT, CHOLESTEROL, etc.)
- Ensure temporal alignment and create time-series compatible structures
- Validate measurement units and implement unit conversion where necessary
- Flag duplicate or conflicting measurements from different sources

#### 5. Wide Format Conversion (Secondary Output)
- Develop a transformation function to pivot long format data into wide format
- Create sensible naming conventions for wide-format variables (e.g., `BMI_2023_01_15`, `HEIGHT_baseline`)
- Implement aggregation rules for multiple measurements (most recent, average, first/last)
- Provide user options for handling:
  - Time windows for measurement grouping
  - Missing data imputation strategies
  - Variable selection for wide format output

#### 6. Product Development & User Interface
- Design researcher-friendly interfaces (R functions/package or configuration-driven scripts)
- Create comprehensive documentation including:
  - User guide with examples
  - Data dictionary for output variables
  - Configuration guide for priority settings
  - Troubleshooting common issues
- Develop parameter validation and helpful error messages for users
- Build example workflows and use cases

#### 7. Quality Assurance & Validation
- Implement automated unit tests for all transformation logic
- Create validation reports comparing input vs. output record counts
- Develop data quality dashboards showing:
  - Completeness by variable and source
  - Priority resolution statistics
  - Data quality flags and warnings
- Establish version control and change management processes

#### 8. Performance Optimization
- Optimize DB2 queries for large-scale population data
- Implement chunked processing for memory efficiency
- Profile code performance and identify bottlenecks
- Document system requirements and expected processing times

---

### Technical Deliverables

1. **R Package or Script Library** containing modular functions for:
   - DB2 connection and data extraction
   - Priority-based variable resolution
   - Long format transformation
   - Wide format conversion
   - Quality validation

2. **Configuration System** allowing users to specify:
   - Data source connections and credentials
   - Priority rankings
   - Variable mappings and standardization rules
   - Output format preferences

3. **Documentation Suite**:
   - Technical architecture document
   - User manual with worked examples
   - Data dictionary
   - API/function reference guide

4. **Quality Assurance Materials**:
   - Unit test suite
   - Validation reports template
   - Data quality dashboard

5. **Example Datasets & Use Cases** demonstrating typical research workflows

---

### Key Technical Requirements

- **Language**: R (with packages such as `DBI`, `RODBC`, `data.table`, `dplyr`, `tidyr`)
- **Database**: IBM DB2 connectivity and query optimization
- **Data Formats**: Long format (primary), wide format (secondary)
- **Version Control**: Git repository with clear documentation
- **Testing**: Automated testing framework (e.g., `testthat`)
- **Documentation**: R Markdown or similar for reproducible documentation

---

### Success Criteria

- Pipeline successfully ingests data from multiple DB2 sources without manual intervention
- Priority resolution system correctly handles conflicting variables with full audit trail
- Output data passes all quality validation checks with <5% data loss due to quality issues
- Transformation from raw to long format completes within acceptable timeframes (to be defined based on data volume)
- Researchers can independently configure and run the pipeline with minimal support
- Documentation is sufficient for new users to onboard without direct training
- System is extensible to accommodate new data sources and variables

---

### Considerations & Edge Cases

- **Data Privacy**: Ensure compliance with healthcare data regulations (anonymization, access controls)
- **Backward Compatibility**: Plan for maintaining compatibility as data sources evolve
- **Scalability**: Design for growing data volumes and increasing source counts
- **Reproducibility**: Ensure identical outputs given identical inputs and configurations
- **Multi-user Support**: Consider concurrent usage and configuration management
- **Missing Data Patterns**: Develop strategies for systematic missingness across sources