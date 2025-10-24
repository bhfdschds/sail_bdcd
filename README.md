# SAIL BDCD: Healthcare Data Curation Pipeline

A scalable, researcher-facing data curation pipeline that ingests population-wide healthcare data from multiple IBM DB2 sources, standardizes it into long format, and produces analysis-ready datasets for healthcare research projects.

## What Does This Pipeline Do?

The SAIL BDCD (Better Data, Cleaner Data) pipeline transforms complex, multi-source healthcare data into clean, analysis-ready datasets through:

1. **Multi-source Data Integration** - Connects to multiple IBM DB2 databases and combines data intelligently
2. **Priority-Based Conflict Resolution** - Automatically resolves conflicts when the same variable appears in multiple sources
3. **Long Format Standardization** - Converts raw data into researcher-friendly long format with full audit trails
4. **Temporal Covariate Generation** - Creates time-windowed covariates and outcomes for cohort studies
5. **Quality Validation** - Comprehensive data quality checks and validation reporting

## Key Features

- **Configuration-Driven**: Add/remove data sources without code changes via YAML configuration
- **Transparent Auditing**: Full data lineage tracking showing which source was selected and why
- **Flexible Time Windows**: Generate covariates for any time period before/after index dates
- **Multiple Output Formats**: Long format (audit trail), wide format (comparison), priority-selected (analysis)
- **Production-Ready**: Docker-based deployment with comprehensive testing (77+ tests)

## Quick Start

```r
# 1. Connect to database
source("r-scripts/utility_code/db2_connection.R")
conn <- create_db2_connection()

# 2. Load configuration
source("r-scripts/pipeline_code/read_db2_config_multi_source.R")
config <- read_db2_config("r-scripts/pipeline_code/db2_config_multi_source.yaml")

# 3. Create long format asset
source("r-scripts/pipeline_code/create_long_format_assets.R")
sex_data <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "sex"
)

# 4. Get one row per patient (highest priority source)
sex_master <- get_highest_priority_per_patient(sex_data)

# 5. Generate covariates
source("r-scripts/pipeline_code/generate_covariates.R")
covariates <- generate_covariates_before_index(
  long_data = filtered_events,
  index_dates = cohort_index_dates,
  event_date_col = "admission_date"
)
```

See [GETTING_STARTED.md](GETTING_STARTED.md) for detailed setup instructions.

## Documentation

### Getting Started
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Setup, installation, and first steps
- **[PIPELINE.md](PIPELINE.md)** - Detailed pipeline architecture and data flow

### Technical Documentation
- **[docs/COVARIATE_GENERATION.md](docs/COVARIATE_GENERATION.md)** - Temporal covariate generation guide
- **[docs/LONG_FORMAT_GUIDE.md](docs/LONG_FORMAT_GUIDE.md)** - Long format asset creation
- **[docs/EXAMPLES_INDEX.md](docs/EXAMPLES_INDEX.md)** - Index of all 50+ worked examples

### Testing & Quality
- **[TEST_SUMMARY.md](TEST_SUMMARY.md)** - Comprehensive test suite documentation (77+ tests)

### Project Planning
- **[FUTURE_DEVELOPMENTS.md](FUTURE_DEVELOPMENTS.md)** - Roadmap and planned enhancements

## Project Structure

```
sail_bdcd/
├── README.md                          # This file
├── GETTING_STARTED.md                 # Setup and quickstart guide
├── PIPELINE.md                        # Pipeline architecture
├── FUTURE_DEVELOPMENTS.md             # Roadmap
├── TEST_SUMMARY.md                    # Test documentation
├── docs/                              # Technical documentation
│   ├── COVARIATE_GENERATION.md
│   ├── LONG_FORMAT_GUIDE.md
│   └── EXAMPLES_INDEX.md
├── docker-compose.yml                 # Docker orchestration
├── Dockerfile.rstudio                 # RStudio container
└── r-scripts/
    ├── pipeline_code/                 # Core pipeline functions
    │   ├── create_long_format_assets.R
    │   ├── read_db2_config_multi_source.R
    │   ├── generate_covariates.R
    │   ├── generate_outcomes.R
    │   └── db2_config_multi_source.yaml
    ├── utility_code/                  # Helper functions
    │   ├── db2_connection.R
    │   └── read_yml.R
    ├── data_generation/               # Test data setup
    │   ├── Generate_Tables.R
    │   └── Generate_Data.R
    ├── examples_code/                 # 50+ worked examples
    │   ├── example_00_master_guide.R
    │   ├── example_01_*.R through example_08_*.R
    │   └── covariate_generation_examples.R
    └── tests/                         # Test suite (77+ tests)
        └── testthat/
            ├── test-database-connection.R
            ├── test-create-long-format-assets.R
            ├── test-read-db2-config-multi_source.R
            └── test-generate-covariates.R
```

## Core Capabilities

### 1. Multi-Source Data Integration
Seamlessly query and combine data from multiple DB2 databases with flexible source prioritization:

```yaml
# Configure in db2_config_multi_source.yaml
assets:
  date_of_birth:
    sources:
      - name: "hospital_system"
        priority: 1
        quality_rating: "high"
      - name: "gp_registry"
        priority: 2
        quality_rating: "medium"
```

### 2. Priority-Based Conflict Resolution
When multiple sources provide different values for the same patient, the pipeline:
- Automatically selects the highest priority source
- Provides full audit trail of all available values
- Handles missing data in high-priority sources intelligently

### 3. Long Format Design
Each row represents one source's data for one patient:

| patient_id | source_table | source_priority | sex_code | sex_description |
|------------|--------------|----------------|----------|-----------------|
| 1001       | gp_registry  | 1              | M        | Male            |
| 1001       | hospital_demographics | 2     | 1        | Male            |

Enables:
- Side-by-side source comparison
- Conflict detection
- Data quality assessment
- Full audit trails

### 4. Temporal Covariate Generation
Create time-windowed features for cohort studies:

```r
# Events in last 30 days before index
generate_covariates_time_window(
  long_data = events,
  index_dates = cohort,
  days_before_start = 30,
  days_before_end = 1,
  window_label = "last_30d"
)
```

Supports:
- Multiple time windows simultaneously
- Custom aggregation functions
- Lookback periods to avoid immortal time bias
- Both covariates (before index) and outcomes (after index)

### 5. Data Quality Framework
- Comprehensive validation checks (date ranges, coding standards, LSOA validation)
- Missing data strategies
- Conflict detection and reporting
- Quality dashboards and metrics

## Use Cases

### Healthcare Research
- **Cohort Studies**: Generate baseline covariates from multiple time windows
- **Comorbidity Indices**: Calculate Charlson comorbidity scores from diagnosis codes
- **Utilization Metrics**: Count ED visits, inpatient days, outpatient encounters
- **Medication Adherence**: Calculate medication possession ratios

### Data Quality
- **Source Comparison**: Identify systematic differences between data sources
- **Conflict Analysis**: Find and investigate discrepancies
- **Coverage Assessment**: Evaluate completeness by source and variable
- **Validation Reporting**: Generate quality metrics for regulatory compliance

## Technology Stack

- **Language**: R 4.x
- **Database**: IBM DB2 (v11.5.8)
- **Key Packages**: DBI, odbc, dplyr, tidyr, yaml, testthat
- **Deployment**: Docker (RStudio Server + DB2)
- **Testing**: testthat framework with 77+ tests

## Requirements

- Docker & Docker Compose
- Environment variables: `DB_USER`, `DB_PASSWORD` (configured in `.env` file)
- IBM DB2 ODBC CLI driver (included in repository)

## Running the Pipeline

See [GETTING_STARTED.md](GETTING_STARTED.md) for detailed instructions.

**Quick Docker start:**
```bash
docker-compose up -d
# Access RStudio at http://localhost:8787
```

## Testing

Run comprehensive test suite:
```r
library(testthat)
test_dir("r-scripts/tests/testthat")
```

Or via command line:
```bash
Rscript r-scripts/tests/testthat.R
```

See [TEST_SUMMARY.md](TEST_SUMMARY.md) for details on the 77+ tests.

## Examples

The repository includes 50+ worked examples demonstrating every feature:

- **[example_00_master_guide.R](r-scripts/examples_code/example_00_master_guide.R)** - Start here for overview
- **[example_01-08](r-scripts/examples_code/)** - Function-specific examples
- **[covariate_generation_examples.R](r-scripts/examples_code/covariate_generation_examples.R)** - Temporal covariate examples

See [docs/EXAMPLES_INDEX.md](docs/EXAMPLES_INDEX.md) for complete index.

## Contributing

When extending the pipeline:
1. Follow existing code structure and naming conventions
2. Add unit tests for new functions (use testthat)
3. Update configuration schema if adding new asset types
4. Document new features in relevant markdown files
5. Add worked examples for new functionality

## Support

1. **Read the documentation** - Start with [GETTING_STARTED.md](GETTING_STARTED.md)
2. **Check examples** - See [docs/EXAMPLES_INDEX.md](docs/EXAMPLES_INDEX.md)
3. **Review tests** - Test files show expected behavior
4. **Troubleshooting** - See troubleshooting section in [example_00_master_guide.R](r-scripts/examples_code/example_00_master_guide.R)

## License

[Specify license here]

## Citation

If you use this pipeline in your research, please cite:
[Citation information to be added]

## Acknowledgments

Developed for the SAIL Databank to support healthcare research using population-wide linked data.

---

**Quick Links:**
- [Getting Started](GETTING_STARTED.md) - Setup and first steps
- [Pipeline Guide](PIPELINE.md) - Architecture and data flow
- [Covariate Generation](docs/COVARIATE_GENERATION.md) - Temporal features
- [Examples](docs/EXAMPLES_INDEX.md) - 50+ worked examples
- [Tests](TEST_SUMMARY.md) - Test suite documentation
- [Future Plans](FUTURE_DEVELOPMENTS.md) - Roadmap
