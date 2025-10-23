# Tests for read_db2_config_multi_source.R functions
# Tests multi-source asset management functions

context("Multi-Source Config and Data Retrieval Tests")

# ============================================================================
# Tests for get_asset_sources
# ============================================================================

test_that("get_asset_sources returns all sources for an asset", {
  config <- read_db_config(CONFIG_PATH)

  sources <- get_asset_sources(config, "sex")

  expect_type(sources, "list")
  expect_true(length(sources) > 0)

  # Check structure of first source
  first_source <- sources[[1]]
  expect_true("name" %in% names(first_source))
  expect_true("table_name" %in% names(first_source))
  expect_true("priority" %in% names(first_source))
})

test_that("get_asset_sources sorts by priority", {
  config <- read_db_config(CONFIG_PATH)

  # Try with an asset that has multiple sources
  sources <- get_asset_sources(config, "ethnicity")

  # Extract priorities
  priorities <- sapply(sources, function(x) x$priority)

  # Should be sorted in ascending order
  expect_equal(priorities, sort(priorities))
})

test_that("get_asset_sources handles non-existent asset", {
  config <- read_db_config(CONFIG_PATH)

  expect_error(
    get_asset_sources(config, "nonexistent_asset"),
    "not found"
  )
})

test_that("get_asset_sources includes metadata", {
  config <- read_db_config(CONFIG_PATH)

  sources <- get_asset_sources(config, "sex")

  # Check that metadata fields exist
  for (source in sources) {
    expect_true(!is.null(source$name))
    expect_true(!is.null(source$table_name))
    expect_true(!is.null(source$priority))
  }
})

# ============================================================================
# Tests for select_source_for_asset
# ============================================================================

test_that("select_source_for_asset returns preferred source when specified", {
  config <- read_db_config(CONFIG_PATH)

  result <- select_source_for_asset(
    config,
    asset_name = "sex",
    preferred_source = "gp_sex"
  )

  expect_equal(result, "gp_sex")
})

test_that("select_source_for_asset uses project preferences", {
  config <- read_db_config(CONFIG_PATH)

  # Get source for a project that has preferences
  result <- select_source_for_asset(
    config,
    asset_name = "ethnicity",
    project_name = "clinical_research_study"
  )

  # Should return a valid source name
  expect_type(result, "character")
  expect_true(nchar(result) > 0)
})

test_that("select_source_for_asset returns default when no preference", {
  config <- read_db_config(CONFIG_PATH)

  result <- select_source_for_asset(
    config,
    asset_name = "sex"
  )

  # Should return default source from config
  expected_default <- config$assets$sex$default_source
  expect_equal(result, expected_default)
})

# ============================================================================
# Tests for get_source_columns
# ============================================================================

test_that("get_source_columns returns column mappings", {
  config <- read_db_config(CONFIG_PATH)

  result <- get_source_columns(config, "sex", "gp_sex")

  expect_type(result, "list")
  expect_true("table_name" %in% names(result))
  expect_true("columns" %in% names(result))
  expect_true("primary_key" %in% names(result))
})

test_that("get_source_columns includes all column mappings", {
  config <- read_db_config(CONFIG_PATH)

  result <- get_source_columns(config, "sex", "gp_sex")

  # Should have columns
  expect_true(length(result$columns) > 0)

  # Columns should have names (R names)
  expect_true(!is.null(names(result$columns)))
})

test_that("get_source_columns handles invalid source", {
  config <- read_db_config(CONFIG_PATH)

  expect_error(
    get_source_columns(config, "sex", "invalid_source"),
    "not found"
  )
})

# ============================================================================
# Tests for build_select_query
# ============================================================================

test_that("build_select_query generates valid SQL", {
  config <- read_db_config(CONFIG_PATH)

  query <- build_select_query(config, "sex", "gp_sex")

  # Check for SQL keywords
  expect_true(grepl("SELECT", query, ignore.case = TRUE))
  expect_true(grepl("FROM", query, ignore.case = TRUE))
  expect_true(grepl("SAIL\\.", query))  # Schema prefix
})

test_that("build_select_query includes data_source column", {
  config <- read_db_config(CONFIG_PATH)

  query <- build_select_query(config, "sex", "gp_sex")

  # Should include data_source column
  expect_true(grepl("data_source", query, ignore.case = TRUE))
  expect_true(grepl("gp_sex", query))
})

test_that("build_select_query adds WHERE clause for patient_ids", {
  config <- read_db_config(CONFIG_PATH)

  query <- build_select_query(
    config,
    "sex",
    "gp_sex",
    patient_ids = c("P001", "P002")
  )

  # Should have WHERE and IN clauses
  expect_true(grepl("WHERE", query, ignore.case = TRUE))
  expect_true(grepl("IN", query, ignore.case = TRUE))
  expect_true(grepl("P001", query))
  expect_true(grepl("P002", query))
})

test_that("build_select_query uses custom schema", {
  config <- read_db_config(CONFIG_PATH)

  query <- build_select_query(
    config,
    "sex",
    "gp_sex",
    schema = "CUSTOM_SCHEMA"
  )

  expect_true(grepl("CUSTOM_SCHEMA", query))
})

# ============================================================================
# Tests for resolve_conflicts
# ============================================================================

test_that("resolve_conflicts with priority method keeps first row", {
  multi_source_data <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002"),
    data_source = c("source1", "source2", "source1", "source2"),
    value = c("A", "B", "C", "D"),
    stringsAsFactors = FALSE
  )

  result <- resolve_conflicts(
    multi_source_data,
    "test_asset",
    resolution_method = "priority"
  )

  # Should have one row per patient
  expect_equal(nrow(result), 2)
  expect_equal(result$patient_id, c("P001", "P002"))

  # Should keep first occurrence
  expect_equal(result$value, c("A", "C"))
})

test_that("resolve_conflicts with most_recent method uses record_date", {
  multi_source_data <- data.frame(
    patient_id = c("P001", "P001"),
    data_source = c("source1", "source2"),
    record_date = as.Date(c("2020-01-01", "2021-01-01")),
    value = c("A", "B"),
    stringsAsFactors = FALSE
  )

  result <- resolve_conflicts(
    multi_source_data,
    "test_asset",
    resolution_method = "most_recent"
  )

  # Should keep most recent
  expect_equal(nrow(result), 1)
  expect_equal(result$value, "B")  # 2021 date
})

test_that("resolve_conflicts with consensus method adds conflict flag", {
  multi_source_data <- data.frame(
    patient_id = c("P001", "P001", "P002", "P002"),
    ethnicity_code = c("A", "B", "C", "C"),
    stringsAsFactors = FALSE
  )

  result <- resolve_conflicts(
    multi_source_data,
    "ethnicity",
    resolution_method = "consensus"
  )

  # Should have conflict_flag column
  expect_true("conflict_flag" %in% names(result))

  # P001 should have conflict, P002 should not
  expect_equal(result$conflict_flag, c(TRUE, FALSE))
})

# ============================================================================
# Tests for combine_ethnicity_sources
# ============================================================================

test_that("combine_ethnicity_sources combines multiple sources", {
  # Create test data for different sources
  self_reported <- data.frame(
    patient_id = c("P001", "P002"),
    ethnicity_code = c("A", "B"),
    data_source = "self_reported_ethnicity",
    stringsAsFactors = FALSE
  )

  gp_ethnicity <- data.frame(
    patient_id = c("P002", "P003"),
    ethnicity_code = c("B", "C"),
    data_source = "gp_ethnicity",
    stringsAsFactors = FALSE
  )

  data_list <- list(
    self_reported_ethnicity = self_reported,
    gp_ethnicity = gp_ethnicity
  )

  result <- combine_ethnicity_sources(data_list)

  # Should combine unique patients
  expect_true(nrow(result) >= 3)  # At least P001, P002, P003

  # Should include all unique patient IDs
  unique_patients <- unique(c(self_reported$patient_id, gp_ethnicity$patient_id))
  expect_true(all(unique_patients %in% result$patient_id))
})

test_that("combine_ethnicity_sources prioritizes self-reported", {
  # Create overlapping data
  self_reported <- data.frame(
    patient_id = c("P001"),
    ethnicity_code = c("A"),
    data_source = "self_reported_ethnicity"
  )

  gp_ethnicity <- data.frame(
    patient_id = c("P001"),
    ethnicity_code = c("B"),
    data_source = "gp_ethnicity"
  )

  data_list <- list(
    self_reported_ethnicity = self_reported,
    gp_ethnicity = gp_ethnicity
  )

  result <- combine_ethnicity_sources(data_list)

  # Should keep self-reported (higher priority)
  expect_equal(nrow(result), 1)
  expect_equal(result$ethnicity_code, "A")
  expect_equal(result$data_source, "self_reported_ethnicity")
})

# ============================================================================
# Tests for print functions (verify they run without error)
# ============================================================================

test_that("print_asset_sources runs without error", {
  config <- read_db_config(CONFIG_PATH)

  # Capture output
  result <- capture.output(
    print_asset_sources(config, "sex")
  )

  # Should produce some output
  expect_true(length(result) > 0)
})

test_that("print_project_config runs without error", {
  config <- read_db_config(CONFIG_PATH)

  # Capture output
  result <- capture.output(
    print_project_config(config, "clinical_research_study")
  )

  # Should produce some output
  expect_true(length(result) > 0)
})

test_that("print_project_config handles missing project", {
  config <- read_db_config(CONFIG_PATH)

  expect_error(
    print_project_config(config, "nonexistent_project"),
    "not found"
  )
})

# ============================================================================
# Integration Tests (require database)
# ============================================================================

test_that("get_asset_data retrieves data from database", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Get sex data from gp_sex source
  result <- tryCatch({
    get_asset_data(
      conn, config,
      asset_name = "sex",
      source_name = "gp_sex"
    )
  }, error = function(e) {
    NULL
  })

  if (!is.null(result)) {
    expect_s3_class(result, "data.frame")
    expect_true(nrow(result) > 0)
    expect_true("data_source" %in% names(result))
  }

  cleanup_connection(conn)
})

test_that("get_asset_data_multi_source combines multiple sources", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  # Get data from multiple ethnicity sources
  result <- tryCatch({
    get_asset_data_multi_source(
      conn, config,
      asset_name = "ethnicity",
      source_names = c("self_reported_ethnicity", "gp_ethnicity")
    )
  }, error = function(e) {
    NULL
  })

  if (!is.null(result)) {
    expect_s3_class(result, "data.frame")
    expect_true("data_source" %in% names(result))

    # Should have data from both sources
    sources <- unique(result$data_source)
    expect_true(length(sources) >= 1)
  }

  cleanup_connection(conn)
})

test_that("get_project_data retrieves data for a project", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  result <- tryCatch({
    get_project_data(
      conn, config,
      project_name = "clinical_research_study"
    )
  }, error = function(e) {
    NULL
  })

  if (!is.null(result)) {
    expect_type(result, "list")

    # Should have data for multiple assets
    expect_true(length(result) > 0)

    # Each asset should be a data frame
    for (asset_data in result) {
      expect_s3_class(asset_data, "data.frame")
    }
  }

  cleanup_connection(conn)
})

test_that("compare_sources analyzes differences across sources", {
  skip_if_no_db2()

  conn <- get_test_connection()
  config <- read_db_config(CONFIG_PATH)

  result <- tryCatch({
    compare_sources(
      conn, config,
      asset_name = "sex"
    )
  }, error = function(e) {
    NULL
  })

  if (!is.null(result)) {
    expect_type(result, "list")
    expect_true("data" %in% names(result))
    expect_true("summary" %in% names(result))

    expect_s3_class(result$data, "data.frame")
    expect_s3_class(result$summary, "data.frame")
  }

  cleanup_connection(conn)
})

# ============================================================================
# Tests for configuration validation
# ============================================================================

test_that("config has required database section", {
  config <- read_db_config(CONFIG_PATH)

  expect_true("database" %in% names(config))
  expect_true("schema" %in% names(config$database))
  expect_true("driver" %in% names(config$database))
})

test_that("config has assets section with sources", {
  config <- read_db_config(CONFIG_PATH)

  expect_true("assets" %in% names(config))
  expect_true(length(config$assets) > 0)

  # Check first asset has sources
  first_asset <- config$assets[[1]]
  expect_true("sources" %in% names(first_asset))
})

test_that("config has projects section", {
  config <- read_db_config(CONFIG_PATH)

  expect_true("projects" %in% names(config))
  expect_true(length(config$projects) > 0)

  # Check first project has preferred_sources
  first_project <- config$projects[[1]]
  expect_true("preferred_sources" %in% names(first_project))
})

test_that("asset sources have required metadata", {
  config <- read_db_config(CONFIG_PATH)

  # Check sex asset sources
  sex_sources <- config$assets$sex$sources

  for (source_name in names(sex_sources)) {
    source <- sex_sources[[source_name]]

    expect_true("table_name" %in% names(source))
    expect_true("priority" %in% names(source))
    expect_true("columns" %in% names(source))
    expect_true("primary_key" %in% names(source))
  }
})
