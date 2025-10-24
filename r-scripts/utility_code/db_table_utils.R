#' Database Table Utility Functions
#'
#' Functions to save and read data frames as database tables instead of RDS/CSV files.
#' Uses a workspace schema (DB2INST1) for storing intermediate analysis results.

#' Save Data Frame to Database Table
#'
#' Saves a data frame as a database table, replacing it if it already exists.
#'
#' @param conn DBI connection object
#' @param data Data frame to save
#' @param table_name Name for the database table (will be uppercased for DB2)
#' @param schema Schema to use (default: "DB2INST1" - user workspace)
#' @param overwrite Whether to overwrite existing table (default: TRUE)
#'
#' @return Invisible TRUE on success
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' save_to_db(conn, my_data, "DEMOGRAPHICS_COMBINED")
save_to_db <- function(conn, data, table_name,
                       schema = "DB2INST1",
                       overwrite = TRUE) {

  # Validate inputs
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid DBI connection object")
  }
  if (!is.data.frame(data)) {
    stop("data must be a data frame")
  }
  if (nrow(data) == 0) {
    warning("Attempting to save empty data frame to database")
  }

  # Convert table name to uppercase for DB2
  table_name <- toupper(table_name)
  schema <- toupper(schema)

  # Create fully qualified table identifier
  table_id <- DBI::Id(schema = schema, table = table_name)

  tryCatch({
    # Check if table exists
    table_exists <- DBI::dbExistsTable(conn, table_id)

    if (table_exists && !overwrite) {
      stop(sprintf("Table %s.%s already exists and overwrite=FALSE",
                   schema, table_name))
    }

    # Write table to database
    DBI::dbWriteTable(
      conn,
      table_id,
      data,
      overwrite = overwrite,
      row.names = FALSE
    )

    message(sprintf("✓ Saved to database: %s.%s (%d rows, %d columns)",
                    schema, table_name, nrow(data), ncol(data)))

    invisible(TRUE)

  }, error = function(e) {
    stop(sprintf("Failed to save table %s.%s: %s",
                 schema, table_name, e$message), call. = FALSE)
  })
}


#' Read Data Frame from Database Table
#'
#' Reads a data frame from a database table.
#'
#' @param conn DBI connection object
#' @param table_name Name of the database table (will be uppercased for DB2)
#' @param schema Schema to use (default: "DB2INST1" - user workspace)
#'
#' @return Data frame with table contents
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' my_data <- read_from_db(conn, "DEMOGRAPHICS_COMBINED")
read_from_db <- function(conn, table_name, schema = "DB2INST1") {

  # Validate inputs
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid DBI connection object")
  }

  # Convert table name to uppercase for DB2
  table_name <- toupper(table_name)
  schema <- toupper(schema)

  # Create fully qualified table identifier
  table_id <- DBI::Id(schema = schema, table = table_name)

  tryCatch({
    # Check if table exists
    if (!DBI::dbExistsTable(conn, table_id)) {
      stop(sprintf("Table %s.%s does not exist", schema, table_name))
    }

    # Read table from database
    data <- DBI::dbReadTable(conn, table_id)

    message(sprintf("✓ Read from database: %s.%s (%d rows, %d columns)",
                    schema, table_name, nrow(data), ncol(data)))

    return(data)

  }, error = function(e) {
    stop(sprintf("Failed to read table %s.%s: %s",
                 schema, table_name, e$message), call. = FALSE)
  })
}


#' Check if Database Table Exists
#'
#' Checks whether a table exists in the database.
#'
#' @param conn DBI connection object
#' @param table_name Name of the database table (will be uppercased for DB2)
#' @param schema Schema to use (default: "DB2INST1" - user workspace)
#'
#' @return Logical TRUE if table exists, FALSE otherwise
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' if (db_table_exists(conn, "DEMOGRAPHICS_COMBINED")) {
#'   data <- read_from_db(conn, "DEMOGRAPHICS_COMBINED")
#' }
db_table_exists <- function(conn, table_name, schema = "DB2INST1") {

  # Validate inputs
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid DBI connection object")
  }

  # Convert table name to uppercase for DB2
  table_name <- toupper(table_name)
  schema <- toupper(schema)

  # Create fully qualified table identifier
  table_id <- DBI::Id(schema = schema, table = table_name)

  tryCatch({
    exists <- DBI::dbExistsTable(conn, table_id)
    return(exists)
  }, error = function(e) {
    warning(sprintf("Error checking if table %s.%s exists: %s",
                    schema, table_name, e$message))
    return(FALSE)
  })
}


#' List All Tables in Workspace Schema
#'
#' Lists all tables in the workspace schema.
#'
#' @param conn DBI connection object
#' @param schema Schema to list tables from (default: "DB2INST1")
#'
#' @return Character vector of table names
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' tables <- list_workspace_tables(conn)
list_workspace_tables <- function(conn, schema = "DB2INST1") {

  # Validate inputs
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid DBI connection object")
  }

  schema <- toupper(schema)

  tryCatch({
    # Query to list tables in schema
    query <- sprintf(
      "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = '%s' ORDER BY TABNAME",
      schema
    )

    result <- DBI::dbGetQuery(conn, query)

    if (nrow(result) > 0) {
      tables <- result$TABNAME
      message(sprintf("Found %d tables in schema %s", length(tables), schema))
      return(tables)
    } else {
      message(sprintf("No tables found in schema %s", schema))
      return(character(0))
    }

  }, error = function(e) {
    warning(sprintf("Error listing tables in schema %s: %s", schema, e$message))
    return(character(0))
  })
}


#' Delete Database Table
#'
#' Deletes a table from the database if it exists.
#'
#' @param conn DBI connection object
#' @param table_name Name of the database table to delete (will be uppercased for DB2)
#' @param schema Schema containing the table (default: "DB2INST1")
#'
#' @return Invisible TRUE on success
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' delete_db_table(conn, "OLD_ANALYSIS_TABLE")
delete_db_table <- function(conn, table_name, schema = "DB2INST1") {

  # Validate inputs
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid DBI connection object")
  }

  # Convert table name to uppercase for DB2
  table_name <- toupper(table_name)
  schema <- toupper(schema)

  # Create fully qualified table identifier
  table_id <- DBI::Id(schema = schema, table = table_name)

  tryCatch({
    # Check if table exists
    if (!DBI::dbExistsTable(conn, table_id)) {
      message(sprintf("Table %s.%s does not exist, nothing to delete",
                      schema, table_name))
      return(invisible(FALSE))
    }

    # Remove table
    DBI::dbRemoveTable(conn, table_id)

    message(sprintf("✓ Deleted table: %s.%s", schema, table_name))

    invisible(TRUE)

  }, error = function(e) {
    stop(sprintf("Failed to delete table %s.%s: %s",
                 schema, table_name, e$message), call. = FALSE)
  })
}


#' Save Multiple Tables to Database
#'
#' Convenience function to save multiple data frames as database tables.
#'
#' @param conn DBI connection object
#' @param table_list Named list of data frames to save
#' @param schema Schema to use (default: "DB2INST1")
#' @param overwrite Whether to overwrite existing tables (default: TRUE)
#'
#' @return Named character vector of saved table names
#' @export
#'
#' @examples
#' conn <- create_db2_connection()
#' tables <- list(
#'   demographics = demographics_df,
#'   cohort = cohort_df
#' )
#' save_multiple_to_db(conn, tables)
save_multiple_to_db <- function(conn, table_list,
                                schema = "DB2INST1",
                                overwrite = TRUE) {

  if (!is.list(table_list) || is.null(names(table_list))) {
    stop("table_list must be a named list of data frames")
  }

  saved_tables <- character(length(table_list))
  names(saved_tables) <- names(table_list)

  for (name in names(table_list)) {
    data <- table_list[[name]]
    table_name <- toupper(gsub("[^A-Za-z0-9_]", "_", name))

    save_to_db(conn, data, table_name, schema, overwrite)
    saved_tables[name] <- sprintf("%s.%s", schema, table_name)
  }

  message(sprintf("\n✓ Saved %d tables to database", length(saved_tables)))

  return(saved_tables)
}
