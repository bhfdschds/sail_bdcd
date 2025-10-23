#' Create DB2 Database Connection
#'
#' Creates a connection to DB2 database using ODBC.
#' Supports credentials from environment variables or direct parameters.
#'
#' @param config Optional config list with database connection parameters
#' @param driver Database driver (default: "DB2")
#' @param database Database name (default: "DEVDB")
#' @param hostname Database hostname (default: "db")
#' @param port Database port (default: 50000)
#' @param uid User ID (default: from DB_USER env var or "db2inst1")
#' @param pwd Password (default: from DB_PASSWORD env var or "mypassword123")
#' @param protocol Connection protocol (default: "TCPIP")
#'
#' @return DBI connection object
#' @export
#'
#' @examples
#' # Using defaults (environment variables or hardcoded values)
#' conn <- create_db2_connection()
#'
#' # Using config object
#' config <- list(database = list(hostname = "db", database = "DEVDB"))
#' conn <- create_db2_connection(config)
#'
#' # Using explicit parameters
#' conn <- create_db2_connection(hostname = "localhost", uid = "myuser")
create_db2_connection <- function(config = NULL,
                                   driver = NULL,
                                   database = NULL,
                                   hostname = NULL,
                                   port = NULL,
                                   uid = NULL,
                                   pwd = NULL,
                                   protocol = NULL) {

  # Load required libraries
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required but not installed.")
  }
  if (!requireNamespace("odbc", quietly = TRUE)) {
    stop("Package 'odbc' is required but not installed.")
  }

  # Extract from config if provided
  if (!is.null(config) && !is.null(config$database)) {
    db_config <- config$database
    driver <- driver %||% db_config$driver %||% "DB2"
    database <- database %||% db_config$database %||% "DEVDB"
    hostname <- hostname %||% db_config$hostname %||% "db"
    port <- port %||% db_config$port %||% 50000
    protocol <- protocol %||% db_config$protocol %||% "TCPIP"
  } else {
    # Use defaults if not from config
    driver <- driver %||% "DB2"
    database <- database %||% "DEVDB"
    hostname <- hostname %||% "db"
    port <- port %||% 50000
    protocol <- protocol %||% "TCPIP"
  }

  # Get credentials from environment variables or use defaults
  uid <- uid %||% Sys.getenv("DB_USER", "db2inst1")
  pwd <- pwd %||% Sys.getenv("DB_PASSWORD", "mypassword123")

  # Create connection
  tryCatch({
    conn <- DBI::dbConnect(
      odbc::odbc(),
      Driver = driver,
      Database = database,
      Hostname = hostname,
      Port = port,
      UID = uid,
      PWD = pwd,
      Protocol = protocol
    )

    message("Connected to DB2!")
    return(conn)

  }, error = function(e) {
    stop(sprintf("Failed to connect to DB2: %s", e$message), call. = FALSE)
  })
}

# Helper function for NULL coalescing
`%||%` <- function(a, b) if (is.null(a)) b else a

# For backwards compatibility: create a connection when sourced directly
# (only if running as a script, not when sourced by tests)
if (!exists("RSCRIPTS_PATH")) {
  # This is being run as a standalone script
  conn <- create_db2_connection()
  print("Connected to DB2!")
}
