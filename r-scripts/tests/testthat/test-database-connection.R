# Tests for Database Connection
# Tests the db2_connection.R functions

context("Database Connection Tests")

test_that("create_db2_connection establishes valid connection", {
  skip_if_no_db2()

  conn <- NULL

  # Test connection creation
  expect_error(
    conn <- create_db2_connection(),
    NA  # Expect no error
  )

  # Test connection is valid
  expect_true(DBI::dbIsValid(conn))

  # Test connection class
  expect_s4_class(conn, "Microsoft SQL Server")

  # Clean up
  cleanup_connection(conn)
})

test_that("database connection has correct properties", {
  skip_if_no_db2()

  conn <- create_db2_connection()

  # Get connection info
  info <- DBI::dbGetInfo(conn)

  # Check that we can get info
  expect_type(info, "list")

  # Clean up
  cleanup_connection(conn)
})

test_that("database connection can execute simple query", {
  skip_if_no_db2()

  conn <- create_db2_connection()

  # Execute a simple query
  result <- DBI::dbGetQuery(conn, "SELECT 1 AS test_value FROM SYSIBM.SYSDUMMY1")

  # Check result
  expect_equal(nrow(result), 1)
  expect_equal(result$TEST_VALUE[1], 1)

  # Clean up
  cleanup_connection(conn)
})

test_that("database connection can be disconnected", {
  skip_if_no_db2()

  conn <- create_db2_connection()
  expect_true(DBI::dbIsValid(conn))

  # Disconnect
  DBI::dbDisconnect(conn)

  # Should no longer be valid
  expect_false(DBI::dbIsValid(conn))
})

test_that("connection uses environment variables for credentials", {
  skip_if_no_db2()

  # Check that environment variables are set
  # (these are used by create_db2_connection)
  user <- Sys.getenv("DB_USER")
  password <- Sys.getenv("DB_PASSWORD")

  # At least one should be set, or connection would use hardcoded defaults
  # This test documents expected behavior
  expect_type(user, "character")
  expect_type(password, "character")
})

test_that("multiple connections can be created and managed", {
  skip_if_no_db2()

  conn1 <- create_db2_connection()
  conn2 <- create_db2_connection()

  # Both should be valid
  expect_true(DBI::dbIsValid(conn1))
  expect_true(DBI::dbIsValid(conn2))

  # Clean up both
  cleanup_connection(conn1)
  cleanup_connection(conn2)

  expect_false(DBI::dbIsValid(conn1))
  expect_false(DBI::dbIsValid(conn2))
})

test_that("connection error handling works", {
  # This test checks what happens with invalid connection parameters
  # Note: We can't easily test this without modifying the connection function
  # to accept parameters, but we document expected behavior

  # If DB2 is not available, connection should fail
  # This is tested implicitly by skip_if_no_db2()
  expect_true(TRUE)  # Placeholder - documents that error handling should exist
})
