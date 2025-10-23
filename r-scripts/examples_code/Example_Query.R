library(DBI)
library(odbc)
library(dplyr)
library(dbplyr)

# Source the database connection function
source("../utility_code/db2_connection.R")

# Connect using the standardized connection function
# You can set DB_USER and DB_PASSWORD environment variables or use defaults
con <- create_db2_connection()

dbExecute(con, "SET SCHEMA sail")
# Reference remote tables with dbplyr
gp_event_reformatted <- tbl(con, "GP_EVENT_REFORMATTED")
gp_event_codes <- tbl(con, "GP_EVENT_CODES")

# Join the tables on EVENT_CD_ID (example of linking)
joined_data <- gp_event_reformatted %>%
  inner_join(gp_event_codes, by = "EVENT_CD_ID")

# Inspect generated SQL query (optional)
show_query(joined_data)

# To actually execute and collect results into R dataframe:
joined_data %>% collect() %>% head()

# Cleanup
# dbDisconnect(con)
