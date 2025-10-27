library(odbc)
library(DBI)
library(dplyr)
library(lubridate)
library(purrr)

# Source the database connection function
# NOTE: Run this script with working directory set to r-scripts root: setwd("~/scripts")
source("./utility_code/db2_connection.R")

# Connect using the standardized connection function
con <- create_db2_connection()

# ============================================
# PART 1: CREATE SCHEMA AND TABLES
# ============================================

# Create schema sail if not exists
tryCatch({
  dbExecute(con, "CREATE SCHEMA sail")
}, error = function(e) {
  message("Schema sail may already exist: ", e$message)
})

# Set current schema to sail
dbExecute(con, "SET SCHEMA sail")

# Drop tables if they exist (for clean slate)
tables_to_drop <- c(
  "GP_EVENT_REFORMATTED",
  "GP_EVENT_CODES",
  "GP_EVENT_CLEANSED",
  "PATIENT_ALF_CLEANSED",
  "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN",
  "WLGP_CLEANED_GP_REG_MEDIAN",
  "PEDEW_DIAG",
  "PEDEW_SPELL",
  "PEDEW_EPISODE",
  "PEDEW_OPER",
  "PEDEW_SUPERSPELL",
  "PEDEW_SINGLE_DIAG_TABLE",
  "PEDEW_SINGLE_OPER_TABLE",
  "PEDEW_ADMISSION_TABLE"
)

for (table in tables_to_drop) {
  tryCatch({
    dbExecute(con, paste0("DROP TABLE sail.", table))
    message(paste("Dropped table:", table))
  }, error = function(e) {
    message(paste("Could not drop table", table, ":", e$message))
  })
}

# Create tables with corrected schemas
dbExecute(con, "
CREATE TABLE sail.PATIENT_ALF_CLEANSED (
    ALF_PE BIGINT NOT NULL,
    WOB DATE NOT NULL,
    PRAC_CD_PE INTEGER NOT NULL,
    DELTA_KEY CHAR(1) NOT NULL,
    LOCAL_NUM_PE BIGINT NOT NULL,
    ALF_STS_CD CHAR(10) NOT NULL,
    ALF_MTCH_PCT DECIMAL(5,2) NOT NULL,
    LSOA_CD CHAR(15),
    BATCH_NUM SMALLINT NOT NULL,
    PROCESS_DT DATE NOT NULL,
    CREATE_DT DATE NOT NULL,
    AVAIL_FROM_DT DATE NOT NULL,
    OPT_OUT_FLG CHAR(1) NOT NULL,
    SOURCE_EXTRACT INTEGER NOT NULL,
    GNDR_CD CHAR(1) NOT NULL,
    REG_CAT_CD CHAR(10) NOT NULL,
    PRIMARY KEY (ALF_PE)
)")

dbExecute(con, "
CREATE TABLE sail.GP_EVENT_CODES (
    EVENT_CD_ID INT NOT NULL,
    EVENT_CD CHAR(10) NOT NULL,
    IS_READ_V2 SMALLINT NOT NULL,
    IS_READ_V3 SMALLINT NOT NULL,
    IS_VALID_CODE SMALLINT NOT NULL,
    DESCRIPTION VARCHAR(200) NOT NULL,
    EVENT_TYPE CHAR(20) NOT NULL,
    HIERARCHY_LEVEL_1 VARCHAR(50) NOT NULL,
    HIERARCHY_LEVEL_1_DESC VARCHAR(200) NOT NULL,
    HIERARCHY_LEVEL_2 VARCHAR(50) NOT NULL,
    HIERARCHY_LEVEL_2_DESC VARCHAR(200) NOT NULL,
    HIERARCHY_LEVEL_3 VARCHAR(50) NOT NULL,
    HIERARCHY_LEVEL_3_DESC VARCHAR(200) NOT NULL,
    PRIMARY KEY (EVENT_CD_ID)
)")

dbExecute(con, "
CREATE TABLE sail.GP_EVENT_REFORMATTED (
    EVENT_ID BIGINT NOT NULL,
    ALF_E BIGINT NOT NULL,
    ALF_STS_CD CHAR(10) NOT NULL,
    ALF_MTCH_PCT DECIMAL(5,2) NOT NULL, 
    PRAC_CD_E INT NOT NULL,
    EVENT_CD_ID INT NOT NULL,
    EVENT_VAL DECIMAL(10,2),
    EVENT_DT DATE NOT NULL,
    EVENT_YR SMALLINT NOT NULL,
    PRIMARY KEY (EVENT_ID)
)")

dbExecute(con, "
CREATE TABLE sail.GP_EVENT_CLEANSED (
    EVENT_ID BIGINT NOT NULL,
    PRAC_CD_PE INTEGER NOT NULL,
    LOCAL_NUM_PE BIGINT NOT NULL,
    EVENT_CD_VRS CHAR(10) NOT NULL,
    EVENT_CD CHAR(20) NOT NULL,
    EVENT_VAL DECIMAL(10,2),
    EVENT_DT DATE NOT NULL,
    EPISODE CHAR(20),
    SEQUENCE INTEGER NOT NULL,
    DELTA_KEY CHAR(1) NOT NULL,
    BATCH_NUM SMALLINT NOT NULL,
    EVENT_YR SMALLINT NOT NULL,
    CREATE_DT DATE NOT NULL,
    AVAIL_FROM_DT DATE NOT NULL,
    SOURCE_EXTRACT INTEGER NOT NULL,
    PRIMARY KEY (EVENT_ID)
)")

dbExecute(con, "
CREATE TABLE sail.WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN (
    REG_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    PRAC_CD_PE INTEGER NOT NULL,
    START_DATE DATE NOT NULL,
    END_DATE DATE,
    GP_DATA_FLAG INTEGER NOT NULL,
    AVAILABLE_FROM TIMESTAMP NOT NULL,
    PRIMARY KEY (REG_ID)
)")

dbExecute(con, "
CREATE TABLE sail.WLGP_CLEANED_GP_REG_MEDIAN (
    REG_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    START_DATE DATE NOT NULL,
    END_DATE DATE,
    GP_DATA_FLAG INTEGER NOT NULL,
    AVAILABLE_FROM TIMESTAMP NOT NULL,
    PRIMARY KEY (REG_ID)
)")

# PEDEW Tables (Patient Episode Database for Wales - Hospital Data)
dbExecute(con, "
CREATE TABLE sail.PEDEW_SPELL (
    SPELL_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    PROV_UNIT_CD CHAR(10) NOT NULL,
    SPELL_NUM_PE BIGINT NOT NULL,
    ADMIS_DT DATE NOT NULL,
    ADMIS_TIME TIME,
    DISCH_DT DATE,
    DISCH_TIME TIME,
    ADMIS_MTHD_CD CHAR(2),
    ADMIS_SOURCE_CD CHAR(2),
    ADMIS_TYPE CHAR(2),
    SPELL_LOS INTEGER,
    DISCH_MTHD_CD CHAR(2),
    DISCH_DEST_CD CHAR(2),
    PRIMARY KEY (SPELL_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_EPISODE (
    EPISODE_ID BIGINT NOT NULL,
    SPELL_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    EPI_NUM INTEGER NOT NULL,
    EPI_STR_DT DATE NOT NULL,
    EPI_STR_TIME TIME,
    EPI_END_DT DATE,
    EPI_END_TIME TIME,
    CONSULT_CD CHAR(10),
    SPEC_CD CHAR(3),
    DEPT_CD CHAR(3),
    SITE_CD CHAR(5),
    PRIMARY KEY (EPISODE_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_SUPERSPELL (
    SUPERSPELL_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    SPELL_ID BIGINT NOT NULL,
    SUPERSPELL_NUM BIGINT NOT NULL,
    SUPERSPELL_STR_DT DATE NOT NULL,
    SUPERSPELL_END_DT DATE,
    PRIMARY KEY (SUPERSPELL_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_DIAG (
    DIAG_ID BIGINT NOT NULL,
    SPELL_ID BIGINT NOT NULL,
    EPISODE_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    DIAG_CD CHAR(10) NOT NULL,
    DIAG_NUM INTEGER NOT NULL,
    PRIMARY_FLAG CHAR(1),
    PRIMARY KEY (DIAG_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_OPER (
    OPER_ID BIGINT NOT NULL,
    SPELL_ID BIGINT NOT NULL,
    EPISODE_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    OPER_CD CHAR(10) NOT NULL,
    OPER_NUM INTEGER NOT NULL,
    OPER_DT DATE,
    PRIMARY_FLAG CHAR(1),
    PRIMARY KEY (OPER_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_SINGLE_DIAG_TABLE (
    SDT_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    DIAG_CD CHAR(10) NOT NULL,
    DIAG_DT DATE NOT NULL,
    DIAG_SOURCE CHAR(10),
    PRIMARY KEY (SDT_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_SINGLE_OPER_TABLE (
    SOT_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    OPER_CD CHAR(10) NOT NULL,
    OPER_DT DATE NOT NULL,
    OPER_SOURCE CHAR(10),
    PRIMARY KEY (SOT_ID)
)")

dbExecute(con, "
CREATE TABLE sail.PEDEW_ADMISSION_TABLE (
    ADMISSION_ID BIGINT NOT NULL,
    ALF_PE BIGINT NOT NULL,
    SPELL_ID BIGINT NOT NULL,
    ADMIS_DT DATE NOT NULL,
    ADMIS_TYPE CHAR(10),
    ADMIS_SOURCE CHAR(10),
    ADMIS_REASON VARCHAR(200),
    PRIMARY KEY (ADMISSION_ID)
)")

message("All tables created successfully")

# ============================================
# PART 2: GENERATE LINKABLE SYNTHETIC DATA
# ============================================

set.seed(42)
N_PATIENTS <- 10000
BATCH_SIZE <- 1000
today <- Sys.Date()

# Helper functions
random_dates <- function(n, start_date = as.Date("1940-01-01"), end_date = Sys.Date()) {
  start_num <- as.numeric(start_date)
  end_num <- as.numeric(end_date)
  as.Date(sample(start_num:end_num, n, replace = TRUE), origin = "1970-01-01")
}

batch_insert <- function(con, table_name, df, batch_size = BATCH_SIZE) {
  if (nrow(df) == 0) return()
  
  cols <- colnames(df)
  n <- nrow(df)
  
  for (i in seq(1, n, by = batch_size)) {
    batch <- df[i:min(i + batch_size - 1, n),]
    
    # Convert each row to SQL values
    values_list <- list()
    for (j in 1:nrow(batch)) {
      row_values <- character(length(cols))
      for (k in 1:length(cols)) {
        val <- batch[[cols[k]]][j]
        if (is.na(val)) {
          row_values[k] <- "NULL"
        } else if (is.character(val)) {
          row_values[k] <- dbQuoteString(con, val)
        } else if (is.Date(val)) {
          row_values[k] <- dbQuoteString(con, as.character(val))
        } else if (inherits(val, "POSIXct") || inherits(val, "POSIXt")) {
          row_values[k] <- dbQuoteString(con, format(val, "%Y-%m-%d %H:%M:%S"))
        } else {
          row_values[k] <- as.character(val)
        }
      }
      values_list[[j]] <- paste0("(", paste(row_values, collapse = ","), ")")
    }
    
    values <- paste(values_list, collapse = ",\n")
    sql <- paste0("INSERT INTO sail.", table_name, " (", paste(cols, collapse = ","), ") VALUES\n", values)
    
    tryCatch({
      dbExecute(con, sql)
      message(paste("Inserted", min(batch_size, n - i + 1), "rows into", table_name))
    }, error = function(e) {
      message(paste("Error inserting into", table_name, ":", e$message))
    })
  }
}

# 1. PATIENT_ALF_CLEANSED - Core patient demographics
message("Generating patient data...")
patients <- tibble(
  ALF_PE = 1:N_PATIENTS,
  WOB = random_dates(N_PATIENTS, as.Date("1930-01-01"), as.Date("2010-01-01")),
  PRAC_CD_PE = sample(100:999, N_PATIENTS, replace = TRUE),
  DELTA_KEY = 'N',
  LOCAL_NUM_PE = 100000:(100000 + N_PATIENTS - 1),
  ALF_STS_CD = sample(c("ACTIVE", "INACTIVE"), N_PATIENTS, replace = TRUE, prob = c(0.9, 0.1)),
  ALF_MTCH_PCT = round(runif(N_PATIENTS, 95, 100), 2),
  LSOA_CD = paste0("W0100", sprintf("%04d", sample(1000:9999, N_PATIENTS, replace = TRUE))),
  BATCH_NUM = 1,
  PROCESS_DT = today,
  CREATE_DT = today - days(sample(1:365, N_PATIENTS, replace = TRUE)),
  AVAIL_FROM_DT = today - days(sample(1:30, N_PATIENTS, replace = TRUE)),
  OPT_OUT_FLG = sample(c('Y','N'), N_PATIENTS, replace = TRUE, prob = c(0.02, 0.98)),
  SOURCE_EXTRACT = 1,
  GNDR_CD = sample(c('M','F'), N_PATIENTS, replace = TRUE),
  REG_CAT_CD = sample(c("REG_A", "REG_B", "REG_C"), N_PATIENTS, replace = TRUE)
)

batch_insert(con, "PATIENT_ALF_CLEANSED", patients)

# 2. GP_EVENT_CODES - Event code lookup table
message("Generating GP event codes...")

# Realistic GP Read codes and descriptions
read_codes <- tibble(
  EVENT_CD_ID = 1:500,
  EVENT_CD = sprintf("%-10s", paste0(sample(LETTERS, 500, replace = TRUE), 
                                     sample(0:9, 500, replace = TRUE),
                                     sample(0:9, 500, replace = TRUE),
                                     sample(c(".", "00", "z", ""), 500, replace = TRUE))),
  IS_READ_V2 = sample(0:1, 500, replace = TRUE),
  IS_READ_V3 = sample(0:1, 500, replace = TRUE),
  IS_VALID_CODE = sample(c(0,1), 500, replace = TRUE, prob = c(0.05, 0.95)),
  DESCRIPTION = sample(c(
    "Asthma", "Diabetes mellitus", "Hypertension", "Depression",
    "Anxiety", "Back pain", "Chest pain", "Cough", "Fever",
    "Blood pressure reading", "Blood test", "Urine test",
    "Vaccination given", "Medication review", "Annual health check",
    "Smoking cessation advice", "Weight management", "ECG performed",
    "Referral to specialist", "Follow-up appointment"
  ), 500, replace = TRUE),
  EVENT_TYPE = sample(c("DIAGNOSIS", "PROCEDURE", "ADMIN", "OBSERVATION"), 500, replace = TRUE),
  HIERARCHY_LEVEL_1 = sample(c("Clinical", "Administrative", "Preventive"), 500, replace = TRUE),
  HIERARCHY_LEVEL_1_DESC = sample(c("Clinical findings", "Administrative procedures", "Preventive care"), 500, replace = TRUE),
  HIERARCHY_LEVEL_2 = paste0("L2_", sprintf("%03d", 1:500)),
  HIERARCHY_LEVEL_2_DESC = sample(c("Cardiovascular", "Respiratory", "Mental health", "Endocrine", "Musculoskeletal"), 500, replace = TRUE),
  HIERARCHY_LEVEL_3 = paste0("L3_", sprintf("%03d", 1:500)),
  HIERARCHY_LEVEL_3_DESC = sample(c("Primary diagnosis", "Secondary diagnosis", "Procedure", "Investigation"), 500, replace = TRUE)
)

batch_insert(con, "GP_EVENT_CODES", read_codes)

# 3. GP Registrations
message("Generating GP registration data...")
reg_data <- patients %>%
  select(ALF_PE, PRAC_CD_PE) %>%
  mutate(
    REG_ID = row_number(),
    START_DATE = random_dates(n(), as.Date("2015-01-01"), as.Date("2023-01-01")),
    END_DATE = as.Date(NA),
    GP_DATA_FLAG = sample(0:1, n(), replace = TRUE, prob = c(0.1, 0.9)),
    AVAILABLE_FROM = as.POSIXct(Sys.time() - runif(n(), 0, 1e7))
  )

# Some patients have ended registrations
ended_regs <- sample(1:nrow(reg_data), size = floor(nrow(reg_data) * 0.2))
reg_data$END_DATE[ended_regs] <- reg_data$START_DATE[ended_regs] + days(sample(30:730, length(ended_regs), replace = TRUE))

batch_insert(con, "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN", reg_data)
batch_insert(con, "WLGP_CLEANED_GP_REG_MEDIAN", reg_data %>% select(-PRAC_CD_PE))

# 4. GP Events (linked to patients and event codes)
message("Generating GP events...")
n_events <- N_PATIENTS * 5 # average 5 events per patient

# Get active patients only
active_patients <- patients %>% filter(ALF_STS_CD == "ACTIVE")

gp_events_ref <- tibble(
  EVENT_ID = 1:n_events,
  ALF_E = sample(active_patients$ALF_PE, n_events, replace = TRUE),
  ALF_STS_CD = "ACTIVE",
  ALF_MTCH_PCT = round(runif(n_events, 95, 100), 2),
  PRAC_CD_E = sample(100:999, n_events, replace = TRUE),
  EVENT_CD_ID = sample(read_codes$EVENT_CD_ID, n_events, replace = TRUE),
  EVENT_VAL = round(runif(n_events, 0, 200), 2),
  EVENT_DT = random_dates(n_events, Sys.Date() - years(2), Sys.Date()),
  EVENT_YR = year(random_dates(n_events, as.Date("2023-01-01"), as.Date("2024-12-31")))
)

batch_insert(con, "GP_EVENT_REFORMATTED", gp_events_ref)

# Cleansed events (linked via LOCAL_NUM_PE)
gp_events_cleansed <- gp_events_ref %>%
  left_join(patients %>% select(ALF_PE, LOCAL_NUM_PE), by = c("ALF_E" = "ALF_PE")) %>%
  mutate(
    EVENT_ID = EVENT_ID,
    PRAC_CD_PE = PRAC_CD_E,
    EVENT_CD_VRS = sample(c("V2", "V3"), n(), replace = TRUE),
    EVENT_CD = sample(read_codes$EVENT_CD, n(), replace = TRUE),
    EPISODE = paste0("EP", sprintf("%06d", sample(1:999999, n(), replace = TRUE))),
    SEQUENCE = row_number(),
    DELTA_KEY = "N",
    BATCH_NUM = 1,
    CREATE_DT = today,
    AVAIL_FROM_DT = today - days(sample(1:30, n(), replace = TRUE)),
    SOURCE_EXTRACT = 1
  ) %>%
  select(EVENT_ID, PRAC_CD_PE, LOCAL_NUM_PE, EVENT_CD_VRS, EVENT_CD, EVENT_VAL, 
         EVENT_DT, EPISODE, SEQUENCE, DELTA_KEY, BATCH_NUM, EVENT_YR, 
         CREATE_DT, AVAIL_FROM_DT, SOURCE_EXTRACT)

batch_insert(con, "GP_EVENT_CLEANSED", gp_events_cleansed)

# 5. Hospital Data (PEDEW Tables)
message("Generating hospital admission data...")

# About 30% of patients have hospital admissions
hospital_patients <- sample(patients$ALF_PE, size = floor(N_PATIENTS * 0.3))
n_spells <- length(hospital_patients) * 2 # Average 2 admissions per patient

# PEDEW_SPELL - Hospital admissions
spells <- tibble(
  SPELL_ID = 1:n_spells,
  ALF_PE = sample(hospital_patients, n_spells, replace = TRUE),
  PROV_UNIT_CD = sample(c("7A1", "7A2", "7A3", "7A4", "7A5"), n_spells, replace = TRUE),
  SPELL_NUM_PE = 1:n_spells,
  ADMIS_DT = random_dates(n_spells, as.Date("2022-01-01"), as.Date("2024-12-31")),
  ADMIS_TIME = format(as.POSIXct(runif(n_spells, 0, 86400), origin = "1970-01-01", tz = "UTC"), "%H:%M:%S"),
  DISCH_DT = as.Date(NA),
  DISCH_TIME = as.character(NA),
  ADMIS_MTHD_CD = sample(c("11", "12", "13", "21", "22", "23", "24", "25", "28", "2A", "2B", "2C", "2D", "31", "32", "81", "82", "83"), n_spells, replace = TRUE),
  ADMIS_SOURCE_CD = sample(c("19", "29", "39", "40", "49", "51", "52", "53", "54", "55", "56", "65", "66", "79", "85", "87", "88", "89"), n_spells, replace = TRUE),
  ADMIS_TYPE = sample(c("1", "2", "3", "4"), n_spells, replace = TRUE, prob = c(0.4, 0.3, 0.2, 0.1)),
  SPELL_LOS = as.integer(NA),
  DISCH_MTHD_CD = as.character(NA),
  DISCH_DEST_CD = as.character(NA)
)

# Add discharge dates and calculate length of stay
spells <- spells %>%
  mutate(
    DISCH_DT = ADMIS_DT + days(sample(0:30, n(), replace = TRUE, prob = c(rep(0.5/10, 10), rep(0.5/21, 21)))),
    DISCH_TIME = format(as.POSIXct(runif(n(), 0, 86400), origin = "1970-01-01", tz = "UTC"), "%H:%M:%S"),
    SPELL_LOS = as.integer(DISCH_DT - ADMIS_DT),
    DISCH_MTHD_CD = sample(c("1", "2", "3", "4", "5", "8", "9"), n(), replace = TRUE),
    DISCH_DEST_CD = sample(c("19", "29", "30", "48", "49", "50", "51", "52", "53", "54", "65", "66", "79", "84", "85", "87", "88", "89"), n(), replace = TRUE)
  )

batch_insert(con, "PEDEW_SPELL", spells)

# PEDEW_EPISODE - Episodes within spells
episodes_per_spell <- sample(1:3, nrow(spells), replace = TRUE, prob = c(0.7, 0.2, 0.1))
total_episodes <- sum(episodes_per_spell)

episodes <- data.frame()
episode_id <- 1
for (i in 1:nrow(spells)) {
  n_ep <- episodes_per_spell[i]
  for (j in 1:n_ep) {
    ep <- data.frame(
      EPISODE_ID = episode_id,
      SPELL_ID = spells$SPELL_ID[i],
      ALF_PE = spells$ALF_PE[i],
      EPI_NUM = j,
      EPI_STR_DT = spells$ADMIS_DT[i] + days(j - 1),
      EPI_STR_TIME = format(as.POSIXct(runif(1, 0, 86400), origin = "1970-01-01", tz = "UTC"), "%H:%M:%S"),
      EPI_END_DT = spells$ADMIS_DT[i] + days(j),
      EPI_END_TIME = format(as.POSIXct(runif(1, 0, 86400), origin = "1970-01-01", tz = "UTC"), "%H:%M:%S"),
      CONSULT_CD = sample(c("C001", "C002", "C003", "C004", "C005"), 1),
      SPEC_CD = sample(c("100", "101", "110", "120", "130", "140", "150", "160", "170", "180"), 1),
      DEPT_CD = sample(c("A01", "A02", "A03", "B01", "B02"), 1),
      SITE_CD = sample(c("RTE01", "RTE02", "RTE03"), 1)
    )
    episodes <- rbind(episodes, ep)
    episode_id <- episode_id + 1
  }
}

batch_insert(con, "PEDEW_EPISODE", episodes)

# PEDEW_SUPERSPELL - Groups of related spells
superspells <- spells %>%
  group_by(ALF_PE) %>%
  summarise(
    SPELL_ID = first(SPELL_ID),
    SUPERSPELL_STR_DT = min(ADMIS_DT),
    SUPERSPELL_END_DT = max(DISCH_DT)
  ) %>%
  mutate(
    SUPERSPELL_ID = row_number(),
    SUPERSPELL_NUM = row_number()
  ) %>%
  select(SUPERSPELL_ID, ALF_PE, SPELL_ID, SUPERSPELL_NUM, SUPERSPELL_STR_DT, SUPERSPELL_END_DT)

batch_insert(con, "PEDEW_SUPERSPELL", superspells)

# PEDEW_DIAG - Diagnoses (ICD-10 codes)
message("Generating diagnosis data...")
icd10_codes <- c("I10", "E11", "J44", "F32", "M79.3", "N39.0", "K21.0", "J06.9", "R50.9", "Z00.0")
diagnoses <- data.frame()
diag_id <- 1

for (i in 1:nrow(episodes)) {
  n_diag <- sample(1:3, 1, prob = c(0.5, 0.3, 0.2))
  for (j in 1:n_diag) {
    diag <- data.frame(
      DIAG_ID = diag_id,
      SPELL_ID = episodes$SPELL_ID[i],
      EPISODE_ID = episodes$EPISODE_ID[i],
      ALF_PE = episodes$ALF_PE[i],
      DIAG_CD = sample(icd10_codes, 1),
      DIAG_NUM = j,
      PRIMARY_FLAG = ifelse(j == 1, "Y", "N")
    )
    diagnoses <- rbind(diagnoses, diag)
    diag_id <- diag_id + 1
  }
}

batch_insert(con, "PEDEW_DIAG", diagnoses)

# PEDEW_OPER - Operations (OPCS-4 codes)
message("Generating operation data...")
opcs4_codes <- c("K40.2", "K50.2", "W40.1", "T20.2", "G45.0", "M45.3", "L85.0", "J18.3", "H04.3", "Y53.4")
operations <- data.frame()
oper_id <- 1

# About 20% of episodes have operations
episodes_with_ops <- sample(1:nrow(episodes), size = floor(nrow(episodes) * 0.2))

for (i in episodes_with_ops) {
  n_ops <- sample(1:2, 1, prob = c(0.8, 0.2))
  for (j in 1:n_ops) {
    op <- data.frame(
      OPER_ID = oper_id,
      SPELL_ID = episodes$SPELL_ID[i],
      EPISODE_ID = episodes$EPISODE_ID[i],
      ALF_PE = episodes$ALF_PE[i],
      OPER_CD = sample(opcs4_codes, 1),
      OPER_NUM = j,
      OPER_DT = episodes$EPI_STR_DT[i] + days(sample(0:2, 1)),
      PRIMARY_FLAG = ifelse(j == 1, "Y", "N")
    )
    operations <- rbind(operations, op)
    oper_id <- oper_id + 1
  }
}

batch_insert(con, "PEDEW_OPER", operations)

# PEDEW_SINGLE_DIAG_TABLE - Simplified diagnosis view
single_diag <- diagnoses %>%
  filter(PRIMARY_FLAG == "Y") %>%
  left_join(episodes %>% select(EPISODE_ID, EPI_STR_DT), by = "EPISODE_ID") %>%
  mutate(
    SDT_ID = row_number(),
    DIAG_DT = EPI_STR_DT,
    DIAG_SOURCE = "PEDEW"
  ) %>%
  select(SDT_ID, ALF_PE, DIAG_CD, DIAG_DT, DIAG_SOURCE)

batch_insert(con, "PEDEW_SINGLE_DIAG_TABLE", single_diag)

# PEDEW_SINGLE_OPER_TABLE - Simplified operation view
if (nrow(operations) > 0) {
  single_oper <- operations %>%
    filter(PRIMARY_FLAG == "Y") %>%
    mutate(
      SOT_ID = row_number(),
      OPER_SOURCE = "PEDEW"
    ) %>%
    select(SOT_ID, ALF_PE, OPER_CD, OPER_DT, OPER_SOURCE)
  
  batch_insert(con, "PEDEW_SINGLE_OPER_TABLE", single_oper)
}

# PEDEW_ADMISSION_TABLE - Admission summary
admissions <- spells %>%
  mutate(
    ADMISSION_ID = row_number(),
    ADMIS_TYPE = case_when(
      ADMIS_TYPE == "1" ~ "Elective",
      ADMIS_TYPE == "2" ~ "Emergency",
      ADMIS_TYPE == "3" ~ "Transfer",
      TRUE ~ "Other"
    ),
    ADMIS_SOURCE = case_when(
      ADMIS_SOURCE_CD %in% c("19", "29", "39") ~ "Home",
      ADMIS_SOURCE_CD %in% c("51", "52", "53") ~ "NHS Hospital",
      ADMIS_SOURCE_CD %in% c("65", "66") ~ "Care Home",
      TRUE ~ "Other"
    ),
    ADMIS_REASON = sample(c(
      "Chest pain", "Shortness of breath", "Abdominal pain",
      "Scheduled surgery", "Fall", "Infection", "Mental health crisis",
      "Stroke symptoms", "Heart problems", "Routine procedure"
    ), n(), replace = TRUE)
  ) %>%
  select(ADMISSION_ID, ALF_PE, SPELL_ID, ADMIS_DT, ADMIS_TYPE, ADMIS_SOURCE, ADMIS_REASON)

batch_insert(con, "PEDEW_ADMISSION_TABLE", admissions)

# ============================================
# PART 3: VERIFY DATA LINKAGE
# ============================================

message("\n=== Data Generation Complete ===")
message("\nVerifying data linkage...")

# Check record counts
tables <- c(
  "PATIENT_ALF_CLEANSED",
  "GP_EVENT_CODES",
  "GP_EVENT_REFORMATTED",
  "GP_EVENT_CLEANSED",
  "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN",
  "WLGP_CLEANED_GP_REG_MEDIAN",
  "PEDEW_SPELL",
  "PEDEW_EPISODE",
  "PEDEW_SUPERSPELL",
  "PEDEW_DIAG",
  "PEDEW_OPER",
  "PEDEW_SINGLE_DIAG_TABLE",
  "PEDEW_SINGLE_OPER_TABLE",
  "PEDEW_ADMISSION_TABLE"
)

for (table in tables) {
  count <- dbGetQuery(con, paste0("SELECT COUNT(*) as n FROM sail.", table))$n
  message(paste(table, ":", count, "records"))
}

# Verify some key relationships
message("\nVerifying key relationships...")

# Check that all GP events link to valid patients
orphan_events <- dbGetQuery(con, "
  SELECT COUNT(*) as n 
  FROM sail.GP_EVENT_REFORMATTED e 
  LEFT JOIN sail.PATIENT_ALF_CLEANSED p ON e.ALF_E = p.ALF_PE 
  WHERE p.ALF_PE IS NULL
")$n
message(paste("Orphan GP events (no matching patient):", orphan_events))

# Check that all hospital spells link to valid patients
orphan_spells <- dbGetQuery(con, "
  SELECT COUNT(*) as n 
  FROM sail.PEDEW_SPELL s 
  LEFT JOIN sail.PATIENT_ALF_CLEANSED p ON s.ALF_PE = p.ALF_PE 
  WHERE p.ALF_PE IS NULL
")$n
message(paste("Orphan hospital spells (no matching patient):", orphan_spells))

# Check that all episodes link to valid spells
orphan_episodes <- dbGetQuery(con, "
  SELECT COUNT(*) as n 
  FROM sail.PEDEW_EPISODE e 
  LEFT JOIN sail.PEDEW_SPELL s ON e.SPELL_ID = s.SPELL_ID 
  WHERE s.SPELL_ID IS NULL
")$n
message(paste("Orphan episodes (no matching spell):", orphan_episodes))

message("\n=== Data generation and linkage complete! ===")

# Disconnect
dbDisconnect(con)
message("Database connection closed.")