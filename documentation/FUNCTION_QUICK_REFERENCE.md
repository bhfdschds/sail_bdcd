# Function Quick Reference Guide

## Visual Before/After Examples for Each Function

---

## 1. create_long_format_asset()

**What it does:** Extracts data from DB2 and creates standardized long format tables

### Input: DB2 Database Table
```
TABLE: PATIENT_ALF_CLEANSED
ALF_PE  | WOB        | GNDR_CD | CREATE_DT
--------|------------|---------|----------
1001    | 1950-05-15 | M       | 2020-01-01
1002    | 1965-08-22 | F       | 2020-01-01
1003    | 1975-12-10 | M       | 2020-01-01
```

### Output: Long Format Asset
```
date_of_birth_long_format.rds
patient_id | source_table | source_priority | date_of_birth | record_date
-----------|--------------|-----------------|---------------|------------
1001       | gp_dob       | 1               | 1950-05-15    | 2020-01-01
1002       | gp_dob       | 1               | 1965-08-22    | 2020-01-01
1003       | gp_dob       | 1               | 1975-12-10    | 2020-01-01
```

**Key Changes:**
- ✅ Standardized column names (ALF_PE → patient_id, WOB → date_of_birth)
- ✅ Added source metadata (source_table, source_priority)
- ✅ Multiple sources combined into one table

---

## 2. get_highest_priority_per_patient()

**What it does:** Resolves conflicts when multiple sources provide data for same patient

### Input: Multiple Rows Per Patient
```
patient_id | source_table    | source_priority | ethnicity_code
-----------|-----------------|-----------------|---------------
1001       | self_reported   | 1               | W
1001       | hospital_admin  | 2               | W
1001       | gp_record       | 3               | A
1002       | hospital_admin  | 2               | A
1002       | gp_record       | 3               | A
```

### Output: One Row Per Patient
```
patient_id | source_table    | source_priority | ethnicity_code
-----------|-----------------|-----------------|---------------
1001       | self_reported   | 1               | W          ← Priority 1 wins
1002       | hospital_admin  | 2               | A          ← Highest available
```

**Key Changes:**
- ✅ Reduced to unique patients only
- ✅ Kept highest priority source (lowest number)
- ✅ Conflicts resolved automatically

---

## 3. combine_demographics()

**What it does:** Merges separate demographic assets into one table

### Input: Separate Assets
```
DOB Asset                    Sex Asset                    LSOA Asset
patient_id | date_of_birth  patient_id | sex_code        patient_id | lsoa_code
-----------|-------------   -----------|---------        -----------|-----------
1001       | 1950-05-15     1001       | M               1001       | W01000001
1002       | 1965-08-22     1002       | F               1002       | W01000002
1003       | 1975-12-10     1003       | M               1003       | W01000003
```

### Output: Combined Demographics
```
patient_id | date_of_birth | sex_code | ethnicity_code | lsoa_code
-----------|---------------|----------|----------------|----------
1001       | 1950-05-15    | M        | W              | W01000001
1002       | 1965-08-22    | F        | A              | W01000002
1003       | 1975-12-10    | M        | W              | W01000003
```

**Key Changes:**
- ✅ All demographics in one table
- ✅ One row per patient maintained
- ✅ Left joins preserve all patients from DOB table

---

## 4. generate_cohort()

**What it does:** Applies restrictions to create study cohort

### Input: All Demographics
```
patient_id | date_of_birth | sex_code | ethnicity_code | lsoa_code
-----------|---------------|----------|----------------|----------
1001       | 1950-05-15    | M        | W              | W01000001
1002       | 1965-08-22    | F        | A              | W01000002
1003       | 2010-12-10    | M        | W              | W01000003  ← Age 13 (too young)
1004       | 1975-03-18    | NA       | W              | W01000004  ← Missing sex
1005       | 1980-07-22    | M        | NA             | NA         ← Missing LSOA
```

### Parameters
```r
index_date = "2024-01-01"
min_age = 18
max_age = 100
require_known_sex = TRUE
require_lsoa = TRUE
```

### Output: Study Cohort
```
patient_id | index_date | age_at_index | sex_code | ethnicity_code | lsoa_code
-----------|------------|--------------|----------|----------------|----------
1001       | 2024-01-01 | 73.6         | M        | W              | W01000001  ✓
1002       | 2024-01-01 | 58.4         | F        | A              | W01000002  ✓
```

**Console Output:**
```
Excluded 1 patients with age < 18
Excluded 1 patients with unknown sex
Excluded 1 patients with missing LSOA

Final cohort: 2 patients (3 excluded)
```

**Key Changes:**
- ✅ Added index_date column
- ✅ Added age_at_index column
- ✅ Removed patients not meeting criteria
- ❌ Patient 1003: too young
- ❌ Patient 1004: missing sex
- ❌ Patient 1005: missing LSOA

---

## 5. generate_covariates()

**What it does:** Identifies covariates within temporal window before index date

### Input: Disease Data
```
HOSPITAL_ADMISSIONS_LONG_FORMAT.RDS
patient_id | event_date  | code | terminology
-----------|-------------|------|------------
1001       | 2020-05-10  | E11  | ICD10       ← Diabetes (before index)
1001       | 2023-11-20  | E11  | ICD10       ← Diabetes (before index)
1001       | 2024-03-15  | I21  | ICD10       (after index - ignored)
1002       | 2023-08-15  | I10  | ICD10       (not diabetes)
1003       | 2021-06-30  | E11  | ICD10       ← Diabetes (before index)
```

### Lookup Table
```
code | name      | description
-----|-----------|------------------
E10  | diabetes  | Type 1 diabetes
E11  | diabetes  | Type 2 diabetes
I10  | hyperten  | Essential HTN
I21  | heart_atk | Acute MI
```

### Cohort
```
patient_id | index_date
-----------|------------
1001       | 2024-01-01
1002       | 2024-01-01
1003       | 2024-01-01
```

### Parameters
```r
covariate_name = "diabetes"
days_before_start = NULL  # Any time before
days_before_end = 0       # Up to day before index
selection_method = "min"  # First occurrence
```

### Output: Covariate Data
```
patient_id | index_date | covariate_flag | covariate_date | days_to_index
-----------|------------|----------------|----------------|---------------
1001       | 2024-01-01 | TRUE           | 2020-05-10     | -1332
1002       | 2024-01-01 | FALSE          | NA             | NA
1003       | 2024-01-01 | TRUE           | 2021-06-30     | -916
```

**Explanation:**
- Patient 1001: Had diabetes codes on 2020-05-10 and 2023-11-20
  - Selected: 2020-05-10 (earliest, method="min")
  - Days to index: -1332 days (before index)

- Patient 1002: No diabetes codes found
  - Flag: FALSE

- Patient 1003: Had diabetes code on 2021-06-30
  - Flag: TRUE
  - Days to index: -916 days

**Key Changes:**
- ✅ All cohort patients preserved
- ✅ Added binary flag (TRUE/FALSE)
- ✅ Added covariate date (when it occurred)
- ✅ Added days_to_index (negative = before)
- ✅ Multiple events → single row per patient

---

## 6. generate_outcomes()

**What it does:** Identifies outcomes within temporal window after index date

### Input: Disease Data
```
HOSPITAL_ADMISSIONS_LONG_FORMAT.RDS
patient_id | event_date  | code | terminology
-----------|-------------|------|------------
1001       | 2023-11-10  | I21  | ICD10       (before index - ignored)
1001       | 2024-03-15  | I21  | ICD10       ← Heart attack (after index)
1001       | 2024-08-20  | I21  | ICD10       (after index but method=min)
1002       | 2024-06-22  | I63  | ICD10       (stroke, not heart attack)
1003       | 2023-05-10  | I21  | ICD10       (before index - ignored)
```

### Cohort
```
patient_id | index_date
-----------|------------
1001       | 2024-01-01
1002       | 2024-01-01
1003       | 2024-01-01
```

### Parameters
```r
outcome_name = "heart_attack"
days_after_start = 0      # From index date
days_after_end = 365      # Within 1 year
selection_method = "min"  # First occurrence
```

### Output: Outcome Data
```
patient_id | index_date | outcome_flag | outcome_date | days_from_index
-----------|------------|--------------|--------------|----------------
1001       | 2024-01-01 | TRUE         | 2024-03-15   | 74
1002       | 2024-01-01 | FALSE        | NA           | NA
1003       | 2024-01-01 | FALSE        | NA           | NA
```

**Explanation:**
- Patient 1001: Heart attack on 2024-03-15 (74 days after index) ✓
  - Event on 2023-11-10 ignored (before index)
  - Event on 2024-08-20 ignored (method="min" selects first)

- Patient 1002: No heart attack codes (I63 is stroke)
  - Flag: FALSE

- Patient 1003: Heart attack was before index date
  - Flag: FALSE

**Key Changes:**
- ✅ All cohort patients preserved
- ✅ Added binary flag (TRUE/FALSE)
- ✅ Added outcome date (when it occurred)
- ✅ Added days_from_index (positive = after)
- ✅ Only events AFTER index included

---

## 7. Complete Pipeline (run_complete_pipeline)

**What it does:** Orchestrates entire workflow from database to final dataset

### Final Output Structure
```
FINAL_ANALYSIS_DATASET.RDS

patient_id: 1001
  index_date: 2024-01-01
  age_at_index: 73.6
  sex_code: M
  ethnicity_code: W
  lsoa_code: W01000001

  COVARIATES (before index):
    diabetes_flag: TRUE
    diabetes_date: 2020-05-10
    diabetes_days_to_index: -1332

    hypertension_flag: TRUE
    hypertension_date: 2019-03-22
    hypertension_days_to_index: -1746

    copd_flag: FALSE
    copd_date: NA
    copd_days_to_index: NA

    chd_flag: TRUE
    chd_date: 2018-11-15
    chd_days_to_index: -1873

  OUTCOMES (after index):
    heart_attack_flag: TRUE
    heart_attack_date: 2024-03-15
    heart_attack_days_from_index: 74

    stroke_flag: FALSE
    stroke_date: NA
    stroke_days_from_index: NA

    heart_failure_flag: FALSE
    heart_failure_date: NA
    heart_failure_days_from_index: NA

    death_flag: FALSE
    death_date: NA
    death_days_from_index: NA
```

---

## Temporal Window Visual Guide

### Covariates (Lookback)
```
                    days_before_start=365
                           ↓
                           |<------ 365 days ------>|
                           |                        |  Index
Past ←---------------------|------------------------|-------→ Future
                           |                        |
                      Included Events          days_before_end=0

Example Events:
  2022-12-01: Event A (too far back) ✗
  2023-03-15: Event B (within 365 days) ✓
  2023-11-20: Event C (within 365 days) ✓
  2024-01-01: INDEX DATE
  2024-02-10: Event D (after index) ✗
```

### Outcomes (Follow-up)
```
                        days_after_start=0
                               ↓
        Index                  |<------ 365 days ------>|
Past ←---------|--------------[|------------------------|]-------→ Future
               |               |                        |
                          Included Events        days_after_end=365

Example Events:
  2023-11-10: Event A (before index) ✗
  2024-01-01: INDEX DATE
  2024-01-15: Event B (within 365 days) ✓
  2024-08-20: Event C (within 365 days) ✓
  2025-02-01: Event D (beyond 365 days) ✗
```

---

## Data Size Changes Through Pipeline

```
Stage                    Patients    Total Rows    Columns
─────────────────────────────────────────────────────────────
Database Extract         10,000      3,500,000+    varies
Long Format Assets       10,000      3,515,000     8-12
Combined Demographics    10,000      10,000        5
Study Cohort            9,598       9,598         7
With Covariates         9,598       9,598         19
With Outcomes           9,598       9,598         28
Final Dataset           9,598       9,598         35+
```

**Key Insight:** Pipeline converts millions of event records into a single analysis-ready table with one row per patient.

---

## Common Parameter Combinations

### 1. Baseline Comorbidities
```r
# Any diagnosis before study entry
generate_covariates(
  covariate_name = "diabetes",
  days_before_start = NULL,   # Any time before
  days_before_end = 0,        # Up to day before index
  selection_method = "min"    # First diagnosis
)
```

### 2. Recent Medications
```r
# Prescriptions in last 90 days
generate_covariates(
  covariate_name = "statin",
  days_before_start = 90,     # Last 90 days
  days_before_end = 0,        # Up to day before index
  selection_method = "max"    # Most recent
)
```

### 3. Short-term Outcomes
```r
# Events within 30 days after
generate_outcomes(
  outcome_name = "readmission",
  days_after_start = 0,       # From index
  days_after_end = 30,        # Within 30 days
  selection_method = "min"    # First occurrence
)
```

### 4. Long-term Outcomes
```r
# Any event during follow-up
generate_outcomes(
  outcome_name = "death",
  days_after_start = 0,       # From index
  days_after_end = NULL,      # Unlimited
  selection_method = "min"    # First (only) occurrence
)
```

---

## Selection Method: "min" vs "max"

### When to use "min" (earliest date)
- ✅ First diagnosis (covariate)
- ✅ First outcome event
- ✅ Disease onset

```r
selection_method = "min"

Events: 2020-05-10, 2022-08-15, 2023-11-20
Result: 2020-05-10  ← Selects earliest
```

### When to use "max" (latest date)
- ✅ Most recent medication
- ✅ Latest lab value
- ✅ Last visit before index

```r
selection_method = "max"

Events: 2020-05-10, 2022-08-15, 2023-11-20
Result: 2023-11-20  ← Selects latest
```

---

## Quality Checks at Each Stage

Every function prints progress and quality metrics:

```
=== Creating long format table for: diabetes ===
Including 1 source tables

Extracting from: hospital_admissions...
  ✓ Retrieved 500,000 rows
  Columns: patient_id, event_date, code, terminology

✓ Long format table created: 500,000 rows, 8 columns
  Unique patients: 9,856
  Sources included: 1

=== Generating Covariate: diabetes ===

Found 2 codes for 'diabetes'
Filtered to 45,678 records

--- Quality Report ---
1. Cohort Coverage:
   Patients with 'diabetes': 2,345 / 9,598 (24.4%)

2. Timeframe Coverage:
   Date range: 2010-01-01 to 2023-12-31

3. Top Codes (showing first 10):
   code  name      n_events  n_patients
   E11   diabetes  42,000    2,200
   E10   diabetes  3,678     345

Matched 45,234 records to 2,345 cohort patients
After temporal filter: 45,234 records

=== Covariate Summary ===
Patients with covariate: 2,345 / 9,598 (24.4%)
Mean days before index: 987.3
Median days before index: 765.0
```

---

This quick reference provides visual examples of what each function does to your data at every step of the pipeline.
