# Data Pipeline Flow Documentation

## Visual Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              IBM DB2 DATABASE                                │
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
│  │ PATIENT_ALF_     │  │ PEDW_EPISODE     │  │ WLGP_GP_EVENT_   │          │
│  │ CLEANSED         │  │ (Hospital        │  │ CLEANSED         │          │
│  │ (Demographics)   │  │  Admissions)     │  │ (Primary Care)   │          │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘          │
│  ┌──────────────────┐  ┌──────────────────┐                                │
│  │ WLGP_MEDICATION_ │  │ ADDE_DEATHS      │                                │
│  │ CLEANSED         │  │ (Death Records)  │                                │
│  │ (Prescriptions)  │  │                  │                                │
│  └──────────────────┘  └──────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ STEP 1: Extract & Curate
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LONG FORMAT ASSETS                                   │
│                     (create_long_format_asset)                               │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │ DEMOGRAPHICS ASSETS (4 tables)                                     │     │
│  │                                                                     │     │
│  │  date_of_birth_long_format.rds                                     │     │
│  │  ┌─────────────┬──────────────┬─────────────┬──────────────┐      │     │
│  │  │ patient_id  │ source_table │ source_prio │ date_of_birth│      │     │
│  │  ├─────────────┼──────────────┼─────────────┼──────────────┤      │     │
│  │  │    1001     │   gp_dob     │      1      │  1950-05-15  │      │     │
│  │  │    1002     │   gp_dob     │      1      │  1965-08-22  │      │     │
│  │  └─────────────┴──────────────┴─────────────┴──────────────┘      │     │
│  │                                                                     │     │
│  │  sex_long_format.rds, ethnicity_long_format.rds,                  │     │
│  │  lsoa_long_format.rds (similar structure)                         │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │ DISEASE & TREATMENT ASSETS (4 tables)                              │     │
│  │                                                                     │     │
│  │  hospital_admissions_long_format.rds                               │     │
│  │  ┌─────────────┬──────────────┬──────────┬──────────┬────────┐    │     │
│  │  │ patient_id  │ source_table │ event_dt │   code   │ termin │    │     │
│  │  ├─────────────┼──────────────┼──────────┼──────────┼────────┤    │     │
│  │  │    1001     │     pedw     │2023-03-15│   I21    │ ICD10  │    │     │
│  │  │    1001     │     pedw     │2023-07-22│   E11    │ ICD10  │    │     │
│  │  │    1002     │     pedw     │2023-05-10│   I10    │ ICD10  │    │     │
│  │  └─────────────┴──────────────┴──────────┴──────────┴────────┘    │     │
│  │                                                                     │     │
│  │  primary_care_long_format.rds, medicines_long_format.rds,         │     │
│  │  deaths_long_format.rds (similar structure)                       │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ get_highest_priority_per_patient()
                                      │ combine_demographics()
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     COMBINED DEMOGRAPHICS TABLE                              │
│                                                                              │
│  demographics_combined.rds                                                   │
│  ┌────────────┬──────────────┬──────────┬─────────────┬────────────┐        │
│  │ patient_id │ date_of_birth│ sex_code │ethnicity_cd │  lsoa_code │        │
│  ├────────────┼──────────────┼──────────┼─────────────┼────────────┤        │
│  │    1001    │  1950-05-15  │    M     │      W      │ W01000001  │        │
│  │    1002    │  1965-08-22  │    F     │      A      │ W01000002  │        │
│  │    1003    │  1975-12-10  │    M     │      W      │ W01000003  │        │
│  │    1004    │  1980-03-18  │    F     │     NA      │ W01000004  │        │
│  └────────────┴──────────────┴──────────┴─────────────┴────────────┘        │
│                                                                              │
│  Rows: 10,000 patients                                                       │
│  Columns: 5 (patient_id + 4 demographics)                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ STEP 2: Generate Cohort
                                      │ generate_cohort()
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STUDY COHORT                                        │
│                                                                              │
│  study_cohort.rds                                                            │
│  ┌────────┬────────────┬──────────────┬──────────┬────────┬─────────┐       │
│  │patient │ index_date │ age_at_index │ sex_code │ethnic  │  lsoa   │       │
│  ├────────┼────────────┼──────────────┼──────────┼────────┼─────────┤       │
│  │  1001  │ 2024-01-01 │    73.6      │    M     │   W    │W0100001 │       │
│  │  1002  │ 2024-01-01 │    58.4      │    F     │   A    │W0100002 │       │
│  │  1003  │ 2024-01-01 │    48.1      │    M     │   W    │W0100003 │       │
│  └────────┴────────────┴──────────────┴──────────┴────────┴─────────┘       │
│                                                                              │
│  Restrictions Applied:                                                       │
│  • Age 18-100 at index date: Excluded 234 patients                          │
│  • Known sex required: Excluded 45 patients                                 │
│  • LSOA required: Excluded 123 patients                                     │
│                                                                              │
│  Rows: 9,598 patients (402 excluded)                                         │
│  Columns: 6                                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                  ┌───────────────────┼───────────────────┐
                  │                   │                   │
          STEP 3: Covariates   STEP 4: Outcomes         │
          (lookback)           (follow-up)               │
                  │                   │                   │
                  ▼                   ▼                   ▼
┌──────────────────────────┐ ┌──────────────────────┐
│   COVARIATES TABLE       │ │   OUTCOMES TABLE     │
│                          │ │                      │
│ cohort_with_covariates   │ │ cohort_with_outcomes │
│        .rds              │ │        .rds          │
└──────────────────────────┘ └──────────────────────┘
         │                              │
         │                              │
         └──────────────┬───────────────┘
                        │
                        │ STEP 5: Combine
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      FINAL ANALYSIS DATASET                                  │
│                                                                              │
│  final_analysis_dataset.rds                                                  │
│  ┌────────┬────────────┬─────┬──────────┬───────────┬──────────┬─────────┐  │
│  │patient │ index_date │ age │ diabetes │ diabetes  │heart_atk │heart_atk│  │
│  │  _id   │            │     │  _flag   │  _date    │  _flag   │  _date  │  │
│  ├────────┼────────────┼─────┼──────────┼───────────┼──────────┼─────────┤  │
│  │  1001  │ 2024-01-01 │73.6 │   TRUE   │2023-07-22 │   TRUE   │2024-03  │  │
│  │  1002  │ 2024-01-01 │58.4 │   FALSE  │    NA     │   FALSE  │   NA    │  │
│  │  1003  │ 2024-01-01 │48.1 │   TRUE   │2022-11-10 │   FALSE  │   NA    │  │
│  └────────┴────────────┴─────┴──────────┴───────────┴──────────┴─────────┘  │
│                                                                              │
│  + hypertension_flag, hypertension_date, copd_flag, copd_date, chd_flag...  │
│  + stroke_flag, stroke_date, heart_failure_flag, death_flag...              │
│                                                                              │
│  Rows: 9,598 patients                                                        │
│  Columns: 25+ (demographics + covariates + outcomes)                         │
│                                                                              │
│  READY FOR STATISTICAL ANALYSIS                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Processing Steps

### **STEP 1: Extract & Curate Long Format Assets**

#### Function: `create_long_format_asset()`
**File:** `create_long_format_assets.R`

**Input:**
- DB2 database connection
- YAML configuration file
- Asset name (e.g., "date_of_birth", "hospital_admissions")
- Optional: patient_ids to filter

**Processing:**
1. **Read configuration** from YAML for specified asset
2. **Build SQL queries** for each source table
3. **Execute queries** against DB2 database
4. **Retrieve data** from multiple sources
5. **Add metadata** (source_table, source_priority, source_quality)
6. **Combine sources** into single long format table (one row per source per patient)
7. **Standardize** column names to match YAML specification

**Output:** Long format data frame

**Example Transformation:**

```
DATABASE TABLE: PATIENT_ALF_CLEANSED
┌──────────┬──────────────┬──────────┐
│  ALF_PE  │     WOB      │CREATE_DT │
├──────────┼──────────────┼──────────┤
│   1001   │  1950-05-15  │2020-01-01│
│   1002   │  1965-08-22  │2020-01-01│
└──────────┴──────────────┴──────────┘

                 ↓ create_long_format_asset()

LONG FORMAT: date_of_birth_long_format.rds
┌────────────┬──────────────┬──────────────┬─────────────┬──────────────┐
│ patient_id │ source_table │source_priority│date_of_birth│ record_date  │
├────────────┼──────────────┼──────────────┼─────────────┼──────────────┤
│    1001    │    gp_dob    │      1       │ 1950-05-15  │  2020-01-01  │
│    1002    │    gp_dob    │      1       │ 1965-08-22  │  2020-01-01  │
└────────────┴──────────────┴──────────────┴─────────────┴──────────────┘
```

**Data Changes:**
- **Before:** Raw database tables with varying column names
- **After:** Standardized long format with metadata
- **Rows:** Same as database (may have multiple rows per patient if multiple sources)
- **Columns:** Original columns + source metadata (3-6 additional columns)

---

### **STEP 1b: Resolve Multi-Source Conflicts**

#### Function: `get_highest_priority_per_patient()`
**File:** `create_long_format_assets.R`

**Input:** Long format table (potentially multiple rows per patient)

**Processing:**
1. **Group by patient_id**
2. **Sort by source_priority** (ascending - lower is better)
3. **Select first row** per patient (highest priority source)

**Output:** Wide format table (one row per patient)

**Example Transformation:**

```
BEFORE: Multiple sources per patient
┌────────────┬──────────────┬──────────────┬───────────────┐
│ patient_id │ source_table │source_priority│ ethnicity_code│
├────────────┼──────────────┼──────────────┼───────────────┤
│    1001    │self_reported │      1       │      W        │ ← Selected
│    1001    │   hospital   │      2       │      W        │
│    1002    │   hospital   │      2       │      A        │ ← Selected
│    1002    │      gp      │      3       │      A        │
└────────────┴──────────────┴──────────────┴───────────────┘

                 ↓ get_highest_priority_per_patient()

AFTER: One row per patient
┌────────────┬──────────────┬──────────────┬───────────────┐
│ patient_id │ source_table │source_priority│ ethnicity_code│
├────────────┼──────────────┼──────────────┼───────────────┤
│    1001    │self_reported │      1       │      W        │
│    1002    │   hospital   │      2       │      A        │
└────────────┴──────────────┴──────────────┴───────────────┘
```

**Data Changes:**
- **Before:** Multiple rows per patient (if multiple sources available)
- **After:** One row per patient (highest priority source selected)
- **Rows:** Reduced to unique patients
- **Columns:** Same

---

### **STEP 1c: Combine Demographics**

#### Function: `combine_demographics()`
**File:** `generate_cohort.R`

**Input:**
- DOB asset (one row per patient)
- Sex asset (one row per patient)
- Ethnicity asset (optional, one row per patient)
- LSOA asset (optional, one row per patient)

**Processing:**
1. **Start with DOB** as base table
2. **Left join Sex** by patient_id
3. **Left join Ethnicity** by patient_id (if provided)
4. **Left join LSOA** by patient_id (if provided)
5. **Select core columns** (drop source metadata)

**Output:** Combined demographics table

**Example Transformation:**

```
DOB Asset              Sex Asset              Ethnicity Asset
┌──────┬──────────┐   ┌──────┬────────┐     ┌──────┬──────────┐
│patient│date_of   │   │patient│sex_code│    │patient│ethnicity │
│  _id │  birth   │   │  _id │        │     │  _id │  _code   │
├──────┼──────────┤   ├──────┼────────┤     ├──────┼──────────┤
│ 1001 │1950-05-15│   │ 1001 │   M    │     │ 1001 │    W     │
│ 1002 │1965-08-22│   │ 1002 │   F    │     │ 1002 │    A     │
│ 1003 │1975-12-10│   │ 1003 │   M    │     │ 1003 │    W     │
└──────┴──────────┘   └──────┴────────┘     └──────┴──────────┘

                 ↓ combine_demographics()

Combined Demographics
┌────────────┬──────────────┬──────────┬───────────────┬────────────┐
│ patient_id │ date_of_birth│ sex_code │ ethnicity_code│ lsoa_code  │
├────────────┼──────────────┼──────────┼───────────────┼────────────┤
│    1001    │  1950-05-15  │    M     │      W        │ W01000001  │
│    1002    │  1965-08-22  │    F     │      A        │ W01000002  │
│    1003    │  1975-12-10  │    M     │      W        │ W01000003  │
└────────────┴──────────────┴──────────┴───────────────┴────────────┘
```

**Data Changes:**
- **Before:** 4 separate tables (DOB, Sex, Ethnicity, LSOA)
- **After:** 1 combined table
- **Rows:** Same as DOB table (base)
- **Columns:** All demographic fields merged (5 columns typical)

---

### **STEP 2: Generate Study Cohort**

#### Function: `generate_cohort()`
**File:** `generate_cohort.R`

**Input:**
- Combined demographics table
- Index date (single date or per-patient vector)
- Restriction parameters (age, sex, ethnicity, LSOA requirements)

**Processing:**
1. **Add index_date** column to demographics
2. **Calculate age_at_index** using date_of_birth and index_date
   ```r
   age = (index_date - date_of_birth) / 365.25
   ```
3. **Apply age restrictions**
   - Filter: `age_at_index >= min_age`
   - Filter: `age_at_index <= max_age`
4. **Apply sex restriction**
   - Filter: `!is.na(sex_code) & sex_code != ""`
5. **Apply ethnicity restriction** (if required)
   - Filter: `!is.na(ethnicity_code) & ethnicity_code != ""`
6. **Apply LSOA restriction** (if required)
   - Filter: `!is.na(lsoa_code) & lsoa_code != ""`
7. **Log exclusions** at each step
8. **Print summary statistics**

**Output:** Study cohort table

**Example Transformation:**

```
BEFORE: All patients with demographics
┌────────┬────────────┬──────────────┬──────────┬────────┬─────────┐
│patient │            │ date_of_birth│ sex_code │ethnic  │  lsoa   │
│  _id   │            │              │          │        │         │
├────────┼────────────┼──────────────┼──────────┼────────┼─────────┤
│  1001  │            │  1950-05-15  │    M     │   W    │W0100001 │
│  1002  │            │  1965-08-22  │    F     │   A    │W0100002 │
│  1003  │            │  2010-12-10  │    M     │   W    │W0100003 │ ← Too young
│  1004  │            │  1975-03-18  │    NA    │   W    │W0100004 │ ← Missing sex
│  1005  │            │  1980-07-22  │    M     │   NA   │   NA    │ ← Missing LSOA
└────────┴────────────┴──────────────┴──────────┴────────┴─────────┘

   ↓ generate_cohort(index_date = "2024-01-01",
                     min_age = 18, max_age = 100,
                     require_known_sex = TRUE,
                     require_lsoa = TRUE)

AFTER: Cohort meeting all criteria
┌────────┬────────────┬──────────────┬──────────┬────────┬─────────┐
│patient │ index_date │ age_at_index │ sex_code │ethnic  │  lsoa   │
├────────┼────────────┼──────────────┼──────────┼────────┼─────────┤
│  1001  │ 2024-01-01 │    73.6      │    M     │   W    │W0100001 │
│  1002  │ 2024-01-01 │    58.4      │    F     │   A    │W0100002 │
└────────┴────────────┴──────────────┴──────────┴────────┴─────────┘

Excluded:
  • 1 patient: age < 18
  • 1 patient: missing sex
  • 1 patient: missing LSOA
```

**Data Changes:**
- **Before:** All patients (10,000)
- **After:** Eligible patients only (9,598)
- **Rows:** Reduced by exclusions (~4% typical)
- **Columns:** Added index_date and age_at_index (7 columns)

---

### **STEP 3: Generate Covariates**

#### Function: `generate_covariates()`
**File:** `generate_covariates.R`

**Input:**
- Disease/treatment long format asset
- Study cohort (with patient_id and index_date)
- Lookup table (code → covariate name mapping)
- Covariate name to extract
- Temporal window parameters (days_before_start, days_before_end)
- Selection method (min/max)

**Processing:**

**Phase 1: Filter by Codes**
1. **Filter lookup table** for covariate_name
2. **Extract relevant codes** (e.g., E10, E11 for diabetes)
3. **Filter disease data** to only matching codes

**Phase 2: Quality Assessment**
4. **Calculate coverage**: % of cohort with any matching code
5. **Summarize timeframe**: earliest to latest event date
6. **Count by code**: frequency of each code
7. **Print quality report**

**Phase 3: Temporal Filtering**
8. **Join with cohort** to get index_date for each patient
9. **Calculate days_from_index** for each event
   ```r
   days_from_index = event_date - index_date
   ```
10. **Apply lookback window**:
    - Keep events where: `days_from_index <= -days_before_end`
    - Keep events where: `days_from_index >= -days_before_start` (if specified)

**Phase 4: Select Per Patient**
11. **Group by patient_id**
12. **Select date** per patient based on method:
    - `min`: Earliest (first diagnosis)
    - `max`: Latest (most recent)

**Phase 5: Create Output**
13. **Left join** result back to cohort (preserves all patients)
14. **Create covariate_flag**: TRUE if covariate found, FALSE otherwise
15. **Add covariate_date**: Date of selected event (NA if not found)
16. **Calculate days_to_index** (negative value indicating days before)

**Output:** Covariate data frame

**Example Transformation:**

```
DISEASE DATA: hospital_admissions_long_format.rds
┌────────────┬──────────────┬───────────┬────────────┐
│ patient_id │ event_date   │   code    │terminology │
├────────────┼──────────────┼───────────┼────────────┤
│    1001    │  2022-03-15  │    E11    │   ICD10    │ ← Diabetes
│    1001    │  2023-07-22  │    E11    │   ICD10    │ ← Diabetes
│    1001    │  2023-08-10  │    I21    │   ICD10    │
│    1002    │  2023-05-10  │    I10    │   ICD10    │
│    1003    │  2021-11-20  │    E11    │   ICD10    │ ← Diabetes
└────────────┴──────────────┴───────────┴────────────┘

LOOKUP TABLE:
┌──────┬──────────┬──────────────────┐
│ code │   name   │  description     │
├──────┼──────────┼──────────────────┤
│  E10 │ diabetes │ Type 1 diabetes  │
│  E11 │ diabetes │ Type 2 diabetes  │
│  I10 │ hyperten │ Essential HTN    │
│  I21 │ heart_atk│ Acute MI         │
└──────┴──────────┴──────────────────┘

COHORT:
┌────────────┬────────────┐
│ patient_id │ index_date │
├────────────┼────────────┤
│    1001    │ 2024-01-01 │
│    1002    │ 2024-01-01 │
│    1003    │ 2024-01-01 │
└────────────┴────────────┘

   ↓ generate_covariates(covariate_name = "diabetes",
                         days_before_start = NULL,  # Any time
                         days_before_end = 0,       # Before index
                         selection_method = "min")  # First

RESULT:
┌────────────┬────────────┬────────────────┬────────────────┬──────────────┐
│ patient_id │ index_date │ covariate_flag │ covariate_date │days_to_index │
├────────────┼────────────┼────────────────┼────────────────┼──────────────┤
│    1001    │ 2024-01-01 │     TRUE       │  2022-03-15    │    -657      │
│    1002    │ 2024-01-01 │     FALSE      │      NA        │      NA      │
│    1003    │ 2024-01-01 │     TRUE       │  2021-11-20    │    -772      │
└────────────┴────────────┴────────────────┴────────────────┴──────────────┘

Notes:
• Patient 1001: Had diabetes codes on 2022-03-15 and 2023-07-22
              Selected 2022-03-15 (earliest = "min")
• Patient 1002: No diabetes codes → flag = FALSE
• Patient 1003: Had diabetes code on 2021-11-20 → flag = TRUE
```

**Data Changes:**
- **Before:** Cohort (9,598 patients, 7 columns)
- **After:** Cohort + covariate columns (9,598 patients, 10 columns)
- **Rows:** Same (all cohort patients preserved)
- **Columns:** Added 3 (covariate_flag, covariate_date, days_to_index)

**Temporal Window Examples:**

```
Timeline:  <-------|--------------------|--------->
                   |                    |
           days_before_start    days_before_end
                                     Index Date

Example 1: Any time before index
  days_before_start = NULL, days_before_end = 0
  Includes: All events before index date

Example 2: Last 365 days
  days_before_start = 365, days_before_end = 0
  Includes: Events between 365 days and 0 days before index

Example 3: Between 365 and 30 days before
  days_before_start = 365, days_before_end = 30
  Includes: Events between 365 and 30 days before index
```

---

### **STEP 4: Generate Outcomes**

#### Function: `generate_outcomes()`
**File:** `generate_outcomes.R`

**Input:**
- Disease/treatment long format asset
- Study cohort (with patient_id and index_date)
- Lookup table (code → outcome name mapping)
- Outcome name to extract
- Temporal window parameters (days_after_start, days_after_end)
- Selection method (min/max)

**Processing:**

*[Nearly identical to generate_covariates, but with follow-up window instead of lookback]*

**Phase 1-2:** Same filtering and quality assessment

**Phase 3: Temporal Filtering (DIFFERENT)**
8. **Join with cohort** to get index_date
9. **Calculate days_from_index** for each event
10. **Apply follow-up window**:
    - Keep events where: `days_from_index >= days_after_start`
    - Keep events where: `days_from_index <= days_after_end` (if specified)

**Phase 4:** Same selection per patient

**Phase 5:** Create output with outcome_flag, outcome_date, days_from_index

**Output:** Outcome data frame

**Example Transformation:**

```
DISEASE DATA: hospital_admissions_long_format.rds
┌────────────┬──────────────┬───────────┐
│ patient_id │ event_date   │   code    │
├────────────┼──────────────┼───────────┤
│    1001    │  2023-11-10  │    I21    │ ← Before index
│    1001    │  2024-03-15  │    I21    │ ← After index ✓
│    1002    │  2024-06-22  │    I63    │ ← After index ✓
│    1003    │  2023-08-10  │    I21    │ ← Before index
└────────────┴──────────────┴───────────┘

COHORT:
┌────────────┬────────────┐
│ patient_id │ index_date │
├────────────┼────────────┤
│    1001    │ 2024-01-01 │
│    1002    │ 2024-01-01 │
│    1003    │ 2024-01-01 │
└────────────┴────────────┘

   ↓ generate_outcomes(outcome_name = "heart_attack",
                       days_after_start = 0,     # From index
                       days_after_end = 365,     # Within 1 year
                       selection_method = "min") # First

RESULT:
┌────────────┬────────────┬──────────────┬──────────────┬────────────────┐
│ patient_id │ index_date │ outcome_flag │ outcome_date │days_from_index │
├────────────┼────────────┼──────────────┼──────────────┼────────────────┤
│    1001    │ 2024-01-01 │     TRUE     │  2024-03-15  │      74        │
│    1002    │ 2024-01-01 │     FALSE    │      NA      │      NA        │
│    1003    │ 2024-01-01 │     FALSE    │      NA      │      NA        │
└────────────┴────────────┴──────────────┴──────────────┴────────────────┘

Notes:
• Patient 1001: Heart attack on 2024-03-15 (74 days after index) ✓
• Patient 1002: Stroke code (I63), not heart attack (I21)
• Patient 1003: Heart attack was before index date (excluded)
```

**Data Changes:**
- **Before:** Cohort (9,598 patients, 7 columns)
- **After:** Cohort + outcome columns (9,598 patients, 10 columns)
- **Rows:** Same (all cohort patients preserved)
- **Columns:** Added 3 (outcome_flag, outcome_date, days_from_index)

**Temporal Window Examples:**

```
Timeline:  <-----------|--------------------|--------->
                       |                    |
                  Index Date        days_after_end
                  days_after_start

Example 1: Any time after index
  days_after_start = 0, days_after_end = NULL
  Includes: All events on or after index date

Example 2: Within 365 days
  days_after_start = 0, days_after_end = 365
  Includes: Events between index and 365 days after

Example 3: Between 30 and 365 days after
  days_after_start = 30, days_after_end = 365
  Includes: Events between 30 and 365 days after index
```

---

### **STEP 5: Combine Final Dataset**

#### Function: Left joins in main pipeline
**File:** `complete_pipeline_example.R`

**Input:**
- Cohort (9,598 patients, 7 columns)
- Covariates (9,598 patients, multiple covariate columns)
- Outcomes (9,598 patients, multiple outcome columns)

**Processing:**
1. **Start with cohort**
2. **Left join covariates** by patient_id (drop duplicate index_date)
3. **Left join outcomes** by patient_id (drop duplicate index_date)
4. **Calculate summary statistics**
5. **Export final dataset**

**Output:** Final analysis-ready dataset

**Example Transformation:**

```
COHORT (Base):
┌────────┬────────────┬─────┬────────┐
│patient │ index_date │ age │sex_code│
├────────┼────────────┼─────┼────────┤
│  1001  │ 2024-01-01 │73.6 │   M    │
│  1002  │ 2024-01-01 │58.4 │   F    │
└────────┴────────────┴─────┴────────┘

COVARIATES:
┌────────┬────────────┬────────────────┬──────────────────┬─────────────────┐
│patient │ index_date │ diabetes_flag  │ hypertension_flag│    copd_flag    │
├────────┼────────────┼────────────────┼──────────────────┼─────────────────┤
│  1001  │ 2024-01-01 │     TRUE       │      TRUE        │      FALSE      │
│  1002  │ 2024-01-01 │     FALSE      │      TRUE        │      FALSE      │
└────────┴────────────┴────────────────┴──────────────────┴─────────────────┘

OUTCOMES:
┌────────┬────────────┬──────────────────┬─────────────┬──────────────┐
│patient │ index_date │ heart_attack_flag│ stroke_flag │  death_flag  │
├────────┼────────────┼──────────────────┼─────────────┼──────────────┤
│  1001  │ 2024-01-01 │      TRUE        │    FALSE    │    FALSE     │
│  1002  │ 2024-01-01 │      FALSE       │    TRUE     │    FALSE     │
└────────┴────────────┴──────────────────┴─────────────┴──────────────┘

   ↓ Left join covariates, then left join outcomes

FINAL DATASET:
┌────┬──────┬───┬───┬────────┬────────┬──────┬────────┬──────┬──────┐
│pat │index │age│sex│diabetes│hyperten│ copd │heart_  │stroke│death │
│ id │ date │   │   │  _flag │  _flag │ flag │atk_flag│ flag │ flag │
├────┼──────┼───┼───┼────────┼────────┼──────┼────────┼──────┼──────┤
│1001│01-01 │74 │ M │  TRUE  │  TRUE  │FALSE │  TRUE  │FALSE │FALSE │
│1002│01-01 │58 │ F │  FALSE │  TRUE  │FALSE │  FALSE │ TRUE │FALSE │
└────┴──────┴───┴───┴────────┴────────┴──────┴────────┴──────┴──────┘

+ diabetes_date, hypertension_date, copd_date, chd_date
+ heart_attack_date, stroke_date, death_date
+ days_to_index for each covariate, days_from_index for each outcome
```

**Data Changes:**
- **Before:** 3 separate tables (cohort, covariates, outcomes)
- **After:** 1 merged table
- **Rows:** Same as cohort (9,598 patients)
- **Columns:** Combined (25+ columns typical)
  - 7 from cohort
  - 3 × 4 covariates = 12 (flag, date, days_to_index for each)
  - 3 × 3 outcomes = 9 (flag, date, days_from_index for each)

---

## Data Flow Summary

### Volume Changes

```
Stage                      Patients    Columns    Data Type
─────────────────────────────────────────────────────────────
1. Database Extract        10,000      varies     Multiple tables
2. Long Format Assets      10,000      8-12       Long format
3. Combined Demographics   10,000      5          Wide format
4. Study Cohort            9,598       7          Wide format (filtered)
5. With Covariates         9,598       19         Wide format (enriched)
6. With Outcomes           9,598       28         Wide format (enriched)
7. Final Dataset           9,598       35+        Wide format (complete)
```

### Processing Time Estimates

```
Stage                      Typical Duration    Bottleneck
────────────────────────────────────────────────────────────
1. Database Extract        2-5 minutes         DB2 query performance
2. Multi-source Resolution 1-2 minutes         Data volume
3. Demographics Combine    <30 seconds         In-memory joins
4. Cohort Generation       <30 seconds         Simple filters
5. Covariate Generation    3-10 minutes        DB2 query + temporal logic
6. Outcome Generation      3-10 minutes        DB2 query + temporal logic
7. Final Merge             <1 minute           In-memory joins

TOTAL: 10-30 minutes (depends on patient count and data volume)
```

### Quality Checks

Each step includes built-in validation:

1. **Database Extract**: Row counts, column name verification
2. **Multi-source**: Conflict detection and reporting
3. **Demographics**: Completeness percentages
4. **Cohort**: Exclusion counts and reasons
5. **Covariates**: Coverage %, code frequency, temporal range
6. **Outcomes**: Coverage %, code frequency, temporal range
7. **Final**: Summary statistics, missingness report

---

## Key Design Decisions

### 1. Long Format for Diseases
- **Why:** Preserves all events for temporal analysis
- **Trade-off:** Larger file sizes, but more flexible

### 2. Wide Format for Cohort
- **Why:** Easier for statistical analysis (one row = one patient)
- **Trade-off:** Need to pivot for some analyses

### 3. Left Joins for Covariates/Outcomes
- **Why:** Preserves all cohort patients (missing = FALSE)
- **Trade-off:** None (correct approach for this use case)

### 4. Flags + Dates + Days
- **Why:**
  - Flags: Easy binary analysis
  - Dates: Required for validation/auditing
  - Days: Ready for time-to-event analysis
- **Trade-off:** More columns, but comprehensive

### 5. Quality Reports by Default
- **Why:** Catch data issues early
- **Trade-off:** More console output, but transparent

---

This pipeline transforms raw database tables into analysis-ready datasets while preserving data quality, transparency, and auditability at every step.
