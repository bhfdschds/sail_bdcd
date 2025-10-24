# Overview of Data Pipeline

## curate_long_assets

### Demographics
- DOB  
- SEX  
- Ethnicity  
- LSOA  

## Curate Disease and Treatments Assets
- Applies summary quality and validation assessments before curating
  - Time Coverage, pats, distinct pats
  - Variable completeness (percentage)
- Long form hospital admissions data  
- Long form primary care data  
- Long form primary care medicines data  
- Long form deaths data

## generate_cohort
- Uses the demographics asset
- Applies restrictions
  - Age at index date
  - Known Sex
  - known Ethnicity
  - LSOA at index date

## generate_covariates
- Uses the Disease and Treatments assets  
- Uses a lookup table with:
  - code  
  - name  
  - description  
  - terminology
- Applies summary quality and validation assessments before filtering dates
  - cohort counts and percentage by 'name'
  - timeframe coverage and percentage by 'name'
  - Counts and percentages for each 'code' and 'name' descending.
- Applies a filter before the index date and selects either min or max, producing a flag and date  
- Applies a filter before the index date between *x* and *y* days and selects either min or max, producing a flag and date  
- Adds an option to calculate time in days between the index date and covariate date  

## generate_outcomes
- Uses the Disease and Treatments assets  
- Uses a lookup table with:
  - code  
  - name  
  - description  
  - terminology
- Applies summary quality and validation assessments before filtering dates
  - cohort counts and percentage by 'name'
  - timeframe coverage and percentage by 'name'
  - Counts and percentages for each 'code' and 'name' descending.
- Applies a filter after the index date and selects either min or max, producing a flag and date  
- Applies a filter after the index date between *x* and *y* days and selects either min or max, producing a flag and date  
- Adds an option to calculate time in days between the index date and outcome date  

## Final Step
- Combine cohort with covariates and outcomes  
