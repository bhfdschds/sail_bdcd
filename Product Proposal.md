# Overview of Data Pipeline

## curate_long_assets

### Demographics
- DOB  
- SEX  
- Ethnicity  
- LSOA  

## Curate Disease and Treatments Assets
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
- Applies a filter before the index date and selects either min or max, producing a flag and date  
- Applies a filter before the index date between *x* and *y* days and selects either min or max, producing a flag and date  
- Adds an option to calculate time in days between the index date and covariate date  
- Summarises by *name*  

## generate_outcomes
- Uses the Disease and Treatments assets  
- Uses a lookup table with:
  - code  
  - name  
  - description  
  - terminology  
- Applies a filter after the index date and selects either min or max, producing a flag and date  
- Applies a filter after the index date between *x* and *y* days and selects either min or max, producing a flag and date  
- Adds an option to calculate time in days between the index date and outcome date  
- Summarises by *name*  

## Final Step
- Combine cohort with covariates and outcomes  
