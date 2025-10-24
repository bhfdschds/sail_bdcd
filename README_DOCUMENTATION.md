# Pipeline Documentation Index

## 📚 Documentation Overview

This repository now contains comprehensive documentation for the healthcare data curation pipeline. Here's your guide to all documentation files:

---

## 🎯 Quick Start

**New to the pipeline?** Start here:

1. **[REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md)** - Overview of what was added and why
2. **[PIPELINE_VISUAL_DIAGRAM.txt](PIPELINE_VISUAL_DIAGRAM.txt)** - ASCII visual of entire pipeline flow
3. **[FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md)** - Before/after examples for each function

---

## 📖 Documentation Files

### 1. **REFACTORING_SUMMARY.md**
   - **Purpose:** High-level overview of refactoring work
   - **Contents:**
     - What functions were added
     - Code simplification principles
     - File structure
     - Summary statistics
   - **Read if:** You want to understand what changed and why

### 2. **PIPELINE_FLOW_DOCUMENTATION.md**
   - **Purpose:** Detailed technical documentation of data transformations
   - **Contents:**
     - Step-by-step processing details
     - Data transformations at each stage
     - Row/column changes throughout pipeline
     - Quality checks and validations
     - Processing time estimates
   - **Read if:** You need to understand exactly what happens to the data

### 3. **PIPELINE_VISUAL_DIAGRAM.txt**
   - **Purpose:** Visual ASCII diagram of entire pipeline
   - **Contents:**
     - Database layer → Final dataset flow
     - Box diagrams showing data structure at each stage
     - Temporal window visualizations
     - Data volume changes
     - Processing time summary
   - **Read if:** You're a visual learner or need to present the pipeline

### 4. **FUNCTION_QUICK_REFERENCE.md**
   - **Purpose:** Quick lookup for each function with examples
   - **Contents:**
     - Before/after examples for every function
     - Common parameter combinations
     - Temporal window guide
     - "min" vs "max" selection explanation
     - Quality check outputs
   - **Read if:** You need to use a specific function right now

### 5. **r-scripts/pipeline_code/README_NEW_FUNCTIONS.md**
   - **Purpose:** User guide for new functions
   - **Contents:**
     - Complete parameter documentation
     - Usage examples
     - Common use cases
     - Troubleshooting guide
     - Testing instructions
   - **Read if:** You're implementing the pipeline in your project

### 6. **Generic curation plan.md**
   - **Purpose:** Original pipeline specification
   - **Contents:**
     - High-level pipeline overview
     - Requirements for each stage
     - Design goals
   - **Read if:** You want to see the original requirements

---

## 🚀 Usage Workflow

### For First-Time Users:

```
1. Read: REFACTORING_SUMMARY.md (10 min)
   ↓ Understand what exists

2. View: PIPELINE_VISUAL_DIAGRAM.txt (5 min)
   ↓ Visualize the flow

3. Read: r-scripts/pipeline_code/README_NEW_FUNCTIONS.md (20 min)
   ↓ Learn how to use functions

4. Run: r-scripts/examples_code/complete_pipeline_example.R
   ↓ Test with your data

5. Reference: FUNCTION_QUICK_REFERENCE.md (as needed)
   ↓ Look up specific functions
```

### For Implementation:

```
1. Review: PIPELINE_FLOW_DOCUMENTATION.md
   ↓ Understand data transformations

2. Customize: Lookup tables and temporal windows
   ↓ Match your study design

3. Test: Run tests in r-scripts/tests/testthat/
   ↓ Verify functionality

4. Deploy: Use complete_pipeline_example.R as template
   ↓ Production ready
```

---

## 📁 File Locations

### Core Pipeline Code
```
r-scripts/pipeline_code/
├── create_long_format_assets.R     # Step 1: Extract from DB2
├── generate_cohort.R               # Step 2: Create cohort
├── generate_covariates.R           # Step 3: Add covariates
├── generate_outcomes.R             # Step 4: Add outcomes
├── db2_config_multi_source.yaml    # Database configuration
└── README_NEW_FUNCTIONS.md         # User guide
```

### Tests
```
r-scripts/tests/testthat/
├── test_generate_cohort.R          # 10 tests
├── test_generate_covariates.R      # 10 tests
└── test_generate_outcomes.R        # 11 tests
```

### Examples
```
r-scripts/examples_code/
└── complete_pipeline_example.R     # End-to-end example
```

### Documentation (Root Directory)
```
/
├── README_DOCUMENTATION.md         # This file
├── REFACTORING_SUMMARY.md          # Overview
├── PIPELINE_FLOW_DOCUMENTATION.md  # Technical details
├── PIPELINE_VISUAL_DIAGRAM.txt     # ASCII visuals
├── FUNCTION_QUICK_REFERENCE.md     # Quick lookup
└── Generic curation plan.md        # Original spec
```

---

## 🎓 Learning Path by Role

### **Data Analyst**
You want to use the pipeline for analysis:
1. [PIPELINE_VISUAL_DIAGRAM.txt](PIPELINE_VISUAL_DIAGRAM.txt) - See the big picture
2. [README_NEW_FUNCTIONS.md](r-scripts/pipeline_code/README_NEW_FUNCTIONS.md) - Learn the functions
3. [complete_pipeline_example.R](r-scripts/examples_code/complete_pipeline_example.R) - Run example
4. [FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md) - Quick lookup as needed

### **Data Scientist**
You want to understand transformations:
1. [REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md) - What was built
2. [PIPELINE_FLOW_DOCUMENTATION.md](PIPELINE_FLOW_DOCUMENTATION.md) - How data transforms
3. [FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md) - Before/after examples
4. Source code in `r-scripts/pipeline_code/`

### **Software Engineer**
You want to maintain/extend the code:
1. [REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md) - Architecture overview
2. Test files in `r-scripts/tests/testthat/`
3. Source code in `r-scripts/pipeline_code/`
4. [PIPELINE_FLOW_DOCUMENTATION.md](PIPELINE_FLOW_DOCUMENTATION.md) - Processing details

### **Clinical Researcher**
You want to design a study:
1. [Generic curation plan.md](Generic curation plan.md) - Original requirements
2. [PIPELINE_VISUAL_DIAGRAM.txt](PIPELINE_VISUAL_DIAGRAM.txt) - Visual flow
3. [README_NEW_FUNCTIONS.md](r-scripts/pipeline_code/README_NEW_FUNCTIONS.md) - Temporal windows
4. [FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md) - Common use cases

---

## 🔍 Finding Information

### "How do I...?"

| Question | See Document | Section |
|----------|--------------|---------|
| Run the pipeline | README_NEW_FUNCTIONS.md | Quick Start |
| Understand temporal windows | FUNCTION_QUICK_REFERENCE.md | Temporal Window Visual Guide |
| See what functions exist | REFACTORING_SUMMARY.md | New Core Functions |
| Know what data looks like at each stage | PIPELINE_FLOW_DOCUMENTATION.md | Detailed Processing Steps |
| Get parameter documentation | README_NEW_FUNCTIONS.md | Parameters tables |
| See before/after examples | FUNCTION_QUICK_REFERENCE.md | All sections |
| Understand performance | PIPELINE_FLOW_DOCUMENTATION.md | Processing Time Estimates |
| Troubleshoot issues | README_NEW_FUNCTIONS.md | Troubleshooting |
| Run tests | README_NEW_FUNCTIONS.md | Testing |

### "I want to understand..."

| Topic | See Document | Section |
|-------|--------------|---------|
| Overall architecture | REFACTORING_SUMMARY.md | Overview |
| Data flow | PIPELINE_VISUAL_DIAGRAM.txt | Entire file |
| Each function in detail | PIPELINE_FLOW_DOCUMENTATION.md | Detailed Processing Steps |
| Code simplification principles | REFACTORING_SUMMARY.md | Code Simplification Principles |
| Why certain decisions were made | PIPELINE_FLOW_DOCUMENTATION.md | Key Design Decisions |
| Row/column changes | PIPELINE_FLOW_DOCUMENTATION.md | Data Changes sections |
| Quality checks | FUNCTION_QUICK_REFERENCE.md | Quality Checks at Each Stage |

---

## 📊 Pipeline Summary (Quick Facts)

### Input
- **Source:** IBM DB2 database
- **Tables:** 8+ tables (demographics + clinical)
- **Patients:** ~10,000
- **Events:** Millions of clinical records

### Output
- **Format:** Single wide-format table
- **Rows:** One per patient (~9,600 after restrictions)
- **Columns:** 35+ (demographics + covariates + outcomes)
- **File:** CSV and RDS formats
- **Status:** ✅ Analysis-ready

### Processing
- **Time:** 25-45 minutes
- **Steps:** 7 main stages
- **Functions:** 13 core functions
- **Tests:** 31 test cases
- **Quality:** Built-in validation at every step

---

## 🎯 Key Features

### Simple & Clear
- ✅ One function = one purpose
- ✅ Clear parameter names
- ✅ No hidden complexity
- ✅ Verbose console output

### Flexible
- ✅ Configurable temporal windows
- ✅ Custom lookup tables
- ✅ Optional quality reports
- ✅ Batch or single processing

### Production-Ready
- ✅ Comprehensive testing
- ✅ Error handling
- ✅ Quality validation
- ✅ Performance optimized

### Well-Documented
- ✅ 6 documentation files
- ✅ Before/after examples
- ✅ Visual diagrams
- ✅ Usage guides

---

## 🛠️ Common Tasks

### 1. Run Complete Pipeline
```r
source("r-scripts/examples_code/complete_pipeline_example.R")
results <- run_complete_pipeline(
  patient_ids = 1001:2000,
  index_date = as.Date("2024-01-01")
)
```
**See:** [complete_pipeline_example.R](r-scripts/examples_code/complete_pipeline_example.R)

### 2. Generate Single Covariate
```r
source("r-scripts/pipeline_code/generate_covariates.R")
diabetes <- generate_covariates(
  disease_treatment_asset = disease_data,
  cohort = cohort,
  lookup_table = lookup,
  covariate_name = "diabetes"
)
```
**See:** [FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md#5-generate_covariates)

### 3. Customize Temporal Windows
```r
# Last 90 days before index
recent_meds <- generate_covariates(
  ...,
  days_before_start = 90,
  days_before_end = 0,
  selection_method = "max"  # Most recent
)
```
**See:** [README_NEW_FUNCTIONS.md](r-scripts/pipeline_code/README_NEW_FUNCTIONS.md#temporal-windows)

### 4. Run Tests
```r
testthat::test_dir("r-scripts/tests/testthat")
```
**See:** [README_NEW_FUNCTIONS.md](r-scripts/pipeline_code/README_NEW_FUNCTIONS.md#testing)

---

## 💡 Tips for Using Documentation

### Quick Answers
Use **FUNCTION_QUICK_REFERENCE.md** for:
- "What does this function do?"
- "What will my data look like after?"
- "What parameters should I use?"

### Deep Dives
Use **PIPELINE_FLOW_DOCUMENTATION.md** for:
- Understanding exact transformations
- Debugging data issues
- Performance optimization
- Quality validation

### Visual Understanding
Use **PIPELINE_VISUAL_DIAGRAM.txt** for:
- Presenting to stakeholders
- Teaching new team members
- Understanding big picture
- Seeing data volume changes

### Implementation
Use **README_NEW_FUNCTIONS.md** for:
- Step-by-step instructions
- Parameter documentation
- Troubleshooting
- Common use cases

---

## 📞 Getting Help

### 1. Check Documentation
- Start with this index
- Follow learning path for your role
- Use "Finding Information" tables above

### 2. Review Examples
- [complete_pipeline_example.R](r-scripts/examples_code/complete_pipeline_example.R)
- [FUNCTION_QUICK_REFERENCE.md](FUNCTION_QUICK_REFERENCE.md)

### 3. Run Tests
- Verify your setup is working
- See examples of correct usage

### 4. Check Source Code
- Functions have detailed comments
- Examples in each file

---

## 🎬 Next Steps

1. **Read** [REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md) (10 minutes)
2. **View** [PIPELINE_VISUAL_DIAGRAM.txt](PIPELINE_VISUAL_DIAGRAM.txt) (5 minutes)
3. **Explore** [complete_pipeline_example.R](r-scripts/examples_code/complete_pipeline_example.R)
4. **Run** tests to verify setup
5. **Start** building your study cohort!

---

## 📝 Document Versions

| Document | Created | Purpose |
|----------|---------|---------|
| Generic curation plan.md | Original | Requirements specification |
| REFACTORING_SUMMARY.md | 2025-10-24 | Refactoring overview |
| PIPELINE_FLOW_DOCUMENTATION.md | 2025-10-24 | Technical deep dive |
| PIPELINE_VISUAL_DIAGRAM.txt | 2025-10-24 | Visual representation |
| FUNCTION_QUICK_REFERENCE.md | 2025-10-24 | Function examples |
| README_NEW_FUNCTIONS.md | 2025-10-24 | User guide |
| README_DOCUMENTATION.md | 2025-10-24 | This index (you are here) |

---

**Happy analyzing! 🎉**

All documentation is designed to work together. Start with this index, follow the recommended paths, and refer back as needed.
