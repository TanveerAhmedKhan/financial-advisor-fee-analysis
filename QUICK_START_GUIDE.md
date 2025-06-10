# Quick Start Guide - Financial Advisor Fee Data Processing

## TL;DR - Run These Commands in Order

```bash
# Step 1: Process individual CSV files
python src/process_csv_files.py

# Step 2: Combine processed files
python src/combine_processed_files.py

# Step 3: Add product structure analysis
python src/add_product_structure.py
```

## What Each Script Does

### 1. `process_csv_files.py`
- **Input:** Raw CSV files in `data/raw/`
- **Output:** Processed files in `data/processed/` (with "processed_" prefix)
- **Purpose:** Clean and standardize individual CSV files
- **Runtime:** ~5-30 minutes depending on file count

### 2. `combine_processed_files.py`
- **Input:** All processed CSV files from Step 1
- **Output:** `cleaned_fee_data_ordered.csv` (all records consolidated)
- **Purpose:** Consolidate all processed files into unified dataset
- **Runtime:** ~1-5 minutes

### 3. `add_product_structure.py`
- **Input:** `cleaned_fee_data_ordered.csv` from Step 2
- **Output:** `cleaned_fee_data_with_products.csv` (final dataset)
- **Purpose:** Analyze fee structures and identify product groupings
- **Runtime:** ~10-60 minutes depending on data complexity

## Prerequisites Checklist

- [ ] Python 3.7+ installed
- [ ] Required packages: pandas, numpy, logging, datetime, json, re, glob, collections
- [ ] Raw CSV files placed in `data/raw/` directory
- [ ] Write permissions for `data/processed/` directory

## Directory Structure

```
project_root/
├── src/
│   ├── process_csv_files.py      # Step 1
│   ├── combine_processed_files.py # Step 2
│   └── add_product_structure.py   # Step 3
├── data/
│   ├── raw/                      # Your input CSV files go here
│   └── processed/                # Output files (created automatically)
└── README_PROCESSING_INSTRUCTIONS.md
```

## Key Output Files

| File | Description | Created by |
|------|-------------|------------|
| `processed_*.csv` | Individual cleaned files | Step 1 |
| `cleaned_fee_data_ordered.csv` | All records combined | Step 2 |
| `cleaned_fee_data_with_products.csv` | **Final output with product analysis** | Step 3 |

## Error Checking

After each step, verify:

1. **Step 1:** Check that `data/processed/` contains `processed_*.csv` files
2. **Step 2:** Verify `cleaned_fee_data_ordered.csv` exists
3. **Step 3:** Confirm `cleaned_fee_data_with_products.csv` is created

## Log Files

- `data_processing.log` - Steps 1 & 2 details
- `product_structure.log` - Step 3 details

## Common Issues

| Problem | Solution |
|---------|----------|
| "No CSV files found" | Place raw CSV files in `data/raw/` |
| "Permission denied" | Check write permissions for `data/processed/` |
| "Module not found" | Install required Python packages |
| Script hangs | Check log files for progress/errors |

## Need More Details?

See `README_PROCESSING_INSTRUCTIONS.md` for comprehensive documentation including:
- Detailed explanations of each processing step
- Data structure requirements
- Advanced troubleshooting
- Performance considerations
- Recovery procedures

## Success Indicators

✅ **Step 1 Complete:** Multiple `processed_*.csv` files in `data/processed/`
✅ **Step 2 Complete:** Consolidated CSV file `cleaned_fee_data_ordered.csv` created
✅ **Step 3 Complete:** Final file with Product1-Product8 columns added

The final `cleaned_fee_data_with_products.csv` file is your complete, analyzed dataset ready for use!
