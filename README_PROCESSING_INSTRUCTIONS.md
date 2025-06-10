# Financial Advisor Fee Data Processing Instructions

This README provides step-by-step instructions for running the financial advisor fee data processing pipeline. The pipeline consists of three main Python scripts that must be executed in a specific order.

## Overview

The data processing pipeline transforms raw CSV files containing financial advisor fee structures into cleaned, consolidated datasets with product structure analysis. The pipeline consists of three sequential stages:

1. **Individual File Processing** (`process_csv_files.py`)
2. **Data Consolidation** (`combine_processed_files.py`)
3. **Product Structure Analysis** (`add_product_structure.py`)

## Prerequisites

### Python Environment
- Python 3.7 or higher
- Required packages: pandas, numpy, logging, datetime, json, re, glob, collections

### Data Structure
Ensure your project has the following directory structure:
```
project_root/
├── src/
│   ├── process_csv_files.py
│   ├── combine_processed_files.py
│   └── add_product_structure.py
├── data/
│   ├── raw/                    # Input CSV files
│   └── processed/              # Output directory (created automatically)
└── README_PROCESSING_INSTRUCTIONS.md
```

### Input Data
- Raw CSV files should be placed in the `data/raw/` directory
- Files should contain financial advisor fee structure data extracted from regulatory filings
- Expected columns include: File Name, Annual fee threshold 1-8, Flat Fee, Minimum investment, Negotiable, etc.

## Step-by-Step Processing Instructions

### Step 1: Process Individual CSV Files

**Script:** `src/process_csv_files.py`

**Purpose:**
- Processes each raw CSV file individually
- Cleans and standardizes fee data
- Extracts adviser IDs and filing dates from filenames
- Handles fee thresholds, percentages, and flat fees
- Creates processed files with consistent column structure

**How to Run:**
```bash
cd project_root
python src/process_csv_files.py
```

**What it does:**
- Reads all CSV files from `data/raw/`
- Applies data cleaning and transformation rules
- Extracts fee information from complex text patterns
- Handles currency conversions and threshold ranges
- Saves processed files to `data/processed/` with "processed_" prefix
- Creates a processing record to track completed files
- Generates detailed logs in `data_processing.log`

**Key Features:**
- Skips already processed files (resumable)
- Handles "Under $X" threshold patterns
- Extracts fee ranges and single values
- Processes minimum investment requirements
- Standardizes negotiable fee information

**Expected Output:**
- Multiple `processed_*.csv` files in `data/processed/`
- `data/processing_record.json` tracking file
- `data_processing.log` with processing details

### Step 2: Combine Processed Files

**Script:** `src/combine_processed_files.py`

**Purpose:**
- Consolidates all processed CSV files into a unified dataset
- Extracts adviser IDs and maintains data integrity
- Preserves original file order

**How to Run:**
```bash
cd project_root
python src/combine_processed_files.py
```

**What it does:**
- Combines all `processed_*.csv` files from `data/processed/`
- Extracts adviser IDs from filenames using regex patterns
- Creates consolidated dataset: `cleaned_fee_data_ordered.csv` - All records
- Maintains original processing order
- Handles data integrity validation

**Key Features:**
- Maintains data lineage through source file tracking
- Validates data integrity before saving
- Preserves original processing order

**Expected Output:**
- `data/processed/cleaned_fee_data_ordered.csv` - Complete consolidated dataset
- Updated `data_processing.log` with consolidation details

### Step 3: Add Product Structure Analysis

**Script:** `src/add_product_structure.py`

**Purpose:**
- Analyzes fee structures to identify distinct "products" or fee schedules
- Groups fee thresholds and percentages into logical product structures
- Adds product columns to the cleaned dataset

**How to Run:**
```bash
cd project_root
python src/add_product_structure.py
```

**What it does:**
- Reads `cleaned_fee_data_ordered.csv` from Step 2
- Applies sophisticated product identification algorithms
- Processes fee structures in four phases:
  1. Simple product structures (monotonic thresholds/fees)
  2. Fee range cases (threshold patterns with fee ranges)
  3. Multiple product structures (complex fee schedules)
  4. Multiple fee schedule detection
- Adds Product1-Product8 columns with formatted product information
- Saves enhanced dataset with product structure analysis

**Key Features:**
- Handles complex fee structures with multiple products
- Supports fee ranges and threshold variations
- Processes "Under $X" patterns correctly
- Groups consecutive threshold ranges into single products
- Validates product structures for consistency

**Expected Output:**
- `data/processed/cleaned_fee_data_with_products.csv` - Final dataset with product analysis
- `product_structure.log` with detailed processing information

## Processing Pipeline Summary

```
Raw CSV Files (data/raw/)
         ↓
Step 1: process_csv_files.py
         ↓
Processed Files (data/processed/processed_*.csv)
         ↓
Step 2: combine_processed_files.py
         ↓
Consolidated File (cleaned_fee_data_ordered.csv)
         ↓
Step 3: add_product_structure.py
         ↓
Final Output (cleaned_fee_data_with_products.csv)
```

## Important Notes

### Execution Order
**CRITICAL:** Scripts must be run in the exact order specified above. Each script depends on the output of the previous step.

### Error Handling
- All scripts include comprehensive error handling and logging
- Processing can be resumed if interrupted (Step 1 tracks completed files)
- Check log files for detailed error information if issues occur

### Data Validation
- Each step validates input data before processing
- Scripts will skip processing if valid output already exists
- Use force regeneration flags if you need to reprocess existing data

### Performance Considerations
- Step 1 processes files individually and can take significant time for large datasets
- Step 2 loads all processed files into memory simultaneously
- Step 3 performs complex analysis and may require substantial processing time

### Output Files
- All output files are saved in CSV format with headers
- Original data order is preserved throughout the pipeline
- Product structure information is added as additional columns, not separate files

## Troubleshooting

### Common Issues
1. **Missing input files:** Ensure raw CSV files are in `data/raw/`
2. **Permission errors:** Check write permissions for `data/processed/` directory
3. **Memory issues:** For very large datasets, consider processing in smaller batches
4. **Column mismatches:** Verify input CSV files have expected column structure

### Log Files
- `data_processing.log` - Steps 1 and 2 processing details
- `product_structure.log` - Step 3 product analysis details

### Recovery
- Step 1: Delete entries from `data/processing_record.json` to reprocess specific files
- Step 2: Delete consolidated CSV files to force regeneration
- Step 3: Delete product-enhanced CSV file to rerun product analysis

## Expected Results

After successful completion of all three steps, you will have:

1. **Individual processed files** - Clean, standardized data for each input file
2. **Consolidated dataset** - Combined data from all processed files
3. **Product-enhanced dataset** - Final analysis with fee structure products identified

The final output (`cleaned_fee_data_with_products.csv`) contains all original data plus:
- Standardized fee information
- Adviser ID extraction
- Product structure analysis (Product1-Product8 columns)
- Data quality flags and metadata

This dataset is ready for further analysis, reporting, or integration with other systems.
