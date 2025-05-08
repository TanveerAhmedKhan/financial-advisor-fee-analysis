#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Advisor Fee Analysis - Data Processing Script

This script processes CSV files containing fee structures extracted from regulatory filings.
It reads each file individually, cleans and transforms the data, and saves the processed
output to a separate directory.

Author: TanveerAhmedKhan
"""

import os
import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)

# Constants
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'
PROCESSING_RECORD_FILE = 'data/processing_record.json'

def setup_directories():
    """Create necessary directories if they don't exist."""
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    # Create a record file if it doesn't exist
    if not os.path.exists(PROCESSING_RECORD_FILE):
        with open(PROCESSING_RECORD_FILE, 'w') as f:
            json.dump({"processed_files": []}, f)

def load_processing_record():
    """Load the record of processed files."""
    try:
        with open(PROCESSING_RECORD_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"processed_files": []}

def update_processing_record(filename):
    """Update the record with a newly processed file."""
    record = load_processing_record()
    if filename not in record["processed_files"]:
        record["processed_files"].append(filename)
        with open(PROCESSING_RECORD_FILE, 'w') as f:
            json.dump(record, f, indent=2)

def clean_percentage(value):
    """Extract percentage values from strings."""
    if pd.isna(value) or value == 'N/a' or value == 'N/A':
        return np.nan

    # Try to extract percentage values
    percentage_match = re.search(r'(\d+\.?\d*)%', str(value))
    if percentage_match:
        return float(percentage_match.group(1)) / 100  # Convert to decimal

    return value

def clean_dollar_amount(value):
    """Extract dollar amounts from strings."""
    if pd.isna(value) or value == 'N/a' or value == 'N/A':
        return np.nan

    # Try to extract dollar values
    dollar_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', str(value))
    if dollar_match:
        # Remove commas and convert to float
        amount = dollar_match.group(1).replace(',', '')
        return float(amount)

    return value

def clean_minimum_investment(value):
    """Clean and standardize minimum investment values."""
    if pd.isna(value) or value == 'No' or value == 'N/a' or value == 'N/A':
        return np.nan

    # Extract dollar amounts
    dollar_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', str(value))
    if dollar_match:
        # Remove commas and convert to float
        amount = dollar_match.group(1).replace(',', '')
        return float(amount)

    return value

def convert_yes_no(value):
    """Convert Yes/No values to boolean."""
    if isinstance(value, str):
        if value.lower() == 'yes':
            return True
        elif value.lower() == 'no':
            return False
    return value

def extract_year_month(filename):
    """Extract year and month from filename if available."""
    # Pattern for structured filenames like fee_analysis_adv_brochures_2020_apr.zip_extracted
    year_match = re.search(r'_(\d{4})_', filename)
    month_match = re.search(r'_(\d{4})_([a-z]+)', filename)

    # For formadv_part2 files, try to extract year from the filename
    formadv_match = re.search(r'formadv_part2_(\d+)_extracted', filename)

    year = None
    month = None

    if year_match:
        year = year_match.group(1)
        if month_match:
            month = month_match.group(2)
    elif formadv_match:
        # For formadv files, we don't have direct year/month in filename
        # We'll extract this information from the File Name column later
        pass

    return year, month

def extract_adviser_ids(filename):
    """Extract adviser IDs from filename."""
    match = re.search(r'(\d+)_(\d+)_\d+_(\d{8})_', filename)
    if match:
        id1 = match.group(1)
        id2 = match.group(2)
        filing_date = match.group(3)
        try:
            filing_date = datetime.strptime(filing_date, '%Y%m%d')
            return id1, id2, filing_date
        except:
            return id1, id2, None
    return None, None, None

def process_csv_file(file_path, output_dir):
    """
    Process a single CSV file.

    Args:
        file_path: Path to the input CSV file
        output_dir: Directory to save the processed file

    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        filename = os.path.basename(file_path)
        logging.info(f"Processing file: {filename}")

        # Read the CSV file
        df = pd.read_csv(file_path, low_memory=False)

        # Basic data cleaning
        # Replace problematic values
        df = df.replace(['N/a', 'N/A'], np.nan)

        # Extract month if available from filename (we'll only keep month, not year)
        _, month = extract_year_month(filename)

        # Extract adviser IDs and filing date from File Name column
        if 'File Name' in df.columns:
            df[['Adviser_ID1', 'Adviser_ID2', 'Filing_Date']] = df['File Name'].apply(
                lambda x: pd.Series(extract_adviser_ids(str(x)))
            )

            # If we couldn't extract month from filename, try to extract from File Name column
            if month is None:
                # Function to extract month from File Name
                def extract_month_from_file_name(file_name):
                    if pd.isna(file_name):
                        return None

                    # Try to extract month from file_name
                    month_match = re.search(r'_(\d{4})_([a-z]+)', str(file_name))
                    file_month = month_match.group(2) if month_match else None

                    return file_month

                # Apply extraction to File Name column
                df['Month'] = df['File Name'].apply(extract_month_from_file_name)
            else:
                # Use the month extracted from filename
                df['Month'] = month
        else:
            # Add empty columns for consistency
            df['Adviser_ID1'] = None
            df['Adviser_ID2'] = None
            df['Filing_Date'] = None
            df['Month'] = month

        # Extract year from filing date - this will be our only year column
        if 'Filing_Date' in df.columns and not df['Filing_Date'].isna().all():
            df['Filing_Year'] = df['Filing_Date'].dt.year
        else:
            df['Filing_Year'] = None

        # Clean and transform specific columns
        if 'Flat Fee' in df.columns:
            df['Flat_Fee_Flag'] = df['Flat Fee'].apply(convert_yes_no)

            # Extract flat fee value and clean the raw data
            def extract_flat_fee_value(fee_str):
                if pd.isna(fee_str) or fee_str == 'No' or fee_str == 'N/a' or fee_str == 'N/A' or fee_str == '-1' or \
                   fee_str == '"No fee information available."' or fee_str == 'No fee information available.':
                    return np.nan, None

                # Store the original value for reference
                original_value = str(fee_str)

                # Extract percentage
                pct_match = re.search(r'(\d+\.?\d*)%', str(fee_str))
                if pct_match:
                    return float(pct_match.group(1)) / 100, original_value  # Convert to decimal

                # Extract percentage range
                range_match = re.search(r'(\d+\.?\d*)% - (\d+\.?\d*)%', str(fee_str))
                if range_match:
                    # Take the average
                    min_pct = float(range_match.group(1)) / 100
                    max_pct = float(range_match.group(2)) / 100
                    return (min_pct + max_pct) / 2, original_value

                # Extract dollar amount
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', str(fee_str))
                if dollar_match:
                    return float(dollar_match.group(1).replace(',', '')), original_value

                return np.nan, original_value

            # Apply extraction and store both the value and original string
            df['Flat_Fee_Value'], df['Flat_Fee_Original'] = zip(*df['Flat Fee'].apply(extract_flat_fee_value))
        else:
            # Add empty columns for consistency
            df['Flat_Fee_Flag'] = None
            df['Flat_Fee_Value'] = None
            df['Flat_Fee_Original'] = None

        # Process fee thresholds
        for i in range(1, 9):
            col_name = f'Annual fee threshold {i}' if i == 1 else f'Annual fee Threshold {i}'
            if col_name in df.columns:
                # Extract fee percentage and preserve range information
                def extract_fee_info(fee_str):
                    if pd.isna(fee_str) or fee_str == 'N/a' or fee_str == 'N/A' or fee_str == '-1':
                        return np.nan, np.nan, np.nan, False

                    # Store original string
                    original_str = str(fee_str)
                    is_range = False

                    # Check for irregular fee structure with range
                    # Example: "$2,500+ (0.75% - 1%) [VERIFIED]"
                    irregular_range_match = re.search(r'\((\d+\.?\d*)% - (\d+\.?\d*)%\)', original_str)
                    if irregular_range_match:
                        min_pct = float(irregular_range_match.group(1)) / 100
                        max_pct = float(irregular_range_match.group(2)) / 100
                        avg_pct = (min_pct + max_pct) / 2
                        is_range = True
                        return min_pct, max_pct, avg_pct, is_range

                    # Extract percentage in parentheses
                    pct_match = re.search(r'\((\d+\.?\d*)%\)', original_str)
                    if pct_match:
                        pct_value = float(pct_match.group(1)) / 100
                        return pct_value, pct_value, pct_value, is_range

                    # Extract standalone percentage
                    standalone_match = re.search(r'(\d+\.?\d*)%', original_str)
                    if standalone_match:
                        pct_value = float(standalone_match.group(1)) / 100
                        return pct_value, pct_value, pct_value, is_range

                    return np.nan, np.nan, np.nan, is_range

                # Apply extraction and store min, max, and avg percentages
                df[f'Fee_Pct_Min_{i}'], df[f'Fee_Pct_Max_{i}'], df[f'Fee_Pct_{i}'], df[f'Fee_Is_Range_{i}'] = zip(
                    *df[col_name].apply(extract_fee_info)
                )

                # Extract threshold range and minimum fee
                def extract_threshold_range(threshold_str):
                    if pd.isna(threshold_str) or threshold_str == 'N/a' or threshold_str == 'N/A' or threshold_str == '-1':
                        return np.nan, np.nan, np.nan, False

                    original_str = str(threshold_str)
                    has_min_fee = False
                    min_fee = np.nan

                    # Check for minimum fee with percentage range
                    # Example: "$2,500+ (0.75% - 1%) [VERIFIED]"
                    min_fee_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)\+', original_str)
                    if min_fee_match and '(' in original_str and '%' in original_str:
                        min_fee = float(min_fee_match.group(1).replace(',', ''))
                        has_min_fee = True
                        # For this case, we don't have a traditional threshold range
                        return 0, np.inf, min_fee, has_min_fee

                    # Extract dollar range
                    range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?) - \$(\d+(?:,\d+)*(?:\.\d+)?)', original_str)
                    if range_match:
                        lower = float(range_match.group(1).replace(',', ''))
                        upper = float(range_match.group(2).replace(',', ''))
                        return lower, upper, min_fee, has_min_fee

                    # Extract lower bound with plus (standard case)
                    if min_fee_match:
                        lower = float(min_fee_match.group(1).replace(',', ''))
                        return lower, np.inf, min_fee, has_min_fee

                    # Extract upper bound with less than
                    upper_match = re.search(r'< \$(\d+(?:,\d+)*(?:\.\d+)?)', original_str)
                    if upper_match:
                        upper = float(upper_match.group(1).replace(',', ''))
                        return 0, upper, min_fee, has_min_fee

                    # Extract single dollar amount
                    single_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', original_str)
                    if single_match:
                        amount = float(single_match.group(1).replace(',', ''))
                        return amount, amount, min_fee, has_min_fee

                    return np.nan, np.nan, min_fee, has_min_fee

                df[f'Threshold_Lower_{i}'], df[f'Threshold_Upper_{i}'], df[f'Min_Fee_{i}'], df[f'Has_Min_Fee_{i}'] = zip(
                    *df[col_name].apply(extract_threshold_range)
                )

        # Clean minimum investment
        if 'Minimum investment (Amount/No)' in df.columns:
            def extract_min_investment(min_str):
                if pd.isna(min_str) or min_str == 'N/a' or min_str == 'N/A' or min_str == 'No' or min_str == '-1':
                    return 0, 0

                # Extract dollar amount
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', str(min_str))
                if dollar_match:
                    amount = float(dollar_match.group(1).replace(',', ''))
                    return amount, 1

                return 0, 0

            df['Min_Investment_Amount'], df['Has_Min_Investment'] = zip(
                *df['Minimum investment (Amount/No)'].apply(extract_min_investment)
            )
        else:
            # Add empty columns for consistency
            df['Min_Investment_Amount'] = 0
            df['Has_Min_Investment'] = 0

        # Process negotiable fees
        if 'Negotiable (Yes/No)' in df.columns:
            df['Is_Negotiable'] = df['Negotiable (Yes/No)'].apply(
                lambda x: 1 if not pd.isna(x) and str(x).lower() == 'yes' else 0
            )
        else:
            # Add empty column for consistency
            df['Is_Negotiable'] = 0

        # Process negotiable threshold
        if 'Negotiable threshold (Number/ N/A)' in df.columns:
            def extract_negotiable_threshold(thresh_str):
                if pd.isna(thresh_str) or thresh_str == 'N/a' or thresh_str == 'N/A' or thresh_str == 'No' or thresh_str == '-1':
                    return 0

                # Extract dollar amount
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', str(thresh_str))
                if dollar_match:
                    amount = float(dollar_match.group(1).replace(',', ''))
                    return amount

                return 0

            df['Negotiable_Threshold'] = df['Negotiable threshold (Number/ N/A)'].apply(extract_negotiable_threshold)
        else:
            # Add empty column for consistency
            df['Negotiable_Threshold'] = 0

        # Calculate verification ratio
        if 'Verification Summary' in df.columns:
            def calculate_verification_ratio(verification_str):
                if pd.isna(verification_str) or verification_str == '':
                    return 0

                # Count the number of verified cells
                cells = str(verification_str).split(',')
                return len(cells) / 13  # Total possible cells is 13

            df['Verification_Ratio'] = df['Verification Summary'].apply(calculate_verification_ratio)
        else:
            # Add empty column for consistency
            df['Verification_Ratio'] = 0

        # Identify products based on fee structure and handle irregular fee structures
        def identify_products(row):
            products = []
            current_product = []
            fee_ranges = []
            min_fees = []

            # First, check for irregular fee structures with ranges in any threshold
            for i in range(1, 9):
                is_range = row.get(f'Fee_Is_Range_{i}', False)
                has_min_fee = row.get(f'Has_Min_Fee_{i}', False)

                if is_range:
                    min_pct = row.get(f'Fee_Pct_Min_{i}')
                    max_pct = row.get(f'Fee_Pct_Max_{i}')
                    min_fee = row.get(f'Min_Fee_{i}')

                    if not pd.isna(min_pct) and not pd.isna(max_pct):
                        fee_ranges.append((i, min_pct, max_pct))

                if has_min_fee:
                    min_fee = row.get(f'Min_Fee_{i}')
                    if not pd.isna(min_fee):
                        min_fees.append((i, min_fee))

            # If we found fee ranges, prioritize them
            if fee_ranges:
                # Sort by threshold index
                fee_ranges.sort(key=lambda x: x[0])

                # Create a product with the fee ranges
                for idx, (i, min_pct, max_pct) in enumerate(fee_ranges):
                    # Use position in fee_ranges list + 1 as the new index
                    new_idx = idx + 1
                    current_product.append((new_idx, 0, min_pct))  # Use min percentage

                products.append(current_product)

                # Also store the max percentages in a separate product
                if len(fee_ranges) > 1:
                    max_product = []
                    for idx, (i, min_pct, max_pct) in enumerate(fee_ranges):
                        new_idx = idx + 1
                        max_product.append((new_idx, 0, max_pct))  # Use max percentage

                    products.append(max_product)

                # If we have min fees, add them to the product info
                if min_fees:
                    for i, min_fee in min_fees:
                        # Add min fee to product metadata
                        pass  # This would be implemented in a more complex way

                return products

            # If no fee ranges, proceed with regular product identification
            # Check thresholds 1-8 for fee structures
            for i in range(1, 9):
                fee_pct = row.get(f'Fee_Pct_{i}')
                lower_bound = row.get(f'Threshold_Lower_{i}')

                # Also check for irregular fee structures in threshold 7
                if i == 7 and pd.isna(fee_pct):
                    # Check if there's an irregular fee structure in threshold 7
                    col_name = 'Annual fee Threshold 7'
                    if col_name in row and not pd.isna(row[col_name]) and '%' in str(row[col_name]):
                        # Try to extract fee information
                        if 'Fee_Pct_Min_7' in row and not pd.isna(row['Fee_Pct_Min_7']):
                            fee_pct = row['Fee_Pct_Min_7']
                            lower_bound = 0  # Default to 0 for irregular structures

                if not pd.isna(fee_pct) and not pd.isna(lower_bound):
                    # If this is the first threshold or if the lower bound is 0/near 0, it's a new product
                    if not current_product or lower_bound < 1000:  # Assuming thresholds below $1000 indicate a new product
                        if current_product:
                            products.append(current_product)
                        current_product = [(i, lower_bound, fee_pct)]
                    else:
                        current_product.append((i, lower_bound, fee_pct))

            # Add the last product if it exists
            if current_product:
                products.append(current_product)

            return products

        df['Products'] = df.apply(identify_products, axis=1)
        df['Num_Products'] = df['Products'].apply(len)

        # Restructure fee information to ensure it's reported in fee_pct_1 first, then _2, _3, etc.
        def restructure_fee_info(row):
            # Get all fee percentages and their thresholds
            fee_info = []
            for i in range(1, 9):
                fee_pct = row.get(f'Fee_Pct_{i}')
                fee_min = row.get(f'Fee_Pct_Min_{i}')
                fee_max = row.get(f'Fee_Pct_Max_{i}')
                is_range = row.get(f'Fee_Is_Range_{i}', False)
                lower_bound = row.get(f'Threshold_Lower_{i}')
                upper_bound = row.get(f'Threshold_Upper_{i}')
                min_fee = row.get(f'Min_Fee_{i}')
                has_min_fee = row.get(f'Has_Min_Fee_{i}', False)

                if not pd.isna(fee_pct) or is_range or has_min_fee:
                    fee_info.append((i, fee_pct, fee_min, fee_max, is_range, lower_bound, upper_bound, min_fee, has_min_fee))

            # If we have fee ranges, prioritize them
            range_info = [info for info in fee_info if info[4]]  # is_range is True

            if range_info:
                # For the specific case mentioned: "$2,500+ (0.75% - 1%) [VERIFIED]"
                # We want to populate fee_pct_1 and fee_pct_2 with 1% and 0.75%, respectively
                for i, (orig_idx, fee_pct, fee_min, fee_max, is_range, lower_bound, upper_bound, min_fee, has_min_fee) in enumerate(range_info):
                    # Clear the original fee percentage
                    row[f'Fee_Pct_{orig_idx}'] = np.nan

                    # Set the min and max percentages in fee_pct_1 and fee_pct_2
                    if i == 0:  # First range found
                        row['Fee_Pct_1'] = fee_max  # Higher percentage first (1%)
                        row['Fee_Pct_2'] = fee_min  # Lower percentage second (0.75%)

                        # Set the minimum fee if available
                        if has_min_fee and not pd.isna(min_fee):
                            row['Min_Investment_Amount'] = min_fee
                            row['Has_Min_Investment'] = 1

            return row

        # Apply restructuring to each row
        df = df.apply(restructure_fee_info, axis=1)

        # Calculate effective fees for different portfolio sizes
        def calculate_effective_fee(products, aum):
            if not products:
                return np.nan

            # Sort products by first fee (cheapest first)
            products_with_fees = [p for p in products if p and len(p) > 0 and len(p[0]) > 2 and p[0][2] is not None]
            if not products_with_fees:
                return np.nan

            products_with_fees.sort(key=lambda p: p[0][2])

            # Calculate effective fee for the cheapest product
            cheapest_product = products_with_fees[0]

            # Sort thresholds by lower bound
            thresholds = sorted(cheapest_product, key=lambda t: t[1])

            total_fee = 0
            remaining_aum = aum

            for i, (_, lower, fee_pct) in enumerate(thresholds):
                if i < len(thresholds) - 1:
                    next_lower = thresholds[i+1][1]
                    bracket_size = next_lower - lower
                    if remaining_aum <= 0:
                        break
                    bracket_fee = min(bracket_size, remaining_aum) * fee_pct
                    total_fee += bracket_fee
                    remaining_aum -= bracket_size
                else:
                    # Last threshold
                    if remaining_aum > 0:
                        total_fee += remaining_aum * fee_pct

            return total_fee / aum if aum > 0 else np.nan

        # Calculate effective fees for $1M and $5M portfolios
        df['Effective_Fee_1M'] = df['Products'].apply(lambda p: calculate_effective_fee(p, 1000000))
        df['Effective_Fee_5M'] = df['Products'].apply(lambda p: calculate_effective_fee(p, 5000000))

        # Add metadata
        df['Processing_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['Source_Filename'] = filename

        # Save the processed file
        output_path = os.path.join(output_dir, f"processed_{filename}")
        df.to_csv(output_path, index=False)

        logging.info(f"Successfully processed and saved: {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return False

def main():
    """Main function to process all CSV files."""
    setup_directories()
    record = load_processing_record()
    processed_files = record["processed_files"]

    # Get all CSV files in the raw data directory
    csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith('.csv')]
    total_files = len(csv_files)

    logging.info(f"Found {total_files} CSV files in {RAW_DATA_DIR}")
    logging.info(f"{len(processed_files)} files have been processed previously")

    # Process each file that hasn't been processed yet
    files_processed = 0
    for i, filename in enumerate(csv_files):
        if filename in processed_files:
            logging.info(f"Skipping already processed file: {filename}")
            continue

        file_path = os.path.join(RAW_DATA_DIR, filename)
        success = process_csv_file(file_path, PROCESSED_DATA_DIR)

        if success:
            update_processing_record(filename)
            files_processed += 1

        # Log progress
        logging.info(f"Progress: {i+1}/{total_files} files ({(i+1)/total_files*100:.1f}%)")

    logging.info(f"Processing complete. {files_processed} new files processed.")

if __name__ == "__main__":
    main()
