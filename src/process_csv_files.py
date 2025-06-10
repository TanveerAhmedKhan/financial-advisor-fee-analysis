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
    """
    Extract year and month from filename if available.

    Note: This function is kept for backward compatibility but is no longer used
    in the main processing flow. Month is now extracted directly from Filing_Date.
    """
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

        # Extract adviser IDs and filing date from File Name column
        if 'File Name' in df.columns:
            df[['Adviser_ID1', 'Adviser_ID2', 'Filing_Date']] = df['File Name'].apply(
                lambda x: pd.Series(extract_adviser_ids(str(x)))
            )

            # Extract month directly from Filing_Date
            def extract_month_from_filing_date(filing_date):
                if pd.isna(filing_date):
                    return None

                # Convert month number to month name (abbreviated)
                month_names = {
                    1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun',
                    7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'
                }

                try:
                    # Extract month number from datetime object
                    month_num = filing_date.month
                    return month_names.get(month_num)
                except:
                    return None

            # Apply extraction to Filing_Date column
            df['Month'] = df['Filing_Date'].apply(extract_month_from_filing_date)
        else:
            # Add empty columns for consistency
            df['Adviser_ID1'] = None
            df['Adviser_ID2'] = None
            df['Filing_Date'] = None
            df['Month'] = None

        # Extract year from filing date - this will be our only year column
        if 'Filing_Date' in df.columns and not df['Filing_Date'].isna().all():
            df['Filing_Year'] = df['Filing_Date'].dt.year
        else:
            df['Filing_Year'] = None

        # Clean and transform specific columns
        if 'Flat Fee' in df.columns:
            # Function to clean and standardize flat fee values
            def clean_flat_fee(fee_str):
                # Handle NaN, None, and empty strings as missing data
                if pd.isna(fee_str) or fee_str is None or fee_str == '' or fee_str == '-1':
                    return np.nan

                # Convert to string for consistent processing
                fee_str = str(fee_str).strip()

                # Remove quotes and normalize
                fee_str = fee_str.replace('"', '').replace("'", "").strip()

                # Handle "No fee information available" and similar phrases as missing data
                if ('no fee information' in fee_str.lower() or
                    'response not available' in fee_str.lower() or
                    'n/a' in fee_str.lower() or
                    fee_str.lower() == 'na'):
                    return np.nan

                # Handle explicit No cases
                if fee_str.lower() in ['no', 'none', 'no fee', 'no fee.']:
                    return "No"

                # Handle explicit Yes cases
                if fee_str.lower() == 'yes':
                    return "Yes"

                # Check for flat fee indicators with "No"
                if ('flat' in fee_str.lower() or 'fixed' in fee_str.lower()) and 'no' in fee_str.lower():
                    return "No"

                # Check for fee ranges (indicated by hyphen between values)
                # This pattern looks for dollar amounts, percentages, or numbers separated by a hyphen
                if re.search(r'(\$\d+[\d,]*(\.\d+)?|\d+[\d,]*(\.\d+)?%|\d+[\d,]*(\.\d+)?) ?- ?(\$\d+[\d,]*(\.\d+)?|\d+[\d,]*(\.\d+)?%|\d+[\d,]*(\.\d+)?)', fee_str):
                    return "No"  # Fee ranges are not flat fees

                # Check for flat fee or fixed fee indicators
                if 'flat' in fee_str.lower() or 'fixed' in fee_str.lower():
                    return "Yes"

                # If it contains a currency symbol, number with comma, or percentage, it's likely a flat fee
                # But only if it's not part of a range (which we checked above)
                if (re.search(r'[\$\£\€\¥]|\d+,\d+|\d+\.\d+%|chf', fee_str.lower()) or
                    re.search(r'\d+ ?%', fee_str.lower())):
                    return "Yes"

                # If it mentions "negotiable" or specific fee terms, it's likely a flat fee
                if ('negotiable' in fee_str.lower() or 'fee' in fee_str.lower() or
                    'amount' in fee_str.lower() or 'rate' in fee_str.lower() or
                    'percentage' in fee_str.lower() or 'management' in fee_str.lower()):
                    return "Yes"

                # For any remaining unclassified values, treat as missing data
                return np.nan

            # Store the original flat fee values
            df['Flat_Fee_Original'] = df['Flat Fee'].copy()

            # Clean the flat fee column
            df['Flat Fee'] = df['Flat Fee'].apply(clean_flat_fee)

            # Convert to boolean flag (NaN values will become False)
            df['Flat_Fee_Flag'] = df['Flat Fee'].apply(lambda x: True if x == "Yes" else False)

            # Extract flat fee value from the original string
            def extract_flat_fee_value(fee_str):
                if pd.isna(fee_str) or fee_str == 'No' or fee_str == 'N/a' or fee_str == 'N/A' or fee_str == '-1' or \
                   fee_str == '"No fee information available."' or fee_str == 'No fee information available.':
                    return np.nan

                # Extract dollar amount range
                dollar_range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?) - \$(\d+(?:,\d+)*(?:\.\d+)?)', str(fee_str))
                if dollar_range_match:
                    min_amount = float(dollar_range_match.group(1).replace(',', ''))
                    max_amount = float(dollar_range_match.group(2).replace(',', ''))
                    # Return both values as a tuple
                    return (min_amount, max_amount)

                # Extract percentage range
                pct_range_match = re.search(r'(\d+\.?\d*)% - (\d+\.?\d*)%', str(fee_str))
                if pct_range_match:
                    min_pct = float(pct_range_match.group(1)) / 100
                    max_pct = float(pct_range_match.group(2)) / 100
                    # Return both values as a tuple
                    return (min_pct, max_pct)

                # Extract percentage
                pct_match = re.search(r'(\d+\.?\d*)%', str(fee_str))
                if pct_match:
                    pct_value = float(pct_match.group(1)) / 100
                    return pct_value  # Convert to decimal

                # Extract dollar amount
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', str(fee_str))
                if dollar_match:
                    return float(dollar_match.group(1).replace(',', ''))

                return np.nan

            # Apply extraction to get the numerical value(s)
            extracted_values = df['Flat_Fee_Original'].apply(extract_flat_fee_value)

            # Create columns for lower and upper bounds
            df['Flat_Fee_Lower'] = np.nan
            df['Flat_Fee_Upper'] = np.nan

            # Process the extracted values
            for idx, value in enumerate(extracted_values):
                if isinstance(value, tuple) and len(value) == 2:
                    # If it's a range (tuple), store lower and upper bounds
                    df.at[idx, 'Flat_Fee_Lower'] = value[0]
                    df.at[idx, 'Flat_Fee_Upper'] = value[1]
                else:
                    # If it's a single value, store it in both columns
                    df.at[idx, 'Flat_Fee_Lower'] = value
                    df.at[idx, 'Flat_Fee_Upper'] = value
        else:
            # Add empty columns for consistency
            df['Flat_Fee_Flag'] = None
            df['Flat_Fee_Lower'] = None
            df['Flat_Fee_Upper'] = None
            df['Flat_Fee_Original'] = None

        # Process fee thresholds
        for i in range(1, 9):
            col_name = f'Annual fee threshold {i}' if i == 1 else f'Annual fee Threshold {i}'
            if col_name in df.columns:
                # Extract fee percentage and preserve range information
                def extract_fee_info(fee_str, col_idx):
                    # Improved handling of empty or invalid cells
                    if pd.isna(fee_str) or fee_str == 'N/a' or fee_str == 'N/A' or fee_str == '-1' or str(fee_str).strip() == '':
                        return np.nan, np.nan, False, col_idx

                    # Store original string
                    original_str = str(fee_str)
                    is_range = False

                    # First priority: Check for a percentage range within parentheses
                    range_match = re.search(r'\(([\d\.]+)% ?- ?([\d\.]+)%\)', original_str)
                    if range_match:
                        min_pct = float(range_match.group(1)) / 100
                        max_pct = float(range_match.group(2)) / 100
                        is_range = True
                        return min_pct, max_pct, is_range, col_idx

                    # Second priority: Check for a single percentage within parentheses
                    pct_match = re.search(r'\(([\d\.]+)%\)', original_str)
                    if pct_match:
                        pct_value = float(pct_match.group(1)) / 100
                        return pct_value, pct_value, is_range, col_idx

                    # Third priority: Check for percentage range NOT in parentheses (like "0.32% - 2.50%")
                    range_no_parens = re.search(r'([\d\.]+)% ?- ?([\d\.]+)%', original_str)
                    if range_no_parens:
                        min_pct = float(range_no_parens.group(1)) / 100
                        max_pct = float(range_no_parens.group(2)) / 100
                        is_range = True
                        return min_pct, max_pct, is_range, col_idx

                    # Fourth priority: Check for single percentage NOT in parentheses
                    single_pct_no_parens = re.search(r'([\d\.]+)%', original_str)
                    if single_pct_no_parens:
                        pct_value = float(single_pct_no_parens.group(1)) / 100
                        return pct_value, pct_value, is_range, col_idx

                    # If no percentage found, return NaN
                    return np.nan, np.nan, is_range, col_idx

                # Apply extraction and store min and max percentages
                # Extract all values but only store the ones we need (excluding Fee_Source)
                fee_info_values = df[col_name].apply(lambda x: extract_fee_info(x, i))
                df[f'Fee_Pct_Min_{i}'] = [val[0] for val in fee_info_values]
                df[f'Fee_Pct_Max_{i}'] = [val[1] for val in fee_info_values]
                df[f'Fee_Is_Range_{i}'] = [val[2] for val in fee_info_values]
                # We're not storing Fee_Source anymore

                # Extract threshold range - enhanced approach with "Under" pattern support
                def extract_threshold_range(threshold_str):
                    # Improved handling of empty or invalid cells
                    if pd.isna(threshold_str) or threshold_str == 'N/a' or threshold_str == 'N/A' or threshold_str == '-1' or str(threshold_str).strip() == '':
                        return np.nan, np.nan

                    original_str = str(threshold_str)

                    # Helper function to parse currency amounts with various formats
                    def parse_currency_amount(amount_str):
                        """Parse currency amounts handling K, M, B suffixes and comma separators."""
                        if not amount_str:
                            return None

                        # Remove commas and convert to lowercase for processing
                        clean_str = amount_str.replace(',', '').lower()

                        # Handle K (thousands) suffix
                        if 'k' in clean_str:
                            num_match = re.search(r'(\d+(?:\.\d+)?)', clean_str)
                            if num_match:
                                return float(num_match.group(1)) * 1000

                        # Handle M (millions) suffix
                        elif 'm' in clean_str:
                            num_match = re.search(r'(\d+(?:\.\d+)?)', clean_str)
                            if num_match:
                                return float(num_match.group(1)) * 1000000

                        # Handle B (billions) suffix
                        elif 'b' in clean_str:
                            num_match = re.search(r'(\d+(?:\.\d+)?)', clean_str)
                            if num_match:
                                return float(num_match.group(1)) * 1000000000

                        # Handle regular numbers
                        else:
                            num_match = re.search(r'(\d+(?:\.\d+)?)', clean_str)
                            if num_match:
                                return float(num_match.group(1))

                        return None

                    # NEW: Extract "Under" patterns (case-insensitive)
                    # Patterns like "Under $500,000", "under $500K", "Under $0.5M"
                    under_patterns = [
                        r'under\s+\$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?)',  # "under $500K", "under $0.5M"
                        r'under\s+\$(\d+(?:,\d+)*(?:\.\d+)?)',        # "under $500,000"
                    ]

                    for pattern in under_patterns:
                        under_match = re.search(pattern, original_str, re.IGNORECASE)
                        if under_match:
                            amount_str = under_match.group(1)
                            upper_amount = parse_currency_amount(amount_str)
                            if upper_amount is not None:
                                # "Under $X" becomes "$0 - $X"
                                return 0, upper_amount

                    # Extract dollar range
                    range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?) - \$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?)', original_str, re.IGNORECASE)
                    if range_match:
                        lower_str = range_match.group(1)
                        upper_str = range_match.group(2)
                        lower = parse_currency_amount(lower_str)
                        upper = parse_currency_amount(upper_str)
                        if lower is not None and upper is not None:
                            return lower, upper

                    # Extract lower bound with plus
                    plus_patterns = [
                        r'\$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?)\+',  # "$500K+", "$0.5M+"
                        r'\$(\d+(?:,\d+)*(?:\.\d+)?)\+',        # "$500,000+"
                    ]

                    for pattern in plus_patterns:
                        plus_match = re.search(pattern, original_str, re.IGNORECASE)
                        if plus_match:
                            amount_str = plus_match.group(1)
                            lower = parse_currency_amount(amount_str)
                            if lower is not None:
                                return lower, np.inf

                    # Extract upper bound with less than
                    upper_patterns = [
                        r'< \$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?)',  # "< $500K"
                        r'< \$(\d+(?:,\d+)*(?:\.\d+)?)',        # "< $500,000"
                    ]

                    for pattern in upper_patterns:
                        upper_match = re.search(pattern, original_str, re.IGNORECASE)
                        if upper_match:
                            amount_str = upper_match.group(1)
                            upper = parse_currency_amount(amount_str)
                            if upper is not None:
                                return 0, upper

                    # Extract single dollar amount
                    single_patterns = [
                        r'\$(\d+(?:,\d+)*(?:\.\d+)?[kmb]?)',  # "$500K", "$0.5M"
                        r'\$(\d+(?:,\d+)*(?:\.\d+)?)',        # "$500,000"
                    ]

                    for pattern in single_patterns:
                        single_match = re.search(pattern, original_str, re.IGNORECASE)
                        if single_match:
                            amount_str = single_match.group(1)
                            amount = parse_currency_amount(amount_str)
                            if amount is not None:
                                return amount, amount

                    return np.nan, np.nan

                # Initialize threshold columns with NaN values
                df[f'Threshold_Lower_{i}'], df[f'Threshold_Upper_{i}'] = zip(
                    *df[col_name].apply(extract_threshold_range)
                )

                # Special handling: If we have fee percentages but no threshold information,
                # default to $0+ (i.e., Threshold_Lower = 0.0, Threshold_Upper = inf)
                has_fee_data = (~pd.isna(df[f'Fee_Pct_Min_{i}'])) | (~pd.isna(df[f'Fee_Pct_Max_{i}']))
                has_threshold_data = (~pd.isna(df[f'Threshold_Lower_{i}'])) | (~pd.isna(df[f'Threshold_Upper_{i}']))

                # For rows that have fee data but no threshold data, set default thresholds
                default_threshold_mask = has_fee_data & (~has_threshold_data)
                df.loc[default_threshold_mask, f'Threshold_Lower_{i}'] = 0.0
                df.loc[default_threshold_mask, f'Threshold_Upper_{i}'] = np.inf

        # Clean minimum investment
        if 'Minimum investment (Amount/No)' in df.columns:
            def extract_min_investment(min_str):
                # Default values
                amount = 0
                has_min = 0
                currency = "USD"  # Default currency

                # Handle missing or negative values
                if pd.isna(min_str) or min_str == '-1':
                    return amount, has_min, currency

                # Convert to string for consistent processing
                min_str = str(min_str).strip()

                # Handle explicit "No" values
                if min_str.lower() in ['no', 'none', 'n/a', 'na', 'not applicable']:
                    return amount, has_min, currency

                # Handle explicit "Yes" values (without specific amount)
                if min_str.lower() == 'yes':
                    return 1, 1, currency  # Use 1 as a placeholder for "Yes" without specific amount

                # Handle "Negotiable" values
                if 'negotiable' in min_str.lower():
                    return 1, 1, currency  # Use 1 as a placeholder for negotiable minimums

                # Handle "Varies" or "Depends" values
                if 'varies' in min_str.lower() or 'depend' in min_str.lower():
                    return 1, 1, currency  # Use 1 as a placeholder

                # Handle foreign currencies (e.g., CHF, EUR, GBP)
                foreign_currency_patterns = [
                    (r'CHF\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'CHF'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*CHF', 'CHF'),  # Handle "1,000 CHF" format
                    (r'EUR\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'EUR'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*EUR', 'EUR'),  # Handle "1,000 EUR" format
                    (r'GBP\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'GBP'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*GBP', 'GBP'),  # Handle "1,000 GBP" format
                    (r'£\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'GBP'),
                    (r'€\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'EUR')
                ]

                for pattern, curr in foreign_currency_patterns:
                    match = re.search(pattern, min_str)
                    if match:
                        amount = float(match.group(1).replace(',', ''))
                        has_min = 1
                        currency = curr
                        return amount, has_min, currency

                # Handle dollar ranges - extract the lower bound
                dollar_range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)(?:\s*[-–]\s*\$(\d+(?:,\d+)*(?:\.\d+)?))?', min_str)
                if dollar_range_match:
                    # Get the first amount (lower bound)
                    amount = float(dollar_range_match.group(1).replace(',', ''))
                    has_min = 1
                    return amount, has_min, currency

                # Handle "up to $X" patterns - use the amount as minimum
                up_to_match = re.search(r'up to \$(\d+(?:,\d+)*(?:\.\d+)?)', min_str.lower())
                if up_to_match:
                    amount = float(up_to_match.group(1).replace(',', ''))
                    has_min = 1
                    return amount, has_min, currency

                # Handle text with "million" or "billion"
                million_match = re.search(r'(\d+(?:\.\d+)?)\s*million', min_str.lower())
                if million_match:
                    amount = float(million_match.group(1)) * 1000000
                    has_min = 1
                    return amount, has_min, currency

                billion_match = re.search(r'(\d+(?:\.\d+)?)\s*billion', min_str.lower())
                if billion_match:
                    amount = float(billion_match.group(1)) * 1000000000
                    has_min = 1
                    return amount, has_min, currency

                # Extract any dollar amount as a fallback
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', min_str)
                if dollar_match:
                    amount = float(dollar_match.group(1).replace(',', ''))
                    has_min = 1
                    return amount, has_min, currency

                # Extract any numeric value as a last resort
                numeric_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)', min_str)
                if numeric_match:
                    amount = float(numeric_match.group(1).replace(',', ''))
                    has_min = 1
                    return amount, has_min, currency

                # If we get here, there's text but no recognizable amount
                if len(min_str) > 0:
                    return 1, 1, currency  # Use 1 as a placeholder for text without specific amount

                return amount, has_min, currency

            # Apply extraction and unpack the results
            extraction_results = df['Minimum investment (Amount/No)'].apply(extract_min_investment)
            df['Min_Investment_Amount'] = [result[0] for result in extraction_results]
            df['Has_Min_Investment'] = [result[1] for result in extraction_results]
            df['Min_Investment_Currency'] = [result[2] for result in extraction_results]
        else:
            # Add empty columns for consistency
            df['Min_Investment_Amount'] = 0
            df['Has_Min_Investment'] = 0
            df['Min_Investment_Currency'] = "USD"

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
                # Default values
                amount = 0
                currency = "USD"  # Default currency

                if pd.isna(thresh_str) or thresh_str == 'N/a' or thresh_str == 'N/A' or thresh_str == 'No' or thresh_str == '-1':
                    return amount, currency

                # Convert to string for consistent processing
                thresh_str = str(thresh_str).strip()

                # Handle foreign currencies (e.g., CHF, EUR, GBP)
                foreign_currency_patterns = [
                    (r'CHF\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'CHF'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*CHF', 'CHF'),  # Handle "1,000 CHF" format
                    (r'EUR\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'EUR'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*EUR', 'EUR'),  # Handle "1,000 EUR" format
                    (r'GBP\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'GBP'),
                    (r'(\d+(?:,\d+)*(?:\.\d+)?)\s*GBP', 'GBP'),  # Handle "1,000 GBP" format
                    (r'£\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'GBP'),
                    (r'€\s*(\d+(?:,\d+)*(?:\.\d+)?)', 'EUR')
                ]

                for pattern, curr in foreign_currency_patterns:
                    match = re.search(pattern, thresh_str)
                    if match:
                        amount = float(match.group(1).replace(',', ''))
                        currency = curr
                        return amount, currency

                # Handle dollar ranges - extract the lower bound
                dollar_range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)(?:\s*[-–]\s*\$(\d+(?:,\d+)*(?:\.\d+)?))?', thresh_str)
                if dollar_range_match:
                    # Get the first amount (lower bound)
                    amount = float(dollar_range_match.group(1).replace(',', ''))
                    return amount, currency

                # Handle "up to $X" patterns - use the amount as minimum
                up_to_match = re.search(r'up to \$(\d+(?:,\d+)*(?:\.\d+)?)', thresh_str.lower())
                if up_to_match:
                    amount = float(up_to_match.group(1).replace(',', ''))
                    return amount, currency

                # Handle text with "million" or "billion"
                million_match = re.search(r'(\d+(?:\.\d+)?)\s*million', thresh_str.lower())
                if million_match:
                    amount = float(million_match.group(1)) * 1000000
                    return amount, currency

                billion_match = re.search(r'(\d+(?:\.\d+)?)\s*billion', thresh_str.lower())
                if billion_match:
                    amount = float(billion_match.group(1)) * 1000000000
                    return amount, currency

                # Extract any dollar amount as a fallback
                dollar_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', thresh_str)
                if dollar_match:
                    amount = float(dollar_match.group(1).replace(',', ''))
                    return amount, currency

                # Extract any numeric value as a last resort
                numeric_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)', thresh_str)
                if numeric_match:
                    amount = float(numeric_match.group(1).replace(',', ''))
                    return amount, currency

                return amount, currency

            # Apply extraction and unpack the results
            negotiable_results = df['Negotiable threshold (Number/ N/A)'].apply(extract_negotiable_threshold)
            df['Negotiable_Threshold'] = [result[0] for result in negotiable_results]
            df['Negotiable_Threshold_Currency'] = [result[1] for result in negotiable_results]
        else:
            # Add empty columns for consistency
            df['Negotiable_Threshold'] = 0
            df['Negotiable_Threshold_Currency'] = "USD"

        # Restructure fee information to ensure it's reported in fee_pct_min_1 first, then _2, _3, etc.
        def restructure_fee_info(row):
            # Simple approach: For each tier, check if there's a percentage range
            for i in range(1, 9):
                col_name = f'Annual fee threshold {i}' if i == 1 else f'Annual fee Threshold {i}'
                if not pd.isna(row.get(col_name)) and row.get(col_name) != '-1' and str(row.get(col_name)).strip() != '':
                    # Get the threshold string
                    threshold_str = str(row.get(col_name))

                    # First priority: Look for percentage range pattern in parentheses: (X% - Y%)
                    range_match = re.search(r'\(([\d\.]+)% ?- ?([\d\.]+)%\)', threshold_str)
                    if range_match:
                        # Extract min and max percentages
                        min_pct = float(range_match.group(1)) / 100
                        max_pct = float(range_match.group(2)) / 100

                        # Set the values directly
                        row[f'Fee_Pct_Min_{i}'] = min_pct
                        row[f'Fee_Pct_Max_{i}'] = max_pct
                        row[f'Fee_Is_Range_{i}'] = True
                        continue  # Skip other checks for this tier

                    # Second priority: Check for single percentage in parentheses: (X%)
                    single_pct_match = re.search(r'\(([\d\.]+)%\)', threshold_str)
                    if single_pct_match:
                        pct_value = float(single_pct_match.group(1)) / 100
                        row[f'Fee_Pct_Min_{i}'] = pct_value
                        row[f'Fee_Pct_Max_{i}'] = pct_value
                        row[f'Fee_Is_Range_{i}'] = False
                        continue  # Skip other checks for this tier

                    # Third priority: Look for percentage range NOT in parentheses: X% - Y%
                    range_no_parens = re.search(r'([\d\.]+)% ?- ?([\d\.]+)%', threshold_str)
                    if range_no_parens:
                        min_pct = float(range_no_parens.group(1)) / 100
                        max_pct = float(range_no_parens.group(2)) / 100

                        row[f'Fee_Pct_Min_{i}'] = min_pct
                        row[f'Fee_Pct_Max_{i}'] = max_pct
                        row[f'Fee_Is_Range_{i}'] = True
                        continue  # Skip other checks for this tier

                    # Fourth priority: Check for single percentage NOT in parentheses: X%
                    single_pct_no_parens = re.search(r'([\d\.]+)%', threshold_str)
                    if single_pct_no_parens:
                        pct_value = float(single_pct_no_parens.group(1)) / 100
                        row[f'Fee_Pct_Min_{i}'] = pct_value
                        row[f'Fee_Pct_Max_{i}'] = pct_value
                        row[f'Fee_Is_Range_{i}'] = False

            return row

        # Apply restructuring to each row
        df = df.apply(restructure_fee_info, axis=1)

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
