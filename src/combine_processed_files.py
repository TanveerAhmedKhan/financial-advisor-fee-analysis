#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Advisor Fee Analysis - Data Consolidation Script

This script combines all processed CSV files into consolidated datasets:
1. cleaned_fee_data_ordered.csv - All processed records with original order preserved
2. unique_fee_data_ordered.csv - Unique records per adviser per year with original order preserved

It addresses issues with duplicate observations and ensures all unique advisers are included.

Author: TanveerAhmedKhan
"""

# Add the current directory to the path so this script can be run directly
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import os
import pandas as pd
import numpy as np
import glob
import logging
from datetime import datetime

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
PROCESSED_DATA_DIR = 'data/processed'
OUTPUT_DIR = 'data/processed'

def combine_processed_files():
    """
    Combine all processed CSV files into a single dataset.

    Returns:
        pandas.DataFrame: Combined dataset with all processed records
    """
    logging.info("Combining processed CSV files...")

    # Get all processed CSV files (excluding the final consolidated files)
    processed_files = [f for f in glob.glob(f"{PROCESSED_DATA_DIR}/processed_*.csv")
                      if not os.path.basename(f).endswith('cleaned_fee_data_ordered.csv')
                      and not os.path.basename(f).endswith('unique_fee_data_ordered.csv')]

    logging.info(f"Found {len(processed_files)} processed CSV files")

    # Check if we have processed files
    if not processed_files:
        logging.warning("No processed CSV files found. Checking if we need to force regeneration...")

        # Check if the consolidated files already exist
        cleaned_path = os.path.join(PROCESSED_DATA_DIR, "cleaned_fee_data_ordered.csv")
        unique_path = os.path.join(PROCESSED_DATA_DIR, "unique_fee_data_ordered.csv")

        if os.path.exists(cleaned_path) and os.path.exists(unique_path):
            logging.info("Consolidated files already exist. Loading them for verification...")

            try:
                # Load the existing consolidated files
                cleaned_df = pd.read_csv(cleaned_path, low_memory=False)
                unique_df = pd.read_csv(unique_path, low_memory=False)

                # Check if they have the expected number of rows and columns
                if 'Adviser_ID1' in cleaned_df.columns and 'Adviser_ID2' in cleaned_df.columns:
                    unique_advisers = cleaned_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
                    logging.info(f"Existing cleaned file has {cleaned_df.shape[0]} rows and {unique_advisers} unique advisers")

                    # Check if the unique file has the expected number of rows
                    if 'Adviser_ID1' in unique_df.columns and 'Adviser_ID2' in unique_df.columns:
                        unique_advisers_in_unique = unique_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
                        logging.info(f"Existing unique file has {unique_df.shape[0]} rows and {unique_advisers_in_unique} unique advisers")

                        # If the counts match, we can use the existing files
                        if unique_advisers == unique_advisers_in_unique:
                            logging.info("Existing consolidated files appear to be valid. No need to regenerate.")
                            return cleaned_df

                logging.warning("Existing consolidated files need to be regenerated.")
            except Exception as e:
                logging.error(f"Error checking existing consolidated files: {str(e)}")

        # If we get here, we need to regenerate the consolidated files
        logging.warning("No processed files found and existing consolidated files are invalid or missing.")
        logging.warning("Please run the process_csv_files.py script first to generate processed files.")
        return None

    # Initialize an empty list to store dataframes
    dfs = []

    # Read each processed CSV file
    for file in processed_files:
        try:
            df = pd.read_csv(file, low_memory=False)
            # Add a column to track the source file
            df['source_file'] = os.path.basename(file)
            dfs.append(df)
            logging.info(f"Read {file} with {df.shape[0]} rows")
        except Exception as e:
            logging.error(f"Error reading {file}: {str(e)}")

    # Combine all dataframes
    if not dfs:
        logging.error("No valid processed files found")
        return None

    combined_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Combined data has {combined_df.shape[0]} rows and {combined_df.shape[1]} columns")

    # Add a column to track original order
    combined_df['original_order'] = range(len(combined_df))

    return combined_df

def extract_adviser_ids(df):
    """
    Extract adviser IDs from the dataset.

    Args:
        df: pandas.DataFrame with adviser information

    Returns:
        pandas.DataFrame: Dataset with adviser IDs extracted
    """
    logging.info("Extracting adviser IDs...")

    # Check if ID columns already exist
    if 'Adviser_ID1' in df.columns and 'Adviser_ID2' in df.columns:
        logging.info("Adviser ID columns already exist")
        return df

    # Function to extract IDs from filename
    def extract_ids(filename):
        if pd.isna(filename):
            return pd.Series([None, None, None])

        # Extract IDs from the filename
        import re
        # Try different patterns to match the file name format
        match = re.search(r'\\(\d+)_(\d+)_\d+_(\d{8})_', str(filename))
        if match:
            id1 = match.group(1)
            id2 = match.group(2)
            filing_date = match.group(3)
            try:
                filing_date = pd.to_datetime(filing_date, format='%Y%m%d')
            except:
                filing_date = None
            return pd.Series([id1, id2, filing_date])

        # Try alternative pattern for extracted files
        match = re.search(r'(\d+)_(\d+)_\d+_(\d{8})_', str(filename))
        if match:
            id1 = match.group(1)
            id2 = match.group(2)
            filing_date = match.group(3)
            try:
                filing_date = pd.to_datetime(filing_date, format='%Y%m%d')
            except:
                filing_date = None
            return pd.Series([id1, id2, filing_date])

        return pd.Series([None, None, None])

    # Apply extraction to filename column if it exists
    if 'File Name' in df.columns:
        df[['Adviser_ID1', 'Adviser_ID2', 'Filing_Date']] = df['File Name'].apply(extract_ids)

        # Extract year from filing date
        if 'Filing_Date' in df.columns:
            df['Filing_Year'] = df['Filing_Date'].dt.year

    # Count unique advisers
    unique_advisers = df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
    logging.info(f"Extracted {unique_advisers} unique adviser IDs")

    return df

def create_unique_dataset(df):
    """
    Create a dataset with unique records per adviser per year.

    Args:
        df: pandas.DataFrame with all records

    Returns:
        pandas.DataFrame: Dataset with unique records per adviser per year
    """
    logging.info("Creating unique dataset...")

    # Make a copy to avoid modifying the original
    unique_df = df.copy()

    # Ensure Filing_Date is in datetime format
    if 'Filing_Date' in unique_df.columns:
        unique_df['Filing_Date'] = pd.to_datetime(unique_df['Filing_Date'], errors='coerce')

        # Extract year if not already present
        if 'Filing_Year' not in unique_df.columns:
            unique_df['Filing_Year'] = unique_df['Filing_Date'].dt.year

    # Sort and deduplicate based on Adviser IDs, Filing Date, and Text Length
    if 'Adviser_ID1' in unique_df.columns and 'Adviser_ID2' in unique_df.columns and 'Filing_Date' in unique_df.columns and 'Text Length' in unique_df.columns:
        # First sort by original order to maintain it as a secondary sort key
        unique_df = unique_df.sort_values('original_order')

        # Ensure Filing_Date is in datetime format and not NaN
        unique_df['Filing_Date'] = pd.to_datetime(unique_df['Filing_Date'], errors='coerce')
        unique_df['Filing_Date'] = unique_df['Filing_Date'].fillna(pd.Timestamp('1900-01-01'))

        # Ensure Text Length is numeric
        unique_df['Text Length'] = pd.to_numeric(unique_df['Text Length'], errors='coerce')
        unique_df['Text Length'] = unique_df['Text Length'].fillna(0)

        # Create a composite score for sorting
        # This prioritizes records with more text and more recent filing dates
        # Convert datetime to int64 timestamp first, then to int32 if needed
        unique_df['Composite_Score'] = unique_df['Filing_Date'].astype('int64') / 10**18 + unique_df['Text Length'] / 1000

        # Sort by adviser IDs, composite score (higher is better)
        unique_df = unique_df.sort_values(
            ['Adviser_ID1', 'Adviser_ID2', 'Composite_Score'],
            ascending=[True, True, False]
        )

        # Drop duplicates keeping the first occurrence (which will be the record with highest composite score)
        # Use Adviser_ID1 and Adviser_ID2 for deduplication
        unique_df = unique_df.drop_duplicates(subset=['Adviser_ID1', 'Adviser_ID2'])

        # Remove the temporary column
        unique_df = unique_df.drop('Composite_Score', axis=1)

        # Restore original order to preserve the order from raw files
        unique_df = unique_df.sort_values('original_order')

        logging.info(f"Unique dataset has {unique_df.shape[0]} rows")
    else:
        logging.warning("Required columns for creating unique dataset not found")

    return unique_df

def main():
    """Main function to combine processed files and create consolidated datasets."""
    logging.info("Starting data consolidation process...")

    # Check if the consolidated files already exist
    cleaned_output_path = os.path.join(OUTPUT_DIR, "cleaned_fee_data_ordered.csv")
    unique_output_path = os.path.join(OUTPUT_DIR, "unique_fee_data_ordered.csv")

    # Force regeneration flag - set to True to always regenerate the files
    force_regeneration = True  # Keep this set to True to ensure consistent column structure

    if os.path.exists(cleaned_output_path) and os.path.exists(unique_output_path) and not force_regeneration:
        logging.info("Consolidated files already exist. Verifying them...")

        try:
            # Load the existing consolidated files
            cleaned_df = pd.read_csv(cleaned_output_path, low_memory=False)
            unique_df = pd.read_csv(unique_output_path, low_memory=False)

            # Check if they have the expected columns
            required_columns = ['Adviser_ID1', 'Adviser_ID2']
            if all(col in cleaned_df.columns for col in required_columns) and all(col in unique_df.columns for col in required_columns):
                # Count unique advisers
                cleaned_unique_advisers = cleaned_df[required_columns].dropna().drop_duplicates().shape[0]
                unique_unique_advisers = unique_df[required_columns].dropna().drop_duplicates().shape[0]

                logging.info(f"Existing cleaned file has {cleaned_df.shape[0]} rows and {cleaned_unique_advisers} unique advisers")
                logging.info(f"Existing unique file has {unique_df.shape[0]} rows and {unique_unique_advisers} unique advisers")

                # Check if the counts match
                if cleaned_unique_advisers == unique_unique_advisers:
                    logging.info("Existing consolidated files appear to be valid.")
                    logging.info("Data consolidation complete")
                    return
                else:
                    logging.warning("Unique adviser counts don't match between files. Regenerating...")
            else:
                logging.warning("Required columns missing from existing files. Regenerating...")
        except Exception as e:
            logging.error(f"Error verifying existing consolidated files: {str(e)}")
            logging.warning("Regenerating consolidated files...")

    # Combine all processed files
    combined_df = combine_processed_files()
    if combined_df is None:
        logging.error("Failed to combine processed files")
        return

    # Extract adviser IDs
    combined_df = extract_adviser_ids(combined_df)

    # Create unique dataset
    unique_df = create_unique_dataset(combined_df)

    # Save with original order preserved
    combined_df.to_csv(cleaned_output_path, index=False)
    unique_df.to_csv(unique_output_path, index=False)

    # Log statistics
    logging.info(f"Saved cleaned dataset with {combined_df.shape[0]} rows to {cleaned_output_path}")
    logging.info(f"Saved unique dataset with {unique_df.shape[0]} rows to {unique_output_path}")

    # Count unique advisers in each dataset
    combined_unique_advisers = combined_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
    unique_unique_advisers = unique_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]

    logging.info(f"Cleaned dataset has {combined_unique_advisers} unique advisers")
    logging.info(f"Unique dataset has {unique_unique_advisers} unique advisers")

    logging.info("Data consolidation complete")

if __name__ == "__main__":
    main()
