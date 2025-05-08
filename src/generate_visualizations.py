#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Advisor Fee Analysis - Data Visualization Script

This script generates visualizations from the processed fee data.
It includes all visualizations needed for the fee data analysis project.

Author: TanveerAhmedKhan
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
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
IMAGES_DIR = 'images'

def setup_directories():
    """Create necessary directories if they don't exist."""
    os.makedirs(IMAGES_DIR, exist_ok=True)
    logging.info(f"Ensured {IMAGES_DIR} directory exists")

def load_data():
    """
    Load the processed data files.

    Returns:
        tuple: (cleaned_df, unique_df) - The cleaned and unique datasets
    """
    logging.info("Loading processed data files...")

    cleaned_path = os.path.join(PROCESSED_DATA_DIR, "cleaned_fee_data_ordered.csv")
    unique_path = os.path.join(PROCESSED_DATA_DIR, "unique_fee_data_ordered.csv")

    # Check if the files exist
    if not os.path.exists(cleaned_path) or not os.path.exists(unique_path):
        logging.error(f"Consolidated files not found at {cleaned_path} or {unique_path}")
        logging.error("Please run the combine_processed_files.py script first to generate the consolidated files")
        return None, None

    try:
        cleaned_df = pd.read_csv(cleaned_path, low_memory=False)
        logging.info(f"Loaded cleaned dataset with {cleaned_df.shape[0]} rows")

        # Verify the data
        if 'Adviser_ID1' in cleaned_df.columns and 'Adviser_ID2' in cleaned_df.columns:
            unique_advisers = cleaned_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
            logging.info(f"Cleaned dataset has {unique_advisers} unique advisers")
        else:
            logging.warning("Required columns missing from cleaned dataset")
    except Exception as e:
        logging.error(f"Error loading cleaned dataset: {str(e)}")
        cleaned_df = None

    try:
        unique_df = pd.read_csv(unique_path, low_memory=False)
        logging.info(f"Loaded unique dataset with {unique_df.shape[0]} rows")

        # Verify the data
        if 'Adviser_ID1' in unique_df.columns and 'Adviser_ID2' in unique_df.columns:
            unique_advisers = unique_df[['Adviser_ID1', 'Adviser_ID2']].dropna().drop_duplicates().shape[0]
            logging.info(f"Unique dataset has {unique_advisers} unique advisers")
        else:
            logging.warning("Required columns missing from unique dataset")
    except Exception as e:
        logging.error(f"Error loading unique dataset: {str(e)}")
        unique_df = None

    return cleaned_df, unique_df

def plot_fee_distribution(df, output_path):
    """
    Plot the distribution of effective fees.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    plt.figure(figsize=(12, 8))

    # Plot effective fees for $1M and $5M portfolios
    if 'Effective_Fee_1M' in df.columns:
        sns.histplot(df['Effective_Fee_1M'].dropna() * 100, kde=True, label='$1M Portfolio', alpha=0.7)

    if 'Effective_Fee_5M' in df.columns:
        sns.histplot(df['Effective_Fee_5M'].dropna() * 100, kde=True, label='$5M Portfolio', alpha=0.7)

    plt.title('Distribution of Effective Fees')
    plt.xlabel('Effective Fee (%)')
    plt.ylabel('Count')
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved fee distribution plot to {output_path}")

def plot_fee_by_year(df, output_path):
    """
    Plot the average effective fee by year.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Filing_Year' not in df.columns or ('Effective_Fee_1M' not in df.columns and 'Effective_Fee_5M' not in df.columns):
        logging.warning("Required columns for fee by year plot not found")
        return

    # Group by year and calculate average fees
    yearly_fees = df.groupby('Filing_Year').agg({
        'Effective_Fee_1M': 'mean',
        'Effective_Fee_5M': 'mean'
    }).reset_index()

    plt.figure(figsize=(12, 8))

    if 'Effective_Fee_1M' in yearly_fees.columns:
        plt.plot(yearly_fees['Filing_Year'], yearly_fees['Effective_Fee_1M'] * 100,
                marker='o', linestyle='-', label='$1M Portfolio')

    if 'Effective_Fee_5M' in yearly_fees.columns:
        plt.plot(yearly_fees['Filing_Year'], yearly_fees['Effective_Fee_5M'] * 100,
                marker='s', linestyle='-', label='$5M Portfolio')

    plt.title('Average Effective Fee by Year')
    plt.xlabel('Year')
    plt.ylabel('Average Effective Fee (%)')
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved fee by year plot to {output_path}")

def plot_negotiable_fees(df, output_path):
    """
    Plot the proportion of negotiable fees.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Is_Negotiable' not in df.columns:
        logging.warning("Required column for negotiable fees plot not found")
        return

    # Calculate proportion of negotiable fees
    negotiable_counts = df['Is_Negotiable'].value_counts(normalize=True) * 100

    plt.figure(figsize=(10, 8))
    negotiable_counts.plot(kind='pie', autopct='%1.1f%%', startangle=90,
                          colors=['#ff9999','#66b3ff'], labels=['Non-negotiable', 'Negotiable'])

    plt.title('Proportion of Negotiable Fees')
    plt.ylabel('')

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved negotiable fees plot to {output_path}")

def plot_minimum_investment(df, output_path):
    """
    Plot the distribution of minimum investment amounts.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Min_Investment_Amount' not in df.columns:
        logging.warning("Required column for minimum investment plot not found")
        return

    # Filter out zero values and extreme outliers
    min_investments = df[df['Min_Investment_Amount'] > 0]['Min_Investment_Amount']
    min_investments = min_investments[min_investments < min_investments.quantile(0.99)]

    plt.figure(figsize=(12, 8))
    sns.histplot(min_investments, kde=True, bins=30)

    plt.title('Distribution of Minimum Investment Amounts')
    plt.xlabel('Minimum Investment ($)')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)

    # Format x-axis with dollar signs
    plt.ticklabel_format(style='plain', axis='x')

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved minimum investment plot to {output_path}")

def plot_flat_fee_distribution(df, output_path):
    """
    Plot the distribution of flat fees.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Flat_Fee_Value' not in df.columns:
        logging.warning("Required column for flat fee plot not found")
        return

    # Filter out NaN values and extreme outliers
    flat_fees = df['Flat_Fee_Value'].dropna()
    flat_fees = flat_fees[flat_fees < flat_fees.quantile(0.99)]

    plt.figure(figsize=(12, 8))
    sns.histplot(flat_fees * 100, kde=True, bins=30)

    plt.title('Distribution of Flat Fees')
    plt.xlabel('Flat Fee (%)')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved flat fee distribution plot to {output_path}")

def plot_fee_structure_counts(df, output_path):
    """
    Plot the count of different fee structures.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Num_Products' not in df.columns:
        logging.warning("Required column for fee structure counts plot not found")
        return

    # Count the number of products
    product_counts = df['Num_Products'].value_counts().sort_index()

    plt.figure(figsize=(12, 8))
    product_counts.plot(kind='bar', color='skyblue')

    plt.title('Number of Fee Structures per Adviser')
    plt.xlabel('Number of Fee Structures')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved fee structure counts plot to {output_path}")

def plot_fee_reduction(df, output_path):
    """
    Plot the distribution of fee reduction percentage from $1M to $5M portfolio.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Effective_Fee_1M' not in df.columns or 'Effective_Fee_5M' not in df.columns:
        logging.warning("Required columns for fee reduction plot not found")
        return

    # Calculate fee reduction percentage
    df['Fee_Reduction_Pct'] = (df['Effective_Fee_1M'] - df['Effective_Fee_5M']) / df['Effective_Fee_1M'] * 100

    # Filter out NaN and infinite values
    fee_reduction = df['Fee_Reduction_Pct'].replace([np.inf, -np.inf], np.nan).dropna()

    plt.figure(figsize=(12, 8))
    sns.histplot(fee_reduction, bins=30, kde=True)

    plt.title('Distribution of Fee Reduction Percentage from $1M to $5M Portfolio')
    plt.xlabel('Fee Reduction (%)')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved fee reduction plot to {output_path}")

def plot_threshold_fee_relation(df, output_path):
    """
    Plot the relationship between number of thresholds and effective fees.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    # Create a column for number of thresholds
    df['Num_Thresholds'] = 0
    for i in range(1, 9):
        df['Num_Thresholds'] += (~df[f'Fee_Pct_{i}'].isna()).astype(int)

    # Group by number of thresholds and calculate average fees
    threshold_fee_relation = df.groupby('Num_Thresholds')[['Effective_Fee_1M', 'Effective_Fee_5M']].mean() * 100

    plt.figure(figsize=(12, 8))
    threshold_fee_relation.plot(kind='bar')

    plt.title('Average Effective Fees by Number of Thresholds')
    plt.xlabel('Number of Thresholds')
    plt.ylabel('Effective Fee (%)')
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend(['$1M Portfolio', '$5M Portfolio'])

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved threshold-fee relation plot to {output_path}")

def plot_verification_ratio(df, output_path):
    """
    Plot the distribution of verification ratios.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Verification_Ratio' not in df.columns:
        logging.warning("Required column for verification ratio plot not found")
        return

    plt.figure(figsize=(12, 8))
    sns.histplot(df['Verification_Ratio'], bins=20, kde=True)

    plt.title('Distribution of Verification Ratios')
    plt.xlabel('Verification Ratio')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved verification ratio plot to {output_path}")

def plot_fee_structure_trend(df, output_path):
    """
    Plot the trend of fee structure types over time.

    Args:
        df: pandas.DataFrame with fee data
        output_path: Path to save the plot
    """
    if 'Filing_Year' not in df.columns or 'Has_Flat_Fee' not in df.columns:
        logging.warning("Required columns for fee structure trend plot not found")
        return

    # Group by year and flat fee flag
    fee_structure_counts = df.groupby(['Filing_Year', 'Flat_Fee_Flag']).size().unstack(fill_value=0)

    # Rename columns for clarity
    if 1 in fee_structure_counts.columns:
        fee_structure_counts = fee_structure_counts.rename(columns={0: 'AUM-based', 1: 'Flat Fee'})

    # Calculate percentages
    fee_structure_pct = fee_structure_counts.div(fee_structure_counts.sum(axis=1), axis=0) * 100

    plt.figure(figsize=(12, 8))

    if 'Flat Fee' in fee_structure_pct.columns:
        fee_structure_pct['Flat Fee'].plot(kind='line', marker='o', linewidth=2, color='red')

    plt.title('Percentage of Advisers with Flat Fee Structure Over Time')
    plt.xlabel('Year')
    plt.ylabel('Percentage of Advisers (%)')
    plt.grid(True, alpha=0.3)

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logging.info(f"Saved fee structure trend plot to {output_path}")

def main():
    """Main function to generate visualizations."""
    logging.info("Starting visualization generation...")

    # Setup directories
    setup_directories()

    # Load data
    cleaned_df, unique_df = load_data()

    if cleaned_df is None or unique_df is None:
        logging.error("Failed to load data for visualization")
        return

    # Generate basic visualizations
    plot_fee_distribution(unique_df, os.path.join(IMAGES_DIR, 'fee_distribution.png'))
    plot_fee_by_year(unique_df, os.path.join(IMAGES_DIR, 'fee_by_year.png'))
    plot_negotiable_fees(unique_df, os.path.join(IMAGES_DIR, 'negotiable_fees.png'))
    plot_minimum_investment(unique_df, os.path.join(IMAGES_DIR, 'minimum_investment.png'))
    plot_flat_fee_distribution(unique_df, os.path.join(IMAGES_DIR, 'flat_fee_distribution.png'))
    plot_fee_structure_counts(unique_df, os.path.join(IMAGES_DIR, 'fee_structure_counts.png'))

    # Generate additional visualizations from fee_data_summary.py
    plot_fee_reduction(unique_df, os.path.join(IMAGES_DIR, 'fee_reduction_distribution.png'))
    plot_threshold_fee_relation(unique_df, os.path.join(IMAGES_DIR, 'fees_by_thresholds.png'))
    plot_verification_ratio(unique_df, os.path.join(IMAGES_DIR, 'verification_ratio_distribution.png'))
    plot_fee_structure_trend(unique_df, os.path.join(IMAGES_DIR, 'flat_fee_trend.png'))

    logging.info("Visualization generation complete")

if __name__ == "__main__":
    main()
