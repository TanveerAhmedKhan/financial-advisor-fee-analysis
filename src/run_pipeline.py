#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Advisor Fee Analysis - Pipeline Runner

This script runs the entire data processing and visualization pipeline:
1. Process raw CSV files
2. Combine processed files into consolidated datasets
3. Generate visualizations

Author: TanveerAhmedKhan
"""

import os
import logging
import subprocess
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)

def run_script(script_path, description):
    """
    Run a Python script and log the output.
    
    Args:
        script_path: Path to the script to run
        description: Description of the script for logging
    
    Returns:
        bool: True if the script ran successfully, False otherwise
    """
    logging.info(f"Running {description}...")
    
    try:
        start_time = time.time()
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        end_time = time.time()
        
        logging.info(f"{description} completed successfully in {end_time - start_time:.2f} seconds")
        return True
    
    except subprocess.CalledProcessError as e:
        logging.error(f"{description} failed with error: {e}")
        logging.error(f"Output: {e.output}")
        logging.error(f"Error: {e.stderr}")
        return False

def main():
    """Main function to run the entire pipeline."""
    logging.info("Starting data processing pipeline...")
    
    # Step 1: Process raw CSV files
    if not run_script('src/process_csv_files.py', 'CSV file processing'):
        logging.error("Pipeline stopped due to error in CSV file processing")
        return
    
    # Step 2: Combine processed files
    if not run_script('src/combine_processed_files.py', 'Data consolidation'):
        logging.error("Pipeline stopped due to error in data consolidation")
        return
    
    # Step 3: Generate visualizations
    if not run_script('src/generate_visualizations.py', 'Visualization generation'):
        logging.error("Pipeline stopped due to error in visualization generation")
        return
    
    logging.info("Data processing pipeline completed successfully")

if __name__ == "__main__":
    main()
