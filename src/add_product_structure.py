#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Advisor Fee Analysis - Product Structure Identification

This script adds product structure columns to the cleaned fee data. It identifies
distinct "products" or "fee schedules" based on logical groupings of fee thresholds
and percentages.

Author: TanveerAhmedKhan
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import re
import math
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('product_structure.log'),
        logging.StreamHandler()
    ]
)

# Constants
# Use absolute path relative to the project root, not the src directory
PROCESSED_DATA_DIR = os.path.join('data', 'processed')
INPUT_FILE = 'cleaned_fee_data_ordered.csv'
OUTPUT_FILE = 'cleaned_fee_data_with_products.csv'

def format_threshold(value):
    """Format threshold values in a human-readable way."""
    if pd.isna(value) or value is None:
        return ""

    if value == np.inf:
        return "+"

    # Convert string to float if needed
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return value  # Return as is if conversion fails

    # Format large numbers with K, M, B suffixes
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"${value/1_000:.0f}K"
    else:
        return f"${value:.0f}"

def format_percentage(value):
    """Format percentage values in a human-readable way."""
    if pd.isna(value) or value is None:
        return "N/A"  # Return N/A for None values (representing no fee)

    # Handle string values
    if isinstance(value, str):
        if "%" in value:
            return value  # Already formatted as percentage
        try:
            value = float(value)
        except ValueError:
            return value  # Return as is if conversion fails

    # Convert from decimal to percentage and format
    return f"{value*100:.2f}%".rstrip('0').rstrip('.') + '%'

def identify_products(row):
    """
    Identify distinct products based on fee thresholds and percentages using a sequential, rule-based approach.

    This function processes fee structures in four phases of increasing complexity:
    1. Simple product structures (single products with monotonic thresholds and fees)
    2. Fee range cases (thresholds follow a pattern but fees are expressed as ranges)
    3. Multiple product structures (multiple distinct fee schedules)
    4. Multiple fee schedules detection (identifies and separates multiple fee schedules in a single record)

    Products are ONLY extracted from Annual fee threshold 1-8 columns.
    Flat fee data is completely ignored for product extraction.

    Args:
        row: A pandas Series representing a row in the dataframe

    Returns:
        A list of product dictionaries, where each product contains:
        - thresholds: List of threshold values
        - upper_bounds: List of threshold upper bounds
        - fees: List of fee percentages
    """
    products = []

    # Collect and preprocess all threshold tiers from Annual fee threshold 1-8 columns ONLY
    tiers = collect_tiers(row)

    # If no threshold tiers found, return empty list
    # We do NOT process flat fee data for product extraction
    if not tiers:
        return products

    # Check if there are multiple fee schedules in the data
    # This is indicated by repeated threshold ranges with different fees
    multiple_schedules = detect_multiple_fee_schedules(tiers)

    if multiple_schedules:
        # Phase 4: Handle multiple fee schedules in a single record
        multi_schedule_products = process_multiple_fee_schedules(tiers)
        if multi_schedule_products and validate_products(multi_schedule_products):
            logging.debug("Phase 4: Extracted multiple fee schedules")
            return multi_schedule_products

    # Sort tiers by lower threshold to ensure proper ordering
    tiers.sort(key=lambda x: x['lower'] if not pd.isna(x['lower']) else float('inf'))

    # Phase 1: Extract simple product structures
    simple_products = extract_simple_products(tiers)
    if simple_products and validate_products(simple_products):
        logging.debug("Phase 1: Extracted simple product structure")
        return simple_products

    # Phase 2: Handle fee range cases
    range_products = handle_fee_ranges(tiers)
    if range_products and validate_products(range_products):
        logging.debug("Phase 2: Extracted products with fee ranges")
        return range_products

    # Phase 3: Process multiple product structures
    complex_products = process_multiple_products(tiers)
    if complex_products and validate_products(complex_products):
        logging.debug("Phase 3: Extracted multiple product structures")
        return complex_products

    # Fallback: If all phases fail, return an empty list
    logging.debug("All phases failed to extract valid products")
    return []

def collect_tiers(row):
    """
    Collect all threshold tiers from the row data.

    ONLY collects data from Annual fee threshold 1-8 columns.
    Flat fee data is completely ignored.

    Args:
        row: A pandas Series representing a row in the dataframe

    Returns:
        List of tier dictionaries
    """
    tiers = []

    # Collect threshold tiers from Annual fee threshold 1-8 columns ONLY
    for i in range(1, 9):
        threshold_lower = row.get(f'Threshold_Lower_{i}')
        threshold_upper = row.get(f'Threshold_Upper_{i}')
        fee_min = row.get(f'Fee_Pct_Min_{i}')
        fee_max = row.get(f'Fee_Pct_Max_{i}')
        is_range = row.get(f'Fee_Is_Range_{i}')

        # Skip if no threshold data at all
        if pd.isna(threshold_lower) and pd.isna(threshold_upper):
            continue

        # Include tiers with valid thresholds even if fees are NaN
        # This handles cases like "$0 - $150,000 (N/A)" where the tier
        # is part of a fee structure but has no fee itself
        tiers.append({
            'index': i,
            'lower': threshold_lower,
            'upper': threshold_upper,
            'fee_min': fee_min,
            'fee_max': fee_max,
            'is_range': is_range
        })

    return tiers

def extract_simple_products(tiers):
    """
    Phase 1: Extract simple product structures.

    Identifies single products with monotonically increasing thresholds and
    monotonically decreasing (or flat) fee percentages.

    Args:
        tiers: List of tier dictionaries

    Returns:
        List of product dictionaries
    """
    # If no tiers, return empty list
    if not tiers:
        return []

    # Preprocess tiers to handle "Less than" patterns
    preprocessed_tiers = []
    for tier in tiers:
        new_tier = tier.copy()
        lower_str = str(tier['lower']).lower() if not pd.isna(tier['lower']) else ""

        # Handle special threshold descriptions
        if "less than" in lower_str or "up to" in lower_str or "under" in lower_str:
            new_tier['lower'] = 0
            new_tier['less_than_original'] = tier['lower']  # Store original value for reference

        preprocessed_tiers.append(new_tier)

    # Group tiers into potential fee schedules
    fee_schedules = []

    # Sort tiers by lower threshold to ensure proper ordering
    sorted_tiers = sorted(preprocessed_tiers, key=lambda x: (x['lower'] if not pd.isna(x['lower']) else float('inf')))

    # First pass: identify distinct starting points (typically $0)
    starting_indices = []
    for i, tier in enumerate(sorted_tiers):
        # Skip tiers with fee ranges for now - they'll be handled in Phase 2
        if tier['is_range'] == True and tier['fee_min'] != tier['fee_max']:
            continue

        # Check if this tier starts at $0 or is the first tier
        if tier['lower'] == 0 or i == 0:
            starting_indices.append(i)

    # If no starting points found, add the first tier as a starting point
    if not starting_indices and sorted_tiers:
        starting_indices.append(0)

    # Second pass: build continuous sequences from each starting point
    for start_idx in starting_indices:
        current_schedule = [sorted_tiers[start_idx]]
        current_upper = sorted_tiers[start_idx]['upper']

        for i in range(start_idx + 1, len(sorted_tiers)):
            # Skip tiers with fee ranges for now
            if sorted_tiers[i]['is_range'] == True and sorted_tiers[i]['fee_min'] != sorted_tiers[i]['fee_max']:
                continue

            # Check if this tier continues the sequence
            # Allow for larger gaps (within 0.1% of the value or 1000 units, whichever is larger)
            tolerance = max(1000, current_upper * 0.001)  # 0.1% or at least 1000 units

            if abs(sorted_tiers[i]['lower'] - current_upper) <= tolerance:
                # This tier continues the sequence (with a small gap allowed)
                current_schedule.append(sorted_tiers[i])
                current_upper = sorted_tiers[i]['upper']
            # Check if this tier overlaps with the current sequence
            elif sorted_tiers[i]['lower'] < current_upper - tolerance:
                # This tier overlaps with the current sequence, skip it
                continue
            # Check if there's a gap in the sequence
            elif sorted_tiers[i]['lower'] > current_upper + tolerance:
                # This tier doesn't continue the sequence, stop here
                break

        # Add the schedule if it has at least one tier
        if current_schedule:
            fee_schedules.append(current_schedule)

    # Third pass: validate each fee schedule
    valid_products = []

    for schedule in fee_schedules:
        # Skip schedules with only one tier - they'll be handled in Phase 3
        if len(schedule) <= 1:
            continue

        # Check for monotonically decreasing fees (ignoring NaN values)
        is_valid = True
        valid_fee_tiers = [t for t in schedule if not pd.isna(t['fee_min'])]

        for i in range(1, len(valid_fee_tiers)):
            # Fees should decrease or stay the same as thresholds increase
            if valid_fee_tiers[i]['fee_min'] > valid_fee_tiers[i-1]['fee_min']:
                is_valid = False
                break

        if is_valid:
            # Create a product from the valid schedule
            thresholds = []
            upper_bounds = []
            fees = []

            for t in schedule:
                thresholds.append(t['lower'])
                upper_bounds.append(t['upper'])
                # Handle NaN fees by using None or a special marker
                if pd.isna(t['fee_min']):
                    fees.append(None)  # Use None for N/A fees
                else:
                    fees.append(t['fee_min'])

            valid_products.append({
                "thresholds": thresholds,
                "upper_bounds": upper_bounds,
                "fees": fees
            })

    return valid_products

def handle_fee_ranges(tiers):
    """
    Phase 2: Handle fee range cases.

    For cases where thresholds follow a clear pattern but fees are expressed as ranges,
    create separate products for each fee range while maintaining the threshold structure.

    Args:
        tiers: List of tier dictionaries

    Returns:
        List of product dictionaries
    """
    products = []

    # Check if there are any fee ranges
    has_fee_ranges = any(
        tier['is_range'] == True and tier['fee_min'] != tier['fee_max']
        for tier in tiers
    )

    if not has_fee_ranges:
        return []  # No fee ranges, not applicable for Phase 2

    # Preprocess tiers to handle "Less than" patterns
    preprocessed_tiers = []
    for tier in tiers:
        new_tier = tier.copy()
        lower_str = str(tier['lower']).lower() if not pd.isna(tier['lower']) else ""

        # Handle special threshold descriptions
        if "less than" in lower_str or "up to" in lower_str or "under" in lower_str:
            new_tier['lower'] = 0
            new_tier['less_than_original'] = tier['lower']  # Store original value for reference

        preprocessed_tiers.append(new_tier)

    # Group tiers into continuous fee schedules
    fee_schedules = []

    # Sort tiers by lower threshold to ensure proper ordering
    sorted_tiers = sorted(preprocessed_tiers, key=lambda x: (x['lower'] if not pd.isna(x['lower']) else float('inf')))

    # First pass: identify distinct starting points (typically $0)
    starting_indices = []
    for i, tier in enumerate(sorted_tiers):
        # Check if this tier starts at $0 or is the first tier
        if tier['lower'] == 0 or i == 0:
            starting_indices.append(i)
        # Check if this tier's lower bound is less than or equal to the previous tier's lower bound
        elif i > 0 and tier['lower'] <= sorted_tiers[i-1]['lower']:
            starting_indices.append(i)

    # If no starting points found, add the first tier as a starting point
    if not starting_indices and sorted_tiers:
        starting_indices.append(0)

    # Second pass: build continuous sequences from each starting point
    for start_idx in starting_indices:
        current_schedule = [sorted_tiers[start_idx]]
        current_upper = sorted_tiers[start_idx]['upper']

        for i in range(start_idx + 1, len(sorted_tiers)):
            # Check if this tier continues the sequence
            # Allow for small gaps (within 0.001% of the value)
            tolerance = max(1, current_upper * 0.00001)  # 0.001% or at least 1 unit

            if abs(sorted_tiers[i]['lower'] - current_upper) <= tolerance:
                # This tier continues the sequence (with a small gap allowed)
                current_schedule.append(sorted_tiers[i])
                current_upper = sorted_tiers[i]['upper']
            # Check if this tier overlaps with the current sequence
            elif sorted_tiers[i]['lower'] < current_upper - tolerance:
                # This tier overlaps with the current sequence, skip it
                continue
            # Check if there's a gap in the sequence
            elif sorted_tiers[i]['lower'] > current_upper + tolerance:
                # This tier doesn't continue the sequence, stop here
                break

        # Add the schedule if it has at least one tier
        if current_schedule:
            fee_schedules.append(current_schedule)

    # Third pass: process each fee schedule
    for schedule in fee_schedules:
        # Check if this schedule has any fee ranges
        schedule_has_ranges = any(
            tier['is_range'] == True and tier['fee_min'] != tier['fee_max']
            for tier in schedule
        )

        if schedule_has_ranges:
            # Group tiers by whether they have the same fee range pattern
            # This helps maintain the structure of the fee schedule
            range_groups = []
            current_group = []

            for tier in schedule:
                if not current_group:
                    # First tier in the group
                    current_group.append(tier)
                else:
                    # Check if this tier continues the sequence
                    prev_tier = current_group[-1]
                    # Allow for larger gaps (within 0.1% of the value or 1000 units, whichever is larger)
                    tolerance = max(1000, prev_tier['upper'] * 0.001)  # 0.1% or at least 1000 units

                    if abs(tier['lower'] - prev_tier['upper']) <= tolerance:
                        # This tier continues the sequence (with a small gap allowed)
                        current_group.append(tier)
                    else:
                        # This tier doesn't continue the sequence, start a new group
                        range_groups.append(current_group)
                        current_group = [tier]

            # Add the last group if it has data
            if current_group:
                range_groups.append(current_group)

            # Process each group
            for group in range_groups:
                # Check if this group has any fee ranges
                group_has_ranges = any(
                    tier['is_range'] == True and tier['fee_min'] != tier['fee_max']
                    for tier in group
                )

                if group_has_ranges:
                    # Special case: If we have a single tier with a fee range, create two separate products
                    # This handles cases like "0.32% - 2.50%" which should become two products:
                    # Product 1: ($0+, 0.32%) and Product 2: ($0+, 2.50%)
                    if (len(group) == 1 and
                        group[0]['is_range'] == True and
                        group[0]['fee_min'] != group[0]['fee_max'] and
                        not pd.isna(group[0]['fee_min']) and
                        not pd.isna(group[0]['fee_max'])):

                        tier = group[0]

                        # Create first product with minimum fee
                        product1 = {
                            "thresholds": [tier['lower']],
                            "upper_bounds": [tier['upper']],
                            "fees": [tier['fee_min']]
                        }
                        products.append(product1)

                        # Create second product with maximum fee
                        product2 = {
                            "thresholds": [tier['lower']],
                            "upper_bounds": [tier['upper']],
                            "fees": [tier['fee_max']]
                        }
                        products.append(product2)
                    else:
                        # Default behavior: Create a single product that shows fee ranges where they exist
                        # This consolidates fee ranges into one product instead of creating separate min/max products
                        product = {"thresholds": [], "upper_bounds": [], "fees": []}

                        for tier in group:
                            product["thresholds"].append(tier['lower'])
                            product["upper_bounds"].append(tier['upper'])

                            # For fee ranges, create a range string; for fixed fees, use the single value
                            if tier['is_range'] == True and tier['fee_min'] != tier['fee_max']:
                                # Create a fee range string (e.g., "0.50%-1.25%")
                                fee_range = f"{tier['fee_min']}-{tier['fee_max']}"
                                product["fees"].append(fee_range)
                            else:
                                # Use the single fee value, handling NaN as None
                                if pd.isna(tier['fee_min']):
                                    product["fees"].append(None)
                                else:
                                    product["fees"].append(tier['fee_min'])

                        # Only add the product if it has data
                        if product["thresholds"] and product["fees"]:
                            products.append(product)
                else:
                    # No fee ranges in this group, add it as a single product
                    product = {"thresholds": [], "upper_bounds": [], "fees": []}

                    for tier in group:
                        product["thresholds"].append(tier['lower'])
                        product["upper_bounds"].append(tier['upper'])
                        # Handle NaN fees as None
                        if pd.isna(tier['fee_min']):
                            product["fees"].append(None)
                        else:
                            product["fees"].append(tier['fee_min'])

                    # Only add the product if it has data
                    if product["thresholds"] and product["fees"]:
                        products.append(product)
        else:
            # No fee ranges in this schedule, add it as a single product
            product = {"thresholds": [], "upper_bounds": [], "fees": []}

            for tier in schedule:
                product["thresholds"].append(tier['lower'])
                product["upper_bounds"].append(tier['upper'])
                # Handle NaN fees as None
                if pd.isna(tier['fee_min']):
                    product["fees"].append(None)
                else:
                    product["fees"].append(tier['fee_min'])

            # Only add the product if it has data
            if product["thresholds"] and product["fees"]:
                products.append(product)

    return products

def process_multiple_products(tiers):
    """
    Phase 3: Process multiple product structures.

    Handle complex cases where multiple distinct fee schedules exist.
    Identify new products when thresholds reset to $0 or when a threshold
    is lower than the previous tier's upper bound.

    Args:
        tiers: List of tier dictionaries

    Returns:
        List of product dictionaries
    """
    # If no tiers, return empty list
    if not tiers:
        return []

    # Preprocess tiers to handle "Less than" patterns
    preprocessed_tiers = []
    for tier in tiers:
        new_tier = tier.copy()
        lower_str = str(tier['lower']).lower() if not pd.isna(tier['lower']) else ""

        # Handle special threshold descriptions
        if "less than" in lower_str or "up to" in lower_str or "under" in lower_str:
            # Extract the threshold value from "Less than X" or "Up to X"
            # Store the original upper bound
            original_upper = new_tier['upper']

            # Set lower bound to 0 and upper bound to the extracted value
            new_tier['lower'] = 0
            # Keep the original upper bound if it was explicitly set
            if original_upper != np.inf:
                new_tier['upper'] = original_upper

            new_tier['less_than_original'] = tier['lower']  # Store original value for reference

        preprocessed_tiers.append(new_tier)

    # Group tiers into potential fee schedules based on continuity
    # A fee schedule is a sequence of tiers where each tier's lower bound
    # equals the previous tier's upper bound
    fee_schedules = []

    # Sort tiers by index to maintain original order
    # This is important for preserving the intended structure
    sorted_tiers = sorted(preprocessed_tiers, key=lambda x: x['index'])

    # First pass: identify potential starting points for fee schedules
    # A starting point is typically a tier with lower bound of 0,
    # or a tier that doesn't continue from any previous tier
    starting_indices = []

    for i, tier in enumerate(sorted_tiers):
        # Check if this tier starts at $0
        if tier['lower'] == 0:
            starting_indices.append(i)
        # Check if this tier's lower bound doesn't match any previous tier's upper bound
        elif i > 0:
            # Check if this tier continues from any previous tier
            continues_from_previous = False
            for j in range(i):
                if tier['lower'] == sorted_tiers[j]['upper']:
                    continues_from_previous = True
                    break

            if not continues_from_previous:
                starting_indices.append(i)

    # If no starting points found, add the first tier as a starting point
    if not starting_indices and sorted_tiers:
        starting_indices.append(0)

    # Second pass: build continuous sequences from each starting point
    for start_idx in starting_indices:
        current_schedule = [sorted_tiers[start_idx]]
        current_upper = sorted_tiers[start_idx]['upper']

        # Find all tiers that continue from this starting point
        # in a depth-first manner to capture all branches
        visited = set([start_idx])
        stack = [(current_upper,)]

        while stack:
            parent_upper = stack.pop()[0]

            # Find all tiers that continue from this parent
            for i in range(len(sorted_tiers)):
                # Check for exact match or small gap (within 0.001% of the value)
                if i not in visited:
                    # Calculate the tolerance based on the parent_upper value
                    tolerance = max(1000, parent_upper * 0.001)  # 0.1% or at least 1000 units

                    # Check if the lower bound is within tolerance of the upper bound
                    if abs(sorted_tiers[i]['lower'] - parent_upper) <= tolerance:
                        # This tier continues from the parent (with a small gap allowed)
                        current_schedule.append(sorted_tiers[i])
                        visited.add(i)
                        stack.append((sorted_tiers[i]['upper'],))

        # Add the schedule if it has data
        if current_schedule:
            # Sort the schedule by lower bound to ensure proper ordering
            current_schedule.sort(key=lambda x: x['lower'])
            fee_schedules.append(current_schedule)

    # Third pass: check each fee schedule for continuity and split if necessary
    continuous_schedules = []

    for schedule in fee_schedules:
        # Check for continuity within the schedule
        segments = []
        current_segment = [schedule[0]]
        current_upper = schedule[0]['upper']

        for i in range(1, len(schedule)):
            # Check if this tier continues from the previous one
            # Allow for larger gaps (within 0.1% of the value or 1000 units, whichever is larger)
            tolerance = max(1000, current_upper * 0.001)  # 0.1% or at least 1000 units

            if abs(schedule[i]['lower'] - current_upper) <= tolerance:
                # This tier continues the sequence (with a small gap allowed)
                current_segment.append(schedule[i])
                current_upper = schedule[i]['upper']
            else:
                # This tier doesn't continue the sequence, start a new segment
                segments.append(current_segment)
                current_segment = [schedule[i]]
                current_upper = schedule[i]['upper']

        # Add the last segment if it has data
        if current_segment:
            segments.append(current_segment)

        # Add all segments to the continuous schedules
        continuous_schedules.extend(segments)

    # Fourth pass: create products from the continuous schedules
    products = []

    for schedule in continuous_schedules:
        # Check if this schedule has any fee ranges
        schedule_has_ranges = any(
            tier['is_range'] == True and tier['fee_min'] != tier['fee_max']
            for tier in schedule
        )

        if schedule_has_ranges:
            # Special case: If we have a single tier with a fee range, create two separate products
            # This handles cases like "0.32% - 2.50%" which should become two products:
            # Product 1: ($0+, 0.32%) and Product 2: ($0+, 2.50%)
            if (len(schedule) == 1 and
                schedule[0]['is_range'] == True and
                schedule[0]['fee_min'] != schedule[0]['fee_max'] and
                not pd.isna(schedule[0]['fee_min']) and
                not pd.isna(schedule[0]['fee_max'])):

                tier = schedule[0]

                # Create first product with minimum fee
                product1 = {
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_min']]
                }
                products.append(product1)

                # Create second product with maximum fee
                product2 = {
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_max']]
                }
                products.append(product2)
            else:
                # Default behavior: Create a single product that shows fee ranges where they exist
                # This consolidates fee ranges into one product instead of creating separate min/max products
                product = {"thresholds": [], "upper_bounds": [], "fees": []}

                for tier in schedule:
                    product["thresholds"].append(tier['lower'])
                    product["upper_bounds"].append(tier['upper'])

                    # For fee ranges, create a range string; for fixed fees, use the single value
                    if tier['is_range'] == True and tier['fee_min'] != tier['fee_max']:
                        # Create a fee range string (e.g., "0.50%-1.25%")
                        fee_range = f"{tier['fee_min']}-{tier['fee_max']}"
                        product["fees"].append(fee_range)
                    else:
                        # Use the single fee value, handling NaN as None
                        if pd.isna(tier['fee_min']):
                            product["fees"].append(None)
                        else:
                            product["fees"].append(tier['fee_min'])

                # Only add the product if it has data
                if product["thresholds"] and product["fees"]:
                    products.append(product)
        else:
            # No fee ranges in this schedule, add it as a single product
            product = {"thresholds": [], "upper_bounds": [], "fees": []}

            for tier in schedule:
                product["thresholds"].append(tier['lower'])
                product["upper_bounds"].append(tier['upper'])
                # Handle NaN fees as None
                if pd.isna(tier['fee_min']):
                    product["fees"].append(None)
                else:
                    product["fees"].append(tier['fee_min'])

            # Only add the product if it has data
            if product["thresholds"] and product["fees"]:
                products.append(product)

    # Fifth pass: handle single tiers that weren't part of any continuous schedule
    # This ensures we don't miss any tiers that don't fit into a continuous sequence
    for tier in sorted_tiers:
        # Check if this tier is already part of a product
        is_in_product = False
        for schedule in continuous_schedules:
            if any(t['index'] == tier['index'] for t in schedule):
                is_in_product = True
                break

        if not is_in_product:
            # This tier isn't part of any product, add it as a single-tier product
            if (tier['is_range'] == True and tier['fee_min'] != tier['fee_max'] and
                not pd.isna(tier['fee_min']) and not pd.isna(tier['fee_max'])):
                # Special case: Create two separate products for single tier with fee range
                # Product 1: minimum fee
                products.append({
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_min']]
                })
                # Product 2: maximum fee
                products.append({
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_max']]
                })
            else:
                # Handle NaN fees as None or single fee values
                fee_value = None if pd.isna(tier['fee_min']) else tier['fee_min']
                products.append({
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [fee_value]
                })

    # Final pass: merge products with identical threshold structures
    # This helps consolidate products that might have been split unnecessarily
    merged_products = []
    processed_indices = set()

    for i in range(len(products)):
        if i in processed_indices:
            continue

        current_product = products[i]
        processed_indices.add(i)

        # Check if there are other products with the same threshold structure
        for j in range(i + 1, len(products)):
            if j in processed_indices:
                continue

            # Check if the threshold structures match
            if (len(current_product["thresholds"]) == len(products[j]["thresholds"]) and
                all(current_product["thresholds"][k] == products[j]["thresholds"][k] for k in range(len(current_product["thresholds"]))) and
                all(current_product["upper_bounds"][k] == products[j]["upper_bounds"][k] for k in range(len(current_product["upper_bounds"])))):

                # These products have the same threshold structure
                # Check if they have different fee structures
                if current_product["fees"] != products[j]["fees"]:
                    # Different fee structures, keep both products
                    pass
                else:
                    # Same fee structure, mark the second product as processed
                    processed_indices.add(j)

        # Add the current product to the merged products
        merged_products.append(current_product)

    return merged_products

def validate_products(products):
    """
    Validate that the extracted products meet basic criteria.

    Args:
        products: List of product dictionaries

    Returns:
        Boolean indicating whether the products are valid
    """
    if not products:
        return False

    for product in products:
        # Each product must have thresholds and fees
        if not product.get("thresholds") or not product.get("fees"):
            return False

        # Thresholds and fees must have the same length
        if len(product.get("thresholds", [])) != len(product.get("fees", [])):
            return False

        # If upper_bounds are present, they must have the same length as thresholds
        if "upper_bounds" in product and len(product["upper_bounds"]) != len(product["thresholds"]):
            return False

    return True

def format_product(product):
    """
    Format a product dictionary into a string representation.

    Args:
        product: A dictionary with thresholds, upper_bounds, and fees

    Returns:
        A formatted string representation of the product
    """
    if not product["thresholds"] or not product["fees"]:
        return ""

    # Format thresholds with ranges
    threshold_ranges = []
    for i, lower in enumerate(product["thresholds"]):
        if "upper_bounds" in product and i < len(product["upper_bounds"]):
            upper = product["upper_bounds"][i]

            # Format the threshold range
            if upper == np.inf:
                threshold_ranges.append(f"{format_threshold(lower)}+")
            else:
                threshold_ranges.append(f"{format_threshold(lower)}-{format_threshold(upper)}")
        else:
            # Fallback if upper bounds are not available
            threshold_ranges.append(format_threshold(lower))

    thresholds_str = ", ".join(threshold_ranges)

    # Format fees
    formatted_fees = []
    for i, fee in enumerate(product["fees"]):
        if isinstance(fee, str):
            # Check if it's a fee range (contains a hyphen between two numbers)
            if '-' in fee and not fee.startswith('Flat Fee:'):
                # It's a fee range like "0.005-0.0125"
                try:
                    parts = fee.split('-')
                    if len(parts) == 2:
                        min_fee = float(parts[0])
                        max_fee = float(parts[1])
                        formatted_range = f"{format_percentage(min_fee)}-{format_percentage(max_fee)}"
                        formatted_fees.append(formatted_range)
                    else:
                        # Fallback if parsing fails
                        formatted_fees.append(fee)
                except (ValueError, IndexError):
                    # Fallback if parsing fails
                    formatted_fees.append(fee)
            else:
                # If it's already a string (e.g., "Flat Fee: $1000"), use it as is
                formatted_fees.append(fee)
        else:
            # Format the fee as a percentage
            formatted_fees.append(format_percentage(fee))

    fees_str = ", ".join(formatted_fees)

    return f"({thresholds_str}) ({fees_str})"

def main():
    """Main function to add product structure columns to the fee data."""
    input_path = os.path.join(PROCESSED_DATA_DIR, INPUT_FILE)
    output_path = os.path.join(PROCESSED_DATA_DIR, OUTPUT_FILE)

    logging.info(f"Loading data from {input_path}")

    # Load the data
    try:
        df = pd.read_csv(input_path, low_memory=False)
        logging.info(f"Loaded {df.shape[0]} rows from {input_path}")
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        return

    # Initialize product columns
    for i in range(1, 9):
        df[f'Product{i}'] = ""

    # Process each row
    logging.info("Identifying products for each row")
    total_rows = df.shape[0]

    for idx, row in df.iterrows():
        if idx % 1000 == 0:
            logging.info(f"Processing row {idx}/{total_rows} ({idx/total_rows*100:.1f}%)")

        # Identify products
        products = identify_products(row)

        # Check if we have a single continuous fee schedule that's been split into multiple products
        # This happens when all products have the same fee structure but different threshold ranges
        if len(products) > 1:
            # Check if this is a case where we have a single fee schedule split into multiple products
            is_single_schedule = True

            # Check if all products have continuous thresholds
            all_thresholds = []
            all_fees = []

            for product in products:
                all_thresholds.extend(product["thresholds"])
                all_fees.extend(product["fees"])

            # Sort thresholds and check if they form a continuous sequence
            sorted_indices = sorted(range(len(all_thresholds)), key=lambda i: all_thresholds[i])
            sorted_thresholds = [all_thresholds[i] for i in sorted_indices]
            sorted_fees = [all_fees[i] for i in sorted_indices]

            # Check if the fees are monotonically decreasing or equal
            # We'll allow for non-monotonic fees if they're part of a continuous threshold sequence
            # This handles cases where the fee increases at a higher threshold
            is_continuous_sequence = True
            for i in range(1, len(sorted_thresholds)):
                # Check if the thresholds form a continuous sequence
                tolerance = max(1000, sorted_thresholds[i-1] * 0.001)  # 0.1% or at least 1000 units
                if abs(sorted_thresholds[i] - sorted_thresholds[i-1]) > tolerance * 10:
                    is_continuous_sequence = False
                    break

            # If it's a continuous sequence, we'll consider it a single schedule
            # even if the fees are non-monotonic
            if is_continuous_sequence:
                is_single_schedule = True
            else:
                # If it's not a continuous sequence, check if the fees are monotonically decreasing
                # Skip this check if we have fee range strings, as they can't be easily compared
                try:
                    for i in range(1, len(sorted_fees)):
                                # Skip comparison if either fee is a string (fee range) or None
                        if (isinstance(sorted_fees[i], str) or isinstance(sorted_fees[i-1], str) or
                            sorted_fees[i] is None or sorted_fees[i-1] is None):
                            continue
                        if sorted_fees[i] > sorted_fees[i-1]:
                            is_single_schedule = False
                            break
                except (TypeError, ValueError):
                    # If comparison fails due to mixed types, assume it's a single schedule
                    pass

            # If it's a single schedule, merge the products
            if is_single_schedule:
                merged_product = {
                    "thresholds": sorted_thresholds,
                    "upper_bounds": [],  # We'll calculate these
                    "fees": sorted_fees
                }

                # Calculate upper bounds
                for i in range(len(sorted_thresholds) - 1):
                    merged_product["upper_bounds"].append(sorted_thresholds[i+1])

                # Add the last upper bound (infinity or the last upper bound from the original products)
                if products[-1]["upper_bounds"][-1] == np.inf:
                    merged_product["upper_bounds"].append(np.inf)
                else:
                    merged_product["upper_bounds"].append(sorted_thresholds[-1] * 2)  # Use a reasonable upper bound

                # Replace the products with the merged product
                products = [merged_product]

        # Format and add products to the dataframe
        for i, product in enumerate(products[:8], 1):
            df.at[idx, f'Product{i}'] = format_product(product)

    # Save the updated dataframe
    logging.info(f"Saving updated data to {output_path}")
    logging.info(f"DataFrame shape before saving: {df.shape}")
    logging.info(f"DataFrame columns: {list(df.columns)}")

    try:
        # Ensure the output directory exists
        output_dir = os.path.dirname(output_path)
        logging.info(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Output directory exists: {os.path.exists(output_dir)}")

        # Save the dataframe
        logging.info(f"Starting to save DataFrame to: {output_path}")
        df.to_csv(output_path, index=False)
        logging.info(f"DataFrame.to_csv() completed")

        # Verify the file was created
        logging.info(f"Checking if file exists: {output_path}")
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            logging.info(f"✅ Successfully saved file: {output_path} ({file_size:.1f} MB)")
        else:
            logging.error(f"❌ File was not created: {output_path}")
            # Try to list directory contents
            try:
                dir_contents = os.listdir(output_dir)
                logging.info(f"Directory contents: {dir_contents}")
            except Exception as list_error:
                logging.error(f"Error listing directory: {list_error}")

    except Exception as e:
        logging.error(f"❌ Error saving file to {output_path}: {str(e)}")
        logging.error(f"Exception type: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")

        # Try alternative path
        alternative_path = f"cleaned_fee_data_with_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logging.info(f"Attempting to save to alternative path: {alternative_path}")
        try:
            df.to_csv(alternative_path, index=False)
            if os.path.exists(alternative_path):
                logging.info(f"✅ Successfully saved to alternative path: {alternative_path}")
            else:
                logging.error(f"❌ Alternative path also failed: {alternative_path}")
        except Exception as e2:
            logging.error(f"❌ Failed to save to alternative path: {str(e2)}")
            return

    logging.info("Processing complete")

def detect_multiple_fee_schedules(tiers):
    """
    Detect if there are multiple fee schedules in the data.

    This is indicated by repeated threshold ranges with different fees,
    or by non-monotonic thresholds (e.g., a later tier has a lower threshold than an earlier tier).

    Args:
        tiers: List of tier dictionaries

    Returns:
        Boolean indicating whether multiple fee schedules are detected
    """
    if not tiers or len(tiers) <= 1:
        return False

    # Check for overlapping threshold ranges
    threshold_ranges = defaultdict(list)

    # Group tiers by their threshold ranges
    for tier in tiers:
        key = (tier['lower'], tier['upper'])
        threshold_ranges[key].append(tier)

    # Check if any threshold range appears multiple times with different fees
    for key, tier_group in threshold_ranges.items():
        if len(tier_group) > 1:
            # Check if the fees are different
            fees = set()
            for tier in tier_group:
                fees.add(tier['fee_min'])

            if len(fees) > 1:
                return True

    # Check for non-monotonic thresholds
    sorted_tiers = sorted(tiers, key=lambda x: x['index'])

    # Find potential starting points of new fee schedules
    for i in range(1, len(sorted_tiers)):
        if sorted_tiers[i]['lower'] <= sorted_tiers[i-1]['lower']:
            # This is a non-monotonic threshold, indicating a new fee schedule
            return True

    # Check for zero-value thresholds
    zero_threshold_count = 0
    for tier in tiers:
        if tier['lower'] == 0 and tier['upper'] == 0:
            zero_threshold_count += 1

    if zero_threshold_count > 1:
        # Multiple zero-value thresholds indicate multiple products
        return True

    # Check for non-monotonic fees
    sorted_tiers = sorted(tiers, key=lambda x: x['lower'])

    for i in range(1, len(sorted_tiers)):
        if sorted_tiers[i]['fee_min'] > sorted_tiers[i-1]['fee_min']:
            # This is a non-monotonic fee, indicating a new fee schedule
            return True

    return False

def process_multiple_fee_schedules(tiers):
    """
    Process multiple fee schedules in a single record.

    This function identifies and separates multiple fee schedules based on:
    1. Repeated threshold ranges with different fees
    2. Non-monotonic thresholds

    Args:
        tiers: List of tier dictionaries

    Returns:
        List of product dictionaries
    """
    if not tiers:
        return []

    products = []

    # Sort tiers by their original index to maintain the order they appeared in the data
    sorted_tiers = sorted(tiers, key=lambda x: x['index'])

    # Identify potential starting points of fee schedules
    schedule_start_indices = [0]  # Always include the first tier

    for i in range(1, len(sorted_tiers)):
        # Check if this tier's lower bound is less than or equal to the previous tier's lower bound
        if sorted_tiers[i]['lower'] <= sorted_tiers[i-1]['lower']:
            schedule_start_indices.append(i)

    # Process each fee schedule
    for i in range(len(schedule_start_indices)):
        start_idx = schedule_start_indices[i]
        end_idx = schedule_start_indices[i+1] if i+1 < len(schedule_start_indices) else len(sorted_tiers)

        # Extract the fee schedule
        schedule = sorted_tiers[start_idx:end_idx]

        # Skip empty schedules
        if not schedule:
            continue

        # Check if this schedule has any fee ranges
        schedule_has_ranges = any(
            tier['is_range'] == True and tier['fee_min'] != tier['fee_max']
            for tier in schedule
        )

        if schedule_has_ranges:
            # Special case: If we have a single tier with a fee range, create two separate products
            # This handles cases like "0.32% - 2.50%" which should become two products:
            # Product 1: ($0+, 0.32%) and Product 2: ($0+, 2.50%)
            if (len(schedule) == 1 and
                schedule[0]['is_range'] == True and
                schedule[0]['fee_min'] != schedule[0]['fee_max'] and
                not pd.isna(schedule[0]['fee_min']) and
                not pd.isna(schedule[0]['fee_max'])):

                tier = schedule[0]

                # Create first product with minimum fee
                product1 = {
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_min']]
                }
                products.append(product1)

                # Create second product with maximum fee
                product2 = {
                    "thresholds": [tier['lower']],
                    "upper_bounds": [tier['upper']],
                    "fees": [tier['fee_max']]
                }
                products.append(product2)
            else:
                # Default behavior: Create a single product that shows fee ranges where they exist
                # This consolidates fee ranges into one product instead of creating separate min/max products
                product = {"thresholds": [], "upper_bounds": [], "fees": []}

                for tier in schedule:
                    product["thresholds"].append(tier['lower'])
                    product["upper_bounds"].append(tier['upper'])

                    # For fee ranges, create a range string; for fixed fees, use the single value
                    if tier['is_range'] == True and tier['fee_min'] != tier['fee_max']:
                        # Create a fee range string (e.g., "0.50%-1.25%")
                        fee_range = f"{tier['fee_min']}-{tier['fee_max']}"
                        product["fees"].append(fee_range)
                    else:
                        # Use the single fee value, handling NaN as None
                        if pd.isna(tier['fee_min']):
                            product["fees"].append(None)
                        else:
                            product["fees"].append(tier['fee_min'])

                # Only add the product if it has data
                if product["thresholds"] and product["fees"]:
                    products.append(product)
        else:
            # No fee ranges, create a single product
            product = {"thresholds": [], "upper_bounds": [], "fees": []}

            for tier in schedule:
                product["thresholds"].append(tier['lower'])
                product["upper_bounds"].append(tier['upper'])
                # Handle NaN fees as None
                if pd.isna(tier['fee_min']):
                    product["fees"].append(None)
                else:
                    product["fees"].append(tier['fee_min'])

            # Only add the product if it has data
            if product["thresholds"] and product["fees"]:
                products.append(product)

    return products

def validate_products(products):
    """
    Validate that the extracted products are valid.

    Args:
        products: List of product dictionaries

    Returns:
        Boolean indicating whether the products are valid
    """
    if not products:
        return False

    for product in products:
        # Check that all required fields are present
        if not all(key in product for key in ['thresholds', 'upper_bounds', 'fees']):
            return False

        # Check that all lists have the same length
        if not (len(product['thresholds']) == len(product['upper_bounds']) == len(product['fees'])):
            return False

        # Check that all lists have at least one element
        if not (product['thresholds'] and product['upper_bounds'] and product['fees']):
            return False

    return True

if __name__ == "__main__":
    main()
