import pandas as pd
import re

def extract_fee_structure(fee_str_list):
    """
    Extract fee structure from a list of fee threshold strings.

    Args:
        fee_str_list: List of strings representing fee thresholds

    Returns:
        List of tuples (lower_bound, upper_bound, fee_percentage)
    """
    fee_structure = []

    for fee_str in fee_str_list:
        if pd.isna(fee_str) or fee_str == 'N/a' or fee_str == 'N/A' or fee_str == '-1':
            continue

        # Extract threshold range and fee percentage
        range_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?) - \$(\d+(?:,\d+)*(?:\.\d+)?)', fee_str)
        pct_match = re.search(r'\((\d+\.?\d*)%\)', fee_str)

        if range_match and pct_match:
            lower = float(range_match.group(1).replace(',', ''))
            upper = float(range_match.group(2).replace(',', ''))
            fee_pct = float(pct_match.group(1)) / 100
            fee_structure.append((lower, upper, fee_pct))
            continue

        # Extract lower bound with plus
        lower_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)\+', fee_str)
        if lower_match and pct_match:
            lower = float(lower_match.group(1).replace(',', ''))
            fee_pct = float(pct_match.group(1)) / 100
            fee_structure.append((lower, float('inf'), fee_pct))
            continue

        # Extract upper bound with less than
        upper_match = re.search(r'< \$(\d+(?:,\d+)*(?:\.\d+)?)', fee_str)
        if upper_match and pct_match:
            upper = float(upper_match.group(1).replace(',', ''))
            fee_pct = float(pct_match.group(1)) / 100
            fee_structure.append((0, upper, fee_pct))
            continue

    # Sort by lower bound
    fee_structure.sort(key=lambda x: x[0])

    return fee_structure

def calculate_effective_fee(fee_structure, portfolio_value):
    """
    Calculate the effective fee for a given portfolio value.

    Args:
        fee_structure: List of tuples (lower_bound, upper_bound, fee_percentage)
        portfolio_value: Portfolio value in dollars

    Returns:
        Effective fee percentage
    """
    if not fee_structure:
        return None

    total_fee = 0
    remaining_value = portfolio_value

    for i, (lower, upper, fee_pct) in enumerate(fee_structure):
        if i < len(fee_structure) - 1:
            next_lower = fee_structure[i+1][0]
            bracket_size = next_lower - lower
            if remaining_value <= 0:
                break
            bracket_fee = min(bracket_size, remaining_value) * fee_pct
            total_fee += bracket_fee
            remaining_value -= bracket_size
        else:
            # Last threshold
            if remaining_value > 0:
                total_fee += remaining_value * fee_pct

    return total_fee / portfolio_value if portfolio_value > 0 else None

def main():
    # Example fee structure
    example_fee_structure = [
        (0, 1000000, 0.01),        # 1% for first $1M
        (1000000, 5000000, 0.0075)  # 0.75% for $1M-$5M
    ]

    # Example portfolio values
    portfolio_values = [500000, 1000000, 2500000, 5000000, 10000000]

    print("Example Fee Structure:")
    for lower, upper, fee_pct in example_fee_structure:
        if upper == float('inf'):
            print(f"  ${lower:,.2f}+ at {fee_pct:.2%}")
        else:
            print(f"  ${lower:,.2f} - ${upper:,.2f} at {fee_pct:.2%}")

    print("\nEffective Fees for Different Portfolio Sizes:")
    for portfolio_value in portfolio_values:
        effective_fee = calculate_effective_fee(example_fee_structure, portfolio_value)
        if effective_fee is not None:
            print(f"  ${portfolio_value:,.2f}: {effective_fee:.2%} (${portfolio_value * effective_fee:,.2f} annually)")
        else:
            print(f"  ${portfolio_value:,.2f}: Could not calculate effective fee")

if __name__ == "__main__":
    main()
