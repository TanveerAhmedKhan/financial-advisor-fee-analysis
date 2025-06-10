#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Environment setup script for comprehensive financial advisor fee data processing.

This script sets up the Python environment and installs required dependencies.

Usage:
    python setup_environment.py

Author: Augment Agent
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed")
        print(f"   Error: {e.stderr.strip()}")
        return False

def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    print(f"🐍 Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8 or higher is required")
        return False
    
    print("✅ Python version is compatible")
    return True

def setup_virtual_environment():
    """Set up virtual environment using uv or venv."""
    print("\n" + "="*60)
    print("SETTING UP VIRTUAL ENVIRONMENT")
    print("="*60)
    
    # Check if uv is available
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
        print("✅ uv package manager found")
        use_uv = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("ℹ️  uv not found, using standard venv")
        use_uv = False
    
    # Create virtual environment
    if use_uv:
        if not run_command("uv venv .venv", "Creating virtual environment with uv"):
            return False
    else:
        if not run_command("python -m venv .venv", "Creating virtual environment with venv"):
            return False
    
    return True

def install_dependencies():
    """Install required dependencies."""
    print("\n" + "="*60)
    print("INSTALLING DEPENDENCIES")
    print("="*60)
    
    # Determine activation command based on OS
    if os.name == 'nt':  # Windows
        activate_cmd = ".venv\\Scripts\\activate"
        pip_cmd = ".venv\\Scripts\\pip"
    else:  # Unix/Linux/macOS
        activate_cmd = "source .venv/bin/activate"
        pip_cmd = ".venv/bin/pip"
    
    # Install dependencies
    dependencies = [
        "numpy>=1.20.0",
        "pandas>=1.3.0", 
        "matplotlib>=3.4.0",
        "seaborn>=0.11.0",
        "tqdm>=4.60.0"
    ]
    
    for dep in dependencies:
        if not run_command(f"{pip_cmd} install {dep}", f"Installing {dep}"):
            print(f"⚠️  Failed to install {dep}, continuing...")
    
    # Update requirements.txt
    if not run_command(f"{pip_cmd} freeze > requirements.txt", "Updating requirements.txt"):
        print("⚠️  Failed to update requirements.txt")
    
    return True

def verify_installation():
    """Verify that the installation was successful."""
    print("\n" + "="*60)
    print("VERIFYING INSTALLATION")
    print("="*60)
    
    # Determine python command based on OS
    if os.name == 'nt':  # Windows
        python_cmd = ".venv\\Scripts\\python"
    else:  # Unix/Linux/macOS
        python_cmd = ".venv/bin/python"
    
    test_imports = [
        "import pandas; print(f'✅ pandas {pandas.__version__}')",
        "import numpy; print(f'✅ numpy {numpy.__version__}')",
        "import matplotlib; print(f'✅ matplotlib {matplotlib.__version__}')",
    ]
    
    for test in test_imports:
        if not run_command(f'{python_cmd} -c "{test}"', "Testing import"):
            print("⚠️  Some imports failed, but continuing...")
    
    return True

def main():
    """Main setup function."""
    print("="*80)
    print("COMPREHENSIVE FEE PROCESSOR - ENVIRONMENT SETUP")
    print("="*80)
    print()
    print("This script will set up the Python environment for processing")
    print("financial advisor fee data.")
    print()
    
    # Check Python version
    if not check_python_version():
        return 1
    
    # Setup virtual environment
    if not setup_virtual_environment():
        print("❌ Failed to set up virtual environment")
        return 1
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Failed to install dependencies")
        return 1
    
    # Verify installation
    verify_installation()
    
    print("\n" + "="*80)
    print("SETUP COMPLETED")
    print("="*80)
    print()
    print("Next steps:")
    print("1. Activate the virtual environment:")
    if os.name == 'nt':  # Windows
        print("   .venv\\Scripts\\activate")
    else:  # Unix/Linux/macOS
        print("   source .venv/bin/activate")
    print()
    print("2. Run the comprehensive processing script:")
    print("   python run_comprehensive_processing.py")
    print()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
