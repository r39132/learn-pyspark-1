#!/usr/bin/env python3
"""
Setup Verification and Health Check

Verifies that all components are properly configured:
- Python and Java versions
- Virtual environment
- Required packages
- Environment variables
- Jupyter configuration
- Sample data
"""

import sys
import os
import subprocess
from pathlib import Path

def check_mark(condition):
    return "✅" if condition else "❌"

def check_python_version():
    """Check Python version"""
    print("\n📌 Python Version")
    version = sys.version_info
    meets_req = version.major == 3 and version.minor >= 12
    print(f"  {check_mark(meets_req)} Python {version.major}.{version.minor}.{version.micro}")
    if not meets_req:
        print("     ⚠️  Python 3.12+ required")
    return meets_req

def check_java_version():
    """Check Java version"""
    print("\n📌 Java Version")
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True)
        output = result.stderr
        if 'version "17' in output or 'version "11' in output:
            print(f"  ✅ Java installed (17 recommended)")
            print(f"     {output.split()[2]}")
            return True
        else:
            print(f"  ⚠️  Java found but version unclear")
            print(f"     {output.split()[2] if len(output.split()) > 2 else 'Unknown'}")
            return False
    except FileNotFoundError:
        print("  ❌ Java not found")
        print("     Install Java 17 with: brew install openjdk@17")
        return False

def check_venv():
    """Check virtual environment"""
    print("\n📌 Virtual Environment")
    in_venv = hasattr(sys, 'real_prefix') or (
        hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
    )
    print(f"  {check_mark(in_venv)} Virtual environment active")
    if not in_venv:
        print("     ⚠️  Activate with: source .venv/bin/activate")
    return in_venv

def check_packages():
    """Check required packages"""
    print("\n📌 Required Packages")
    packages = {
        'pyspark': 'PySpark',
        'pandas': 'Pandas',
        'jupyter': 'Jupyter',
        'notebook': 'Notebook',
        'dotenv': 'python-dotenv'
    }
    
    all_installed = True
    for package, name in packages.items():
        try:
            __import__(package)
            print(f"  ✅ {name}")
        except ImportError:
            print(f"  ❌ {name}")
            all_installed = False
    
    if not all_installed:
        print("\n     Install with: uv pip install -r requirements.txt")
    
    return all_installed

def check_env_file():
    """Check .env file"""
    print("\n📌 Environment Configuration")
    env_exists = Path('.env').exists()
    example_exists = Path('.env.example').exists()
    
    print(f"  {check_mark(env_exists)} .env file exists")
    print(f"  {check_mark(example_exists)} .env.example exists")
    
    if not env_exists and example_exists:
        print("     ℹ️  Create with: cp .env.example .env")
    
    if env_exists:
        from dotenv import load_dotenv
        load_dotenv()
        
        spark_master = os.getenv('SPARK_MASTER')
        print(f"  ✅ SPARK_MASTER: {spark_master}")
        
        jupyter_port = os.getenv('JUPYTER_PORT', '8888')
        print(f"  ✅ JUPYTER_PORT: {jupyter_port}")
    
    return env_exists

def check_directories():
    """Check project structure"""
    print("\n📌 Project Structure")
    dirs = {
        'notebooks': 'Notebooks directory',
        'jobs': 'Jobs directory',
        'utils': 'Utils directory',
        'data': 'Data directory',
        'docs': 'Docs directory'
    }
    
    all_exist = True
    for dir_name, description in dirs.items():
        exists = Path(dir_name).exists()
        print(f"  {check_mark(exists)} {description}")
        if not exists:
            all_exist = False
    
    return all_exist

def check_notebooks():
    """Check notebooks"""
    print("\n📌 Jupyter Notebooks")
    notebooks = [
        '01_dataframe_basics.ipynb',
        '02_aggregations.ipynb',
        '03_joins.ipynb',
        '04_analytics_udfs.ipynb',
        '05_search_indexing.ipynb'
    ]
    
    all_exist = True
    for nb in notebooks:
        path = Path('notebooks') / nb
        exists = path.exists()
        print(f"  {check_mark(exists)} {nb}")
        if not exists:
            all_exist = False
    
    return all_exist

def check_version_files():
    """Check version manager files"""
    print("\n📌 Version Manager Files")
    python_ver = Path('.python-version').exists()
    java_ver = Path('.java-version').exists()
    
    print(f"  {check_mark(python_ver)} .python-version")
    print(f"  {check_mark(java_ver)} .java-version")
    
    return python_ver and java_ver

def check_pyspark():
    """Try to create a SparkSession"""
    print("\n📌 PySpark Test")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("HealthCheck") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        # Create a simple DataFrame
        data = [(1, "test")]
        df = spark.createDataFrame(data, ["id", "name"])
        count = df.count()
        
        spark.stop()
        
        print(f"  ✅ SparkSession created successfully")
        print(f"  ✅ DataFrame operations work")
        return True
    except Exception as e:
        print(f"  ❌ PySpark test failed: {e}")
        return False

def main():
    print("="*70)
    print("🔍 PySpark Learning Project - Setup Verification")
    print("="*70)
    
    checks = [
        ("Python Version", check_python_version),
        ("Java Version", check_java_version),
        ("Virtual Environment", check_venv),
        ("Required Packages", check_packages),
        ("Environment Configuration", check_env_file),
        ("Project Structure", check_directories),
        ("Jupyter Notebooks", check_notebooks),
        ("Version Manager Files", check_version_files),
        ("PySpark Test", check_pyspark),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n  ❌ Error checking {name}: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "="*70)
    print("📊 Summary")
    print("="*70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        print(f"  {check_mark(result)} {name}")
    
    print(f"\n  Passed: {passed}/{total}")
    
    if passed == total:
        print("\n  🎉 All checks passed! You're ready to start learning.")
        print("\n  Next steps:")
        print("    1. Read: docs/concepts.md")
        print("    2. Start Jupyter: python start_jupyter.py")
        print("    3. Open: http://localhost:8888")
        print("    4. Begin with: notebooks/01_dataframe_basics.ipynb")
    else:
        print("\n  ⚠️  Some checks failed. Please fix the issues above.")
        print("\n  Quick fixes:")
        print("    • Activate venv: source .venv/bin/activate")
        print("    • Install packages: uv pip install -r requirements.txt")
        print("    • Setup env: cp .env.example .env")
        print("    • See: TROUBLESHOOTING.md")
    
    print("="*70)
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
