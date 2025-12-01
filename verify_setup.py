#!/usr/bin/env python3
"""
Setup verification script for PySpark Learning Project.

Run this to verify your environment is correctly configured before
starting the learning modules.
"""

import sys
import os
import subprocess


def print_header(text):
    """Print a section header"""
    print(f"\n{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}\n")


def check_python_version():
    """Check Python version"""
    print("🐍 Checking Python version...")
    version = sys.version_info
    
    if version.major == 3 and version.minor >= 8:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro} (Good!)")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro}")
        print(f"   ⚠️  Python 3.8+ required. Please upgrade.")
        return False


def check_java():
    """Check Java installation"""
    print("☕ Checking Java...")
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        output = result.stderr if result.stderr else result.stdout
        
        if "version" in output.lower():
            version_line = output.split('\n')[0]
            print(f"   ✅ {version_line}")
            
            # Check if it's Java 8 or 11
            if 'version "1.8' in output or 'version "8' in output or \
               'version "11' in output or 'version "17' in output:
                print(f"   ✅ Java version is compatible with Spark")
            else:
                print(f"   ⚠️  Java 8, 11, or 17 recommended for Spark")
            return True
        else:
            print("   ❌ Could not determine Java version")
            return False
            
    except FileNotFoundError:
        print("   ❌ Java not found in PATH")
        print("   📝 Install Java 11: https://adoptium.net/")
        return False
    except Exception as e:
        print(f"   ❌ Error checking Java: {e}")
        return False


def check_pyspark():
    """Check if PySpark is installed"""
    print("⚡ Checking PySpark installation...")
    try:
        import pyspark
        print(f"   ✅ PySpark {pyspark.__version__} installed")
        return True
    except ImportError:
        print("   ❌ PySpark not installed")
        print("   📝 Run: pip install -r requirements.txt")
        return False


def check_dependencies():
    """Check other dependencies"""
    print("📦 Checking other dependencies...")
    
    dependencies = {
        'pandas': 'pandas',
        'pyarrow': 'pyarrow',
        'faker': 'faker'
    }
    
    all_ok = True
    for name, import_name in dependencies.items():
        try:
            __import__(import_name)
            print(f"   ✅ {name} installed")
        except ImportError:
            print(f"   ❌ {name} not installed")
            all_ok = False
    
    if not all_ok:
        print("\n   📝 Install missing dependencies: pip install -r requirements.txt")
    
    return all_ok


def check_project_structure():
    """Verify project structure"""
    print("📁 Checking project structure...")
    
    expected_items = [
        ('jobs/01_dataframe_basics.py', 'file'),
        ('jobs/02_aggregations.py', 'file'),
        ('jobs/03_joins.py', 'file'),
        ('jobs/04_analytics_udfs.py', 'file'),
        ('jobs/05_search_indexing.py', 'file'),
        ('utils/spark_session.py', 'file'),
        ('utils/data_generator.py', 'file'),
        ('docs/concepts.md', 'file'),
        ('data/', 'dir'),
        ('output/', 'dir'),
    ]
    
    project_root = os.path.dirname(os.path.abspath(__file__))
    all_ok = True
    
    for item, item_type in expected_items:
        path = os.path.join(project_root, item)
        
        if item_type == 'file':
            if os.path.isfile(path):
                print(f"   ✅ {item}")
            else:
                print(f"   ❌ {item} (missing)")
                all_ok = False
        else:  # directory
            if os.path.isdir(path):
                print(f"   ✅ {item}")
            else:
                print(f"   ❌ {item} (missing)")
                all_ok = False
    
    return all_ok


def test_spark_session():
    """Try to create a SparkSession"""
    print("🧪 Testing SparkSession creation...")
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("SetupTest") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        # Try a simple operation
        data = [(1, "test")]
        df = spark.createDataFrame(data, ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"   ✅ SparkSession created successfully")
        print(f"   ✅ Basic DataFrame operation works")
        return True
        
    except Exception as e:
        print(f"   ❌ Error creating SparkSession: {e}")
        return False


def main():
    """Run all checks"""
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("     PySpark Learning Project - Setup Verification")
    print("🎓 " + "="*66 + " 🎓")
    
    checks = [
        ("Python Version", check_python_version),
        ("Java Installation", check_java),
        ("PySpark Installation", check_pyspark),
        ("Dependencies", check_dependencies),
        ("Project Structure", check_project_structure),
        ("Spark Session", test_spark_session),
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        print_header(check_name)
        results[check_name] = check_func()
    
    # Summary
    print_header("Summary")
    
    passed = sum(results.values())
    total = len(results)
    
    for check_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {check_name}")
    
    print(f"\n{'='*70}")
    print(f"  Result: {passed}/{total} checks passed")
    print(f"{'='*70}\n")
    
    if passed == total:
        print("🎉 All checks passed! You're ready to start learning.")
        print("\n📚 Next steps:")
        print("   1. Read the concepts: cat docs/concepts.md")
        print("   2. Run first job: python jobs/01_dataframe_basics.py")
        print("   3. Open Spark UI: http://localhost:4040 (while job runs)")
        print("\n💡 Tip: Check QUICKSTART.md for detailed instructions")
        return 0
    else:
        print("⚠️  Some checks failed. Please fix the issues above.")
        print("\n🔧 Common solutions:")
        print("   • Missing dependencies: pip install -r requirements.txt")
        print("   • Java not found: Install Java 11 from https://adoptium.net/")
        print("   • Virtual env: source venv/bin/activate")
        print("\n📖 See QUICKSTART.md for detailed troubleshooting")
        return 1


if __name__ == "__main__":
    sys.exit(main())
