# Python Code Quality Guide

This project uses modern Python tooling to maintain code quality and consistency.

## 🎯 Quick Start

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Set up pre-commit hooks (runs checks automatically on git commit)
pre-commit install

# Run all quality checks
make check
```

## 🛠️ Tools Overview

### 1. **Black** - Code Formatter
Automatically formats Python code to a consistent style.

```bash
# Format all code
black jobs/ utils/ *.py

# Check without modifying
black --check jobs/ utils/ *.py

# Format a single file
black jobs/01_dataframe_basics.py
```

**Configuration**: See `[tool.black]` in `pyproject.toml`
- Line length: 100 characters
- Target: Python 3.12+

### 2. **Ruff** - Fast Linter
Ultra-fast Python linter that replaces multiple tools (flake8, isort, pyupgrade, etc.)

```bash
# Lint and auto-fix issues
ruff check --fix jobs/ utils/ *.py

# Just check (no fixes)
ruff check jobs/ utils/ *.py

# Check a single file
ruff check jobs/01_dataframe_basics.py
```

**What it checks**:
- Code style issues (pycodestyle)
- Common bugs (pyflakes, bugbear)
- Import sorting (isort)
- Code modernization (pyupgrade)

**Configuration**: See `[tool.ruff]` in `pyproject.toml`

### 3. **mypy** - Type Checker
Static type checking for Python to catch type-related bugs.

```bash
# Type check all code
mypy jobs/ utils/ *.py

# Check a single file
mypy jobs/01_dataframe_basics.py

# Strict mode
mypy --strict jobs/01_dataframe_basics.py
```

**Configuration**: See `[tool.mypy]` in `pyproject.toml`
- Relaxed mode for learning project (strict mode disabled)
- Ignores missing imports for PySpark and other libraries

### 4. **pytest** - Testing Framework
Testing framework for Python (future use).

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov --cov-report=html

# Run specific test
pytest tests/test_spark_session.py
```

**Configuration**: See `[tool.pytest]` in `pyproject.toml`

### 5. **pre-commit** - Git Hooks
Automatically runs quality checks before each git commit.

```bash
# Install hooks
pre-commit install

# Run manually on all files
pre-commit run --all-files

# Run on staged files only
pre-commit run

# Update hook versions
pre-commit autoupdate
```

**What runs on commit**:
1. Trailing whitespace removal
2. End-of-file fixes
3. YAML/JSON/TOML validation
4. Black formatting
5. Ruff linting
6. mypy type checking

**Configuration**: See `.pre-commit-config.yaml`

## 📝 Makefile Commands

The project includes a Makefile with convenient shortcuts:

```bash
make help              # Show all available commands

# Setup
make setup             # Complete dev environment setup
make install           # Install project dependencies
make install-dev       # Install with dev dependencies

# Quality Checks
make format            # Format code with black and ruff
make lint              # Lint code with ruff
make type-check        # Type check with mypy
make check             # Run all checks (format + lint + type-check)

# Testing
make test              # Run tests
make test-cov          # Run tests with coverage report

# Cleanup
make clean             # Remove generated files and caches

# Running Jobs
make run-job-1         # Run Job 1
make run-job-2         # Run Job 2
make run-job-3         # Run Job 3
make run-job-4         # Run Job 4
make run-job-5         # Run Job 5
make run-all           # Run all jobs (non-interactive)

# Jupyter
make jupyter           # Start Jupyter notebook server
```

## 🔄 Typical Workflow

### Before Committing Code

```bash
# 1. Format your code
make format

# 2. Check for issues
make lint

# 3. Type check
make type-check

# Or run all at once:
make check

# 4. Stage and commit
git add .
git commit -m "Your commit message"
# Pre-commit hooks run automatically here
```

### Daily Development

```bash
# Start your day
source .venv/bin/activate

# Write code...

# Before breaks/end of day
make check
git commit -am "Work in progress"
```

## 🎨 Code Style Guidelines

### Line Length
- **Maximum**: 100 characters
- Black and Ruff enforce this automatically

### Imports
- Sorted automatically by Ruff
- Grouped: stdlib, third-party, local
- Example:
  ```python
  import os
  import sys
  
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import col, sum
  
  from utils.spark_session import get_spark_session
  ```

### Type Hints (Optional but Recommended)
```python
from pyspark.sql import DataFrame, SparkSession

def process_data(spark: SparkSession, path: str) -> DataFrame:
    """Process data from file."""
    return spark.read.csv(path)
```

### Docstrings
```python
def calculate_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate aggregated metrics from transaction data.
    
    Args:
        df: Input DataFrame with transaction records
        
    Returns:
        DataFrame with calculated metrics
    """
    return df.groupBy("user_id").agg(sum("amount"))
```

## 🚫 Common Issues and Fixes

### Issue: "Line too long"
**Fix**: Black handles this automatically. Run `make format`.

### Issue: "Import not sorted"
**Fix**: Ruff handles this. Run `ruff check --fix`.

### Issue: "Unused import"
**Fix**: Remove it manually or run `ruff check --fix`.

### Issue: Pre-commit hook fails
**Fix**:
```bash
# See what failed
git status

# Fix the issues
make check

# Stage fixes
git add .

# Retry commit
git commit -m "Your message"
```

### Issue: mypy errors about PySpark types
**Expected**: PySpark has limited type stubs. Add `# type: ignore` for known issues:
```python
result = df.collect()  # type: ignore
```

## 📚 Additional Resources

- **Black**: https://black.readthedocs.io/
- **Ruff**: https://docs.astral.sh/ruff/
- **mypy**: https://mypy.readthedocs.io/
- **pytest**: https://docs.pytest.org/
- **pre-commit**: https://pre-commit.com/

## 🎯 Benefits

✅ **Consistency**: Same code style across the project  
✅ **Quality**: Catch bugs and issues early  
✅ **Automation**: Tools do the work for you  
✅ **Learning**: Better understand Python best practices  
✅ **Collaboration**: Easier code reviews and contributions  

## 🔧 Customization

All tool configurations are in `pyproject.toml`. You can adjust:
- Line length
- Enabled/disabled rules
- Strictness levels
- File exclusions

Example (in `pyproject.toml`):
```toml
[tool.black]
line-length = 120  # Change from 100 to 120

[tool.ruff]
lint.ignore = ["E501"]  # Ignore specific rule
```

---

**Happy coding!** 🎉 The tools are here to help, not hinder.
