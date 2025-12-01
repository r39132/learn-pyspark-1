# PySpark Learning Project 🚀

A hands-on, progressive learning path for mastering PySpark. Built for developers who want to learn by doing.

## 🌟 What Makes This Special?

✅ **Structured Learning Path** - 5 progressive modules from basics to advanced  
✅ **Real-world Use Cases** - Analytics, search, recommendations, and more  
✅ **Heavily Commented Code** - Learn from inline explanations  
✅ **Self-contained** - Generates sample data automatically  
✅ **Production-ready Patterns** - Best practices you'll actually use  

## ⚡ Quick Start (5 minutes)

### Prerequisites (macOS)

Before you begin, ensure you have these tools installed:

- **Homebrew** - Package manager for macOS
  ```bash
  # Install if needed: /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ```

- **pyenv** - Python version manager
  ```bash
  brew install pyenv
  ```

- **jenv** - Java version manager  
  ```bash
  brew install jenv
  ```

- **uv** - Fast Python package installer
  ```bash
  # Install via brew (recommended)
  brew install uv
  # Or via pipx: pipx install uv
  ```

### Option A: Jupyter Notebooks (Recommended for Learning)

```bash
# 1. Setup Python 3.12+ and Java 17
pyenv install 3.12.0
pyenv local 3.12.0
jenv add /path/to/java-17  # or: jenv local 17

# 2. Install dependencies with uv (fast!)
uv venv 
source .venv/bin/activate
uv pip install -r requirements.txt

# 3. Setup environment (copy .env.example to .env and customize)
cp .env.example .env

# 4. Start Jupyter Notebook Server
python start_jupyter.py

# 5. Open your browser to http://localhost:8888
# 6. Start with notebooks/01_dataframe_basics.ipynb
```

### Option B: Python Scripts

```bash
# Setup steps 1-3 same as above

# 4. Verify setup
python verify_setup.py

# 5. Run jobs directly
python jobs/01_dataframe_basics.py
```

### ⚠️ Common Setup Issue: JAVA_HOME

If you get a `FileNotFoundError: [Errno 2] No such file or directory: './bin/spark-submit'` error when starting Spark:

**Problem**: Jupyter notebooks don't automatically inherit shell environment variables from jenv.

**Solution**:
1. Find your Java installation path:
   ```bash
   jenv versions  # Lists installed Java versions
   # Look for 17.0.x version, e.g., 17.0.8.1
   ```

2. Add `JAVA_HOME` to your `.env` file:
   ```bash
   # Add at the top of .env
   JAVA_HOME=/Users/YOUR_USERNAME/.jenv/versions/17.0.8.1
   ```

3. The notebook's first cell will automatically load this and set the environment variable.

**Verify it works**:
```bash
# Test Spark can start with Java
source .venv/bin/activate
JAVA_HOME=~/.jenv/versions/17.0.8.1 python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').master('local[1]').getOrCreate(); print('✅ Success'); spark.stop()"
```

**Need help?** See [docs/QUICKSTART.md](docs/QUICKSTART.md) for detailed setup instructions.

## 🎯 Project Structure

```
learn-pyspark-1/
├── notebooks/               # 🎓 Jupyter notebooks (start here!)
├── jobs/                    # Python script versions
├── utils/                   # Shared utilities
├── docs/                    # All documentation
├── data/                    # Sample datasets (auto-generated)
└── output/                  # Job outputs
```

### 📖 Documentation

**Learning Resources:**
- **[Core Concepts](docs/concepts.md)** - Spark fundamentals explained
- **[Cheat Sheet](docs/CHEATSHEET.md)** - Quick reference guide
- **[Learning Flow](docs/LEARNING_FLOW.md)** - Visual learning roadmap

**Setup & Configuration:**
- **[Quick Start](docs/QUICKSTART.md)** - Detailed setup instructions
- **[Troubleshooting](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Environment Setup](docs/ENVIRONMENT_SETUP.md)** - Configure .env and Jupyter

**Development:**
- **[Code Quality Guide](docs/CODE_QUALITY.md)** - Python hygiene tools and best practices
- **[Notebooks Guide](notebooks/README.md)** - Using Jupyter notebooks

## 📚 Learning Modules (4 hours total)

Each module available as both Jupyter notebook (recommended) and Python script:

### Job 1: DataFrame Basics (30 min)
**Concepts**: DataFrames, schemas, basic transformations
- Creating DataFrames from various sources
- Reading and writing data (CSV, JSON, Parquet)
- Basic transformations: `select()`, `filter()`, `withColumn()`
- Column expressions and operators
- Showing and inspecting data

### Job 2: Aggregations (45 min)
**Concepts**: GroupBy operations, aggregate functions, window functions
- `groupBy()` and aggregate functions (sum, avg, count, etc.)
- Multiple aggregations at once
- Window functions for running totals, rankings
- Partitioning and ordering data

### Job 3: Joins (45 min)
**Concepts**: Different join types, broadcast joins, data relationships
- Inner, outer, left, right joins
- Cross joins and self joins
- Broadcast joins for small tables
- Handling duplicate column names

### Job 4: Advanced Analytics & UDFs (60 min)
**Concepts**: User-defined functions, complex analytics, ML prep
- Creating and registering UDFs
- Pandas UDFs for better performance
- Complex aggregations and pivoting
- Data preparation for ML pipelines

### Job 5: Search Indexing (60 min)
**Concepts**: Text processing, inverted indexes, personalization
- Text tokenization and processing
- Building inverted search indexes
- TF-IDF scoring
- Product recommendation scenarios

## 🚀 Getting Started

### Prerequisites
- Python 3.12+ (managed with pyenv)
- Java 17 (managed with jenv)
- uv (fast Python package installer)

### Installation

```bash
# Install version managers (one-time setup)
brew install pyenv jenv  # macOS
# For other OS, see QUICKSTART.md

# Install uv (fast package installer)
brew install uv  # or: pip install uv

# Set up Python 3.12+
pyenv install 3.12.0
pyenv local 3.12.0

# Set up Java 17
jenv add /path/to/java-17
jenv local 17

# Create virtual environment with uv
uv venv 
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies (much faster with uv!)
uv pip install -r requirements.txt

# Optional: Install development tools for code quality
uv pip install -e ".[dev]"
pre-commit install
```### Running Jobs

Each job is self-contained and can be run independently:

```bash
# Run a specific job
python jobs/01_dataframe_basics.py

# Or run all jobs in sequence
for job in jobs/*.py; do python "$job"; done
```

## 💡 Learning Approach

1. **Read the concepts** - Start with `docs/concepts.md` for foundational knowledge
2. **Run each job** - Execute jobs in order, observe the output
3. **Read the code** - Each job is heavily commented with explanations
4. **Experiment** - Modify the code, try different transformations
5. **Build on it** - Use these patterns for your own use cases

## 📊 Sample Data

Sample datasets are generated automatically when you run the jobs. You can also generate them manually:

```bash
python utils/data_generator.py
```

## 🎓 Key Takeaways

By completing this project, you'll understand:
- How Spark distributes data across a cluster
- Lazy evaluation and action vs transformation
- Efficient data processing patterns
- When to use different join strategies
- How to optimize Spark jobs
- Real-world use cases: analytics, aggregations, search, personalization

## 🔧 Tips

- **Start small**: Test with small datasets first
- **Check the plan**: Use `.explain()` to understand query execution
- **Monitor**: Use Spark UI (http://localhost:4040) when jobs run
- **Partition wisely**: Proper partitioning is key to performance
- **Cache strategically**: Use `.cache()` for reused DataFrames

## 🛠️ Development & Code Quality

This project includes Python hygiene tools for maintaining code quality:

### Quick Commands (using Makefile)

```bash
# Complete setup with dev tools
make setup

# Format code with black and ruff
make format

# Lint code with ruff
make lint

# Type check with mypy
make type-check

# Run all checks
make check

# Clean generated files
make clean

# Run specific job
make run-job-1

# Run all jobs (non-interactive)
make run-all

# Start Jupyter
make jupyter
```

### Tools Included

- **Black** - Opinionated code formatter
- **Ruff** - Fast Python linter (replaces flake8, isort, and more)
- **mypy** - Static type checker
- **pytest** - Testing framework
- **pre-commit** - Git hooks for automatic checks before commits

### Manual Usage

```bash
# Format code
black jobs/ utils/ *.py
ruff check --fix jobs/ utils/ *.py

# Lint without fixing
ruff check jobs/ utils/ *.py

# Type checking
mypy jobs/ utils/ *.py

# Run pre-commit hooks manually
pre-commit run --all-files
```

### Configuration Files

- `pyproject.toml` - Main project configuration (black, ruff, mypy, pytest)
- `.flake8` - Flake8 configuration (legacy compatibility)
- `.pre-commit-config.yaml` - Pre-commit hooks configuration
- `.editorconfig` - Editor settings for consistent code style
- `Makefile` - Convenient commands for common tasks

Happy Sparking! 🎉
