# Quick Start Guide

Welcome to your PySpark learning journey! Follow these steps to get started.

## Prerequisites

1. **Python 3.12+** (managed with pyenv)
2. **Java 17** (managed with jenv, required by Spark)
3. **uv** (fast Python package installer)

### Installing Version Managers

**macOS:**
```bash
# Install pyenv (Python version manager)
brew install pyenv
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
source ~/.zshrc

# Install jenv (Java version manager)
brew install jenv
echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(jenv init -)"' >> ~/.zshrc
source ~/.zshrc

# Install Java 17
brew install openjdk@17
jenv add /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Install uv (fast package installer)
pip install uv
```

**Linux (Ubuntu/Debian):**
```bash
# Install pyenv
curl https://pyenv.run | bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
source ~/.bashrc

# Install jenv
git clone https://github.com/jenv/jenv.git ~/.jenv
echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(jenv init -)"' >> ~/.bashrc
source ~/.bashrc

# Install Java 17
sudo apt-get update
sudo apt-get install openjdk-17-jdk
jenv add /usr/lib/jvm/java-17-openjdk-amd64

# Install uv
pip install uv
```

**Windows:**
```powershell
# Install pyenv-win
Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"

# Install Java 17 from Adoptium
# Download from: https://adoptium.net/temurin/releases/?version=17

# Install uv
pip install uv
```

### Verify Installations

```bash
# Check pyenv
pyenv --version

# Check jenv
jenv --version

# Check uv
uv --version
```

## Setup (5 minutes)

### For Jupyter Notebooks (Recommended)

1. **Set Python version to 3.12+**:
   ```bash
   # Install Python 3.12
   pyenv install 3.12.0
   
   # Set local version for this project
   cd learn-pyspark-1
   pyenv local 3.12.0
   
   # Verify
   python --version  # Should show 3.12.0
   ```

2. **Set Java version to 17**:
   ```bash
   # If not already added, add Java 17 to jenv
   jenv add /path/to/java-17
   
   # Set local version for this project
   jenv local 17
   
   # Verify
   java -version  # Should show version 17
   ```

3. **Create virtual environment with uv**:
   ```bash
   # uv is much faster than pip!
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

4. **Install dependencies with uv**:
   ```bash
   uv pip install -r requirements.txt
   ```
   
   This installs:
   - PySpark 3.5.0
   - Pandas (for Pandas UDFs)
   - Faker (for sample data generation)
   - Other utilities
   
   **Note**: uv is 10-100x faster than pip!

5. **Setup environment variables**:
   ```bash
   # Copy template and customize
   cp .env.example .env
   # Optionally edit .env to adjust memory settings
   ```

6. **Verify installation**:
   ```bash
   python verify_setup.py
   # Or quick check:
   python -c \"from pyspark.sql import SparkSession; print('PySpark installed successfully!')\"
   ```

7. **Start Jupyter Notebook Server**:
   ```bash
   python start_jupyter.py
   ```
   
   Then open your browser to: http://localhost:8888

### For Python Scripts

Follow steps 1-6 above, then run jobs directly:

```bash
python jobs/01_dataframe_basics.py
```

## Your First Run (2 minutes)

Run the first job to learn DataFrame basics:

```bash
python jobs/01_dataframe_basics.py
```

You should see:
- Sample data being generated
- Various DataFrame operations
- Output showing transformations

**While the job runs**, open http://localhost:4040 in your browser to see the Spark UI!

## Learning Path

Work through the jobs in order:

### 1. DataFrame Basics (30 min)
```bash
python jobs/01_dataframe_basics.py
```
Learn: Creating DataFrames, schemas, basic transformations, reading/writing data

### 2. Aggregations (45 min)
```bash
python jobs/02_aggregations.py
```
Learn: GroupBy, aggregate functions, window functions, rankings

### 3. Joins (45 min)
```bash
python jobs/03_joins.py
```
Learn: Inner/outer/cross joins, broadcast joins, multiple joins

### 4. Analytics & UDFs (60 min)
```bash
python jobs/04_analytics_udfs.py
```
Learn: UDFs, Pandas UDFs, RFM analysis, cohort analysis, funnel analysis

### 5. Search Indexing (60 min)
```bash
python jobs/05_search_indexing.py
```
Learn: Text processing, inverted indexes, TF-IDF, recommendations

## Tips for Success

### 1. Read the Concepts First
Before running jobs, read the foundational concepts:
```bash
# Open in your editor
open docs/concepts.md
```

### 2. Run Jobs Interactively
- Read the code comments as you go
- Pause between sections
- Try modifying parameters

### 3. Use the Spark UI
While jobs run, monitor them at http://localhost:4040:
- See query execution plans
- Monitor resource usage
- Understand shuffle operations

### 4. Experiment!
After each job:
- Modify the transformations
- Try different filters
- Add your own analysis

### 5. Start Small
The sample datasets are intentionally small (1,000-10,000 rows) so you can:
- Run quickly
- Understand output easily
- Experiment freely

## Common Issues & Solutions

### Issue: "Java not found"
**Solution**: Install Java 8 or 11:
```bash
# macOS
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Linux
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Add to ~/.bashrc or ~/.zshrc to make permanent
```

### Issue: "Port 4040 already in use"
**Solution**: Another Spark session is running. Either:
- Close other Spark jobs
- Or ignore (the new job will use 4041, 4042, etc.)

### Issue: "Module not found: pyspark"
**Solution**: Activate your virtual environment:
```bash
source venv/bin/activate
```

### Issue: Slow performance
**Solution**: This is expected for local mode. Key optimizations:
- Reduce `spark.sql.shuffle.partitions` (already set to 4)
- Use `.cache()` for reused DataFrames
- Filter data early
- Use built-in functions over UDFs

## Sample Data

The first job automatically generates sample datasets:
- **users.csv**: 1,000 users with demographics
- **products.json**: 200 products across categories
- **transactions.csv**: 5,000 purchase transactions
- **reviews.csv**: 3,000 product reviews
- **clickstream.csv**: 10,000 user events

All data is fake but realistic, created with Faker.

## What's Next?

After completing all 5 jobs:

1. **Modify the Jobs**: Change the analysis, add new metrics
2. **Use Your Own Data**: Replace sample data with real datasets
3. **Explore Advanced Topics**:
   - Spark Streaming for real-time data
   - MLlib for machine learning
   - GraphX for graph analytics
4. **Deploy to Cluster**: Try running on AWS EMR, Databricks, or similar
5. **Read the Docs**: [Official PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## Project Structure Reference

```
learn-pyspark-1/
├── README.md              # Project overview
├── QUICKSTART.md          # This file
├── requirements.txt       # Python dependencies
├── .gitignore            # Git ignore rules
│
├── docs/
│   └── concepts.md        # Core PySpark concepts (READ THIS FIRST!)
│
├── utils/
│   ├── spark_session.py   # SparkSession utilities
│   └── data_generator.py  # Sample data generation
│
├── jobs/                  # Learning jobs (run in order)
│   ├── 01_dataframe_basics.py
│   ├── 02_aggregations.py
│   ├── 03_joins.py
│   ├── 04_analytics_udfs.py
│   └── 05_search_indexing.py
│
├── data/                  # Sample datasets (auto-generated)
└── output/                # Job outputs (created as needed)
```

## Getting Help

- **Concepts unclear?** Read `docs/concepts.md`
- **Code questions?** Comments in each job explain the code
- **Errors?** Check "Common Issues" section above
- **Want more?** Check [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## Ready to Start?

```bash
# 1. Activate environment
source venv/bin/activate

# 2. Read concepts
cat docs/concepts.md  # or open in your editor

# 3. Run first job
python jobs/01_dataframe_basics.py

# 4. Open Spark UI in browser
open http://localhost:4040
```

Happy learning! 🚀
