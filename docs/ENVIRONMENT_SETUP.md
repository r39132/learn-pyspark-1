# Environment Setup Guide

This guide explains how to configure your `.env` file and start working with the project.

## Environment Variables

The project uses a `.env` file to configure Spark and Jupyter settings.

### 1. Create Your .env File

```bash
# Copy the example file
cp .env.example .env
```

### 2. Configure Settings

Edit `.env` to customize your setup:

```bash
# PySpark Configuration
SPARK_HOME=                    # Leave empty, managed by PySpark
PYSPARK_PYTHON=python          # Python executable
PYSPARK_DRIVER_PYTHON=jupyter  # Use Jupyter for notebooks

# Spark Settings (adjust based on your system)
SPARK_MASTER=local[*]          # Use all CPU cores
SPARK_DRIVER_MEMORY=4g         # Reduce if you have less RAM
SPARK_EXECUTOR_MEMORY=4g       # Reduce if you have less RAM

# Jupyter Configuration
JUPYTER_PORT=8888              # Default Jupyter port
JUPYTER_TOKEN=                 # Leave empty for no password

# Project Paths (auto-detected, usually no changes needed)
PROJECT_ROOT=/path/to/learn-pyspark-1
DATA_DIR=${PROJECT_ROOT}/data
OUTPUT_DIR=${PROJECT_ROOT}/output

# Spark UI
SPARK_UI_PORT=4040             # Spark monitoring UI
```

### 3. Memory Settings

Adjust based on your system:

- **16GB RAM**: Use 4g for driver and executor
- **8GB RAM**: Use 2g for driver and executor  
- **4GB RAM**: Use 1g for driver and executor

```bash
# For systems with 8GB RAM
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

## Starting Jupyter

### Method 1: Using Helper Script (Recommended)

```bash
# Activate virtual environment
source .venv/bin/activate

# Start Jupyter with proper config
python start_jupyter.py
```

This script:
- Loads environment variables from `.env`
- Starts Jupyter in the `notebooks/` directory
- Configures PySpark integration
- Opens on port specified in `.env`

### Method 2: Manual Start

```bash
cd notebooks
jupyter notebook --port 8888 --no-browser
```

## Accessing the Environment

### Jupyter Notebook

1. Start Jupyter: `python start_jupyter.py`
2. Open browser: http://localhost:8888
3. Navigate to first notebook: `01_dataframe_basics.ipynb`
4. Run cells with Shift+Enter

### Spark UI

Monitor your Spark jobs:
1. Start running a notebook or script
2. Open browser: http://localhost:4040
3. View jobs, stages, executors, environment

### Python Scripts

Run jobs directly without Jupyter:

```bash
source .venv/bin/activate
python jobs/01_dataframe_basics.py
```

## Troubleshooting

### Port Conflicts

If port 8888 is in use:

```bash
# Option 1: Change port in .env
JUPYTER_PORT=8889

# Option 2: Kill existing process
lsof -ti:8888 | xargs kill -9
```

### Memory Errors

If you get OutOfMemory errors:

```bash
# Reduce memory in .env
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

### Spark UI Not Available

If http://localhost:4040 doesn't work:

- Check if a Spark job is actually running
- Try http://localhost:4041 (next available port)
- Check for port conflicts

### Environment Not Loading

```bash
# Verify .env exists
ls -la .env

# Test loading
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('SPARK_MASTER'))"
```

## Best Practices

### 1. Always Activate Virtual Environment

```bash
source .venv/bin/activate
```

### 2. Use .env for Configuration

Don't hard-code paths or settings. Use `.env`:

```python
from dotenv import load_dotenv
load_dotenv()

import os
data_dir = os.getenv('DATA_DIR', './data')
```

### 3. Monitor Resource Usage

- Keep Spark UI open while learning
- Watch memory usage in Activity Monitor/Task Manager
- Reduce memory settings if needed

### 4. Clean Restart

If things get weird:

```bash
# Stop all Jupyter kernels
# Then clean Spark metadata
rm -rf metastore_db derby.log

# Restart Jupyter
python start_jupyter.py
```

## Next Steps

After setup:

1. ✅ **Read**: [docs/concepts.md](docs/concepts.md) - Understand fundamentals
2. ✅ **Start**: `notebooks/01_dataframe_basics.ipynb` - Begin learning
3. ✅ **Monitor**: http://localhost:4040 - Watch Spark UI
4. ✅ **Reference**: [docs/CHEATSHEET.md](docs/CHEATSHEET.md) - Quick lookup

Happy Learning! 🚀
