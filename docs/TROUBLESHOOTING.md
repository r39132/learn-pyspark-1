# Troubleshooting Guide

Common issues and their solutions when setting up and running the PySpark learning project.

## Setup Issues

### ❌ "Java not found" or "JAVA_HOME not set"

**Problem**: Spark requires Java 17 to run.

**Solution with jenv (recommended):**

**macOS:**
```bash
# Install Java 17
brew install openjdk@17

# Install jenv if not already installed
brew install jenv
echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(jenv init -)"' >> ~/.zshrc
source ~/.zshrc

# Add Java 17 to jenv
jenv add /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Set Java 17 for this project
cd learn-pyspark-1
jenv local 17

# Enable jenv to set JAVA_HOME
jenv enable-plugin export
```

**Linux (Ubuntu/Debian):**
```bash
# Install Java 17
sudo apt-get update
sudo apt-get install openjdk-17-jdk

# Install jenv
git clone https://github.com/jenv/jenv.git ~/.jenv
echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(jenv init -)"' >> ~/.bashrc
source ~/.bashrc

# Add Java 17 to jenv
jenv add /usr/lib/jvm/java-17-openjdk-amd64

# Set Java 17 for this project
cd learn-pyspark-1
jenv local 17

# Enable jenv to set JAVA_HOME
jenv enable-plugin export
```

**Windows:**
1. Download Java 17 from [Adoptium](https://adoptium.net/temurin/releases/?version=17)
2. Install it
3. Set JAVA_HOME in Environment Variables:
   - Right-click "This PC" → Properties → Advanced System Settings
   - Environment Variables → New (System Variable)
   - Variable: `JAVA_HOME`, Value: `C:\Program Files\Java\jdk-17`
4. Add to PATH: `%JAVA_HOME%\bin`

**Verify:**
```bash
java -version  # Should show version 17
jenv version   # Should show 17
echo $JAVA_HOME  # Should show the Java path
```

---

### ❌ "Module not found: pyspark"

**Problem**: PySpark not installed or virtual environment not activated.

**Solution**:

1. **Check Python version:**
   ```bash
   pyenv local  # Should show 3.12.0 or higher
   python --version
   ```

2. **Activate virtual environment:**
   ```bash
   source .venv/bin/activate  # macOS/Linux
   .venv\Scripts\activate     # Windows
   ```

3. **Install dependencies with uv:**
   ```bash
   uv pip install -r requirements.txt
   ```

4. **Verify installation:**
   ```bash
   python -c "import pyspark; print(pyspark.__version__)"
   ```

---

### ❌ "pip: command not found"

**Problem**: pip not installed or not in PATH.

**Solution**:

```bash
# Try pip3 instead
pip3 install -r requirements.txt

# Or install pip
python -m ensurepip --upgrade
```

---

### ❌ Python version too old

**Problem**: Project requires Python 3.12+.

**Solution with pyenv (recommended):**

```bash
# Install pyenv if not already installed
# macOS: brew install pyenv
# Linux: curl https://pyenv.run | bash

# Install Python 3.12
pyenv install 3.12.0

# Set it for this project
cd learn-pyspark-1
pyenv local 3.12.0

# Verify
python --version  # Should show 3.12.0
```

**Or list available versions:**
```bash
pyenv install --list | grep 3.12
```

---

## Runtime Issues

### ❌ "Port 4040 already in use"

**Problem**: Another Spark application is already running.

**Solution**:

This is actually not a problem! Spark will automatically use the next available port (4041, 4042, etc.).

If you want to stop other Spark sessions:
```python
# In your Python code
spark.stop()
```

Or just ignore it - multiple Spark UIs can run simultaneously.

---

### ❌ "Address already in use" for other ports

**Problem**: Port conflict with another application.

**Solution**:

Find and kill the process:

**macOS/Linux:**
```bash
# Find process using port 4040
lsof -i :4040

# Kill it
kill -9 <PID>
```

**Windows:**
```cmd
# Find process
netstat -ano | findstr :4040

# Kill it
taskkill /PID <PID> /F
```

---

### ❌ "Py4JJavaError" or Java exceptions

**Problem**: Java/Spark error during execution.

**Common causes and solutions:**

1. **Memory issues:**
   ```python
   spark = SparkSession.builder \
       .config("spark.driver.memory", "2g") \
       .config("spark.executor.memory", "2g") \
       .getOrCreate()
   ```

2. **Too many partitions:**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "4")  # Lower for local
   ```

3. **Corrupted metastore:**
   ```bash
   # Delete the Derby metastore
   rm -rf metastore_db derby.log
   ```

---

### ❌ Jobs are very slow

**Problem**: Performance issues in local mode.

**Solutions**:

1. **This is normal for local mode** - Spark is designed for clusters

2. **Reduce shuffle partitions:**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "4")
   ```

3. **Use smaller datasets for learning:**
   ```python
   # Sample data
   df = df.sample(0.1)  # 10% sample
   ```

4. **Cache frequently used DataFrames:**
   ```python
   df.cache()
   ```

5. **Filter early:**
   ```python
   # Good - filter first
   df.filter(col("status") == "active").select("id", "name")
   
   # Bad - filter after selecting
   df.select("id", "name", "status").filter(col("status") == "active")
   ```

---

### ❌ "java.lang.OutOfMemoryError"

**Problem**: Not enough memory allocated to Spark.

**Solution**:

Increase memory allocation:

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

Or process smaller chunks of data.

---

### ❌ "No module named 'pandas'"

**Problem**: Pandas not installed (required for Pandas UDFs).

**Solution**:

```bash
pip install pandas pyarrow
```

---

### ❌ Data files not found

**Problem**: Sample data hasn't been generated yet.

**Solution**:

The first job automatically generates sample data. But you can also:

```bash
python utils/data_generator.py
```

---

### ❌ "TypeError: 'JavaPackage' object is not callable"

**Problem**: Incorrect import or function call.

**Solution**:

Common mistakes:

```python
# Wrong
from pyspark.sql.functions import sum
df.select(sum(col("amount")))  # sum is Python built-in!

# Right
from pyspark.sql.functions import sum as spark_sum
df.select(spark_sum(col("amount")))

# Or
from pyspark.sql.functions import sum
df.select(sum("amount"))  # Pass column name as string
```

---

## Platform-Specific Issues

### macOS: "xcrun: error: invalid active developer path"

**Problem**: Command Line Tools not installed.

**Solution**:

```bash
xcode-select --install
```

---

### Windows: "access denied" or permission errors

**Problem**: Windows file permissions.

**Solution**:

Run terminal as Administrator, or:

```bash
# Give full permissions to project folder
icacls "C:\path\to\learn-pyspark-1" /grant Users:F /t
```

---

### Linux: "Permission denied" when running scripts

**Problem**: Scripts not executable.

**Solution**:

```bash
chmod +x run_all_jobs.py verify_setup.py
chmod +x jobs/*.py
```

---

## Development Issues

### ❌ IDE showing "Import could not be resolved"

**Problem**: IDE not using the virtual environment.

**Solution**:

**VS Code:**
1. Press Cmd/Ctrl+Shift+P
2. Type "Python: Select Interpreter"
3. Choose the one in `./venv/bin/python`

**PyCharm:**
1. Settings → Project → Python Interpreter
2. Add Interpreter → Existing Environment
3. Select `./venv/bin/python`

---

### ❌ Changes not taking effect

**Problem**: Python caching old bytecode.

**Solution**:

```bash
# Remove cached files
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -name "*.pyc" -delete
```

---

## Still Having Issues?

### Diagnostic Steps

1. **Run the verification script:**
   ```bash
   python verify_setup.py
   ```

2. **Check versions:**
   ```bash
   python --version
   java -version
   pip list | grep pyspark
   ```

3. **Try a minimal example:**
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.master("local[1]").getOrCreate()
   df = spark.createDataFrame([(1, "test")], ["id", "value"])
   df.show()
   spark.stop()
   ```

4. **Check logs:**
   - Look for `derby.log` in project directory
   - Check console output for stack traces

5. **Clean slate:**
   ```bash
   # Remove virtual environment and start fresh
   rm -rf .venv
   rm -rf metastore_db derby.log
   
   # Verify versions
   pyenv local  # Should show 3.12.0+
   jenv local   # Should show 17
   
   # Recreate with uv
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   ```

### Getting Help

1. **Read the error message** - Often tells you exactly what's wrong
2. **Check the documentation** - See [docs/concepts.md](concepts.md)
3. **Search the error** - Google/Stack Overflow are your friends
4. **Simplify** - Try with smaller data or simpler code
5. **Ask for help** - Include:
   - Your OS and Python version
   - Full error message
   - What you were trying to do
   - What you already tried

## Prevention Tips

✅ **Always activate virtual environment** before running anything  
✅ **Keep dependencies updated** - `pip install --upgrade -r requirements.txt`  
✅ **Start with small data** when testing  
✅ **Read error messages carefully** - they usually tell you the problem  
✅ **Use the verification script** - Run `python verify_setup.py` first  
✅ **Monitor resources** - Check Spark UI at http://localhost:4040  

## Quick Reference

```bash
# Check versions
pyenv local    # Should be 3.12.0+
jenv local     # Should be 17

# Activate environment
source .venv/bin/activate

# Verify setup
python verify_setup.py

# Install/update dependencies
uv pip install -r requirements.txt

# Start Jupyter (recommended)
python start_jupyter.py

# Or run Python scripts
python jobs/01_dataframe_basics.py

# Generate sample data
python utils/data_generator.py

# Clean up
rm -rf metastore_db derby.log __pycache__ .venv
```

## Jupyter-Specific Issues

### ❌ Port 8888 already in use

```bash
# Find and kill process
lsof -ti:8888 | xargs kill -9

# Or use a different port
JUPYTER_PORT=8889 python start_jupyter.py
```

### ❌ Kernel dies when running cells

**Possible causes:**
- Not enough memory allocated to Spark
- Java not configured correctly

**Solutions:**
```bash
# 1. Check Java
java -version  # Should be 17

# 2. Reduce memory in .env
# Edit .env and set:
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# 3. Restart Jupyter
```

### ❌ Cannot import utils modules

**Solution:** Make sure you're running cells in order. The setup cells add the project to Python path.

```python
# This cell must run first in each notebook:
import sys
import os
project_root = os.path.dirname(os.getcwd())
sys.path.append(project_root)
```

### ❌ .env file not loaded

**Solution:**
```bash
# Make sure .env exists
cp .env.example .env

# Verify it's being loaded
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('SPARK_MASTER'))"
```

---

**Remember**: Most issues are environment-related. When in doubt, recreate the virtual environment and reinstall dependencies!
