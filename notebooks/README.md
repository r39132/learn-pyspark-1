# PySpark Learning Notebooks

This directory contains Jupyter notebooks for hands-on PySpark learning.

## 📚 Notebooks Overview

1. **01_dataframe_basics.ipynb** (30 min)
   - Creating DataFrames
   - Basic transformations
   - Reading/writing data
   
2. **02_aggregations.ipynb** (45 min)
   - GroupBy operations
   - Aggregate functions
   - Window functions
   
3. **03_joins.ipynb** (45 min)
   - All join types
   - Broadcast joins
   - Multiple joins
   
4. **04_analytics_udfs.ipynb** (60 min)
   - User-defined functions
   - Pandas UDFs
   - RFM analysis
   
5. **05_search_indexing.ipynb** (60 min)
   - Text processing
   - Inverted indexes
   - TF-IDF scoring

## 🚀 Getting Started

### 1. Start Jupyter Server

From the project root:

```bash
# Activate virtual environment
source .venv/bin/activate

# Start Jupyter
python start_jupyter.py
```

Or manually:

```bash
cd notebooks
jupyter notebook --port 8888
```

### 2. Access Jupyter

Open your browser to: http://localhost:8888

### 3. Monitor Spark

While running notebooks, monitor Spark UI at: http://localhost:4040

## 💡 Tips

- **Run cells in order**: Each notebook builds on previous cells
- **Restart kernel**: Use `Kernel > Restart` to clear Spark session
- **Check Spark UI**: Monitor job execution and understand performance
- **Experiment**: Modify code and see what happens!
- **Read comments**: Each cell is heavily documented

## 🔧 Troubleshooting

### Port Already in Use

```bash
# Find and kill process on port 8888
lsof -ti:8888 | xargs kill -9
```

### Kernel Dies

- Check Java version: `java -version` (should be 17)
- Check memory: Reduce `SPARK_DRIVER_MEMORY` in `.env`
- Restart kernel: `Kernel > Restart`

### Can't Import Modules

```bash
# Reinstall dependencies
uv pip install -r requirements.txt
```

## 📖 Documentation

- [Main README](../README.md)
- [Concepts Guide](../docs/concepts.md)
- [Cheat Sheet](../docs/CHEATSHEET.md)
- [Troubleshooting](../TROUBLESHOOTING.md)

## 🎯 Learning Path

1. Read [concepts.md](../docs/concepts.md) first
2. Start with notebook 01
3. Run all cells in order
4. Experiment and modify code
5. Check Spark UI to understand execution
6. Move to next notebook

Happy Learning! ⚡✨
