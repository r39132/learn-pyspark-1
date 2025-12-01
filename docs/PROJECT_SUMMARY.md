# PySpark Learning Project - Complete!

🎉 **Your comprehensive PySpark learning environment is ready!**

## What You Have

A structured, hands-on PySpark learning project with:

### 📚 Educational Content
- **Detailed concepts guide** (`docs/concepts.md`) - Read this first!
- **5 progressive learning jobs** - Each builds on previous concepts
- **Heavily commented code** - Learn from inline explanations
- **Practical examples** - Real-world use cases

### 🛠️ Project Structure
```
learn-pyspark-1/
├── README.md           ← Project overview and roadmap
├── QUICKSTART.md       ← Get started in 5 minutes
├── requirements.txt    ← Python dependencies
├── run_all_jobs.py     ← Run all jobs in sequence
│
├── docs/
│   └── concepts.md     ← Core PySpark concepts (READ THIS!)
│
├── utils/
│   ├── spark_session.py   ← SparkSession utilities
│   └── data_generator.py  ← Sample data generation
│
└── jobs/               ← 5 learning modules
    ├── 01_dataframe_basics.py      (30 min)
    ├── 02_aggregations.py          (45 min)
    ├── 03_joins.py                 (45 min)
    ├── 04_analytics_udfs.py        (60 min)
    └── 05_search_indexing.py       (60 min)
```

## Learning Journey

### Job 1: DataFrame Basics (30 min)
**What you'll learn:**
- Creating DataFrames from various sources
- Defining schemas explicitly vs inference
- Basic transformations: `select()`, `filter()`, `withColumn()`
- Column operations and expressions
- Reading/writing CSV, JSON, Parquet
- Understanding actions vs transformations

**Key takeaway:** DataFrames are immutable, transformations are lazy

### Job 2: Aggregations (45 min)
**What you'll learn:**
- `groupBy()` and aggregate functions
- Multiple aggregations simultaneously
- Window functions for running totals
- Ranking functions: `row_number()`, `rank()`, `dense_rank()`
- `lag()` and `lead()` for time-series
- Practical analytics patterns

**Key takeaway:** Window functions give you row-level operations with group context

### Job 3: Joins (45 min)
**What you'll learn:**
- Inner, left, right, full outer joins
- Cross joins for Cartesian products
- Multiple joins to combine datasets
- Broadcast joins for performance
- Handling duplicate column names
- Self joins for hierarchical data

**Key takeaway:** Broadcast small tables to avoid expensive shuffles

### Job 4: Advanced Analytics & UDFs (60 min)
**What you'll learn:**
- Regular UDFs and Pandas UDFs
- RFM analysis for customer segmentation
- Cohort analysis for retention
- Pivot tables and data reshaping
- Statistical functions
- Funnel analysis for conversions

**Key takeaway:** Pandas UDFs are 10-100x faster than regular UDFs

### Job 5: Search Indexing (60 min)
**What you'll learn:**
- Text preprocessing and tokenization
- Building inverted indexes
- Term Frequency (TF) calculation
- TF-IDF scoring for relevance
- Product search implementation
- Simple recommendation systems
- Text similarity with Jaccard coefficient

**Key takeaway:** Spark excels at large-scale text processing

## Quick Start

### Jupyter Notebooks (Recommended)

```bash
# 1. Setup versions (one time)
pyenv install 3.12.0
pyenv local 3.12.0
jenv local 17  # Ensure Java 17 is installed

# 2. Setup environment
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env

# 4. Start Jupyter
python start_jupyter.py

# 5. Open http://localhost:8888
# 6. Start with notebooks/01_dataframe_basics.ipynb
```

### Python Scripts

```bash
# Setup steps 1-3 same as above

# 4. Read concepts
cat docs/concepts.md  # or open in editor

# 5. Run first job
python jobs/01_dataframe_basics.py

# 4. Open Spark UI
open http://localhost:4040

# 5. Continue with other jobs...
python jobs/02_aggregations.py
python jobs/03_joins.py
python jobs/04_analytics_udfs.py
python jobs/05_search_indexing.py

# Or run all at once (with pauses)
python run_all_jobs.py
```

## Key Concepts to Remember

### 1. Lazy Evaluation
- Transformations build a plan (lazy)
- Actions trigger execution (eager)
- Spark optimizes the entire plan

### 2. Partitioning
- Data split into chunks
- Each partition processed in parallel
- Control with `repartition()`, `coalesce()`

### 3. Shuffle
- Moving data across executors
- Expensive operation (disk + network)
- Triggered by: `groupBy()`, `join()`, `orderBy()`

### 4. Optimizations
- Use built-in functions over UDFs
- Broadcast small tables in joins
- Filter data early
- Cache reused DataFrames
- Use Parquet for storage

### 5. Best Practices
- Define schemas explicitly
- Avoid `collect()` on large data
- Monitor with Spark UI
- Partition data thoughtfully
- Test with small datasets first

## Use Cases Covered

✅ **Data Transformation** - Cleaning, filtering, enriching data  
✅ **Aggregation** - Sum, average, count, group operations  
✅ **Analytics** - Customer segmentation, cohort analysis, funnels  
✅ **Joins** - Combining multiple data sources  
✅ **Personalization** - Recommendations, user scoring  
✅ **Search** - Building indexes, TF-IDF, text processing  

## Sample Datasets

Auto-generated realistic data:
- **1,000 users** - Demographics, signup dates
- **200 products** - Multiple categories and brands  
- **5,000 transactions** - Purchase history
- **3,000 reviews** - Product feedback
- **10,000 clickstream events** - User behavior

## Next Steps

After completing all jobs:

### 1. Experiment
- Modify transformations
- Add new analysis
- Try different aggregations
- Break things and fix them!

### 2. Use Your Data
- Replace sample data with real datasets
- Apply patterns to your use cases
- Start small, then scale up

### 3. Go Deeper
- **Streaming**: Real-time data processing with Structured Streaming
- **ML**: Machine learning with PySpark MLlib
- **GraphX**: Graph analytics
- **Performance tuning**: Advanced optimization

### 4. Deploy
- Try on cloud platforms (AWS EMR, Databricks, Azure Synapse)
- Learn cluster management
- Production best practices

### 5. Resources
- [Official PySpark Docs](https://spark.apache.org/docs/latest/api/python/)
- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [PySpark GitHub Examples](https://github.com/apache/spark/tree/master/examples/src/main/python)

## Tips for Success

1. **Start with concepts** - Read `docs/concepts.md` first
2. **Run jobs in order** - Each builds on previous lessons
3. **Read the code** - Don't just run it, understand it
4. **Use Spark UI** - Monitor at http://localhost:4040
5. **Experiment freely** - Modify code, try variations
6. **Take notes** - Document patterns you'll reuse
7. **Start small** - Test with sample data first
8. **Ask questions** - Search docs when confused

## Common Patterns Reference

### Reading Data
```python
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.json("path/to/file.json")
df = spark.read.parquet("path/to/file.parquet")
```

### Basic Transformations
```python
df.select("col1", "col2")
df.filter(col("age") > 18)
df.withColumn("new_col", col("old_col") * 2)
df.groupBy("category").agg(sum("amount"))
```

### Joins
```python
df1.join(df2, "key", "inner")
df1.join(broadcast(df2), "key", "inner")  # Broadcast join
```

### Window Functions
```python
window = Window.partitionBy("user_id").orderBy("date")
df.withColumn("running_total", sum("amount").over(window))
```

### UDFs
```python
@pandas_udf(StringType())
def my_udf(s: pd.Series) -> pd.Series:
    return s.str.upper()
```

## Troubleshooting

**Import errors?** → Activate virtual environment  
**Java not found?** → Install JDK 8 or 11  
**Slow performance?** → Normal for local mode  
**Port 4040 in use?** → Another Spark job running

See `QUICKSTART.md` for detailed solutions.

## You're Ready!

Everything is set up for your PySpark learning journey. The project is:

✅ **Structured** - Clear progression from basics to advanced  
✅ **Hands-on** - Learn by doing, not just reading  
✅ **Practical** - Real-world use cases and patterns  
✅ **Comprehensive** - Covers major PySpark features  
✅ **Self-contained** - Everything you need is here  

**Start your journey:**
```bash
python jobs/01_dataframe_basics.py
```

Happy Sparking! 🚀✨

---

*Questions or issues? Review the code comments, check the concepts guide, or consult the official PySpark documentation.*
