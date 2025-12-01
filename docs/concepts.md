# PySpark Core Concepts

A comprehensive guide to understanding Apache Spark and PySpark fundamentals.

## What is Apache Spark?

Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in multiple languages (Python, Scala, Java, R) and an optimized engine that supports general computation graphs.

**Key advantages:**
- **Speed**: 100x faster than Hadoop MapReduce (in-memory)
- **Ease of use**: Rich APIs and interactive shells
- **Unified platform**: Batch, streaming, SQL, ML, and graph processing
- **Runs anywhere**: Hadoop, Kubernetes, standalone, or in the cloud

## PySpark Architecture

### 1. Driver and Executors

```
┌──────────────┐
│    Driver    │  ← Your Python program
│   Program    │    - Creates SparkContext
└──────┬───────┘    - Builds execution plan
       │            - Schedules tasks
       │
       ├─────────────────────┬─────────────────────┐
       │                     │                     │
┌──────▼───────┐    ┌────────▼──────┐    ┌────────▼──────┐
│  Executor 1  │    │  Executor 2   │    │  Executor 3   │
│              │    │               │    │               │
│  [Tasks]     │    │  [Tasks]      │    │  [Tasks]      │
│  [Cache]     │    │  [Cache]      │    │  [Cache]      │
└──────────────┘    └───────────────┘    └───────────────┘
     Worker              Worker              Worker
```

- **Driver**: Runs your `main()` function, creates SparkContext, schedules tasks
- **Executors**: Run on worker nodes, execute tasks, store cached data
- **Cluster Manager**: Allocates resources (YARN, Mesos, Kubernetes, Standalone)

### 2. RDD, DataFrame, and Dataset

**RDD (Resilient Distributed Dataset)** - Low-level API
- Immutable, distributed collection of objects
- Type-safe but lacks optimization
- Use when you need fine-grained control

**DataFrame** - High-level API (recommended for Python)
- Distributed collection of data organized into named columns
- Like a table in a relational database
- Catalyst optimizer automatically optimizes queries
- **This is what you'll use most in PySpark**

**Dataset** - Type-safe DataFrame (Scala/Java only)
- Not available in Python

## Core Concepts

### 1. Lazy Evaluation

Spark uses **lazy evaluation** - transformations aren't executed immediately.

```python
# These create a plan but don't execute anything yet
df2 = df1.filter(col("age") > 18)      # Transformation
df3 = df2.select("name", "email")      # Transformation

# This triggers execution of all transformations above
df3.show()                             # Action
```

**Transformations** (lazy) - Return a new DataFrame:
- `select()`, `filter()`, `where()`, `groupBy()`, `join()`, `withColumn()`, etc.

**Actions** (eager) - Trigger computation and return results:
- `show()`, `count()`, `collect()`, `write()`, `take()`, etc.

**Why lazy?**
- Spark can optimize the entire pipeline
- Avoid unnecessary computation
- Enable fault tolerance

### 2. Partitioning

Data in Spark is divided into **partitions** - chunks processed in parallel.

```
DataFrame with 1 million rows, 4 partitions:

Partition 1: Rows 1-250,000      → Executor 1
Partition 2: Rows 250,001-500,000 → Executor 2
Partition 3: Rows 500,001-750,000 → Executor 3
Partition 4: Rows 750,001-1M      → Executor 4
```

**Key points:**
- Each partition is processed by one task on one executor
- More partitions = more parallelism (up to a point)
- Default partition count: Number of cores or 200 for operations like `groupBy`
- Control with: `repartition()`, `coalesce()`, `partitionBy()`

### 3. Wide vs Narrow Transformations

**Narrow transformations** - Each input partition contributes to only one output partition
- Examples: `map()`, `filter()`, `select()`
- No data shuffling needed
- Fast and efficient

```
Input Partition 1  →  Output Partition 1
Input Partition 2  →  Output Partition 2
Input Partition 3  →  Output Partition 3
```

**Wide transformations** - Each input partition contributes to multiple output partitions
- Examples: `groupBy()`, `join()`, `orderBy()`
- Requires **shuffle** - data movement across the network
- Expensive operation

```
Input Partition 1  ─┐
Input Partition 2  ─┼→  Shuffle  →  Output Partition 1
Input Partition 3  ─┘              Output Partition 2
```

### 4. Shuffling

**Shuffle** = Moving data between partitions across executors

**When does shuffling occur?**
- `groupBy()`, `reduceByKey()`, `join()`, `orderBy()`, `repartition()`, `distinct()`

**Why is shuffle expensive?**
- Disk I/O (write intermediate data)
- Network I/O (transfer data)
- Serialization/deserialization

**Optimization tips:**
- Minimize wide transformations
- Use broadcast joins for small tables
- Filter data early
- Use appropriate partition keys

### 5. Catalyst Optimizer

Spark SQL's **Catalyst** optimizer automatically optimizes your DataFrame operations.

```python
# You write this:
df.filter(col("age") > 18).select("name", "age")

# Catalyst optimizes it:
# 1. Push down filter before select (process less data)
# 2. Predicate pushdown to data source (read less from disk)
# 3. Combine multiple operations into a single stage
```

**Optimization techniques:**
- **Predicate pushdown**: Push filters to data source
- **Column pruning**: Only read needed columns
- **Constant folding**: Evaluate constant expressions once
- **Join reordering**: Choose optimal join order

### 6. Caching and Persistence

Store DataFrames in memory for reuse:

```python
df.cache()  # Same as persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY)
```

**Storage levels:**
- `MEMORY_ONLY`: Store in memory, recompute if doesn't fit
- `MEMORY_AND_DISK`: Spill to disk if memory full (default for cache)
- `DISK_ONLY`: Store only on disk
- `MEMORY_ONLY_SER`: Serialize objects (more memory efficient)

**When to cache:**
- DataFrame used multiple times
- Iterative algorithms
- Interactive exploration

**Remember to unpersist:**
```python
df.unpersist()
```

### 7. Spark Session

The entry point to Spark functionality:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.some.config", "value") \
    .getOrCreate()
```

**Key capabilities:**
- Create DataFrames
- Run SQL queries
- Configure Spark properties
- Access Spark Context

## DataFrame Schema

A schema defines the structure of your DataFrame:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),    # True = nullable
    StructField("age", IntegerType(), False),   # False = not nullable
    StructField("email", StringType(), True)
])
```

**Schema inference:**
- Spark can infer schema from data (convenience)
- Better to define schema explicitly (performance + predictability)

## Column Expressions

Columns are the building blocks of DataFrame operations:

```python
from pyspark.sql.functions import col, lit, when

# Different ways to reference columns
df.select("name")                    # String
df.select(df.name)                   # DataFrame attribute
df.select(df["name"])                # Dictionary style
df.select(col("name"))               # Column function (most flexible)

# Column expressions
df.select(col("price") * 1.1)        # Math operations
df.filter(col("age") > 18)           # Comparisons
df.select(when(col("age") > 18, "Adult").otherwise("Minor"))  # Conditionals
```

## Execution Plan

Understand what Spark will do with `.explain()`:

```python
df.filter(col("age") > 18).explain()
```

**Output shows:**
- Physical plan (how it will execute)
- Optimizations applied
- Shuffle operations

## Performance Tips

1. **Avoid collecting large datasets**: `collect()` brings all data to driver
2. **Use appropriate file formats**: Parquet > CSV (columnar, compressed)
3. **Partition your data**: Especially for time-series data
4. **Broadcast small tables**: For joins with small lookup tables
5. **Use built-in functions**: Always faster than UDFs
6. **Filter early**: Reduce data before expensive operations
7. **Avoid UDFs when possible**: They're slower than native functions
8. **Monitor with Spark UI**: Check http://localhost:4040 during execution

## Common Patterns

### Reading Data
```python
df = spark.read.format("csv").option("header", "true").load("path/to/file.csv")
df = spark.read.json("path/to/file.json")
df = spark.read.parquet("path/to/file.parquet")
```

### Writing Data
```python
df.write.format("parquet").mode("overwrite").save("path/to/output")
df.write.partitionBy("date").parquet("path/to/output")
```

### Transformations
```python
# Select columns
df.select("col1", "col2")

# Add/modify columns
df.withColumn("new_col", col("old_col") * 2)

# Filter rows
df.filter(col("age") > 18)
df.where("age > 18")  # SQL-style

# Remove duplicates
df.dropDuplicates(["email"])
```

## Next Steps

Now that you understand the fundamentals, you're ready to dive into the hands-on jobs:

1. **Start with Job 1**: DataFrame basics and transformations
2. **Run each job**: Observe the output and read the code comments
3. **Experiment**: Modify the code to solidify your understanding
4. **Use Spark UI**: Monitor http://localhost:4040 while jobs run
5. **Read the docs**: [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)

Remember: The best way to learn Spark is by doing. Run the jobs, break things, fix them, and iterate!
