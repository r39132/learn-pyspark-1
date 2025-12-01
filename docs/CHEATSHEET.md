# PySpark Cheat Sheet

Quick reference for common PySpark operations. Keep this handy!

## SparkSession

```python
from pyspark.sql import SparkSession

# Create session
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# Stop session
spark.stop()
```

## Reading Data

```python
# CSV
df = spark.read.csv("path/file.csv", header=True, inferSchema=True)
df = spark.read.option("header", "true").csv("path/")

# JSON
df = spark.read.json("path/file.json")

# Parquet (recommended)
df = spark.read.parquet("path/file.parquet")

# With schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), False)
])
df = spark.read.schema(schema).csv("path/file.csv")
```

## Writing Data

```python
# Parquet (best for Spark)
df.write.parquet("path/output")
df.write.mode("overwrite").parquet("path/output")

# Partitioned write
df.write.partitionBy("year", "month").parquet("path/output")

# CSV
df.write.csv("path/output", header=True)

# Modes: overwrite, append, ignore, error (default)
```

## Basic Transformations

```python
# Select columns
df.select("col1", "col2")
df.select(col("col1"), col("col2"))

# Filter rows
df.filter(col("age") > 18)
df.filter("age > 18")  # SQL string
df.where(col("age") > 18)  # Same as filter

# Multiple conditions
df.filter((col("age") > 18) & (col("city") == "NYC"))
df.filter((col("age") < 18) | (col("age") > 65))

# Add/modify columns
df.withColumn("new_col", col("old_col") * 2)
df.withColumn("age_group", when(col("age") < 30, "Young").otherwise("Old"))

# Drop columns
df.drop("col1", "col2")

# Rename columns
df.withColumnRenamed("old_name", "new_name")

# Remove duplicates
df.dropDuplicates()
df.dropDuplicates(["email"])  # Based on specific columns

# Sort
df.orderBy("age")
df.orderBy(col("age").desc())
df.orderBy(desc("age"), asc("name"))

# Limit
df.limit(10)
```

## Column Operations

```python
from pyspark.sql.functions import *

# Arithmetic
df.select(col("price") * 1.1)
df.select((col("a") + col("b")) / 2)

# String operations
df.select(upper(col("name")))
df.select(lower(col("email")))
df.select(concat(col("first"), lit(" "), col("last")))
df.select(substring(col("text"), 1, 10))
df.select(trim(col("text")))

# Date operations
df.select(year(col("date")))
df.select(month(col("date")))
df.select(datediff(current_date(), col("date")))

# Conditional
df.select(
    when(col("age") < 18, "Minor")
    .when(col("age") < 65, "Adult")
    .otherwise("Senior")
)

# Null handling
df.select(coalesce(col("col1"), lit("default")))
df.fillna(0)
df.fillna({"age": 0, "name": "Unknown"})
df.dropna()
```

## Aggregations

```python
# Simple aggregations
df.count()
df.select(sum("amount"))
df.select(avg("age"), min("age"), max("age"))

# GroupBy
df.groupBy("category").count()
df.groupBy("category").sum("amount")
df.groupBy("category").agg({"amount": "sum", "quantity": "avg"})

# Multiple aggregations
df.groupBy("category").agg(
    count("*").alias("count"),
    sum("amount").alias("total"),
    avg("amount").alias("average")
)

# Distinct count
df.select(countDistinct("user_id"))
```

## Window Functions

```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Define window
window = Window.partitionBy("user_id").orderBy("date")

# Running total
df.withColumn("running_total", sum("amount").over(window))

# Row number
df.withColumn("row_num", row_number().over(window))

# Ranking
df.withColumn("rank", rank().over(window))
df.withColumn("dense_rank", dense_rank().over(window))

# Lag/Lead
df.withColumn("prev_value", lag("amount", 1).over(window))
df.withColumn("next_value", lead("amount", 1).over(window))

# Percentile
df.withColumn("percentile", percent_rank().over(window))
df.withColumn("quartile", ntile(4).over(window))
```

## Joins

```python
# Inner join (default)
df1.join(df2, "key")
df1.join(df2, df1.id == df2.id)

# Join types
df1.join(df2, "key", "inner")
df1.join(df2, "key", "left")    # or "left_outer"
df1.join(df2, "key", "right")   # or "right_outer"
df1.join(df2, "key", "full")    # or "full_outer"
df1.crossJoin(df2)              # Cartesian product

# Broadcast join (for small tables)
df1.join(broadcast(df2), "key")

# Multiple conditions
df1.join(df2, (df1.key1 == df2.key1) & (df1.key2 == df2.key2))

# Self join
df.alias("a").join(df.alias("b"), col("a.id") == col("b.parent_id"))
```

## User-Defined Functions (UDFs)

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# Regular UDF (slow)
def my_function(x):
    return x.upper()

my_udf = udf(my_function, StringType())
df.withColumn("upper_name", my_udf(col("name")))

# Pandas UDF (fast - vectorized)
@pandas_udf(StringType())
def my_pandas_udf(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("upper_name", my_pandas_udf(col("name")))
```

## Actions (Trigger Execution)

```python
# Show data
df.show()
df.show(20, truncate=False)

# Count rows
df.count()

# Collect to driver (CAREFUL with large data!)
rows = df.collect()
first_row = df.first()
first_n = df.take(10)
first_n = df.head(10)

# Write to storage
df.write.parquet("path/")

# Statistics
df.describe().show()
df.summary().show()

# Convert to Pandas (small data only!)
pandas_df = df.toPandas()
```

## Inspecting DataFrames

```python
# Schema
df.printSchema()
df.schema
df.dtypes
df.columns

# Sample rows
df.show(5)
df.head(5)

# Statistics
df.describe("age", "salary").show()
df.select(mean("age"), stddev("salary")).show()

# Count nulls
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Execution plan
df.explain()
df.explain(True)  # Extended
```

## Performance Tips

```python
# Cache frequently used DataFrames
df.cache()
df.persist()
df.unpersist()

# Repartition for better parallelism
df.repartition(10)
df.repartition("key")  # By column

# Coalesce (reduce partitions, no shuffle)
df.coalesce(1)

# Broadcast small tables
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")

# Use built-in functions (avoid UDFs)
# GOOD: df.withColumn("upper", upper(col("name")))
# BAD:  df.withColumn("upper", my_udf(col("name")))
```

## Common Patterns

### Filter Early
```python
# Do this (filter first)
df.filter(col("age") > 18).select("name", "email")

# Not this (filter after)
df.select("name", "email", "age").filter(col("age") > 18)
```

### Chaining Transformations
```python
result = (df
    .filter(col("status") == "active")
    .withColumn("discount", col("price") * 0.1)
    .groupBy("category")
    .agg(sum("discount").alias("total_discount"))
    .orderBy(desc("total_discount"))
)
```

### Pivot Tables
```python
df.groupBy("category").pivot("status").agg(sum("amount"))
```

### Explode Arrays
```python
df.select(col("id"), explode(col("array_column")).alias("item"))
```

### String Split to Array
```python
df.withColumn("words", split(col("sentence"), " "))
```

### Conditional Column
```python
df.withColumn("price_category",
    when(col("price") < 10, "Cheap")
    .when(col("price") < 50, "Moderate")
    .otherwise("Expensive")
)
```

## Spark SQL

```python
# Register as temp view
df.createOrReplaceTempView("users")

# Run SQL
result = spark.sql("""
    SELECT category, COUNT(*) as count
    FROM users
    WHERE age > 18
    GROUP BY category
    ORDER BY count DESC
""")
```

## Configuration

```python
# Set config when creating session
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Set at runtime
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Get config
spark.conf.get("spark.sql.shuffle.partitions")
```

## Debugging

```python
# Print execution plan
df.explain()
df.explain(True)  # More detail

# Check partitions
df.rdd.getNumPartitions()

# Sample data for testing
df.sample(0.1)  # 10% sample
df.sample(False, 0.1, seed=42)  # Reproducible

# Monitor with Spark UI
# Open http://localhost:4040 while job runs
```

## Common Imports

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, count, sum, avg, min, max,
    concat, upper, lower, trim, split, explode,
    year, month, day, datediff, current_date,
    row_number, rank, dense_rank, lag, lead,
    countDistinct, collect_list, collect_set,
    broadcast, udf, pandas_udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, FloatType, DateType, TimestampType,
    ArrayType, MapType
)
```

---

**Pro Tip**: Always use built-in functions over UDFs when possible. They're optimized and much faster!

**Remember**: Transformations are lazy, actions trigger execution. Use `.explain()` to understand what Spark will do.
