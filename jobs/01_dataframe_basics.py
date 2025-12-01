"""
Job 1: DataFrame Basics and Transformations

Learning Objectives:
- Creating DataFrames from various sources
- Understanding DataFrame schema
- Basic transformations: select, filter, withColumn
- Column expressions and operators
- Reading and writing data in different formats

Key Concepts:
- DataFrames are immutable distributed collections
- Transformations are lazy (don't execute until an action is called)
- Each transformation returns a new DataFrame
"""

import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper, lower, concat, when, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from utils.spark_session import get_spark_session, stop_spark_session, get_data_dir
from utils.data_generator import generate_all_datasets


def create_dataframe_from_list(spark):
    """
    LESSON 1: Creating DataFrames from Python lists
    
    This is useful for quick testing and learning, but in production
    you'll typically read from files or databases.
    """
    print("\n" + "="*70)
    print("LESSON 1: Creating DataFrames from Lists")
    print("="*70)
    
    # Simple list of tuples
    data = [
        (1, "Alice", 28, "New York"),
        (2, "Bob", 35, "San Francisco"),
        (3, "Charlie", 42, "Seattle"),
        (4, "Diana", 31, "Boston")
    ]
    
    # Create DataFrame with column names
    df = spark.createDataFrame(data, ["id", "name", "age", "city"])
    
    print("\n📊 Simple DataFrame:")
    df.show()
    
    # Print schema - shows column names and types
    print("\n📋 Schema (inferred automatically):")
    df.printSchema()
    
    return df


def create_dataframe_with_explicit_schema(spark):
    """
    LESSON 2: Creating DataFrames with explicit schema
    
    Defining schema explicitly is better for:
    - Performance (no inference needed)
    - Type safety (you control the types)
    - Clarity (self-documenting code)
    """
    print("\n" + "="*70)
    print("LESSON 2: Creating DataFrames with Explicit Schema")
    print("="*70)
    
    # Define schema explicitly
    schema = StructType([
        StructField("id", IntegerType(), False),        # False = not nullable
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), True),        # True = nullable
        StructField("salary", DoubleType(), True)
    ])
    
    data = [
        (1, "Alice", 28, 75000.0),
        (2, "Bob", 35, 85000.0),
        (3, "Charlie", None, 92000.0),  # Note: age is nullable
        (4, "Diana", 31, None)           # salary is nullable
    ]
    
    df = spark.createDataFrame(data, schema)
    
    print("\n📊 DataFrame with explicit schema:")
    df.show()
    
    print("\n📋 Schema (explicitly defined):")
    df.printSchema()
    
    return df


def read_csv_data(spark, data_dir):
    """
    LESSON 3: Reading data from CSV files
    
    CSV is common but not the most efficient format.
    Always specify options like header, inferSchema, delimiter.
    """
    print("\n" + "="*70)
    print("LESSON 3: Reading CSV Data")
    print("="*70)
    
    users_path = os.path.join(data_dir, "users.csv")
    
    # Read CSV with options
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(users_path)
    
    print(f"\n📁 Read from: {users_path}")
    print(f"📊 Row count: {df.count()}")
    print("\n🔍 First 10 rows:")
    df.show(10)
    
    print("\n📋 Inferred schema:")
    df.printSchema()
    
    return df


def basic_transformations(df):
    """
    LESSON 4: Basic DataFrame transformations
    
    Key transformations:
    - select(): Choose specific columns
    - filter()/where(): Filter rows based on conditions
    - withColumn(): Add or modify columns
    - drop(): Remove columns
    """
    print("\n" + "="*70)
    print("LESSON 4: Basic Transformations")
    print("="*70)
    
    # SELECT: Choose specific columns
    print("\n🔹 select() - Choose columns:")
    df.select("name", "email", "age").show(5)
    
    # SELECT with column expressions
    print("\n🔹 select() with expressions:")
    df.select(
        col("name"),
        col("age"),
        col("city"),
        col("country")
    ).show(5)
    
    # FILTER: Keep only rows that match condition
    print("\n🔹 filter() - Users age > 30:")
    df.filter(col("age") > 30).show(5)
    
    # Multiple filter conditions (AND)
    print("\n🔹 filter() with multiple conditions (AND):")
    df.filter(
        (col("age") > 25) & (col("country") == "USA")
    ).show(5)
    
    # Multiple filter conditions (OR)
    print("\n🔹 filter() with OR condition:")
    df.filter(
        (col("country") == "USA") | (col("country") == "UK")
    ).show(5)
    
    # WHERE: Alternative to filter (same functionality)
    print("\n🔹 where() - Same as filter:")
    df.where("age > 40").show(5)  # Can use SQL-like strings
    
    # WITH_COLUMN: Add new column or modify existing
    print("\n🔹 withColumn() - Add age_group column:")
    df_with_group = df.withColumn(
        "age_group",
        when(col("age") < 30, "Young")
        .when(col("age") < 50, "Middle")
        .otherwise("Senior")
    )
    df_with_group.select("name", "age", "age_group").show(10)
    
    # Multiple withColumn operations (chaining)
    print("\n🔹 Chain multiple withColumn():")
    df_enhanced = df \
        .withColumn("full_name", upper(col("name"))) \
        .withColumn("email_lower", lower(col("email"))) \
        .withColumn("age_plus_10", col("age") + 10)
    
    df_enhanced.select("name", "full_name", "age", "age_plus_10").show(5)
    
    # DROP: Remove columns
    print("\n🔹 drop() - Remove columns:")
    df_reduced = df.drop("user_id", "signup_date")
    print(f"Original columns: {len(df.columns)}, After drop: {len(df_reduced.columns)}")
    df_reduced.show(5)
    
    return df_enhanced


def column_operations(df):
    """
    LESSON 5: Working with columns
    
    Column operations are the heart of DataFrame transformations.
    Learn different ways to reference and manipulate columns.
    """
    print("\n" + "="*70)
    print("LESSON 5: Column Operations and Expressions")
    print("="*70)
    
    # Different ways to reference columns
    print("\n🔹 Different ways to reference columns:")
    print("All these are equivalent:")
    
    # Method 1: String
    result1 = df.select("name")
    
    # Method 2: col() function (recommended - most flexible)
    result2 = df.select(col("name"))
    
    # Method 3: DataFrame attribute
    result3 = df.select(df.name)
    
    # Method 4: Dictionary-style
    result4 = df.select(df["name"])
    
    print("Using col() function (recommended):")
    result2.show(3)
    
    # Column arithmetic
    print("\n🔹 Column arithmetic:")
    df.select(
        col("name"),
        col("age"),
        (col("age") + 10).alias("age_in_10_years"),
        (col("age") * 12).alias("age_in_months")
    ).show(5)
    
    # String operations
    print("\n🔹 String operations:")
    df.select(
        col("name"),
        upper(col("name")).alias("name_upper"),
        lower(col("name")).alias("name_lower"),
        concat(col("name"), lit(" - "), col("city")).alias("name_city")
    ).show(5)
    
    # Conditional expressions with when/otherwise
    print("\n🔹 Conditional expressions (when/otherwise):")
    df.select(
        col("name"),
        col("country"),
        when(col("country") == "USA", "Domestic")
        .when(col("country") == "Canada", "Domestic")
        .otherwise("International")
        .alias("market")
    ).show(10)
    
    # Working with dates (if available)
    if "signup_date" in df.columns:
        print("\n🔹 Date operations:")
        df.select(
            col("name"),
            col("signup_date"),
            year(col("signup_date")).alias("signup_year"),
            month(col("signup_date")).alias("signup_month")
        ).show(5)


def reading_and_writing(spark, data_dir):
    """
    LESSON 6: Reading and writing data in different formats
    
    Formats:
    - CSV: Human-readable, but slow and not type-safe
    - JSON: Good for nested data, but verbose
    - Parquet: Columnar format, fast, compressed (RECOMMENDED)
    """
    print("\n" + "="*70)
    print("LESSON 6: Reading and Writing Different Formats")
    print("="*70)
    
    # Read CSV
    print("\n🔹 Reading CSV:")
    users_df = spark.read.csv(
        os.path.join(data_dir, "users.csv"),
        header=True,
        inferSchema=True
    )
    print(f"Users CSV: {users_df.count()} rows")
    
    # Read JSON
    print("\n🔹 Reading JSON:")
    products_df = spark.read.option("multiLine", "true").json(os.path.join(data_dir, "products.json"))
    print(f"Products JSON: {products_df.count()} rows")
    products_df.show(5)
    
    # Write as Parquet (best practice for Spark)
    print("\n🔹 Writing as Parquet:")
    output_path = os.path.join(data_dir, "users_parquet")
    users_df.write \
        .mode("overwrite") \
        .parquet(output_path)
    print(f"✓ Written to: {output_path}")
    
    # Read back from Parquet
    print("\n🔹 Reading Parquet:")
    users_parquet = spark.read.parquet(output_path)
    print(f"Read back: {users_parquet.count()} rows")
    users_parquet.show(5)
    
    # Compare file sizes (Parquet is much smaller)
    csv_size = os.path.getsize(os.path.join(data_dir, "users.csv"))
    # Note: Parquet creates a directory with multiple files
    print(f"\n📦 CSV size: {csv_size:,} bytes")
    print("   Parquet is typically 5-10x smaller and much faster to read!")
    
    # Writing with partitioning (important for large datasets)
    print("\n🔹 Writing with partitioning:")
    partitioned_path = os.path.join(data_dir, "users_partitioned")
    users_df.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(partitioned_path)
    print(f"✓ Written partitioned data to: {partitioned_path}")
    print("   Partitioning helps with query performance on large datasets!")


def dataframe_actions(df):
    """
    LESSON 7: DataFrame actions
    
    Actions trigger computation. Transformations are lazy, actions are eager.
    
    Common actions:
    - show(): Display data
    - count(): Count rows
    - collect(): Bring all data to driver (BE CAREFUL!)
    - take(): Take first N rows
    - first(): Get first row
    """
    print("\n" + "="*70)
    print("LESSON 7: DataFrame Actions (Trigger Computation)")
    print("="*70)
    
    print("\n🔹 show() - Display data:")
    df.show(5)
    
    print("\n🔹 count() - Count rows:")
    row_count = df.count()
    print(f"Total rows: {row_count}")
    
    print("\n🔹 first() - Get first row:")
    first_row = df.first()
    print(f"First row: {first_row}")
    
    print("\n🔹 take() - Get first N rows:")
    first_3 = df.take(3)
    print(f"First 3 rows: {len(first_3)} rows")
    for row in first_3:
        print(f"  {row}")
    
    print("\n🔹 collect() - Bring all data to driver:")
    print("⚠️  WARNING: Only use collect() on small datasets!")
    print("   It brings ALL data to the driver machine's memory.")
    # Uncomment to see it in action (safe for our small dataset)
    # all_data = df.collect()
    # print(f"Collected {len(all_data)} rows")
    
    print("\n🔹 describe() - Statistical summary:")
    df.describe("age").show()


def main():
    """
    Main function - orchestrates all lessons.
    """
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("   JOB 1: DataFrame Basics and Transformations")
    print("🎓 " + "="*66 + " 🎓\n")
    
    # Initialize Spark
    spark = get_spark_session("Job 1: DataFrame Basics")
    data_dir = get_data_dir()
    
    try:
        # Generate sample data if it doesn't exist
        users_csv = os.path.join(data_dir, "users.csv")
        if not os.path.exists(users_csv):
            print("📁 Sample data not found. Generating...")
            generate_all_datasets(data_dir)
        
        # Run lessons
        create_dataframe_from_list(spark)
        create_dataframe_with_explicit_schema(spark)
        df = read_csv_data(spark, data_dir)
        df_enhanced = basic_transformations(df)
        column_operations(df)
        reading_and_writing(spark, data_dir)
        dataframe_actions(df)
        
        # Final summary
        print("\n" + "="*70)
        print("✅ JOB 1 COMPLETED!")
        print("="*70)
        print("\n📚 What you learned:")
        print("  ✓ Creating DataFrames from lists and files")
        print("  ✓ Defining explicit schemas")
        print("  ✓ Basic transformations: select, filter, withColumn")
        print("  ✓ Column operations and expressions")
        print("  ✓ Reading and writing different formats (CSV, JSON, Parquet)")
        print("  ✓ Understanding actions vs transformations")
        print("\n🎯 Next step: Run Job 2 to learn about aggregations!")
        print("   python jobs/02_aggregations.py\n")
        
    finally:
        # Always stop the Spark session
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
