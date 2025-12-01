"""
Job 2: Aggregations and GroupBy Operations

Learning Objectives:
- GroupBy and aggregate functions (sum, avg, count, min, max)
- Multiple aggregations on the same DataFrame
- Window functions for running totals and rankings
- Partitioning and ordering data

Key Concepts:
- GroupBy creates a GroupedData object (not a DataFrame)
- Aggregations are wide transformations (cause shuffle)
- Window functions allow row-level operations with group context
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, countDistinct, collect_list, collect_set,
    row_number, rank, dense_rank, lag, lead, first, last,
    when, desc, asc
)
from utils.spark_session import get_spark_session, stop_spark_session, get_data_dir
from utils.data_generator import generate_all_datasets


def simple_aggregations(df):
    """
    LESSON 1: Simple aggregations on entire DataFrame
    
    Aggregate functions compute a single value from multiple rows.
    Without groupBy, they work on the entire DataFrame.
    """
    print("\n" + "="*70)
    print("LESSON 1: Simple Aggregations (Entire DataFrame)")
    print("="*70)
    
    print("\n🔹 Count all rows:")
    total_count = df.count()
    print(f"Total transactions: {total_count}")
    
    print("\n🔹 Sum, Average, Min, Max on entire DataFrame:")
    df.select(
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_amount"),
        spark_min("amount").alias("min_amount"),
        spark_max("amount").alias("max_amount"),
        count("*").alias("row_count")
    ).show()
    
    print("\n🔹 Count distinct values:")
    df.select(
        countDistinct("user_id").alias("unique_users"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("status").alias("unique_statuses")
    ).show()


def groupby_aggregations(df):
    """
    LESSON 2: GroupBy with aggregations
    
    GroupBy splits data into groups and applies aggregations to each group.
    This is one of the most common operations in data analysis.
    """
    print("\n" + "="*70)
    print("LESSON 2: GroupBy Aggregations")
    print("="*70)
    
    # Group by single column
    print("\n🔹 Revenue by status:")
    df.groupBy("status") \
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_amount")
        ) \
        .orderBy(desc("total_revenue")) \
        .show()
    
    # Group by multiple columns
    print("\n🔹 Transactions by status and date:")
    df.groupBy("status", "transaction_date") \
        .agg(
            count("*").alias("count"),
            spark_sum("amount").alias("total_amount")
        ) \
        .orderBy("transaction_date", "status") \
        .show(10)
    
    # Multiple aggregations at once
    print("\n🔹 Comprehensive user statistics:")
    user_stats = df.groupBy("user_id") \
        .agg(
            count("*").alias("num_transactions"),
            spark_sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction"),
            spark_min("amount").alias("min_transaction"),
            spark_max("amount").alias("max_transaction"),
            countDistinct("product_id").alias("unique_products")
        ) \
        .orderBy(desc("total_spent"))
    
    print("Top 10 users by total spent:")
    user_stats.show(10)
    
    # Filtering after aggregation (having clause)
    print("\n🔹 Users with more than 10 transactions:")
    user_stats.filter(col("num_transactions") > 10).show(10)
    
    # Collecting values into lists
    print("\n🔹 Products bought by each user (collect_list):")
    df.groupBy("user_id") \
        .agg(
            collect_set("product_id").alias("products_bought")
        ) \
        .show(5, truncate=False)


def advanced_groupby(df):
    """
    LESSON 3: Advanced GroupBy patterns
    
    More sophisticated aggregation patterns you'll use in real-world scenarios.
    """
    print("\n" + "="*70)
    print("LESSON 3: Advanced GroupBy Patterns")
    print("="*70)
    
    # Conditional aggregation
    print("\n🔹 Conditional aggregation (count by condition):")
    df.groupBy("user_id") \
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed"),
            spark_sum(when(col("status") == "cancelled", 1).otherwise(0)).alias("cancelled"),
            spark_sum(when(col("status") == "completed", col("amount")).otherwise(0)).alias("completed_revenue")
        ) \
        .orderBy(desc("total_transactions")) \
        .show(10)
    
    # Aggregation with percentage calculation
    print("\n🔹 Revenue distribution by status (with percentages):")
    status_revenue = df.groupBy("status") \
        .agg(
            count("*").alias("count"),
            spark_sum("amount").alias("revenue")
        )
    
    total_revenue = df.select(spark_sum("amount")).first()[0]
    
    status_revenue.withColumn(
        "percentage",
        spark_round((col("revenue") / total_revenue) * 100, 2)
    ).orderBy(desc("revenue")).show()


def window_functions_intro(df):
    """
    LESSON 4: Introduction to Window Functions
    
    Window functions perform calculations across a set of rows that are
    related to the current row. Unlike groupBy, they don't collapse rows.
    
    Key concepts:
    - Window specification: defines the "window" of rows
    - Partition by: group rows (like groupBy)
    - Order by: order within each partition
    - Frame: which rows in the partition to include
    """
    print("\n" + "="*70)
    print("LESSON 4: Window Functions - Introduction")
    print("="*70)
    
    print("\n🔹 Running total per user:")
    # Define window: partition by user, order by date
    window_spec = Window.partitionBy("user_id").orderBy("transaction_date")
    
    df_with_running_total = df \
        .withColumn("running_total", spark_sum("amount").over(window_spec)) \
        .withColumn("transaction_number", row_number().over(window_spec))
    
    # Show running total for a few users
    df_with_running_total \
        .filter(col("user_id").isin([1, 2, 3])) \
        .select("user_id", "transaction_date", "amount", "transaction_number", "running_total") \
        .orderBy("user_id", "transaction_date") \
        .show(20)
    
    print("\n🔹 Average amount per user (window avg):")
    # Calculate average transaction amount per user
    user_window = Window.partitionBy("user_id")
    
    df.withColumn("user_avg_amount", avg("amount").over(user_window)) \
        .withColumn("diff_from_avg", col("amount") - col("user_avg_amount")) \
        .select("user_id", "amount", "user_avg_amount", "diff_from_avg") \
        .show(10)


def window_functions_ranking(df):
    """
    LESSON 5: Window Functions - Ranking
    
    Ranking functions assign ranks to rows within partitions.
    
    - row_number(): Sequential number (1, 2, 3, 4...)
    - rank(): Rank with gaps (1, 2, 2, 4...)
    - dense_rank(): Rank without gaps (1, 2, 2, 3...)
    """
    print("\n" + "="*70)
    print("LESSON 5: Window Functions - Ranking")
    print("="*70)
    
    print("\n🔹 Top 3 transactions per user:")
    # Rank transactions by amount within each user
    user_amount_window = Window.partitionBy("user_id").orderBy(desc("amount"))
    
    df.withColumn("rank", row_number().over(user_amount_window)) \
        .filter(col("rank") <= 3) \
        .select("user_id", "amount", "transaction_date", "rank") \
        .orderBy("user_id", "rank") \
        .show(20)
    
    print("\n🔹 Difference between rank() and dense_rank():")
    # Create sample data with ties
    sample_data = [
        (1, 100),
        (2, 100),
        (3, 90),
        (4, 90),
        (5, 80),
    ]
    sample_df = spark.createDataFrame(sample_data, ["id", "score"])
    
    window = Window.orderBy(desc("score"))
    
    sample_df.select(
        col("id"),
        col("score"),
        row_number().over(window).alias("row_number"),
        rank().over(window).alias("rank"),
        dense_rank().over(window).alias("dense_rank")
    ).show()
    
    print("  row_number: Always sequential (1,2,3,4,5)")
    print("  rank:       Has gaps after ties (1,1,3,3,5)")
    print("  dense_rank: No gaps (1,1,2,2,3)")


def window_functions_lag_lead(df):
    """
    LESSON 6: Window Functions - Lag and Lead
    
    lag() and lead() access data from previous/next rows.
    Useful for comparing values across rows or calculating deltas.
    """
    print("\n" + "="*70)
    print("LESSON 6: Window Functions - Lag and Lead")
    print("="*70)
    
    print("\n🔹 Compare current transaction with previous one:")
    # Window ordered by date for each user
    user_date_window = Window.partitionBy("user_id").orderBy("transaction_date")
    
    df_with_prev = df \
        .withColumn("prev_amount", lag("amount", 1).over(user_date_window)) \
        .withColumn("next_amount", lead("amount", 1).over(user_date_window)) \
        .withColumn("amount_change", col("amount") - col("prev_amount"))
    
    df_with_prev \
        .filter(col("user_id").isin([1, 2])) \
        .select("user_id", "transaction_date", "prev_amount", "amount", "next_amount", "amount_change") \
        .orderBy("user_id", "transaction_date") \
        .show(15)
    
    print("\n🔹 First and last transaction per user:")
    df.withColumn("first_amount", first("amount").over(user_date_window)) \
        .withColumn("last_amount", last("amount").over(user_date_window)) \
        .select("user_id", "amount", "first_amount", "last_amount") \
        .distinct() \
        .show(10)


def practical_example(transactions_df, products_df):
    """
    LESSON 7: Practical example - Product analytics
    
    Combining what we've learned to solve a real analytics problem.
    """
    print("\n" + "="*70)
    print("LESSON 7: Practical Example - Product Performance Analysis")
    print("="*70)
    
    print("\n🔹 Top performing products:")
    
    # Product-level metrics
    product_metrics = transactions_df \
        .filter(col("status") == "completed") \
        .groupBy("product_id") \
        .agg(
            count("*").alias("num_sales"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_sale_price"),
            countDistinct("user_id").alias("unique_buyers")
        )
    
    # Join with products to get product names
    product_performance = product_metrics \
        .join(products_df, "product_id") \
        .select(
            "product_id",
            "name",
            "category",
            "num_sales",
            "total_revenue",
            "avg_sale_price",
            "unique_buyers"
        ) \
        .orderBy(desc("total_revenue"))
    
    print("Overall product performance:")
    product_performance.show(10, truncate=False)
    
    # Rank products within each category
    print("\n🔹 Top 3 products per category:")
    category_window = Window.partitionBy("category").orderBy(desc("total_revenue"))
    
    product_performance \
        .withColumn("rank_in_category", row_number().over(category_window)) \
        .filter(col("rank_in_category") <= 3) \
        .select("category", "name", "total_revenue", "num_sales", "rank_in_category") \
        .orderBy("category", "rank_in_category") \
        .show(20, truncate=False)


def main():
    """
    Main function - orchestrates all lessons.
    """
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("   JOB 2: Aggregations and Window Functions")
    print("🎓 " + "="*66 + " 🎓\n")
    
    spark = get_spark_session("Job 2: Aggregations")
    data_dir = get_data_dir()
    
    try:
        # Ensure data exists
        transactions_csv = os.path.join(data_dir, "transactions.csv")
        if not os.path.exists(transactions_csv):
            print("📁 Sample data not found. Generating...")
            generate_all_datasets(data_dir)
        
        # Load data
        print("\n📊 Loading datasets...")
        transactions_df = spark.read.csv(
            transactions_csv,
            header=True,
            inferSchema=True
        )
        
        products_df = spark.read.json(os.path.join(data_dir, "products.json"))
        
        print(f"✓ Loaded {transactions_df.count()} transactions")
        print(f"✓ Loaded {products_df.count()} products")
        
        # Run lessons
        simple_aggregations(transactions_df)
        groupby_aggregations(transactions_df)
        advanced_groupby(transactions_df)
        window_functions_intro(transactions_df)
        window_functions_ranking(transactions_df)
        window_functions_lag_lead(transactions_df)
        practical_example(transactions_df, products_df)
        
        # Final summary
        print("\n" + "="*70)
        print("✅ JOB 2 COMPLETED!")
        print("="*70)
        print("\n📚 What you learned:")
        print("  ✓ Simple and complex aggregations")
        print("  ✓ GroupBy operations with multiple dimensions")
        print("  ✓ Window functions for running totals")
        print("  ✓ Ranking functions (row_number, rank, dense_rank)")
        print("  ✓ Lag and Lead for time-series analysis")
        print("  ✓ Practical analytics patterns")
        print("\n🎯 Next step: Run Job 3 to learn about joins!")
        print("   python jobs/03_joins.py\n")
        
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    # Need to reference spark in window functions
    spark = None
    main()
