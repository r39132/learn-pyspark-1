"""
Job 3: Joins and Data Relationships

Learning Objectives:
- Understanding different join types (inner, outer, left, right, cross)
- Join strategies and performance considerations
- Broadcast joins for small tables
- Handling duplicate column names
- Self joins for hierarchical data

Key Concepts:
- Joins are wide transformations (cause shuffle)
- Broadcast joins avoid shuffle for small tables
- Choose join types based on your data requirements
- Proper join keys are critical for correctness and performance
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, count, sum as spark_sum, desc, concat, lit
from utils.spark_session import get_spark_session, stop_spark_session, get_data_dir
from utils.data_generator import generate_all_datasets


def inner_join_example(users_df, transactions_df):
    """
    LESSON 1: Inner Join
    
    Inner join returns only rows that have matching values in both DataFrames.
    This is the most common join type.
    
    Use when: You only want records that exist in both datasets.
    """
    print("\n" + "="*70)
    print("LESSON 1: Inner Join")
    print("="*70)
    
    print("\n🔹 Inner join: Users with their transactions")
    print("   (Only users who have made transactions)")
    
    # Basic inner join
    result = users_df.join(
        transactions_df,
        users_df.user_id == transactions_df.user_id,
        "inner"
    )
    
    print(f"\nUsers: {users_df.count()} rows")
    print(f"Transactions: {transactions_df.count()} rows")
    print(f"After inner join: {result.count()} rows")
    
    # Select specific columns to show
    result.select(
        users_df.user_id,
        users_df.name,
        users_df.email,
        transactions_df.transaction_id,
        transactions_df.amount,
        transactions_df.transaction_date
    ).show(10)
    
    # Count transactions per user
    print("\n🔹 Transactions per user (aggregated):")
    users_df.join(transactions_df, "user_id", "inner") \
        .groupBy("user_id", "name") \
        .agg(
            count("transaction_id").alias("num_transactions"),
            spark_sum("amount").alias("total_spent")
        ) \
        .orderBy(desc("total_spent")) \
        .show(10)


def left_join_example(users_df, transactions_df):
    """
    LESSON 2: Left (Outer) Join
    
    Left join returns all rows from the left DataFrame, and matching rows
    from the right. If no match, right columns will be null.
    
    Use when: You want all records from the left table, regardless of matches.
    """
    print("\n" + "="*70)
    print("LESSON 2: Left Join")
    print("="*70)
    
    print("\n🔹 Left join: All users, with transactions if they exist")
    
    result = users_df.join(
        transactions_df,
        "user_id",
        "left"
    )
    
    print(f"\nUsers: {users_df.count()} rows")
    print(f"After left join: {result.count()} rows")
    
    # Show some users with and without transactions
    result.select(
        "user_id",
        "name",
        "transaction_id",
        "amount"
    ).show(15)
    
    # Find users with no transactions (nulls in transaction columns)
    print("\n🔹 Users with NO transactions:")
    users_without_transactions = result \
        .filter(col("transaction_id").isNull()) \
        .select("user_id", "name", "email") \
        .distinct()
    
    print(f"Found {users_without_transactions.count()} users with no transactions")
    users_without_transactions.show(10)
    
    # Count transactions per user (including zero)
    print("\n🔹 All users with transaction counts (including zero):")
    users_df.join(transactions_df, "user_id", "left") \
        .groupBy("user_id", "name") \
        .agg(
            count("transaction_id").alias("num_transactions"),
            spark_sum("amount").alias("total_spent")
        ) \
        .orderBy("user_id") \
        .show(15)


def right_join_example(users_df, transactions_df):
    """
    LESSON 3: Right (Outer) Join
    
    Right join returns all rows from the right DataFrame, and matching rows
    from the left. If no match, left columns will be null.
    
    Use when: You want all records from the right table.
    (Note: Right join is less common; you can usually use left join instead)
    """
    print("\n" + "="*70)
    print("LESSON 3: Right Join")
    print("="*70)
    
    print("\n🔹 Right join: All transactions, with user info if available")
    
    result = users_df.join(
        transactions_df,
        "user_id",
        "right"
    )
    
    print(f"Transactions: {transactions_df.count()} rows")
    print(f"After right join: {result.count()} rows")
    
    result.select(
        "user_id",
        "name",
        "transaction_id",
        "amount"
    ).show(10)


def full_outer_join_example(users_df, transactions_df):
    """
    LESSON 4: Full Outer Join
    
    Full outer join returns all rows from both DataFrames. If no match,
    the missing side will have nulls.
    
    Use when: You want all records from both tables, regardless of matches.
    """
    print("\n" + "="*70)
    print("LESSON 4: Full Outer Join")
    print("="*70)
    
    print("\n🔹 Full outer join: All users AND all transactions")
    
    result = users_df.join(
        transactions_df,
        "user_id",
        "full"  # or "fullouter"
    )
    
    print(f"Users: {users_df.count()} rows")
    print(f"Transactions: {transactions_df.count()} rows")
    print(f"After full outer join: {result.count()} rows")
    
    # Count nulls on each side
    print("\n🔹 Analyzing the join result:")
    print(f"Rows with missing user data: {result.filter(col('name').isNull()).count()}")
    print(f"Rows with missing transaction data: {result.filter(col('transaction_id').isNull()).count()}")


def cross_join_example(spark):
    """
    LESSON 5: Cross Join
    
    Cross join returns the Cartesian product of two DataFrames.
    Every row from the left is combined with every row from the right.
    
    WARNING: This can create huge results! Use with caution.
    Use when: You need all possible combinations (e.g., product recommendations)
    """
    print("\n" + "="*70)
    print("LESSON 5: Cross Join")
    print("="*70)
    
    # Create small sample data for demonstration
    categories = [("Electronics",), ("Books",), ("Clothing",)]
    sizes = [("S",), ("M",), ("L",)]
    
    categories_df = spark.createDataFrame(categories, ["category"])
    sizes_df = spark.createDataFrame(sizes, ["size"])
    
    print("\n🔹 Cross join: All category-size combinations")
    print(f"Categories: {categories_df.count()} rows")
    print(f"Sizes: {sizes_df.count()} rows")
    
    result = categories_df.crossJoin(sizes_df)
    print(f"After cross join: {result.count()} rows (3 × 3 = 9)")
    
    result.show()
    
    print("\n⚠️  WARNING: Cross joins can create massive results!")
    print("   1000 × 1000 = 1,000,000 rows")
    print("   Use only when you truly need all combinations")


def multiple_joins_example(users_df, transactions_df, products_df):
    """
    LESSON 6: Multiple Joins
    
    Real-world scenarios often require joining multiple DataFrames.
    Chain joins to combine data from various sources.
    """
    print("\n" + "="*70)
    print("LESSON 6: Multiple Joins")
    print("="*70)
    
    print("\n🔹 Join users, transactions, and products together")
    
    # Join transactions with users
    transactions_with_users = transactions_df.join(
        users_df,
        "user_id",
        "inner"
    )
    
    # Then join with products
    full_data = transactions_with_users.join(
        products_df,
        "product_id",
        "inner"
    )
    
    print("\nComplete transaction view with user and product details:")
    full_data.select(
        transactions_df.transaction_id,
        users_df.name.alias("customer_name"),
        products_df.name.alias("product_name"),
        products_df.category,
        transactions_df.amount,
        transactions_df.transaction_date
    ).show(10, truncate=False)
    
    # Practical analysis: Top spending customers by category
    print("\n🔹 Top spenders by product category:")
    full_data.filter(col("status") == "completed") \
        .groupBy("category", users_df.name) \
        .agg(
            spark_sum("amount").alias("total_spent"),
            count("transaction_id").alias("num_purchases")
        ) \
        .orderBy("category", desc("total_spent")) \
        .show(20, truncate=False)


def broadcast_join_example(users_df, transactions_df):
    """
    LESSON 7: Broadcast Join (Performance Optimization)
    
    Broadcast join is an optimization for joining a large DataFrame with
    a small one. The small DataFrame is sent to all executors, avoiding shuffle.
    
    Use when:
    - One DataFrame is small (< 10MB typically)
    - You want to avoid expensive shuffle operations
    
    Rule of thumb: Broadcast if one side is < 100MB
    """
    print("\n" + "="*70)
    print("LESSON 7: Broadcast Join (Performance Optimization)")
    print("="*70)
    
    print("\n🔹 Regular join vs Broadcast join")
    
    # Regular join (both DataFrames shuffled)
    print("\n1. Regular join:")
    regular_join = transactions_df.join(users_df, "user_id", "inner")
    print("   Both DataFrames are shuffled across the cluster")
    
    # Broadcast join (small DataFrame sent to all nodes)
    print("\n2. Broadcast join:")
    broadcast_join = transactions_df.join(
        broadcast(users_df),  # Hint to broadcast users_df
        "user_id",
        "inner"
    )
    print("   Users DataFrame is copied to all executors (no shuffle!)")
    
    # Show execution plans
    print("\n📊 Execution plans (use explain() to see the difference):")
    print("\nRegular join plan:")
    regular_join.explain()
    
    print("\n" + "-"*70)
    print("\nBroadcast join plan:")
    broadcast_join.explain()
    
    print("\n💡 Broadcast joins are much faster for small lookup tables!")
    print("   Examples: country codes, product categories, user segments")


def handling_duplicate_columns(users_df, transactions_df):
    """
    LESSON 8: Handling Duplicate Column Names
    
    When joining, both DataFrames might have columns with the same name.
    Learn strategies to handle this.
    """
    print("\n" + "="*70)
    print("LESSON 8: Handling Duplicate Column Names")
    print("="*70)
    
    print("\n🔹 Problem: Both DataFrames have 'user_id' column")
    
    # Strategy 1: Use the join column name directly (it's deduplicated)
    print("\n1. Join on the common column:")
    result1 = users_df.join(transactions_df, "user_id", "inner")
    print("   The join column appears only once")
    result1.select("user_id", "name", "transaction_id").show(5)
    
    # Strategy 2: Use explicit column references with DataFrame prefixes
    print("\n2. Use DataFrame prefixes for clarity:")
    result2 = users_df.alias("u").join(
        transactions_df.alias("t"),
        col("u.user_id") == col("t.user_id"),
        "inner"
    ).select(
        col("u.user_id").alias("user_id"),
        col("u.name"),
        col("t.transaction_id"),
        col("t.amount")
    )
    result2.show(5)
    
    # Strategy 3: Rename columns before joining
    print("\n3. Rename columns to avoid conflicts:")
    transactions_renamed = transactions_df.withColumnRenamed("user_id", "customer_id")
    result3 = users_df.join(
        transactions_renamed,
        users_df.user_id == transactions_renamed.customer_id,
        "inner"
    )
    result3.select("user_id", "customer_id", "name", "transaction_id").show(5)


def self_join_example(spark):
    """
    LESSON 9: Self Join
    
    Self join joins a DataFrame with itself. Useful for hierarchical data
    or finding relationships within the same dataset.
    
    Example: Employee-Manager relationships, product recommendations
    """
    print("\n" + "="*70)
    print("LESSON 9: Self Join")
    print("="*70)
    
    # Create sample employee-manager data
    employees = [
        (1, "Alice", None),      # CEO, no manager
        (2, "Bob", 1),           # Reports to Alice
        (3, "Charlie", 1),       # Reports to Alice
        (4, "Diana", 2),         # Reports to Bob
        (5, "Eve", 2),           # Reports to Bob
        (6, "Frank", 3),         # Reports to Charlie
    ]
    
    emp_df = spark.createDataFrame(employees, ["emp_id", "emp_name", "manager_id"])
    
    print("\n🔹 Employee hierarchy:")
    emp_df.show()
    
    print("\n🔹 Self join: Employees with their manager names")
    # Join employees with themselves to get manager names
    result = emp_df.alias("emp").join(
        emp_df.alias("mgr"),
        col("emp.manager_id") == col("mgr.emp_id"),
        "left"  # Left join to include CEO (no manager)
    ).select(
        col("emp.emp_id"),
        col("emp.emp_name").alias("employee"),
        col("mgr.emp_name").alias("manager")
    )
    
    result.show()


def practical_example(users_df, transactions_df, products_df):
    """
    LESSON 10: Practical Example - Customer 360 View
    
    Combine everything to create a comprehensive customer analysis.
    """
    print("\n" + "="*70)
    print("LESSON 10: Practical Example - Customer 360 View")
    print("="*70)
    
    print("\n🔹 Building a comprehensive customer profile:")
    
    # Join all data
    customer_360 = transactions_df \
        .filter(col("status") == "completed") \
        .join(users_df, "user_id", "inner") \
        .join(products_df, "product_id", "inner")
    
    # Aggregate to customer level
    customer_profile = customer_360.groupBy(
        "user_id",
        "name",
        "email",
        "country",
        "signup_date"
    ).agg(
        count("transaction_id").alias("total_purchases"),
        spark_sum("amount").alias("lifetime_value"),
        spark_sum(col("quantity")).alias("total_items"),
        count(col("category").distinct()).alias("categories_purchased")
    ).orderBy(desc("lifetime_value"))
    
    print("\nTop customers by lifetime value:")
    customer_profile.show(10, truncate=False)
    
    # Category preferences
    print("\n🔹 Customer category preferences:")
    category_prefs = customer_360.groupBy("user_id", "name", "category") \
        .agg(
            count("*").alias("purchases"),
            spark_sum("amount").alias("spent")
        ) \
        .orderBy("user_id", desc("spent"))
    
    category_prefs.show(20, truncate=False)


def main():
    """
    Main function - orchestrates all lessons.
    """
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("   JOB 3: Joins and Data Relationships")
    print("🎓 " + "="*66 + " 🎓\n")
    
    spark = get_spark_session("Job 3: Joins")
    data_dir = get_data_dir()
    
    try:
        # Ensure data exists
        users_csv = os.path.join(data_dir, "users.csv")
        if not os.path.exists(users_csv):
            print("📁 Sample data not found. Generating...")
            generate_all_datasets(data_dir)
        
        # Load data
        print("\n📊 Loading datasets...")
        users_df = spark.read.csv(users_csv, header=True, inferSchema=True)
        transactions_df = spark.read.csv(
            os.path.join(data_dir, "transactions.csv"),
            header=True,
            inferSchema=True
        )
        products_df = spark.read.json(os.path.join(data_dir, "products.json"))
        
        print(f"✓ Loaded {users_df.count()} users")
        print(f"✓ Loaded {transactions_df.count()} transactions")
        print(f"✓ Loaded {products_df.count()} products")
        
        # Run lessons
        inner_join_example(users_df, transactions_df)
        left_join_example(users_df, transactions_df)
        right_join_example(users_df, transactions_df)
        full_outer_join_example(users_df, transactions_df)
        cross_join_example(spark)
        multiple_joins_example(users_df, transactions_df, products_df)
        broadcast_join_example(users_df, transactions_df)
        handling_duplicate_columns(users_df, transactions_df)
        self_join_example(spark)
        practical_example(users_df, transactions_df, products_df)
        
        # Final summary
        print("\n" + "="*70)
        print("✅ JOB 3 COMPLETED!")
        print("="*70)
        print("\n📚 What you learned:")
        print("  ✓ Inner, Left, Right, and Full Outer joins")
        print("  ✓ Cross joins and their use cases")
        print("  ✓ Multiple joins for complex analysis")
        print("  ✓ Broadcast joins for performance")
        print("  ✓ Handling duplicate column names")
        print("  ✓ Self joins for hierarchical data")
        print("  ✓ Building Customer 360 views")
        print("\n🎯 Next step: Run Job 4 to learn about UDFs and advanced analytics!")
        print("   python jobs/04_analytics_udfs.py\n")
        
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
