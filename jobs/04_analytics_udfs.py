"""
Job 4: Advanced Analytics and User-Defined Functions (UDFs)

Learning Objectives:
- Creating and using User-Defined Functions (UDFs)
- Pandas UDFs for better performance
- Complex analytics patterns (cohort analysis, RFM scoring)
- Pivot tables and data reshaping
- Advanced statistical operations

Key Concepts:
- UDFs allow custom Python logic in Spark
- Regular UDFs are slow (serialization overhead)
- Pandas UDFs are much faster (vectorized operations)
- Use built-in functions when possible
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, pandas_udf, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, desc, when, datediff, current_date, expr, lit,
    first, concat, round as spark_round, ntile, percent_rank, split, lower
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, FloatType
import pandas as pd
from utils.spark_session import get_spark_session, stop_spark_session, get_data_dir
from utils.data_generator import generate_all_datasets


def regular_udf_example(df):
    """
    LESSON 1: Regular User-Defined Functions (UDFs)
    
    UDFs let you apply custom Python functions to DataFrame columns.
    
    WARNING: Regular UDFs are slow because:
    - Data is serialized to Python
    - Processed row-by-row
    - Results serialized back to JVM
    
    Use built-in functions when possible!
    """
    print("\n" + "="*70)
    print("LESSON 1: Regular UDFs (User-Defined Functions)")
    print("="*70)
    
    # Define a simple Python function
    def categorize_age(age):
        """Categorize age into groups"""
        if age is None:
            return "Unknown"
        elif age < 25:
            return "Young Adult"
        elif age < 40:
            return "Adult"
        elif age < 60:
            return "Middle Aged"
        else:
            return "Senior"
    
    # Register as UDF
    categorize_age_udf = udf(categorize_age, StringType())
    
    print("\n🔹 Using UDF to categorize ages:")
    result = df.withColumn("age_category", categorize_age_udf(col("age")))
    result.select("name", "age", "age_category").show(10)
    
    # Alternative: Use SQL-style when() instead (FASTER!)
    print("\n🔹 Same logic using when() - MUCH FASTER:")
    result_optimized = df.withColumn(
        "age_category",
        when(col("age").isNull(), "Unknown")
        .when(col("age") < 25, "Young Adult")
        .when(col("age") < 40, "Adult")
        .when(col("age") < 60, "Middle Aged")
        .otherwise("Senior")
    )
    result_optimized.select("name", "age", "age_category").show(10)
    
    print("\n💡 Prefer built-in functions over UDFs for better performance!")


def pandas_udf_example(df):
    """
    LESSON 2: Pandas UDFs (Vectorized UDFs)
    
    Pandas UDFs process data in batches using Apache Arrow, making them
    much faster than regular UDFs.
    
    Types:
    - Scalar: Operate on Series, return Series
    - Grouped Map: Operate on DataFrame, return DataFrame
    """
    print("\n" + "="*70)
    print("LESSON 2: Pandas UDFs (Vectorized - Much Faster!)")
    print("="*70)
    
    # Define a Pandas UDF
    @pandas_udf(StringType())
    def email_domain(emails: pd.Series) -> pd.Series:
        """Extract domain from email addresses"""
        return emails.str.split('@').str[1]
    
    print("\n🔹 Using Pandas UDF to extract email domains:")
    result = df.withColumn("email_domain", email_domain(col("email")))
    result.select("name", "email", "email_domain").show(10)
    
    # Another example: Custom scoring
    @pandas_udf(DoubleType())
    def custom_age_score(ages: pd.Series) -> pd.Series:
        """Calculate a custom score based on age"""
        return ages * 1.5 + 10.0
    
    print("\n🔹 Custom scoring with Pandas UDF:")
    result = df.withColumn("age_score", custom_age_score(col("age")))
    result.select("name", "age", "age_score").show(10)
    
    print("\n💡 Pandas UDFs are 10-100x faster than regular UDFs!")


def pivot_tables(transactions_df, products_df):
    """
    LESSON 3: Pivot Tables
    
    Pivot tables reshape data, turning row values into columns.
    Useful for creating crosstab reports and wide-format data.
    """
    print("\n" + "="*70)
    print("LESSON 3: Pivot Tables")
    print("="*70)
    
    # Join transactions with products
    data = transactions_df.filter(col("status") == "completed") \
        .join(products_df, "product_id")
    
    print("\n🔹 Revenue by category and status:")
    pivot_result = data.groupBy("category") \
        .pivot("status") \
        .agg(spark_sum("amount"))
    
    pivot_result.show()
    
    print("\n🔹 Count of transactions by category and month:")
    # Extract month from date
    data_with_month = data.withColumn(
        "month",
        expr("substring(transaction_date, 6, 2)")
    )
    
    monthly_pivot = data_with_month.groupBy("category") \
        .pivot("month") \
        .agg(count("transaction_id"))
    
    monthly_pivot.show()
    
    print("\n💡 Pivot is great for creating Excel-like reports!")


def rfm_analysis(users_df, transactions_df):
    """
    LESSON 4: RFM Analysis (Recency, Frequency, Monetary)
    
    RFM is a classic marketing analytics technique:
    - Recency: How recently did the customer purchase?
    - Frequency: How often do they purchase?
    - Monetary: How much do they spend?
    
    Used for customer segmentation and targeting.
    """
    print("\n" + "="*70)
    print("LESSON 4: RFM Analysis (Customer Segmentation)")
    print("="*70)
    
    print("\n🔹 Calculating RFM scores for each customer:")
    
    # Calculate RFM metrics
    rfm = transactions_df.filter(col("status") == "completed") \
        .groupBy("user_id") \
        .agg(
            datediff(current_date(), spark_max("transaction_date")).alias("recency"),
            count("transaction_id").alias("frequency"),
            spark_sum("amount").alias("monetary")
        )
    
    # Join with user data
    rfm_with_users = rfm.join(users_df, "user_id") \
        .select("user_id", "name", "recency", "frequency", "monetary")
    
    print("\nRaw RFM metrics:")
    rfm_with_users.orderBy(desc("monetary")).show(10)
    
    # Score each dimension (1-5, where 5 is best)
    # For recency: lower is better (more recent)
    # For frequency and monetary: higher is better
    
    from pyspark.sql import Window
    
    window_spec = Window.orderBy(col("recency"))
    rfm_scored = rfm_with_users \
        .withColumn("r_score", 6 - ntile(5).over(window_spec)) \
        .withColumn("f_score", ntile(5).over(Window.orderBy(col("frequency")))) \
        .withColumn("m_score", ntile(5).over(Window.orderBy(col("monetary"))))
    
    # Calculate overall RFM score
    rfm_scored = rfm_scored.withColumn(
        "rfm_score",
        col("r_score") * 100 + col("f_score") * 10 + col("m_score")
    )
    
    print("\n🔹 RFM Scores (555 is best customer):")
    rfm_scored.select(
        "name", "recency", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_score"
    ).orderBy(desc("rfm_score")).show(15)
    
    # Segment customers
    rfm_segmented = rfm_scored.withColumn(
        "segment",
        when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "Champions")
        .when((col("r_score") >= 3) & (col("f_score") >= 3), "Loyal Customers")
        .when((col("r_score") >= 4) & (col("f_score") <= 2), "Promising")
        .when((col("r_score") <= 2) & (col("f_score") >= 3), "At Risk")
        .when((col("r_score") <= 2) & (col("f_score") <= 2), "Lost")
        .otherwise("Others")
    )
    
    print("\n🔹 Customer Segments:")
    rfm_segmented.groupBy("segment") \
        .agg(
            count("*").alias("customer_count"),
            avg("monetary").alias("avg_monetary")
        ) \
        .orderBy(desc("customer_count")) \
        .show()
    
    print("\n💡 RFM helps identify your best customers and those at risk!")


def cohort_analysis(users_df, transactions_df):
    """
    LESSON 5: Cohort Analysis
    
    Cohort analysis groups customers by a common characteristic (e.g., signup month)
    and tracks their behavior over time.
    
    Useful for understanding customer retention and lifetime value.
    """
    print("\n" + "="*70)
    print("LESSON 5: Cohort Analysis")
    print("="*70)
    
    print("\n🔹 Analyzing customer behavior by signup cohort:")
    
    # Define cohort as signup month
    users_with_cohort = users_df.withColumn(
        "cohort",
        expr("substring(signup_date, 1, 7)")  # YYYY-MM
    )
    
    # Join with transactions
    cohort_data = users_with_cohort.join(
        transactions_df.filter(col("status") == "completed"),
        "user_id"
    )
    
    # Calculate months since signup
    cohort_data = cohort_data.withColumn(
        "transaction_month",
        expr("substring(transaction_date, 1, 7)")
    )
    
    # Aggregate by cohort
    cohort_summary = cohort_data.groupBy("cohort") \
        .agg(
            countDistinct("user_id").alias("cohort_size"),
            count("transaction_id").alias("total_transactions"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value")
        ) \
        .orderBy("cohort")
    
    print("\nCohort performance:")
    cohort_summary.show(20)
    
    # Transaction count by cohort and month
    print("\n🔹 Transactions by cohort over time:")
    cohort_monthly = cohort_data.groupBy("cohort", "transaction_month") \
        .agg(count("transaction_id").alias("transactions")) \
        .orderBy("cohort", "transaction_month")
    
    cohort_monthly.show(20)
    
    print("\n💡 Cohort analysis reveals how different customer groups behave over time!")


def statistical_functions(df):
    """
    LESSON 6: Statistical Functions
    
    Spark provides various statistical functions for data analysis.
    """
    print("\n" + "="*70)
    print("LESSON 6: Statistical Functions")
    print("="*70)
    
    print("\n🔹 Basic statistics:")
    df.select("age").describe().show()
    
    print("\n🔹 Correlation between age and user_id (example):")
    correlation = df.stat.corr("user_id", "age")
    print(f"Correlation: {correlation:.4f}")
    
    print("\n🔹 Percentiles:")
    percentiles = df.stat.approxQuantile("age", [0.25, 0.5, 0.75, 0.95], 0.01)
    print(f"25th percentile: {percentiles[0]}")
    print(f"50th percentile (median): {percentiles[1]}")
    print(f"75th percentile: {percentiles[2]}")
    print(f"95th percentile: {percentiles[3]}")
    
    print("\n🔹 Cross-tabulation (country vs age group):")
    df_with_age_group = df.withColumn(
        "age_group",
        when(col("age") < 30, "<30")
        .when(col("age") < 50, "30-50")
        .otherwise("50+")
    )
    
    crosstab = df_with_age_group.stat.crosstab("country", "age_group")
    crosstab.show()


def funnel_analysis(clickstream_df):
    """
    LESSON 7: Funnel Analysis
    
    Analyze user journeys through different stages (view -> click -> cart -> purchase).
    Critical for conversion optimization.
    """
    print("\n" + "="*70)
    print("LESSON 7: Funnel Analysis")
    print("="*70)
    
    print("\n🔹 Analyzing the purchase funnel:")
    
    # Count users at each stage
    funnel = clickstream_df.groupBy("event_type") \
        .agg(countDistinct("user_id").alias("unique_users")) \
        .orderBy("unique_users", ascending=False)
    
    print("\nUsers at each funnel stage:")
    funnel.show()
    
    # Calculate conversion rates
    print("\n🔹 Conversion rates between stages:")
    
    # Get counts for each stage
    stage_counts = {
        row['event_type']: row['unique_users'] 
        for row in funnel.collect()
    }
    
    # Calculate conversion rates
    if 'view' in stage_counts and 'click' in stage_counts:
        view_to_click = (stage_counts['click'] / stage_counts['view']) * 100
        print(f"View → Click: {view_to_click:.2f}%")
    
    if 'click' in stage_counts and 'add_to_cart' in stage_counts:
        click_to_cart = (stage_counts['add_to_cart'] / stage_counts['click']) * 100
        print(f"Click → Add to Cart: {click_to_cart:.2f}%")
    
    if 'add_to_cart' in stage_counts and 'purchase' in stage_counts:
        cart_to_purchase = (stage_counts['purchase'] / stage_counts['add_to_cart']) * 100
        print(f"Add to Cart → Purchase: {cart_to_purchase:.2f}%")
    
    # Product-level funnel
    print("\n🔹 Top products by engagement:")
    product_funnel = clickstream_df.groupBy("product_id") \
        .agg(
            countDistinct(when(col("event_type") == "view", col("user_id"))).alias("views"),
            countDistinct(when(col("event_type") == "click", col("user_id"))).alias("clicks"),
            countDistinct(when(col("event_type") == "add_to_cart", col("user_id"))).alias("add_to_cart"),
            countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("purchases")
        ) \
        .orderBy(desc("views"))
    
    product_funnel.show(10)


def main():
    """
    Main function - orchestrates all lessons.
    """
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("   JOB 4: Advanced Analytics and UDFs")
    print("🎓 " + "="*66 + " 🎓\n")
    
    spark = get_spark_session("Job 4: Analytics & UDFs")
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
        clickstream_df = spark.read.csv(
            os.path.join(data_dir, "clickstream.csv"),
            header=True,
            inferSchema=True
        )
        
        print(f"✓ Loaded {users_df.count()} users")
        print(f"✓ Loaded {transactions_df.count()} transactions")
        print(f"✓ Loaded {products_df.count()} products")
        print(f"✓ Loaded {clickstream_df.count()} clickstream events")
        
        # Run lessons
        regular_udf_example(users_df)
        pandas_udf_example(users_df)
        pivot_tables(transactions_df, products_df)
        rfm_analysis(users_df, transactions_df)
        cohort_analysis(users_df, transactions_df)
        statistical_functions(users_df)
        funnel_analysis(clickstream_df)
        
        # Final summary
        print("\n" + "="*70)
        print("✅ JOB 4 COMPLETED!")
        print("="*70)
        print("\n📚 What you learned:")
        print("  ✓ Regular UDFs and Pandas UDFs")
        print("  ✓ Pivot tables for data reshaping")
        print("  ✓ RFM analysis for customer segmentation")
        print("  ✓ Cohort analysis for retention tracking")
        print("  ✓ Statistical functions")
        print("  ✓ Funnel analysis for conversion optimization")
        print("\n🎯 Next step: Run Job 5 to learn about search indexing!")
        print("   python jobs/05_search_indexing.py\n")
        
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
