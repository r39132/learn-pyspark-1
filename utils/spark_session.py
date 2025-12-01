"""
Utility module for creating and managing SparkSession instances.

This module provides a centralized way to create SparkSession objects
with consistent configuration across all jobs.
"""

from pyspark.sql import SparkSession
import os


def get_spark_session(app_name="PySpark Learning", local_mode=True):
    """
    Create or get an existing SparkSession.
    
    Args:
        app_name (str): Name of the Spark application
        local_mode (bool): If True, run in local mode with all cores
        
    Returns:
        SparkSession: Configured SparkSession instance
    """
    builder = SparkSession.builder.appName(app_name)
    
    if local_mode:
        # Local mode: use all available cores
        # Format: local[*] means use all cores
        # local[4] would use 4 cores
        builder = builder.master("local[*]")
    
    # Configuration options for better local development experience
    builder = builder.config("spark.sql.shuffle.partitions", "4")  # Default is 200, too high for local
    builder = builder.config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Suppress excessive logging with Log4j2 configuration
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log4j2_path = os.path.join(project_root, "log4j2.properties")
    builder = builder.config("spark.driver.extraJavaOptions", 
                            f"-Dlog4j.configurationFile=file:{log4j2_path}")
    
    spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce console output
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark):
    """
    Stop the SparkSession and release resources.
    
    Args:
        spark (SparkSession): The SparkSession to stop
    """
    if spark:
        spark.stop()


def create_output_dir(base_dir="output"):
    """
    Create output directory if it doesn't exist.
    
    Args:
        base_dir (str): Base directory path
        
    Returns:
        str: Absolute path to output directory
    """
    # Get the project root (parent of utils directory)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(project_root, base_dir)
    
    os.makedirs(output_path, exist_ok=True)
    return output_path


def get_data_dir():
    """
    Get the data directory path.
    
    Returns:
        str: Absolute path to data directory
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data")
    
    os.makedirs(data_path, exist_ok=True)
    return data_path
