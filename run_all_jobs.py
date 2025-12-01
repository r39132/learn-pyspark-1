#!/usr/bin/env python3
"""
Run all PySpark learning jobs in sequence.

This script runs all 5 jobs one after another, giving you a complete
learning experience. Each job will pause before starting so you can
read the output.
"""

import os
import sys
import subprocess
import time


def print_banner(text):
    """Print a nice banner"""
    width = 70
    print("\n" + "=" * width)
    print(f"  {text}")
    print("=" * width + "\n")


def run_job(job_path, job_number, job_name):
    """Run a single job"""
    print_banner(f"JOB {job_number}: {job_name}")
    
    print(f"📂 Running: {os.path.basename(job_path)}")
    print(f"⏰ Starting at: {time.strftime('%H:%M:%S')}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, job_path],
            check=True,
            capture_output=False
        )
        
        print(f"\n✅ Job {job_number} completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Job {job_number} failed with error code {e.returncode}")
        print("Check the output above for error details.")
        return False
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        return False


def main():
    """Run all jobs in sequence"""
    print("\n" + "🎓 " + "=" * 66 + " 🎓")
    print("     PySpark Learning Project - Run All Jobs")
    print("🎓 " + "=" * 66 + " 🎓\n")
    
    print("This script will run all 5 learning jobs in sequence.")
    print("Total estimated time: ~3-4 hours")
    print("\nTips:")
    print("  • Read the output carefully - it's educational!")
    print("  • Open http://localhost:4040 to see Spark UI")
    print("  • Press Ctrl+C to stop at any time")
    print("  • You can run individual jobs later: python jobs/01_dataframe_basics.py")
    
    # Check for non-interactive mode
    non_interactive = '--non-interactive' in sys.argv or '-y' in sys.argv
    
    if not non_interactive:
        response = input("\n📝 Ready to start? (y/n): ").strip().lower()
        if response != 'y':
            print("Exiting. Run individual jobs when ready!")
            return
    
    # Define all jobs
    jobs_dir = os.path.join(os.path.dirname(__file__), "jobs")
    jobs = [
        ("01_dataframe_basics.py", "DataFrame Basics"),
        ("02_aggregations.py", "Aggregations & Window Functions"),
        ("03_joins.py", "Joins & Data Relationships"),
        ("04_analytics_udfs.py", "Advanced Analytics & UDFs"),
        ("05_search_indexing.py", "Search Indexing & Text Processing"),
    ]
    
    # Run each job
    completed = 0
    for i, (job_file, job_name) in enumerate(jobs, 1):
        job_path = os.path.join(jobs_dir, job_file)
        
        if not os.path.exists(job_path):
            print(f"❌ Job file not found: {job_path}")
            continue
        
        # Run the job
        success = run_job(job_path, i, job_name)
        
        if not success:
            print(f"\n⚠️  Stopping after Job {i} due to error or interruption")
            break
        
        completed += 1
        
        # Pause between jobs (except after last one)
        if i < len(jobs):
            print("\n" + "-" * 70)
            print(f"✓ Completed {completed}/{len(jobs)} jobs")
            print("-" * 70)
            
            # Skip pause in non-interactive mode
            if not non_interactive:
                response = input("\n➡️  Press Enter to continue to next job (or 'q' to quit): ").strip().lower()
                if response == 'q':
                    print("Stopping. You can resume anytime by running individual jobs!")
                    break
            else:
                print(f"➡️  Continuing to Job {i+1}...\n")
                time.sleep(1)  # Brief pause for readability
    
    # Final summary
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"\n✅ Completed {completed}/{len(jobs)} jobs")
    
    if completed == len(jobs):
        print("\n🎉 Congratulations! You've completed all PySpark learning jobs!")
        print("\n📚 What you've learned:")
        print("  • DataFrame operations and transformations")
        print("  • Aggregations and window functions")
        print("  • Various join types and strategies")
        print("  • UDFs and advanced analytics patterns")
        print("  • Text processing and search indexing")
        print("\n🎯 Next steps:")
        print("  • Review the code and modify it")
        print("  • Try with your own datasets")
        print("  • Explore PySpark MLlib for machine learning")
        print("  • Deploy on a real cluster (AWS EMR, Databricks, etc.)")
    else:
        print(f"\n📝 You can continue by running the remaining jobs individually:")
        for i in range(completed, len(jobs)):
            job_file, job_name = jobs[i]
            print(f"  python jobs/{job_file}")
    
    print("\n💡 Happy Sparking!\n")


if __name__ == "__main__":
    main()
