#!/usr/bin/env python3
"""
Convert PySpark job Python files to Jupyter notebooks

This script converts the job Python files to notebook format.
Run this once to create all notebooks from the Python files.
"""

import json
import os
import re

def python_to_notebook(py_file, nb_file):
    """Convert a Python file to a Jupyter notebook"""
    
    with open(py_file, 'r') as f:
        content = f.read()
    
    # Extract docstring at top
    docstring_match = re.search(r'^"""(.*?)"""', content, re.DOTALL)
    intro_text = docstring_match.group(1).strip() if docstring_match else ""
    
    # Remove docstring from content
    if docstring_match:
        content = content[docstring_match.end():].strip()
    
    cells = []
    
    # Add title cell
    title_lines = intro_text.split('\n')
    title = title_lines[0] if title_lines else "PySpark Job"
    title_source = f"# {title}\n\n" + '\n'.join(title_lines[1:]) if len(title_lines) > 1 else f"# {title}"
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": title_source
    })
    
    # Add setup markdown
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": "## Setup: Import Libraries and Initialize Spark"
    })
    
    # Extract imports
    import_lines = []
    remaining_lines = []
    in_imports = True
    
    for line in content.split('\n'):
        if in_imports:
            if line.startswith('import ') or line.startswith('from ') or line.strip() == '' or line.startswith('#'):
                import_lines.append(line)
            else:
                in_imports = False
                remaining_lines.append(line)
        else:
            remaining_lines.append(line)
    
    # Setup cell with imports
    setup_code = '''import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path  
project_root = os.path.dirname(os.getcwd())
sys.path.append(project_root)

''' + '\n'.join([l for l in import_lines if not l.startswith('sys.path') and l.strip() != '']) + '''

print("✅ Libraries imported successfully!")'''
    
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": setup_code
    })
    
    # Spark session cell
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": '''# Initialize Spark Session
spark = get_spark_session("''' + title + '''")
data_dir = get_data_dir()

print(f"✅ Spark session created!")
print(f"📊 Spark UI: http://localhost:4040")
print(f"📁 Data directory: {data_dir}")'''
    })
    
    # Data generation cell
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": '''# Generate sample data if it doesn't exist
from utils.data_generator import generate_all_datasets
import os

data_files = os.path.join(data_dir, "users.csv")
if not os.path.exists(data_files):
    print("📁 Sample data not found. Generating...")
    generate_all_datasets(data_dir)
    print("✅ Sample data generated!")
else:
    print("✅ Sample data already exists.")'''
    })
    
    # Parse functions and convert to cells
    remaining_content = '\n'.join(remaining_lines)
    
    # Find all function definitions
    functions = re.finditer(r'def\s+(\w+)\(.*?\):(.*?)(?=\ndef\s|\Z)', remaining_content, re.DOTALL)
    
    for match in functions:
        func_name = match.group(1)
        func_body = match.group(2).strip()
        
        # Skip main function
        if func_name == 'main':
            continue
        
        # Extract docstring
        doc_match = re.search(r'^\s*"""(.*?)"""', func_body, re.DOTALL)
        if doc_match:
            doc_text = doc_match.group(1).strip()
            func_body = func_body[doc_match.end():].strip()
            
            # Add markdown cell for lesson
            cells.append({
                "cell_type": "markdown",
                "metadata": {},
                "source": "## " + doc_text.split('\n')[0] + "\n\n" + '\n'.join(doc_text.split('\n')[1:])
            })
        
        # Split function body into logical cells (by print statements or comments)
        code_sections = []
        current_section = []
        
        for line in func_body.split('\n'):
            if line.strip().startswith('print("\\n🔹') and current_section:
                code_sections.append('\n'.join(current_section))
                current_section = [line]
            else:
                current_section.append(line)
        
        if current_section:
            code_sections.append('\n'.join(current_section))
        
        # Add code cells
        for section in code_sections:
            if section.strip():
                cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": section.strip()
                })
    
    # Add summary
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": "## Summary\n\n✅ Notebook completed! Check the next notebook to continue learning."
    })
    
    # Add cleanup cell
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": "## Cleanup (Optional)\n\nUncomment and run to stop Spark session:"
    })
    
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": '''# stop_spark_session(spark)
# print("✅ Spark session stopped.")'''
    })
    
    # Create notebook structure
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.12.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    # Write notebook
    with open(nb_file, 'w') as f:
        json.dump(notebook, f, indent= 1)
    
    print(f"✅ Created: {nb_file}")

def main():
    project_root = os.path.dirname(os.path.abspath(__file__))
    jobs_dir = os.path.join(project_root, 'jobs')
    notebooks_dir = os.path.join(project_root, 'notebooks')
    
    os.makedirs(notebooks_dir, exist_ok=True)
    
    print("="*70)
    print("Converting PySpark Jobs to Jupyter Notebooks")
    print("="*70 + "\n")
    
    # Note: 01 already created manually with better structure
    jobs = [
        ('02_aggregations.py', '02_aggregations.ipynb'),
        ('03_joins.py', '03_joins.ipynb'),
        ('04_analytics_udfs.py', '04_analytics_udfs.ipynb'),
        ('05_search_indexing.py', '05_search_indexing.ipynb'),
    ]
    
    for py_file, nb_file in jobs:
        py_path = os.path.join(jobs_dir, py_file)
        nb_path = os.path.join(notebooks_dir, nb_file)
        
        if os.path.exists(py_path):
            try:
                python_to_notebook(py_path, nb_path)
            except Exception as e:
                print(f"❌ Error converting {py_file}: {e}")
        else:
            print(f"⚠️  Not found: {py_path}")
    
    print("\n" + "="*70)
    print("✅ Conversion complete!")
    print("="*70)
    print("\n💡 Note: Notebook 01 was created manually for optimal structure.")
    print("   The converted notebooks may need minor adjustments.")
    print("\n🚀 Start Jupyter: python start_jupyter.py")

if __name__ == "__main__":
    main()
