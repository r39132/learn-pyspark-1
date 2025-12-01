#!/usr/bin/env python3
"""
Start Jupyter Notebook Server for PySpark Learning

This script starts a Jupyter notebook server configured for PySpark.
It loads environment variables and sets up the proper Spark configuration.
"""

import os
import sys
import subprocess
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    # Get project root
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Set up environment variables for PySpark + Jupyter
    env = os.environ.copy()
    
    # Jupyter configuration
    jupyter_port = env.get('JUPYTER_PORT', '8888')
    jupyter_token = env.get('JUPYTER_TOKEN', '')
    
    # Start in notebooks directory
    notebooks_dir = os.path.join(project_root, 'notebooks')
    os.makedirs(notebooks_dir, exist_ok=True)
    
    print("="*70)
    print("🚀 Starting Jupyter Notebook Server for PySpark")
    print("="*70)
    print(f"\n📁 Notebooks directory: {notebooks_dir}")
    print(f"🌐 Jupyter will be available at: http://localhost:{jupyter_port}")
    print(f"📊 Spark UI will be available at: http://localhost:4040")
    print("\n💡 Tips:")
    print("   - Each notebook creates its own Spark session")
    print("   - Monitor Spark UI while running cells")
    print("   - Use Kernel > Restart to clear Spark session")
    print("\n⌨️  Press Ctrl+C to stop the server")
    print("="*70 + "\n")
    
    # Build jupyter command
    cmd = [
        'jupyter', 'notebook',
        '--notebook-dir', notebooks_dir,
        '--port', jupyter_port,
        '--no-browser'
    ]
    
    # Add token if specified
    if jupyter_token:
        cmd.extend(['--NotebookApp.token', jupyter_token])
    else:
        cmd.append('--NotebookApp.token=""')
    
    # Add allow root for docker/container environments
    cmd.append('--allow-root')
    
    try:
        # Start Jupyter
        subprocess.run(cmd, cwd=notebooks_dir, env=env)
    except KeyboardInterrupt:
        print("\n\n✅ Jupyter server stopped.")
        print("="*70)

if __name__ == "__main__":
    main()
