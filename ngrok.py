#!/usr/bin/env python3
"""
Script to run the optimized options processor with ngrok tunnel
"""

import subprocess
import threading
import time
import sys
import os

def run_main_app():
    """Run the main application"""
    try:
        subprocess.run([sys.executable, "finalchain.py"])
    except KeyboardInterrupt:
        print("Main application stopped")

def run_ngrok():
    """Run ngrok tunnel"""
    try:
        # Wait a bit for Flask to start
        time.sleep(5)
        print("Starting ngrok tunnel...")
        subprocess.run(["ngrok", "http", "5000"])
    except KeyboardInterrupt:
        print("Ngrok tunnel stopped")
    except FileNotFoundError:
        print("Ngrok not found. Please install ngrok first.")
        print("Visit: https://ngrok.com/download")

if __name__ == "__main__":
    print("Starting Optimized Options Chain Processor with Ngrok...")
    
    # Start main app in background thread
    app_thread = threading.Thread(target=run_main_app, daemon=True)
    app_thread.start()
    
    # Start ngrok tunnel
    try:
        run_ngrok()
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)
