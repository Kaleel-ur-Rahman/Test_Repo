#!/usr/bin/env python3
"""
Script Name: cron_job_guard.py

Description:
    Checks if a Python job is already running using:
        ps aux | grep python

    If running → exit
    If not → execute the script
"""

import glob
import subprocess
import sys
from datetime import datetime
import os


today = datetime.now()
todayDate = today.strftime("%Y%m%d")

def is_job_running(job_keyword: str) -> bool:
    """
    Check if a job is running using:
        ps aux | grep python | grep <job_keyword> | grep -v grep

    Args:
        job_keyword (str): Unique keyword (script name)

    Returns:
        bool: True if running, False otherwise
    """
    try:
        command = f"ps aux | grep python | grep '{job_keyword}' | grep -v grep"

        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.stdout.strip():
            print("[DEBUG] Matching process found:")
            print(result.stdout)
            return True

        return False

    except Exception as e:
        print(f"[ERROR] Failed to check processes: {e}")
        sys.exit(1)


def execute_script(script_path: str):
    """
    Execute the Python script

    Command:
        python3 <script_path>
    """
    try:
        print(f"[INFO] Executing script: {script_path}")

        subprocess.run(
            ["python3", script_path],
            check=True
        )

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Execution failed: {e}")
        sys.exit(1)


def get_next_filename(file_path: str) -> str:
    """
    Generate next available filename:
    file.json → file_1.json → file_2.json ...
    """
    base, ext = os.path.splitext(file_path)
    counter = 1

    new_file = f"{base}_{counter}{ext}"

    while os.path.exists(new_file):
        counter += 1
        new_file = f"{base}_{counter}{ext}"

    return new_file


def rename_if_exists(file_patterns):
    """
    Rename files if they exist using incremental suffix
    """
    for pattern in file_patterns:
        files = glob.glob(pattern)

        for file_path in files:
            if os.path.exists(file_path):
                new_name = get_next_filename(file_path)

                os.rename(file_path, new_name)
                print(f"[INFO] Renamed: {file_path} → {new_name}")


def main():
    # 🔧 Use a UNIQUE name to avoid false matches
    JOB_KEYWORD = "AutoTraders.py"

    # 🔧 Files to check
    FILE_PATTERNS = [
        f"/opt/apps/UKProjects/Mans_Group/AutoTraders/JSON/running_category_Part_1_{todayDate}.json",
        f"/opt/apps/UKProjects/Mans_Group/AutoTraders/logs/AutoTraders_Part_1_{todayDate}.log",
        f"/opt/apps/UKProjects/Mans_Group/AutoTraders/Output/autotrader_listings_output_Part_1_{todayDate}.csv"
    ]

    # 🔧 Full path to your script
    SCRIPT_PATH = "/opt/apps/UKProjects/Mans_Group/AutoTraders/AutoTraders.py"

    print("[INFO] Checking running Python jobs...")

    if is_job_running(JOB_KEYWORD):
        print("[INFO] Job is already running. Exiting.")
        sys.exit(0)

    # ✅ Rename files BEFORE starting
    rename_if_exists(FILE_PATTERNS)

    print("[INFO] No running job found.")
    execute_script(SCRIPT_PATH)


if __name__ == "__main__":
    main()