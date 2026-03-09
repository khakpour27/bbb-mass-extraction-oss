"""Run legacy pipeline end-to-end: mass_calc_legacy.py + publish_legacy.py"""
import subprocess
import sys
import os

PROPY = r"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\propy.bat"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def run(script):
    print(f"\n{'='*60}")
    print(f"Running {script}...")
    print(f"{'='*60}\n")
    result = subprocess.run(
        [PROPY, os.path.join(SCRIPT_DIR, script)],
        cwd=SCRIPT_DIR,
    )
    if result.returncode != 0:
        print(f"\nERROR: {script} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    print(f"\n{script} completed successfully.")

if __name__ == "__main__":
    run("mass_calc_legacy.py")
    run("publish_legacy.py")
    print("\nLegacy pipeline complete (calc + publish).")
