"""Run fixed pipeline end-to-end: mass_calc_fixed.py + publish_fixed.py

mass_calc_fixed.py = scbm's legacy code + two fixes:
  1. Deep model filter (exclude orphaned infrastructure without tunnel coverage)
  2. Strategy E (fill deep-tunnel NaN holes with terrain to prevent false BFS excavation)
"""
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
    run("mass_calc_fixed.py")
    run("publish_fixed.py")
    print("\nFixed pipeline complete (calc + publish).")
