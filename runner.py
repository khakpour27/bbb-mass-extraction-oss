import subprocess
import sys
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Test mode: process only 10 IFC files")
args = parser.parse_args()

logging.basicConfig(
    filename='run_history.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

scripts = ['mass_calc.py', 'tunnel_vol.py']
mode = "TEST MODE" if args.test else "FULL MODE"
logging.info(f"=== Pipeline started ({mode}) ===")

for script in scripts:
    try:
        logging.info(f"Running {script}...")
        cmd = [sys.executable, script]
        if args.test:
            cmd.append("--test")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            logging.info(f"{script} stdout: {result.stdout[-500:]}")
        logging.info(f"{script} finished successfully")

    except subprocess.CalledProcessError as e:
        logging.error(f"{script} FAILED. Aborted further script runs.")
        logging.error(f"Details: {e.stderr[-2000:] if e.stderr else 'No stderr'}")
        sys.exit(1)

logging.info(f"=== Pipeline completed ({mode}) ===")
