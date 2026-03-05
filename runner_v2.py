"""
runner_v2.py — Performance-optimized pipeline runner.

Calls mass_calc_v2.py (parallelized grid processing) + tunnel_vol.py (unchanged).
Optionally publishes to AGOL comparison folder via publish_v2.py (--publish flag).
Original runner.py is unchanged — rollback by running runner.py instead.

Usage:
    python runner_v2.py                     # Full mode, 12 workers (default)
    python runner_v2.py --test              # Test mode, 12 workers
    python runner_v2.py --workers 8         # Full mode, 8 workers
    python runner_v2.py --sequential        # Full mode, no multiprocessing (debug)
    python runner_v2.py --test --sequential # Test mode, no multiprocessing
    python runner_v2.py --publish           # Full mode + publish to AGOL comparison folder
"""
import subprocess
import sys
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Test mode: process only 10 IFC files")
parser.add_argument("--sequential", action="store_true", help="Disable multiprocessing for grid processing (debug mode)")
parser.add_argument("--workers", type=int, default=12, help="Number of worker processes for grid processing (default: 12)")
parser.add_argument("--publish", action="store_true", help="Publish results to AGOL comparison folder (publish_v2.py)")
args = parser.parse_args()

logging.basicConfig(
    filename='run_history.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

scripts = ['mass_calc_v2.py', 'tunnel_vol.py']
if args.publish:
    scripts.append('publish_v2.py')
mode = "TEST MODE" if args.test else "FULL MODE"
worker_str = "sequential" if args.sequential else f"{args.workers} workers"
publish_str = " + AGOL publish" if args.publish else ""
logging.info(f"=== Pipeline started ({mode}, {worker_str}{publish_str}) ===")

for script in scripts:
    try:
        logging.info(f"Running {script}...")
        cmd = [sys.executable, script]
        if args.test and script != 'publish_v2.py':
            cmd.append("--test")
        # Pass parallelization flags only to mass_calc_v2.py
        if script == 'mass_calc_v2.py':
            if args.sequential:
                cmd.append("--sequential")
            cmd.extend(["--workers", str(args.workers)])
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            logging.info(f"{script} stdout: {result.stdout[-500:]}")
        logging.info(f"{script} finished successfully")

    except subprocess.CalledProcessError as e:
        logging.error(f"{script} FAILED. Aborted further script runs.")
        logging.error(f"Details: {e.stderr[-2000:] if e.stderr else 'No stderr'}")
        sys.exit(1)

logging.info(f"=== Pipeline completed ({mode}, {worker_str}{publish_str}) ===")
