"""
runner_v3.py — Production pipeline runner with deep model filter and performance tiers.

Calls mass_calc_v3.py (deep model filter + parallelized grid processing) + tunnel_vol.py.
Optionally publishes to AGOL via configurable publisher (--publish + --publish-target).

Usage:
    python runner_v3.py                                  # Moderate mode (default)
    python runner_v3.py --aggressive                     # Aggressive parallelism
    python runner_v3.py --test                           # Test mode, moderate
    python runner_v3.py --sequential                     # No multiprocessing (debug)
    python runner_v3.py --publish                        # Publish to optimized folder (default)
    python runner_v3.py --publish --publish-target production  # Publish to production folder
    python runner_v3.py --no-filter                      # Skip deep model filter
"""
import subprocess
import sys
import logging
import argparse

PUBLISH_SCRIPTS = {
    "optimized": "publish_v2.py",
    "legacy": "publish_legacy.py",
    "fixed": "publish_fixed.py",
    "production": "publish.py",
}

parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Test mode: process only 10 IFC files")
parser.add_argument("--sequential", action="store_true", help="Disable multiprocessing (debug mode)")
parser.add_argument("--moderate", action="store_true", help="Moderate parallelism (default)")
parser.add_argument("--aggressive", action="store_true", help="Aggressive parallelism")
parser.add_argument("--publish", action="store_true", help="Publish results to AGOL")
parser.add_argument("--publish-target", type=str, default="optimized",
                    choices=list(PUBLISH_SCRIPTS.keys()),
                    help="AGOL publish target (default: optimized)")
parser.add_argument("--no-filter", action="store_true", help="Skip deep model filter")
parser.add_argument("--fresh", action="store_true", help="Clear IFC cache and reimport all files from source")
args = parser.parse_args()

logging.basicConfig(
    filename='run_history.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

scripts = ['mass_calc_v3.py', 'tunnel_vol.py']
if args.publish:
    publish_script = PUBLISH_SCRIPTS.get(args.publish_target, "publish_v2.py")
    scripts.append(publish_script)

if args.sequential:
    tier = "sequential"
elif args.aggressive:
    tier = "aggressive"
else:
    tier = "moderate"

mode = "TEST MODE" if args.test else "FULL MODE"
filter_str = " [no-filter]" if args.no_filter else " [deep-filter]"
fresh_str = " [fresh]" if args.fresh else ""
publish_str = f" + AGOL publish ({args.publish_target})" if args.publish else ""
logging.info(f"=== Pipeline started ({mode}, {tier}{filter_str}{fresh_str}{publish_str}) ===")

for script in scripts:
    try:
        logging.info(f"Running {script}...")
        cmd = [sys.executable, script]
        if args.test and script not in PUBLISH_SCRIPTS.values():
            cmd.append("--test")
        # Pass flags only to mass_calc_v3.py
        if script == 'mass_calc_v3.py':
            if args.sequential:
                cmd.append("--sequential")
            elif args.aggressive:
                cmd.append("--aggressive")
            else:
                cmd.append("--moderate")
            if args.no_filter:
                cmd.append("--no-filter")
            if args.fresh:
                cmd.append("--fresh")
        # Stream output so pipeline_server can capture it for SharePoint logs
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
        proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        logging.info(f"{script} finished successfully")

    except subprocess.CalledProcessError as e:
        logging.error(f"{script} FAILED. Aborted further script runs.")
        logging.error(f"Details: {e.stderr[-2000:] if e.stderr else 'No stderr'}")
        sys.exit(1)

# Upload Excel to SharePoint when publishing to production
if args.publish and args.publish_target == "production":
    try:
        logging.info("Uploading masseuttak Excel to SharePoint...")
        print("Uploading masseuttak_bb5_mhkk.xlsx to SharePoint...")
        python311 = r"C:\Program Files\Python311\python.exe"
        upload_cmd = [python311, "upload_excel.py"]
        proc = subprocess.Popen(upload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
        proc.wait()
        if proc.returncode != 0:
            logging.warning("Excel upload failed (non-critical)")
            print("WARNING: Excel upload to SharePoint failed (non-critical)")
        else:
            logging.info("Excel upload complete")
    except Exception as e:
        logging.warning(f"Excel upload error (non-critical): {e}")
        print(f"WARNING: Excel upload error: {e}")

logging.info(f"=== Pipeline completed ({mode}, {tier}{filter_str}{fresh_str}{publish_str}) ===")
