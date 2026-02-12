import subprocess
import sys
import logging


logging.basicConfig(
    filename='run_history.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

scripts = ['mass_calc.py', 'tunnel_vol.py', 'publish.py']

for script in scripts:
    try:
        logging.info(f"Running {script}...")
        subprocess.run([sys.executable, script], check=True, capture_output=True, text=True)
        logging.info(f"{script} finished successfully")

    except subprocess.CalledProcessError as e:
        logging.error("Something went wrong. Aborted futher script runs.") 
        logging.error(f"Details: {e.stderr}")
        sys.exit()
