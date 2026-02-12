import subprocess
import sys
import logging


logging.basicConfig(
    filename='run_history.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

scripts = ['mass_calc_prod_pc.py', 'tunnel_vol.py', 'publish_agol.py']

for script in scripts:
    try:
        logging.info(f"Running {script}...")
        subprocess.run([sys.executable, script], check=True)
        logging.info(f"{script} finished successfully")

    except Exception as e:
        logging.error("Something went wrong. Aborted futher script runs.") 
        logging.error(f"Details: {e}")
        sys.exit()
