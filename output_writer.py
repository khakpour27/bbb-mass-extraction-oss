"""Excel and CSV output for volume results."""

import csv
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def write_volumes_csv(volumes: dict[str, float], output_path: str) -> None:
    """Write volume results to a semicolon-delimited CSV."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(list(volumes.keys()))
        writer.writerow(list(volumes.values()))
    logger.info("Wrote CSV: %s", output_path)


def write_volumes_excel(volumes: dict[str, float], output_path: str) -> None:
    """Write volume results to an Excel file with sheet 'Mengder'."""
    df = pd.DataFrame([volumes])
    df.to_excel(output_path, sheet_name="Mengder", index=False)
    logger.info("Wrote Excel: %s", output_path)


def append_tunnel_volumes(
    tunnel_volumes: dict[str, float],
    excel_path: str,
) -> None:
    """Add tunnel volume columns to an existing Excel file."""
    df = pd.read_excel(excel_path, sheet_name="Mengder")
    for key, value in tunnel_volumes.items():
        df[key] = [value]
    df.to_excel(excel_path, sheet_name="Mengder", index=False)
    logger.info("Appended tunnel volumes to %s", excel_path)
