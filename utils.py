"""Shared utilities: file listing, name cleaning, logging setup."""

import glob
import logging
import os


def list_files_by_ext(path: str, ext: str) -> list[str]:
    """Return all files in *path* matching the given extension glob (e.g. '*.ifc')."""
    pattern = os.path.join(path, ext)
    return glob.glob(pattern)


def clean_file_name(filename: str) -> str:
    """Sanitise a filename for use as an identifier.

    Strips the .ifc extension, prefixes names starting with a digit or
    underscore, and replaces illegal characters with underscores.
    """
    name = filename.replace(".ifc", "")
    if name and name[0] in ("_", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
        name = "x_" + name
    illegals = set("-. ()[]:")
    return "".join(c if c not in illegals else "_" for c in name)


def setup_logging(log_path: str) -> None:
    """Configure root logger to write to *log_path* and to the console."""
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        encoding="utf-8",
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)
