"""Shared utilities: file listing, name cleaning, logging setup, memory monitoring."""

import glob
import logging
import os
import platform


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


# ── Memory monitoring ─────────────────────────────────────────────────────────

def get_available_memory() -> float:
    """Return available system RAM in GB."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        return mem.available / (1024 ** 3)
    except ImportError:
        pass

    # Fallback for Windows without psutil
    if platform.system() == "Windows":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]
            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(stat)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return stat.ullAvailPhys / (1024 ** 3)
        except Exception:
            pass

    return 32.0  # conservative default


def estimate_raster_memory(rows: int, cols: int, dtype_bytes: int = 4) -> float:
    """Estimate memory needed for a raster array in GB."""
    return (rows * cols * dtype_bytes) / (1024 ** 3)


def log_memory_usage(logger: logging.Logger, label: str = "") -> None:
    """Log current memory usage for monitoring."""
    avail = get_available_memory()
    prefix = f"[{label}] " if label else ""
    logger.info("%sAvailable memory: %.1f GB", prefix, avail)
