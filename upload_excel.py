"""
upload_excel.py — Upload masseuttak Excel to SharePoint results folder.

Copies masseuttak_bb5.xlsx from the latest output folder to SharePoint as
masseuttak_bb5_mhkk.xlsx via Graph API.

Runs on standard Python 3.11 (not ArcGIS Python) since it needs msal.

Usage:
    python upload_excel.py                     # Upload from latest output
    python upload_excel.py --source path.xlsx  # Upload specific file
"""
import json
import os
import sys
import argparse

# Add server/ to path for graph_client import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "server"))
from graph_client import GraphClient

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server", "config.json")
UPLOAD_FOLDER = "60-WIP/20-Arbeidsområde disipliner/10-10 BIM/IKT-BIM/Automatisert mengdehøsting/Resultatfiler/Tverrfaglig masseuttak"
UPLOAD_FILENAME = "masseuttak_bb5_mhkk.xlsx"


def find_latest_excel():
    """Find masseuttak_bb5.xlsx in the latest output folder."""
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    if not os.path.exists(output_dir):
        return None
    folders = sorted(os.listdir(output_dir))
    if not folders:
        return None
    latest = os.path.join(output_dir, folders[-1], "masseuttak_bb5.xlsx")
    return latest if os.path.exists(latest) else None


def upload(source_path):
    """Upload Excel file to SharePoint."""
    if not os.path.exists(source_path):
        print(f"ERROR: File not found: {source_path}")
        return False

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    graph = GraphClient(
        config["azure"]["tenant_id"],
        config["azure"]["client_id"],
        config["azure"]["client_secret"],
    )

    sp = config["sharepoint"]
    site_id = graph.get_site_id(sp["site_hostname"], sp["site_path"])
    drive_id = graph.get_drive_id(site_id, sp.get("doc_library", "Documents"))

    upload_path = f"{UPLOAD_FOLDER}/{UPLOAD_FILENAME}"

    with open(source_path, "rb") as f:
        content = f.read()

    print(f"Uploading {os.path.basename(source_path)} -> {upload_path}")
    print(f"  Size: {len(content) / 1024:.1f} KB")

    try:
        result = graph.upload_file(
            drive_id,
            upload_path,
            content,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print(f"  Upload OK: {UPLOAD_FILENAME}")
        return True
    except Exception as e:
        print(f"  Upload FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload masseuttak Excel to SharePoint")
    parser.add_argument("--source", type=str, default=None, help="Path to Excel file (default: latest output)")
    args = parser.parse_args()

    source = args.source or find_latest_excel()
    if not source:
        print("ERROR: No masseuttak_bb5.xlsx found in output/")
        sys.exit(1)

    success = upload(source)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
