"""Upload trigger.json template to SharePoint templates/ folder."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "server"))
from graph_client import GraphClient

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server", "config.json")

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

template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server", "templates", "trigger.json")
with open(template_path, "r", encoding="utf-8") as f:
    content = f.read()

upload_path = f"{sp['folder_path']}/templates/trigger.json"
print(f"Uploading trigger.json template to: {upload_path}")
graph.upload_file(drive_id, upload_path, content.encode("utf-8"), "application/json")
print("Done.")
