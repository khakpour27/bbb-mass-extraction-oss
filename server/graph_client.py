"""
graph_client.py — Microsoft Graph API client for SharePoint file operations.

Handles auth (MSAL client credentials) and file CRUD against a SharePoint document library.
"""
import json
import logging
import time
import msal
import requests

log = logging.getLogger(__name__)


class GraphClient:
    """Thin wrapper around Graph API for SharePoint drive operations."""

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(self, tenant_id, client_id, client_secret):
        self._app = msal.ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )
        self._token_cache = None
        self._token_expires = 0

    # ── auth ────────────────────────────────────────────────
    def _get_token(self):
        now = time.time()
        if self._token_cache and now < self._token_expires - 60:
            return self._token_cache
        result = self._app.acquire_token_for_client(scopes=self.SCOPES)
        if "access_token" not in result:
            raise RuntimeError(f"Auth failed: {result.get('error_description', result)}")
        self._token_cache = result["access_token"]
        self._token_expires = now + result.get("expires_in", 3600)
        return self._token_cache

    def _headers(self):
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ── helpers ─────────────────────────────────────────────
    def _request(self, method, url, **kwargs):
        resp = requests.request(method, url, headers=self._headers(), **kwargs)
        if resp.status_code >= 400:
            log.error("Graph API %s %s -> %d: %s", method, url, resp.status_code, resp.text[:500])
        return resp

    # ── site / drive discovery ──────────────────────────────
    def get_site_id(self, hostname, site_path):
        """Get SharePoint site ID from hostname and site path."""
        url = f"{self.GRAPH_BASE}/sites/{hostname}:{site_path}"
        resp = self._request("GET", url)
        resp.raise_for_status()
        return resp.json()["id"]

    def get_drive_id(self, site_id, drive_name="Documents"):
        """Get the default document library drive ID for a site."""
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drives"
        resp = self._request("GET", url)
        resp.raise_for_status()
        for drive in resp.json().get("value", []):
            if drive["name"] == drive_name or drive_name is None:
                return drive["id"]
        # Fallback: return first drive
        drives = resp.json().get("value", [])
        if drives:
            return drives[0]["id"]
        raise RuntimeError(f"No drives found for site {site_id}")

    # ── file operations (all path-based) ────────────────────
    def list_folder(self, drive_id, folder_path):
        """List children of a folder. Returns list of item dicts."""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{folder_path}:/children"
        resp = self._request("GET", url)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return resp.json().get("value", [])

    def upload_file(self, drive_id, file_path, content, content_type="application/octet-stream"):
        """Upload or overwrite a file (up to 4MB). file_path is relative to drive root."""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{file_path}:/content"
        headers = self._headers()
        headers["Content-Type"] = content_type
        resp = requests.put(url, headers=headers, data=content)
        if resp.status_code >= 400:
            log.error("Graph API PUT %s -> %d: %s", url, resp.status_code, resp.text[:500])
        resp.raise_for_status()
        return resp.json()

    def upload_json(self, drive_id, file_path, data):
        """Upload a dict as a JSON file."""
        content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        return self.upload_file(drive_id, file_path, content.encode("utf-8"), "application/json")

    def upload_text(self, drive_id, file_path, text):
        """Upload a string as a text file."""
        return self.upload_file(drive_id, file_path, text.encode("utf-8"), "text/plain")

    def download_file(self, drive_id, file_path, quiet_404=False):
        """Download file content as bytes. Returns None if not found."""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{file_path}:/content"
        resp = requests.get(url, headers=self._headers())
        if resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            log.error("Graph API GET %s -> %d: %s", url, resp.status_code, resp.text[:500])
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.content

    def delete_item(self, drive_id, file_path):
        """Delete a file or folder."""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{file_path}"
        resp = self._request("DELETE", url)
        if resp.status_code == 404:
            return  # already gone
        resp.raise_for_status()

    def create_folder(self, drive_id, parent_path, folder_name):
        """Create a subfolder. Returns item dict."""
        if parent_path:
            url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{parent_path}:/children"
        else:
            url = f"{self.GRAPH_BASE}/drives/{drive_id}/root/children"
        body = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "fail",
        }
        resp = self._request("POST", url, json=body)
        resp.raise_for_status()
        return resp.json()

    def item_exists(self, drive_id, path):
        """Check if a file/folder exists."""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{path}"
        resp = self._request("GET", url)
        return resp.status_code == 200
