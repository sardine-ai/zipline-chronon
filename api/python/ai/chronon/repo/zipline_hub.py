from typing import Optional

import google.auth
import requests
from google.auth.transport.requests import Request


class ZiplineHub:
    def __init__(self, base_url):
        if not base_url:
            raise ValueError("Base URL for ZiplineHub cannot be empty.")
        self.base_url = base_url
        if self.base_url.startswith("https") and self.base_url.endswith(".app"):
            print("Using Google Cloud authentication for ZiplineHub.")
            credentials, project_id = google.auth.default()
            credentials.refresh(Request())
            self.id_token = credentials.id_token

    def call_diff_api(self, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v1/diff"

        diff_request = {
            'namesToHashes': names_to_hashes
        }
        headers = {'Content-Type': 'application/json'}
        if hasattr(self, 'id_token'):
            headers['Authorization'] = f'Bearer {self.id_token}'
        try:
            response = requests.post(url, json=diff_request, headers=headers)
            response.raise_for_status()
            diff_response = response.json()
            return diff_response['diff']
        except requests.RequestException as e:
            print(f"Error calling diff API: {e}")
            raise e

    def call_upload_api(self, diff_confs, branch: str):
        url = f"{self.base_url}/upload/v1/confs"

        upload_request = {
            'diffConfs': diff_confs,
            'branch': branch,
        }
        headers = {'Content-Type': 'application/json'}
        if hasattr(self, 'id_token'):
            headers['Authorization'] = f'Bearer {self.id_token}'

        try:
            response = requests.post(url, json=upload_request, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error calling upload API: {e}")
            raise e

    def call_workflow_start_api(self, conf_name, mode, branch, user, start, end, conf_hash):
        url = f"{self.base_url}/workflow/start"

        workflow_request = {
            'confName': conf_name,
            'confHash': conf_hash,
            'mode': mode,
            'branch': branch,
            'user': user,
            'start': start,
            'end': end,
        }
        headers = {'Content-Type': 'application/json'}
        if hasattr(self, 'id_token'):
            headers['Authorization'] = f'Bearer {self.id_token}'

        try:
            response = requests.post(url, json=workflow_request, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error calling workflow start API: {e}")
            raise e
