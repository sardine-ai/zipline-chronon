from typing import Optional

import requests


class ZiplineHub:
    def __init__(self, base_url):
        if not base_url:
            raise ValueError("Base URL for ZiplineHub cannot be empty.")
        self.base_url = base_url

    def call_diff_api(self, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v1/diff"

        diff_request = {
            'namesToHashes': names_to_hashes
        }
        try:
            response = requests.get(url, json=diff_request)
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
            'branch': branch
        }

        try:
            response = requests.post(url, json=upload_request)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error calling upload API: {e}")
            raise e

    def call_workflow_start_api(self, conf_name, mode, branch, user, start, end):
        url = f"{self.base_url}/workflow/start"

        workflow_request = {
            'confName': conf_name,
            'mode': mode,
            'branch': branch,
            'user': user,
            'start': start,
            'end': end
        }

        try:
            response = requests.post(url, json=workflow_request)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error calling workflow start API: {e}")
            raise e

