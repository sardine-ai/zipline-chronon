import glob
import hashlib
import json
import os
import requests

from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
)

from ai.chronon.cli.compile.serializer import thrift_simple_json
from ai.chronon.orchestration.ttypes import DiffRequest, UploadRequest, Conf

HUB_BASE_URL = "http://localhost:9000"
DIFF_ENDPOINT = "/upload/v1/diff"
UPLOAD_ENDPOINT = "/upload/v1/confs"


def _get_diffed_entities(root_dir: str, branch: str):
    """
    Get entities that have different hashes between local and remote repositories.

    Args:
        root_dir: Root directory of the local repository
        branch: Branch to compare against

    Returns:
        Dictionary of entity names to (binary, hash) tuples for changed entities
    """
    local_repo_entities = _build_local_repo_hashmap(root_dir)
    # return local_repo_entities
    names_and_hashes = {
        name: hash_value for name, (_, hash_value) in local_repo_entities.items()
    }

    # Prepare DiffRequest to send to the Hub service
    diff_request = thrift_simple_json(DiffRequest(namesToHashes=names_and_hashes))

    # Make a GET request to the diff endpoint with the branch in query parameters
    # Using data parameter with json.dumps to handle the serialization correctly
    response = requests.get(
        f"{HUB_BASE_URL}{DIFF_ENDPOINT}",
        data=diff_request,
        headers={"Content-Type": "application/json"},
    )

    changed_entity_names = response.json().get("diff")
    print(f"Found {len(changed_entity_names)} entities to upload")
    # print(local_repo_entities)

    # Return only the entities that need to be uploaded
    return {k: local_repo_entities[k] for k in changed_entity_names}


def _build_local_repo_hashmap(root_dir: str):
    # Returns a map of name -> (tbinary, file_hash)
    results = {}

    # Iterate through each object type folder (staging_queries, group_bys, joins etc)
    for folder_name, obj_class in FOLDER_NAME_TO_CLASS.items():
        folder_path = os.path.join(root_dir, folder_name)
        if not os.path.exists(folder_path):
            continue

        # Find all json files recursively in this folder
        json_files = [
            f
            for f in glob.glob(os.path.join(folder_path, "**/*"), recursive=True)
            if os.path.isfile(f)
        ]

        exceptions = []

        for json_file in json_files:
            try:
                # Read the json file
                with open(json_file, "r") as f:
                    thrift_json = f.read()

                # Extract name from metadata in json
                json_obj = json.loads(thrift_json)
                name = json_obj["metaData"]["name"]

                # Load the json into the appropriate object type based on folder
                md5_hash = hashlib.md5(thrift_json.encode()).hexdigest()
                results[name] = (thrift_json, md5_hash)

            except Exception as e:
                exceptions.append(f"{json_file} - {e}")

        if exceptions:
            error_msg = (
                "The following files had exceptions during upload: \n"
                + "\n".join(exceptions)
                + "\n\n Consider deleting the files (safe operation) and checking "
                + "your thrift version before rerunning your command."
            )
            raise RuntimeError(error_msg)

    return results


def compute_and_upload_diffs(root_dir: str, branch: str):
    """
    Compute differences between local and remote repositories and upload changed entities.

    Args:
        root_dir: Root directory of the local repository
        branch: Branch to upload changes to

    Returns:
        Response message from the upload endpoint
    """
    # Get the entities that need to be uploaded
    diffed_entities = _get_diffed_entities(root_dir, branch)

    if not diffed_entities:
        print("No entities to upload")
        return "No entities to upload"

    # Log the entities that will be uploaded
    entity_keys_str = "\n".join(diffed_entities.keys())
    log_str = "\n\nUploading:\n{entity_keys}".format(entity_keys=entity_keys_str)
    print(log_str)

    # Prepare the upload request
    diff_confs = []

    # Add each entity as a Conf object
    for name, (thrift_json, hash_value) in diffed_entities.items():
        conf = Conf(name=name, hash=hash_value, contents=thrift_json)
        diff_confs.append(conf)

    upload_request = UploadRequest(branch=branch, diffConfs=diff_confs)

    # Make a POST request to the upload endpoint
    # Using data parameter with json.dumps to handle the serialization correctly
    response = requests.post(
        f"{HUB_BASE_URL}{UPLOAD_ENDPOINT}",
        data=thrift_simple_json(upload_request),
        headers={"Content-Type": "application/json"},
    )
    # Return the response from the upload endpoint
    try:
        return response.json().get("message", "Upload successful")
    except json.JSONDecodeError:
        # If the response is not valid JSON, return the response text or status code
        return f"Upload completed with status {response.status_code}: {response.text}"


"""
compute_and_upload_diffs(
    "/Users/varantzanoyan/repos/chronon/api/python/test/sample/production", "test"
)
"""
