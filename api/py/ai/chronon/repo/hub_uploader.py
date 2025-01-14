import glob
import json
import os

from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
)
from ai.chronon.repo.serializer import json2binary


def _get_diffed_entities(root_dir: str, branch: str):
    local_repo_entities = _build_local_repo_hashmap(root_dir)
    names_and_hashes = {name: hash for name, (_, hash) in local_repo_entities.items()}
    changed_entity_names = local_repo_entities  # TODO: Call Zipline hub with `names_and_hashes` as the argument to get back a list of names for diffed hashes on branch
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
                binary = json2binary(thrift_json, obj_class)

                # Store the json content and a hash of it
                results[name] = (binary, hash(thrift_json))

            except Exception as e:
                exceptions.append(f"{json_file} - {e}")

        if exceptions:
            error_msg = (
                f"The following files had exceptions during upload: \n"
                + "\n".join(exceptions)
                + "\n\n Consider deleting the files (safe operation) and checking your thrift version before rerunning your command."
            )
            raise RuntimeError(error_msg)

    return results


def compute_and_upload_diffs(root_dir: str, branch: str):
    diffed_entities = _get_diffed_entities(root_dir, branch)
    print(f"\n\nUploading:\n {"\n".join(diffed_entities.keys())}")
    # TODO make PUT request to ZiplineHub
    return
