import os
import json
import hashlib
from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio
import shutil

client = Minio(
    "localhost:9000",
    access_key="suispapp",
    secret_key="suispappsecret",
    secure=False
)

temp_path_prefix = "/tmp/recovery"

def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)

        
def get_date_from_backup_name(backup_name, prefix):
    date_string = backup_name.replace('/', '').replace(prefix, '')
    date_format = "%Y-%m-%d_%H-%M-%S"
    return datetime.strptime(date_string, date_format)

def get_latest_backup(objects, prefix):
    latest_backup = None
    print("Available backups:")
    for obj in objects:
        print(f"- {obj.object_name}")
        parsed_date = get_date_from_backup_name(obj.object_name, prefix)
        if latest_backup is None or parsed_date > get_date_from_backup_name(latest_backup.object_name, prefix):
            latest_backup = obj
    return latest_backup

def get_metadata_file(latest_backup,client,bucket):
    metadata = None
    for obj in client.list_objects(bucket, prefix=latest_backup.object_name, recursive=True):
        if (obj.object_name.endswith("metadata.json")):
            metadata_data = client.get_object(bucket, obj.object_name)
            metadata = json.loads(metadata_data.read().decode('utf-8'))
            break
    return metadata

def check_hash(file_path, expected_hash, chunk_size=65536):
    sha = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(chunk_size):
            sha.update(chunk)
    return sha.hexdigest() == expected_hash

def retrieve_files(metadata, client, bucket):
    retrieved = 0
    entries = metadata.get("entries", [])
    total = len(entries)
    for entry in entries:
        print(f"Retrieving: {entry['object_name']}")
        temp_path = Path(f"{temp_path_prefix}/{entry['local_path']}")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object(bucket, entry['object_name'], str(temp_path))
        if not check_hash(temp_path, entry['sha256']):
            print(f"Hash doesn't match for {entry['object_name']}, file may be corrupted.")
            continue
        local_path = Path(entry['local_path'])
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(temp_path, local_path)
        retrieved += 1
    shutil.rmtree(temp_path_prefix)
    return (retrieved, total)


def run_recovery():
    config = load_config()
    bucket = config["bucket"]
    prefix = config["prefix"]

    objects = client.list_objects(bucket, prefix=prefix + "/", recursive=False)

    latest_backup = get_latest_backup(objects, prefix)
    if latest_backup is None:
        print("No backups found.")
        return

    print(f"Latest backup: {latest_backup.object_name}")

    metadata = get_metadata_file(latest_backup, client, bucket)
    if metadata is None:
        print("No metadata found in the latest backup.")
        return
            
    (retrieved, total) = retrieve_files(metadata, client, bucket)

    print(f"Recovery completed. Retrieved {retrieved}/{total} files.")
if __name__ == "__main__":
    run_recovery()