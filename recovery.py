import os
import json
import hashlib
from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio
import shutil
import subprocess

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

config = load_config()
        
def get_date_from_backup_name(backup_name):
    date_string = backup_name.replace('/', '').replace(config["prefix"], '')
    date_format = "%Y-%m-%d_%H-%M-%S"
    return datetime.strptime(date_string, date_format)

def get_latest_backup(objects):
    latest_backup = None
    print("Available backups:")
    for obj in objects:
        print(f"- {obj.object_name}")
        parsed_date = get_date_from_backup_name(obj.object_name)
        if latest_backup is None or parsed_date > get_date_from_backup_name(latest_backup.object_name):
            latest_backup = obj
    return latest_backup

def get_metadata_file(latest_backup,client):
    metadata = None
    for obj in client.list_objects(config["bucket"], prefix=latest_backup.object_name, recursive=True):
        if (obj.object_name.endswith("metadata.json")):
            metadata_data = client.get_object(config["bucket"], obj.object_name)
            metadata = json.loads(metadata_data.read().decode('utf-8'))
            break
    return metadata

def check_hash(file_path, expected_hash, chunk_size=65536):
    sha = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(chunk_size):
            sha.update(chunk)
    return sha.hexdigest() == expected_hash

def recover_db(client, entry):
    client.fget_object(config["bucket"], entry['object_name'], config["db"]["db_temp_path"])
    print("Downloaded database dump to temporary path.")
    if not check_hash(Path(config["db"]["db_temp_path"]), entry['sha256']):
        print(f"Hash doesn't match for {entry['object_name']}, database dump may be corrupted.")
        return False
    db_config = config["db"]
    print("Restoring database from dump...")
    command = [
        "pg_restore",
        "-U", db_config["user"],
        "-h", db_config["host"],
        "-p", db_config["port"],
        "-Fc",
        "-d", db_config["dbname"],
        db_config["db_temp_path"]
    ]
    try:
        subprocess.run(command, check=True, env={"PGPASSWORD": db_config["password"]})
        print("Database has been successfully restored.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while restoring the database: {e}")
        return False

def recover_file(client, entry):
    temp_path = Path(f"{temp_path_prefix}/{entry['local_path']}")
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(config["bucket"], entry['object_name'], str(temp_path))
    if not check_hash(temp_path, entry['sha256']):
        print(f"Hash doesn't match for {entry['object_name']}, file may be corrupted.")
        return False
    local_path = Path(entry['local_path'])
    local_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(temp_path, local_path)
    return True

def retrieve_files(metadata, client):
    retrieved = 0
    entries = metadata.get("entries", [])
    total = len(entries)
    for entry in entries:
        print(f"Retrieving: {entry['object_name']}")
        if entry["local_path"] == config["db"]["db_temp_path"]:
            if recover_db(client, entry):
                retrieved += 1
        else:
            if recover_file(client, entry):
                retrieved += 1
    shutil.rmtree(temp_path_prefix)
    return (retrieved, total)


def run_recovery():
    config = load_config()
    bucket = config["bucket"]
    prefix = config["prefix"]

    objects = client.list_objects(bucket, prefix=prefix + "/", recursive=False)

    latest_backup = get_latest_backup(objects)
    if latest_backup is None:
        print("No backups found.")
        return

    print(f"Latest backup: {latest_backup.object_name}")

    metadata = get_metadata_file(latest_backup, client)
    if metadata is None:
        print("No metadata found in the latest backup.")
        return
            
    (retrieved, total) = retrieve_files(metadata, client)

    print(f"Recovery completed. Retrieved {retrieved}/{total} files.")
if __name__ == "__main__":
    run_recovery()