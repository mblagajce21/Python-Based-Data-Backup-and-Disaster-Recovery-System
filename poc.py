import os
import json
import hashlib
from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="suispapp",
    secret_key="suispappsecret",
    secure=False
)

def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)

def sha256_file(path: Path, chunk_size=65536):
    sha = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            sha.update(chunk)
    return sha.hexdigest()

def upload_file(path: Path, bucket: str, object_name: str):
    file_hash = sha256_file(path)
    with path.open("rb") as f:
        size = path.stat().st_size
        client.put_object(bucket, object_name, data=f, length=size)
    print(f"Uploaded: {path} -> {object_name}")
    return {
        "local_path": str(path),
        "object_name": object_name,
        "sha256": file_hash
    }

def upload_folder(folder: Path, bucket: str, dest_prefix: str):
    result = []
    for root, _, files in os.walk(folder):
        for fname in files:
            file_path = Path(root) / fname
            rel = file_path.relative_to(folder).as_posix()
            object_name = f"{dest_prefix}/{rel}"
            info = upload_file(file_path, bucket, object_name)
            result.append(info)
    return result

def run_backup():
    config = load_config()
    sources = config["backup_sources"]
    bucket = config["bucket"]
    prefix = config["prefix"]

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    dest_prefix = f"{prefix}/{timestamp}"

    metadata = {
        "timestamp": timestamp,
        "bucket": bucket,
        "entries": []
    }

    for src in sources:
        folder = Path(src)
        if not folder.exists():
            print(f"Preskacem, ne postoji: {folder}")
            continue
        entries = upload_folder(folder, bucket, f"{dest_prefix}/{folder.name}")
        metadata["entries"].extend(entries)


    metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
    client.put_object(
        bucket,
        f"{dest_prefix}/metadata.json",
        data=BytesIO(metadata_bytes),
        length=len(metadata_bytes)
    )

    print(f"Metadata uploaded -> {dest_prefix}/metadata.json")

if __name__ == "__main__":
    run_backup()