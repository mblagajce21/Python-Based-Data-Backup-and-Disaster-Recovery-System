from minio import Minio
from io import BytesIO
import os
from pathlib import Path


client = Minio(
    "localhost:9000",
    access_key="suispapp",
    secret_key="suispappsecret",
    secure=False
)



def backup_to_bucket(local_path: str, bucket: str, dest_folder: str = "backup"):

    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Path ne postoji: {local_path}")

    if local_path.is_dir():
        for root, _, files in os.walk(local_path):
            for fname in files:
                fpath = Path(root) / fname

                rel_path = fpath.relative_to(local_path)
                object_name = f"{dest_folder}/{rel_path}".replace("\\", "/")

                _upload_file(fpath, bucket, object_name)

    else:
        object_name = f"{dest_folder}/{local_path.name}"
        _upload_file(local_path, bucket, object_name)


def _upload_file(path: Path, bucket: str, object_name: str):
    with path.open("rb") as f:
        size = path.stat().st_size
        client.put_object(bucket, object_name, data=f, length=size)

    print(f"Uploaded: {path} → {bucket}/{object_name}")

if __name__ == "__main__":
    backup_to_bucket(
        local_path="./testni_folder",
        bucket="suispbucket",
        dest_folder="laptop/documents/testni_folder"
    )