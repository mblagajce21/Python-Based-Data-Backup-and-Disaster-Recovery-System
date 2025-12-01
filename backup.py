import os
import json
import hashlib
from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio
import subprocess
import shutil
import time
from email_notifier import send_email


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

def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def get_previous_backup_metadata(bucket: str, source_name: str):
    try:
        objects = client.list_objects(bucket, prefix=f"{source_name}/", recursive=False)
        timestamps = []
        for obj in objects:
            parts = obj.object_name.rstrip('/').split('/')
            if len(parts) >= 2:
                timestamp = parts[1]
                if timestamp and timestamp not in timestamps:
                    timestamps.append(timestamp)
        
        if not timestamps:
            return None
        
        timestamps.sort(reverse=True)
        latest_timestamp = timestamps[0]
        
        metadata_obj = client.get_object(bucket, f"{source_name}/{latest_timestamp}/metadata.json")
        metadata = json.loads(metadata_obj.read().decode('utf-8'))
        return metadata
    except Exception as e:
        print(f"No previous backup found for {source_name}: {e}")
        return None

def upload_file(path: Path, bucket: str, object_name: str):
    file_hash = sha256_file(path)
    file_size = path.stat().st_size
    with path.open("rb") as f:
        client.put_object(bucket, object_name, data=f, length=file_size)
    print(f"Uploaded: {path} -> {object_name}")
    return {
        "local_path": str(path),
        "object_name": object_name,
        "sha256": file_hash,
        "size": file_size
    }

def upload_file_incremental(path: Path, bucket: str, object_name: str, previous_metadata: dict):
    file_hash = sha256_file(path)
    file_size = path.stat().st_size
    
    if previous_metadata:
        for entry in previous_metadata.get("entries", []):
            if entry.get("local_path") == str(path) and entry.get("sha256") == file_hash:
                print(f"Skipped (unchanged): {path}")
                return {
                    "local_path": str(path),
                    "object_name": entry.get("object_name"),
                    "sha256": file_hash,
                    "size": file_size,
                    "skipped": True,
                    "reason": "unchanged"
                }
    
    with path.open("rb") as f:
        client.put_object(bucket, object_name, data=f, length=file_size)
    print(f"Uploaded (new/changed): {path} -> {object_name}")
    return {
        "local_path": str(path),
        "object_name": object_name,
        "sha256": file_hash,
        "size": file_size,
        "skipped": False
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

def upload_folder_incremental(folder: Path, bucket: str, dest_prefix: str, previous_metadata: dict):
    result = []
    for root, _, files in os.walk(folder):
        for fname in files:
            file_path = Path(root) / fname
            rel = file_path.relative_to(folder).as_posix()
            object_name = f"{dest_prefix}/{rel}"
            info = upload_file_incremental(file_path, bucket, object_name, previous_metadata)
            result.append(info)
    return result

def backup_database(db_config, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    command = [
        "pg_dump",
        "-C",
        "-U", db_config["user"], 
        "-h", db_config["host"], 
        "-p", db_config["port"], 
        "-d", db_config["dbname"], 
        "-f", path,
        "-Fc"
    ]
    try:
        env = os.environ.copy()
        env["PGPASSWORD"] = db_config["password"]
        subprocess.run(command, check=True, env=env)
        print(f"Dump file {path} has been successfully created.")
        return path
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while creating the dump: {e}")
        return None

def run_backup():
    """
    Run backup for all configured sources and return a detailed report
    Returns: dict with backup report including status, timing, and metrics
    """
    start_time = time.time()
    start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    config = load_config()
    sources = config["backup_sources"]
    bucket = config["bucket"]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Initialize report
    backup_report = {
        "overall_status": "success",
        "start_time": start_time_str,
        "end_time": "",
        "total_duration": 0,
        "sources": []
    }
    
    overall_success = True

    for source in sources:
        source_start_time = time.time()
        source_name = source["name"]
        source_type = source["type"]
        dest_prefix = f"{source_name}/{timestamp}"
        
        # Initialize source report
        source_report = {
            "name": source_name,
            "type": source_type,
            "status": "success",
            "duration": 0,
            "files_count": 0,
            "total_size": 0,
            "error": ""
        }

        metadata = {
            "timestamp": timestamp,
            "bucket": bucket,
            "source_name": source_name,
            "source_type": source_type,
            "entries": []
        }

        print(f"\n=== Backing up source: {source_name} ({source_type}) ===")

        previous_metadata = get_previous_backup_metadata(bucket, source_name)
        
        try:
            if source_type == "device":
                items = source.get("items", [])
                for item in items:
                    if item["type"] == "folder":
                        folder = Path(item["path"])
                        if not folder.exists():
                            print(f"Preskacem, ne postoji: {folder}")
                            continue
                        entries = upload_folder_incremental(folder, bucket, f"{dest_prefix}/{folder.name}", previous_metadata)
                        metadata["entries"].extend(entries)
                    elif item["type"] == "file":
                        file_path = Path(item["path"])
                        if not file_path.exists():
                            print(f"Preskacem, ne postoji: {file_path}")
                            continue
                        object_name = f"{dest_prefix}/{file_path.name}"
                        info = upload_file_incremental(file_path, bucket, object_name, previous_metadata)
                        metadata["entries"].append(info)

            elif source_type == "database":
                db_config = source["db_config"]
                backup_path = backup_database(db_config, db_config["db_temp_path"])
                if backup_path:
                    backup_path = Path(backup_path)
                    object_name = f"{dest_prefix}/{backup_path.name}"
                    info = upload_file(backup_path, bucket, object_name)
                    info["db_name"] = db_config["dbname"]
                    metadata["entries"].append(info)
                    os.remove(db_config["db_temp_path"])
                else:
                    raise Exception("Database backup failed")

            source_report["files_count"] = len(metadata["entries"])
            source_report["total_size"] = sum(entry.get("size", 0) for entry in metadata["entries"])
            source_report["files_uploaded"] = sum(1 for entry in metadata["entries"] if not entry.get("skipped", False))
            source_report["files_skipped"] = sum(1 for entry in metadata["entries"] if entry.get("skipped", False))
            
            metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
            client.put_object(
                bucket,
                f"{dest_prefix}/metadata.json",
                data=BytesIO(metadata_bytes),
                length=len(metadata_bytes)
            )

            print(f"Metadata uploaded -> {dest_prefix}/metadata.json")
            print(f"=== Completed backup for: {source_name} ===\n")
            
        except Exception as e:
            print(f"!!! Error backing up {source_name}: {e}")
            source_report["status"] = "failed"
            source_report["error"] = str(e)
            overall_success = False
        
        # Record duration
        source_report["duration"] = time.time() - source_start_time
        backup_report["sources"].append(source_report)
    
    # Finalize report
    end_time = time.time()
    backup_report["end_time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    backup_report["total_duration"] = end_time - start_time
    
    # Determine overall status
    failed_count = sum(1 for s in backup_report["sources"] if s["status"] == "failed")
    if failed_count == 0:
        backup_report["overall_status"] = "success"
    elif failed_count == len(backup_report["sources"]):
        backup_report["overall_status"] = "failed"
    else:
        backup_report["overall_status"] = "partial"
    
    return backup_report

if __name__ == "__main__":
    config = load_config()
    report = run_backup()
    
    print("\n" + "="*60)
    print("BACKUP SUMMARY")
    print("="*60)
    print(f"Overall Status: {report['overall_status'].upper()}")
    print(f"Duration: {report['total_duration']:.2f}s")
    print(f"Sources: {len(report['sources'])}")
    for source in report['sources']:
        status_icon = "✓" if source['status'] == 'success' else "✗"
        uploaded = source.get('files_uploaded', source['files_count'])
        skipped = source.get('files_skipped', 0)
        print(f"  {status_icon} {source['name']}: {source['files_count']} files ({uploaded} uploaded, {skipped} skipped), {format_size(source['total_size'])}")
    print("="*60)
    
    send_email(config, report)
