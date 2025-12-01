import os
import json
import hashlib
from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio
import shutil
import subprocess
import sys
from encryption import EncryptionManager

client = Minio(
    "localhost:9000",
    access_key="suispapp",
    secret_key="suispappsecret",
    secure=False
)

temp_path_prefix = "/tmp/recovery"
encryption_manager = None

def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)

config = load_config()

encryption_config = config.get("encryption", {})
if encryption_config.get("enabled", False):
    key_file = encryption_config.get("key_file", "./encryption.key")
    try:
        encryption_manager = EncryptionManager(key_file=key_file)
        print(f"Encryption enabled for recovery using key from: {key_file}")
    except Exception as e:
        print(f"Warning: Failed to initialize encryption: {e}")
        
def get_date_from_backup_name(backup_name, prefix):
    date_string = backup_name.replace('/', '').replace(prefix, '')
    date_format = "%Y-%m-%d_%H-%M-%S"
    return datetime.strptime(date_string, date_format)

def get_available_sources(bucket):
    sources = set()
    try:
        objects = client.list_objects(bucket, recursive=False)
        for obj in objects:
            source_name = obj.object_name.rstrip('/')
            if source_name:
                sources.add(source_name)
    except Exception as e:
        print(f"Error listing sources: {e}")
    return sorted(list(sources))

def get_latest_backup(objects, prefix):
    latest_backup = None
    print(f"\nAvailable backups for '{prefix}':")
    backup_list = []
    for obj in objects:
        print(f"  - {obj.object_name}")
        backup_list.append(obj)
        try:
            parsed_date = get_date_from_backup_name(obj.object_name, prefix)
            if latest_backup is None or parsed_date > get_date_from_backup_name(latest_backup.object_name, prefix):
                latest_backup = obj
        except Exception as e:
            print(f"  Warning: Could not parse date from {obj.object_name}: {e}")
            continue
    return latest_backup

def get_metadata_file(latest_backup, client, bucket):
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

def recover_db(client, entry, bucket):
    db_name = entry.get('db_name', 'unknown')

    db_config = None
    for source in config.get("backup_sources", []):
        if source.get("type") == "database" and source.get("db_config", {}).get("dbname") == db_name:
            db_config = source["db_config"]
            break
    
    if not db_config:
        print(f"Error: No database configuration found for '{db_name}'")
        return False
    
    db_temp_path = db_config["db_temp_path"]
    os.makedirs(os.path.dirname(db_temp_path), exist_ok=True)
    
    object_name = entry['object_name']
    is_encrypted = entry.get('encrypted', False)
    
    if is_encrypted:
        encrypted_temp = db_temp_path + '.enc'
        client.fget_object(bucket, object_name, encrypted_temp)
        
        if encryption_manager:
            try:
                encryption_manager.decrypt_file(encrypted_temp, db_temp_path)
                os.remove(encrypted_temp)
                print(f"  Downloaded and decrypted database dump.")
            except Exception as e:
                print(f"  Error decrypting database dump: {e}")
                return False
        else:
            print(f"  Error: Database dump is encrypted but no encryption key available")
            return False
    else:
        client.fget_object(bucket, object_name, db_temp_path)
        print(f"  Downloaded database dump to temporary path.")
    
    if not check_hash(Path(db_temp_path), entry['sha256']):
        print(f"  Hash doesn't match for {entry['object_name']}, database dump may be corrupted.")
        return False
    
    print(f"  Restoring database '{db_name}' from dump...")
    
    command = [
        "pg_restore",
        "-U", db_config["user"],
        "-h", db_config["host"],
        "-p", db_config["port"],
        "-Fc",
        "--clean",
        "--if-exists",
        "-d", db_config["dbname"],
        db_temp_path
    ]
    
    try:
        env = os.environ.copy()
        env["PGPASSWORD"] = db_config["password"]
        result = subprocess.run(command, check=True, env=env, capture_output=True, text=True)
        print(f"  Database '{db_name}' has been successfully restored.")
        if os.path.exists(db_temp_path):
            os.remove(db_temp_path)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error occurred while restoring the database: {e}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
        return False
    except FileNotFoundError:
        print("  Error: pg_restore command not found. Please install PostgreSQL client tools.")
        print("  On macOS, you can install it with: brew install postgresql")
        return False

def recover_file(client, entry, bucket):
    global encryption_manager
    
    temp_path = Path(f"{temp_path_prefix}/{entry['local_path']}")
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    
    object_name = entry['object_name']
    is_encrypted = entry.get('encrypted', False)
    
    if is_encrypted:
        encrypted_temp = temp_path.with_suffix(temp_path.suffix + '.enc')
        client.fget_object(bucket, object_name, str(encrypted_temp))
        
        if encryption_manager:
            try:
                encryption_manager.decrypt_file(encrypted_temp, temp_path)
                encrypted_temp.unlink()
                print(f"  Decrypted: {object_name}")
            except Exception as e:
                print(f"  Error decrypting {object_name}: {e}")
                return False
        else:
            print(f"  Error: File is encrypted but no encryption key available")
            return False
    else:
        client.fget_object(bucket, object_name, str(temp_path))
    
    if not check_hash(temp_path, entry['sha256']):
        print(f"  Hash doesn't match for {entry['object_name']}, file may be corrupted.")
        return False
    local_path = Path(entry['local_path'])
    local_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(temp_path, local_path)
    return True

def retrieve_files(metadata, client, bucket):
    retrieved = 0
    entries = metadata.get("entries", [])
    total = len(entries)
    source_type = metadata.get("source_type", "unknown")
    
    for entry in entries:
        print(f"  Retrieving: {entry['object_name']}")
        if source_type == "database" or entry['object_name'].endswith('db_backup.dump'):
            if recover_db(client, entry, bucket):
                retrieved += 1
        else:
            if recover_file(client, entry, bucket):
                retrieved += 1
    
    if os.path.exists(temp_path_prefix):
        shutil.rmtree(temp_path_prefix)
    return (retrieved, total)


def run_recovery(source_filter=None):
    config = load_config()
    bucket = config["bucket"]

    available_sources = get_available_sources(bucket)
    
    if not available_sources:
        print("No backup sources found in the bucket.")
        return

    print("\n=== Available Backup Sources ===")
    for i, source in enumerate(available_sources, 1):
        print(f"{i}. {source}")
    
    if source_filter:
        if source_filter not in available_sources:
            print(f"\nError: Source '{source_filter}' not found.")
            print(f"Available sources: {', '.join(available_sources)}")
            return
        selected_sources = [source_filter]
    else:
        print("\nOptions:")
        print("  - Enter source name (e.g., 'Laptop' or 'DB_mockdb')")
        print("  - Enter 'all' to recover all sources")
        print("  - Press Enter to recover all sources")
        
        choice = input("\nYour choice: ").strip()
        
        if choice.lower() == 'all' or choice == '':
            selected_sources = available_sources
        elif choice in available_sources:
            selected_sources = [choice]
        else:
            print(f"Invalid choice. Available sources: {', '.join(available_sources)}")
            return

    for source_name in selected_sources:
        print(f"\n{'='*60}")
        print(f"=== Recovering source: {source_name} ===")
        print(f"{'='*60}")
        
        objects = client.list_objects(bucket, prefix=source_name + "/", recursive=False)

        latest_backup = get_latest_backup(objects, source_name)
        if latest_backup is None:
            print(f"No backups found for source '{source_name}'.")
            continue

        print(f"\nLatest backup: {latest_backup.object_name}")

        metadata = get_metadata_file(latest_backup, client, bucket)
        if metadata is None:
            print("No metadata found in the latest backup.")
            continue
        
        print(f"Source type: {metadata.get('source_type', 'unknown')}")
        print(f"Timestamp: {metadata.get('timestamp', 'unknown')}")
        print(f"Total files: {len(metadata.get('entries', []))}")
                
        (retrieved, total) = retrieve_files(metadata, client, bucket)

        print(f"\n=== Recovery completed for '{source_name}': Retrieved {retrieved}/{total} files ===")

if __name__ == "__main__":
    source_filter = sys.argv[1] if len(sys.argv) > 1 else None
    run_recovery(source_filter)