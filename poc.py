from minio import Minio
from io import BytesIO

def main():
    client = Minio(
        "localhost:9000",
        access_key="suispapp",
        secret_key="suispappsecret",
        secure=False
    )

    bucket = "suispbucket"
    key = "test/.keep"

    client.put_object(
        bucket,
        key,
        data=BytesIO(b""),
        length=0
    )

    print("Folder created!")

if __name__ == "__main__":
    main()