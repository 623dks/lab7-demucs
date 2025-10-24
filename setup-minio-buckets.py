from minio import Minio

# Connect to MinIO
client = Minio(
    "localhost:9000",
    access_key="rootuser",
    secret_key="rootpass123",
    secure=False
)

# Create buckets
buckets = ["queue", "output"]

for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created")
    else:
        print(f"Bucket '{bucket}' already exists")

print("MinIO setup complete!")