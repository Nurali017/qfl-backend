"""MinIO S3-compatible object storage client."""

from minio import Minio
from minio.error import S3Error
from app.config import get_settings

settings = get_settings()

# MinIO client instance
minio_client: Minio | None = None


def get_minio_client() -> Minio:
    """Get or create MinIO client."""
    global minio_client

    if minio_client is None:
        minio_client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

    return minio_client


async def init_minio():
    """Initialize MinIO bucket."""
    client = get_minio_client()
    bucket = settings.minio_bucket

    try:
        # Create bucket if it doesn't exist
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        # Always update bucket policy to ensure latest permissions
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject"],
                    "Resource": [
                        f"arn:aws:s3:::{bucket}/public/*",
                        f"arn:aws:s3:::{bucket}/news_image/*",
                        f"arn:aws:s3:::{bucket}/news/*",
                        f"arn:aws:s3:::{bucket}/leadership/*",
                        f"arn:aws:s3:::{bucket}/coach_photos/*",
                        f"arn:aws:s3:::{bucket}/player_photos/*",
                        f"arn:aws:s3:::{bucket}/document/*",
                        f"arn:aws:s3:::{bucket}/news_content/*",
                        f"arn:aws:s3:::{bucket}/uploads/*",
                        f"arn:aws:s3:::{bucket}/broadcaster_logos/*",
                    ],
                }
            ],
        }
        import json
        try:
            client.set_bucket_policy(bucket, json.dumps(policy))
        except S3Error:
            pass  # Policy may fail over tunnel; bucket already configured in prod

    except S3Error as e:
        raise RuntimeError(f"Failed to initialize MinIO: {e}")

    return client


def get_public_url(object_name: str) -> str:
    """Get public URL for an object (for browser access)."""
    endpoint = settings.minio_public_endpoint
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return f"{endpoint}/{settings.minio_bucket}/{object_name}"
    return f"https://{endpoint}/{settings.minio_bucket}/{object_name}"
