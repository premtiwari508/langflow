from pydantic import BaseModel


class StorageSettings(BaseModel):
    """File storage backend (local filesystem or object storage)."""

    storage_type: str = "local"
    """Storage type for file storage. Defaults to 'local'. Supports 'local' and 's3'."""
    object_storage_bucket_name: str | None = "langflow-bucket"
    """Object storage bucket name for file storage. Defaults to 'langflow-bucket'."""
    object_storage_prefix: str | None = "files"
    """Object storage prefix for file storage. Defaults to 'files'."""
    object_storage_tags: dict[str, str] | None = None
    """Object storage tags for file storage."""
    object_storage_max_pool_connections: int = 50
    """Connections the shared object storage client keeps open per event loop. Defaults to 50.

    The storage service holds one client per event loop, and a client owns one connection
    pool, so this caps how many object storage operations a loop can have in flight before
    they queue. botocore's own default is 10, which is low for a process serving concurrent
    uploads. Size it against what one worker is expected to run at once.
    """
