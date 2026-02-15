"""Configuration package for infotennis_v2."""
from .constants import (
    ATP_ROOT_URL,
    ATP_RESULTS_ARCHIVE_PATH,
    S3_BUCKET,
    S3_RAW_PREFIX,
    MOTHERDUCK_DATABASE,
    LOADED_FILES_TABLE,
    ENDPOINTS,
)

__all__ = [
    "ATP_ROOT_URL",
    "ATP_RESULTS_ARCHIVE_PATH",
    "S3_BUCKET",
    "S3_RAW_PREFIX",
    "MOTHERDUCK_DATABASE",
    "LOADED_FILES_TABLE",
    "ENDPOINTS",
]
