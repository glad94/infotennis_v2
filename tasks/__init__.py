"""Tasks package for infotennis_v2 data pipeline."""
from .scrape_atp_calendar import scrape_atp_results_archive_task
from .s3_storage import upload_to_s3
from .motherduck_load import load_to_motherduck

__all__ = [
    "scrape_atp_results_archive_task",
    "upload_to_s3",
    "load_to_motherduck",
]
