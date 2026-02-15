"""
Centralized configuration module for infotennis_v2.

Loads environment variables from .env file and provides a Settings class
for accessing AWS S3 and MotherDuck credentials.
"""
from dotenv import load_dotenv
import os
from pathlib import Path

# Load .env file from project root
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)


class Settings:
    """Configuration settings loaded from environment variables."""
    
    # AWS S3 Configuration
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "infotennis-v2")
    
    # MotherDuck Configuration
    MOTHERDUCK_TOKEN: str = os.getenv("MOTHERDUCK_TOKEN", "")
    MOTHERDUCK_DATABASE: str = os.getenv("MOTHERDUCK_DATABASE", "infotennis_raw")
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that all required settings are present."""
        required = [
            ("AWS_ACCESS_KEY_ID", cls.AWS_ACCESS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY", cls.AWS_SECRET_ACCESS_KEY),
            ("S3_BUCKET", cls.S3_BUCKET),
            ("MOTHERDUCK_TOKEN", cls.MOTHERDUCK_TOKEN),
        ]
        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        return True
    
    @classmethod
    def get_s3_client_kwargs(cls) -> dict:
        """Get kwargs for boto3 S3 client initialization."""
        return {
            "aws_access_key_id": cls.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": cls.AWS_SECRET_ACCESS_KEY,
            "region_name": cls.AWS_REGION,
        }
    
    @classmethod
    def get_motherduck_connection_string(cls) -> str:
        """Get MotherDuck connection string."""
        return f"md:{cls.MOTHERDUCK_DATABASE}?motherduck_token={cls.MOTHERDUCK_TOKEN}"


# Singleton instance for easy access
settings = Settings()
