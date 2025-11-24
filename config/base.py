import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

class BaseConfig:
    SECRET_KEY = os.getenv("SECRET_KEY", "fallback-dev-secret")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "future": True
    }

    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        f"duckdb:///{BASE_DIR / 'instance' / 'app.duckdb'}"
    )

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")