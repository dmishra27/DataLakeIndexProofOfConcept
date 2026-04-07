"""Configuration shared by the non-stupid index package."""

from __future__ import annotations

import os
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).parents[1]
DELTA_WAREHOUSE_DIR = WORKSPACE_ROOT / "delta-lake" / "warehouse"


def _load_dotenv(dotenv_path: Path) -> None:
    """Load simple KEY=VALUE pairs from a workspace `.env` file."""

    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv(WORKSPACE_ROOT / ".env")

POSTGRES_USER = os.getenv("POSTGRES_USER", "datalake")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "datalake")
POSTGRES_DB = os.getenv("POSTGRES_DB", "datalake")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

DEFAULT_POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
