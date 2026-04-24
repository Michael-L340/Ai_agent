from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEGACY_ENV_FILE = PROJECT_ROOT.parent / ".env"
PROJECT_ENV_FILE = PROJECT_ROOT / ".env"

# Support both the root workspace .env and the subproject .env so the
# connection string works even if it was saved in the older location first.
load_dotenv(LEGACY_ENV_FILE, override=False)
load_dotenv(PROJECT_ENV_FILE, override=True)


class DatabaseConfigError(RuntimeError):
    """Raised when the Neon connection string is missing."""


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise DatabaseConfigError(
            "DATABASE_URL is missing. Copy your Neon connection string into "
            "F:\\AI_Agent\\postgresSQL\\.env or F:\\AI_Agent\\.env."
        )
    return database_url


@contextmanager
def open_connection() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(get_database_url())
    conn.row_factory = dict_row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
