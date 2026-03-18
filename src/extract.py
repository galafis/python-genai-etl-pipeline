"""
Extract Module - Data extraction from multiple sources.

Supports CSV, JSON, API and database extraction.

Author: Gabriel Demetrios Lafis
License: MIT
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from multiple sources into DataFrames.

    Supports CSV files, JSON files, REST APIs and
    SQLite databases.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        logger.info("DataExtractor initialized.")

    def from_csv(
        self,
        path: Union[str, Path],
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Extract data from a CSV file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        df = pd.read_csv(path, encoding=encoding, **kwargs)
        logger.info("Extracted %d rows from %s", len(df), path.name)
        return df

    def from_json(
        self,
        path: Union[str, Path],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Extract data from a JSON file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")
        df = pd.read_json(path, **kwargs)
        logger.info("Extracted %d rows from %s", len(df), path.name)
        return df

    def from_api(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Extract data from a REST API endpoint."""
        import requests

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data.get("results", data.get("data", [data])))
        else:
            raise ValueError(f"Unexpected API response type: {type(data)}")
        logger.info("Extracted %d rows from API: %s", len(df), url)
        return df

    def from_sqlite(
        self,
        db_path: Union[str, Path],
        query: str = "SELECT * FROM users",
    ) -> pd.DataFrame:
        """Extract data from a SQLite database."""
        import sqlite3

        db_path = Path(db_path)
        if not db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        conn = sqlite3.connect(str(db_path))
        try:
            df = pd.read_sql_query(query, conn)
            logger.info("Extracted %d rows from SQLite", len(df))
            return df
        finally:
            conn.close()

    def from_dict(
        self, data: Union[List[Dict[str, Any]], Dict[str, List[Any]]]
    ) -> pd.DataFrame:
        """Extract data from a Python dictionary or list of dicts."""
        df = pd.DataFrame(data)
        logger.info("Extracted %d rows from dict", len(df))
        return df
