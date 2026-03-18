"""
Load Module - Data loading to multiple destinations.

Supports CSV, JSON, SQLite, and Parquet output formats.

Author: Gabriel Demetrios Lafis
License: MIT
"""

import logging
from pathlib import Path
from typing import Any, Union

import pandas as pd

logger = logging.getLogger(__name__)


class DataLoader:
    """Load transformed DataFrames to various destinations."""

    def __init__(self, output_dir: Union[str, Path] = "output") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("DataLoader initialized. Output dir: %s", self.output_dir)

    def to_csv(
        self,
        df: pd.DataFrame,
        filename: str = "output.csv",
        index: bool = False,
        **kwargs: Any,
    ) -> Path:
        """Save DataFrame to CSV."""
        path = self.output_dir / filename
        df.to_csv(path, index=index, **kwargs)
        logger.info("Saved %d rows to %s", len(df), path)
        return path

    def to_json(
        self,
        df: pd.DataFrame,
        filename: str = "output.json",
        orient: str = "records",
        **kwargs: Any,
    ) -> Path:
        """Save DataFrame to JSON."""
        path = self.output_dir / filename
        df.to_json(path, orient=orient, indent=2, force_ascii=False, **kwargs)
        logger.info("Saved %d rows to %s", len(df), path)
        return path

    def to_sqlite(
        self,
        df: pd.DataFrame,
        db_name: str = "output.db",
        table_name: str = "data",
        if_exists: str = "replace",
    ) -> Path:
        """Save DataFrame to SQLite database."""
        import sqlite3

        path = self.output_dir / db_name
        conn = sqlite3.connect(str(path))
        try:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            logger.info("Saved %d rows to %s (table: %s)", len(df), path, table_name)
        finally:
            conn.close()
        return path

    def to_parquet(
        self,
        df: pd.DataFrame,
        filename: str = "output.parquet",
        **kwargs: Any,
    ) -> Path:
        """Save DataFrame to Parquet format."""
        path = self.output_dir / filename
        df.to_parquet(path, index=False, **kwargs)
        logger.info("Saved %d rows to %s", len(df), path)
        return path

    def to_excel(
        self,
        df: pd.DataFrame,
        filename: str = "output.xlsx",
        sheet_name: str = "Sheet1",
        **kwargs: Any,
    ) -> Path:
        """Save DataFrame to Excel."""
        path = self.output_dir / filename
        df.to_excel(path, sheet_name=sheet_name, index=False, **kwargs)
        logger.info("Saved %d rows to %s", len(df), path)
        return path
