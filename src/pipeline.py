"""
ETL Pipeline Orchestrator - Coordinates Extract, Transform, Load.

Orchestrates the full ETL flow with logging, error handling,
and optional GenAI enrichment.

Author: Gabriel Demetrios Lafis
License: MIT
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from .extract import DataExtractor
from .load import DataLoader
from .transform import GenAITransformer

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""

    success: bool
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    duration_seconds: float = 0.0
    output_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)


class ETLPipeline:
    """Orchestrate the full ETL pipeline.

    Coordinates extraction, transformation (with optional GenAI
    enrichment), and loading of data.
    """

    def __init__(
        self,
        extractor: Optional[DataExtractor] = None,
        transformer: Optional[GenAITransformer] = None,
        loader: Optional[DataLoader] = None,
    ) -> None:
        self.extractor = extractor or DataExtractor()
        self.transformer = transformer or GenAITransformer()
        self.loader = loader or DataLoader()
        self._custom_transforms: List[Callable[[pd.DataFrame], pd.DataFrame]] = []
        logger.info("ETLPipeline initialized.")

    def add_transform(
        self, func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "ETLPipeline":
        """Register a custom transformation function."""
        self._custom_transforms.append(func)
        logger.info("Added custom transform: %s", func.__name__)
        return self

    def run(
        self,
        source: Union[str, Path, List[Dict[str, Any]]],
        source_type: str = "csv",
        output_format: str = "csv",
        output_filename: Optional[str] = None,
        clean: bool = True,
        ai_enrich: bool = False,
        ai_source_col: Optional[str] = None,
        ai_target_col: str = "ai_enriched",
        ai_prompt: str = "Describe this briefly: {value}",
    ) -> PipelineResult:
        """Execute the full ETL pipeline.

        Args:
            source: Data source (file path, URL, or dict).
            source_type: Type of source (csv, json, api, sqlite, dict).
            output_format: Output format (csv, json, sqlite, parquet).
            output_filename: Custom output filename.
            clean: Whether to apply data cleaning.
            ai_enrich: Whether to apply GenAI enrichment.
            ai_source_col: Column to enrich with AI.
            ai_target_col: Target column name for AI output.
            ai_prompt: Prompt template for AI enrichment.
        """
        start = time.time()
        result = PipelineResult(success=False)

        try:
            # ---- EXTRACT ----
            logger.info("=== EXTRACT phase ===")
            df = self._extract(source, source_type)
            result.rows_extracted = len(df)
            logger.info("Extracted %d rows", len(df))

            # ---- TRANSFORM ----
            logger.info("=== TRANSFORM phase ===")
            if clean:
                df = self.transformer.clean_data(df)

            for func in self._custom_transforms:
                df = func(df)
                logger.info("Applied transform: %s", func.__name__)

            if ai_enrich and ai_source_col:
                df = self.transformer.enrich_with_ai(
                    df, ai_source_col, ai_target_col, ai_prompt
                )
            result.rows_transformed = len(df)

            # ---- LOAD ----
            logger.info("=== LOAD phase ===")
            fname = output_filename or f"output.{output_format}"
            path = self._load(df, output_format, fname)
            result.rows_loaded = len(df)
            result.output_path = str(path)
            result.success = True

        except Exception as exc:
            logger.error("Pipeline failed: %s", exc)
            result.errors.append(str(exc))

        result.duration_seconds = round(time.time() - start, 2)
        logger.info(
            "Pipeline %s in %.2fs",
            "completed" if result.success else "FAILED",
            result.duration_seconds,
        )
        return result

    def _extract(
        self, source: Union[str, Path, List[Dict[str, Any]]], source_type: str
    ) -> pd.DataFrame:
        """Route extraction to the correct method."""
        if source_type == "dict":
            return self.extractor.from_dict(source)  # type: ignore[arg-type]
        source_path = str(source)
        extractors = {
            "csv": lambda: self.extractor.from_csv(source_path),
            "json": lambda: self.extractor.from_json(source_path),
            "api": lambda: self.extractor.from_api(source_path),
            "sqlite": lambda: self.extractor.from_sqlite(source_path),
        }
        if source_type not in extractors:
            raise ValueError(f"Unsupported source type: {source_type}")
        return extractors[source_type]()

    def _load(self, df: pd.DataFrame, fmt: str, filename: str) -> Path:
        """Route loading to the correct method."""
        loaders = {
            "csv": lambda: self.loader.to_csv(df, filename),
            "json": lambda: self.loader.to_json(df, filename),
            "sqlite": lambda: self.loader.to_sqlite(df, filename),
            "parquet": lambda: self.loader.to_parquet(df, filename),
        }
        if fmt not in loaders:
            raise ValueError(f"Unsupported output format: {fmt}")
        return loaders[fmt]()
