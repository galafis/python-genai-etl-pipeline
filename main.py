#!/usr/bin/env python3
"""
GenAI ETL Pipeline - Main entry point.

Demonstrates the full ETL flow with optional OpenAI enrichment.

Usage:
    python main.py                    # Run without AI
    python main.py --ai               # Run with AI enrichment
    python main.py --source data.csv  # Custom source

Author: Gabriel Demetrios Lafis
License: MIT
"""

import argparse
import logging
import sys
from pathlib import Path

from src.pipeline import ETLPipeline


def setup_logging(level: str = "INFO") -> None:
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Run the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="GenAI ETL Pipeline",
    )
    parser.add_argument(
        "--source",
        default="data/sample_users.csv",
        help="Path to the input data file",
    )
    parser.add_argument(
        "--source-type",
        default="csv",
        choices=["csv", "json", "api", "sqlite"],
        help="Type of data source",
    )
    parser.add_argument(
        "--output-format",
        default="csv",
        choices=["csv", "json", "sqlite", "parquet"],
        help="Output format",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory",
    )
    parser.add_argument(
        "--ai",
        action="store_true",
        help="Enable GenAI enrichment",
    )
    parser.add_argument(
        "--ai-column",
        default="name",
        help="Column to enrich with AI",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Starting GenAI ETL Pipeline")
    logger.info("Source: %s (%s)", args.source, args.source_type)
    logger.info("AI enrichment: %s", "enabled" if args.ai else "disabled")

    pipeline = ETLPipeline()
    pipeline.loader.output_dir = Path(args.output_dir)
    pipeline.loader.output_dir.mkdir(parents=True, exist_ok=True)

    result = pipeline.run(
        source=args.source,
        source_type=args.source_type,
        output_format=args.output_format,
        clean=True,
        ai_enrich=args.ai,
        ai_source_col=args.ai_column if args.ai else None,
        ai_prompt="Generate a personalized marketing message for this customer: {value}",
    )

    if result.success:
        logger.info("Pipeline completed successfully!")
        logger.info("  Rows extracted:    %d", result.rows_extracted)
        logger.info("  Rows transformed:  %d", result.rows_transformed)
        logger.info("  Rows loaded:       %d", result.rows_loaded)
        logger.info("  Duration:          %.2fs", result.duration_seconds)
        logger.info("  Output:            %s", result.output_path)
        return 0
    else:
        logger.error("Pipeline failed: %s", result.errors)
        return 1


if __name__ == "__main__":
    sys.exit(main())
