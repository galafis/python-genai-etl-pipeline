"""
Transform Module - Data transformation with GenAI enrichment.

Uses OpenAI GPT models to intelligently enrich and transform data.

Author: Gabriel Demetrios Lafis
License: MIT
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class GenAITransformer:
    """Transform and enrich data using OpenAI GPT models.

    Provides both standard pandas transformations and
    AI-powered enrichment for text generation, classification,
    and sentiment analysis.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-3.5-turbo",
    ) -> None:
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        self.model = model
        self._client: Any = None
        logger.info("GenAITransformer initialized with model=%s", model)

    @property
    def client(self) -> Any:
        """Lazy-load the OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI

                self._client = OpenAI(api_key=self.api_key)
            except ImportError:
                logger.warning("openai package not installed. AI features disabled.")
                self._client = None
        return self._client

    # ------------------------------------------------------------------
    # Standard transformations
    # ------------------------------------------------------------------
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply standard data cleaning operations."""
        result = df.copy()
        # Remove fully duplicated rows
        initial = len(result)
        result = result.drop_duplicates()
        dropped = initial - len(result)
        if dropped:
            logger.info("Removed %d duplicate rows", dropped)

        # Strip whitespace from string columns
        str_cols = result.select_dtypes(include=["object"]).columns
        for col in str_cols:
            result[col] = result[col].str.strip()

        # Standardize column names
        result.columns = (
            result.columns.str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
            .str.strip("_")
        )
        logger.info("Data cleaned: %d rows, %d cols", len(result), len(result.columns))
        return result

    def add_derived_columns(
        self, df: pd.DataFrame, mappings: Dict[str, str]
    ) -> pd.DataFrame:
        """Add derived columns using pandas eval expressions.

        Args:
            df: Input DataFrame.
            mappings: Dict of {new_col_name: pandas_eval_expression}.
        """
        result = df.copy()
        for col_name, expr in mappings.items():
            try:
                result[col_name] = result.eval(expr)
                logger.info("Added derived column: %s", col_name)
            except Exception as exc:
                logger.error("Failed to add %s: %s", col_name, exc)
        return result

    # ------------------------------------------------------------------
    # GenAI-powered transformations
    # ------------------------------------------------------------------
    def _call_openai(self, prompt: str, max_tokens: int = 256) -> str:
        """Call OpenAI API and return the response text."""
        if not self.client:
            return "[AI unavailable]"
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.7,
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            logger.error("OpenAI API error: %s", exc)
            return "[AI error]"

    def enrich_with_ai(
        self,
        df: pd.DataFrame,
        source_col: str,
        target_col: str,
        prompt_template: str,
        max_tokens: int = 128,
    ) -> pd.DataFrame:
        """Enrich a DataFrame column using GenAI.

        Args:
            df: Input DataFrame.
            source_col: Column with input text.
            target_col: Name for the new AI-generated column.
            prompt_template: Template with {value} placeholder.
            max_tokens: Max tokens per AI response.
        """
        result = df.copy()
        responses: List[str] = []
        for value in result[source_col]:
            prompt = prompt_template.format(value=value)
            resp = self._call_openai(prompt, max_tokens=max_tokens)
            responses.append(resp)
        result[target_col] = responses
        logger.info("AI enrichment complete: %s -> %s", source_col, target_col)
        return result

    def classify_text(
        self,
        df: pd.DataFrame,
        text_col: str,
        categories: List[str],
        target_col: str = "category",
    ) -> pd.DataFrame:
        """Classify text into predefined categories using AI."""
        cats = ", ".join(categories)
        template = (
            f"Classify the following text into exactly one of these categories: "
            f"{cats}. Respond with ONLY the category name.\n\nText: {{value}}"
        )
        return self.enrich_with_ai(df, text_col, target_col, template, max_tokens=20)

    def analyze_sentiment(
        self,
        df: pd.DataFrame,
        text_col: str,
        target_col: str = "sentiment",
    ) -> pd.DataFrame:
        """Analyze sentiment of text using AI."""
        template = (
            "Analyze the sentiment of the following text. "
            "Respond with ONLY one word: Positive, Negative, or Neutral.\n\n"
            "Text: {value}"
        )
        return self.enrich_with_ai(df, text_col, target_col, template, max_tokens=10)

    def generate_summary(
        self,
        df: pd.DataFrame,
        text_col: str,
        target_col: str = "summary",
    ) -> pd.DataFrame:
        """Generate concise summaries using AI."""
        template = (
            "Summarize the following text in one concise sentence "
            "(max 30 words):\n\n{value}"
        )
        return self.enrich_with_ai(df, text_col, target_col, template, max_tokens=60)

    # ------------------------------------------------------------------
    # Batch AI processing with structured output
    # ------------------------------------------------------------------
    def batch_enrich_json(
        self,
        df: pd.DataFrame,
        source_col: str,
        prompt_template: str,
        output_fields: List[str],
    ) -> pd.DataFrame:
        """Enrich with AI returning structured JSON fields.

        Args:
            df: Input DataFrame.
            source_col: Column with input text.
            prompt_template: Template with {value} placeholder.
            output_fields: Expected JSON keys in the AI response.
        """
        result = df.copy()
        records: List[Dict[str, Any]] = []
        for value in result[source_col]:
            prompt = prompt_template.format(value=value)
            full_prompt = (
                f"{prompt}\n\nRespond with a valid JSON object containing "
                f"these keys: {', '.join(output_fields)}. "
                f"Do not include any other text."
            )
            resp = self._call_openai(full_prompt, max_tokens=256)
            try:
                parsed = json.loads(resp)
            except json.JSONDecodeError:
                parsed = {f: None for f in output_fields}
            records.append(parsed)

        enriched = pd.DataFrame(records)
        for field in output_fields:
            if field in enriched.columns:
                result[field] = enriched[field].values
        logger.info("Batch JSON enrichment complete: %d records", len(result))
        return result
