"""
Unit tests for the GenAI ETL Pipeline.
Author: Gabriel Demetrios Lafis
License: MIT
"""
from pathlib import Path

import pandas as pd
import pytest

from src.extract import DataExtractor
from src.load import DataLoader
from src.pipeline import ETLPipeline, PipelineResult
from src.transform import GenAITransformer


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a temporary CSV file."""
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": [" Alice ", "Bob", " Charlie "],
            "age": [30, 25, 35],
            "score": [85.5, 92.0, 78.3],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 3],
            "name": [" Alice ", "Bob", " Charlie ", " Charlie "],
            "value": [100, 200, 300, 300],
        }
    )


class TestDataExtractor:
    def test_from_csv(self, sample_csv: Path) -> None:
        extractor = DataExtractor()
        df = extractor.from_csv(sample_csv)
        assert len(df) == 3
        assert "name" in df.columns

    def test_from_csv_not_found(self) -> None:
        extractor = DataExtractor()
        with pytest.raises(FileNotFoundError):
            extractor.from_csv("nonexistent.csv")

    def test_from_dict(self) -> None:
        extractor = DataExtractor()
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        df = extractor.from_dict(data)
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]

    def test_from_json(self, tmp_path: Path) -> None:
        extractor = DataExtractor()
        json_path = tmp_path / "test.json"
        pd.DataFrame({"x": [1, 2]}).to_json(json_path)
        df = extractor.from_json(json_path)
        assert len(df) == 2


class TestGenAITransformer:
    def test_clean_data_removes_duplicates(
        self, sample_df: pd.DataFrame
    ) -> None:
        transformer = GenAITransformer()
        result = transformer.clean_data(sample_df)
        assert len(result) == 3

    def test_clean_data_strips_whitespace(
        self, sample_df: pd.DataFrame
    ) -> None:
        transformer = GenAITransformer()
        result = transformer.clean_data(sample_df)
        assert result["name"].iloc[0] == "Alice"

    def test_clean_data_standardizes_columns(
        self, sample_df: pd.DataFrame
    ) -> None:
        df = sample_df.rename(columns={"name": "Full Name"})
        transformer = GenAITransformer()
        result = transformer.clean_data(df)
        assert "full_name" in result.columns

    def test_add_derived_columns(self, sample_df: pd.DataFrame) -> None:
        transformer = GenAITransformer()
        result = transformer.add_derived_columns(
            sample_df, {"double_value": "value * 2"}
        )
        assert "double_value" in result.columns
        assert result["double_value"].iloc[0] == 200


class TestDataLoader:
    def test_to_csv(self, sample_df: pd.DataFrame, tmp_path: Path) -> None:
        loader = DataLoader(output_dir=tmp_path)
        path = loader.to_csv(sample_df, "out.csv")
        assert path.exists()
        loaded = pd.read_csv(path)
        assert len(loaded) == len(sample_df)

    def test_to_json(
        self, sample_df: pd.DataFrame, tmp_path: Path
    ) -> None:
        loader = DataLoader(output_dir=tmp_path)
        path = loader.to_json(sample_df, "out.json")
        assert path.exists()

    def test_to_sqlite(
        self, sample_df: pd.DataFrame, tmp_path: Path
    ) -> None:
        loader = DataLoader(output_dir=tmp_path)
        path = loader.to_sqlite(sample_df, "out.db", "test_table")
        assert path.exists()


class TestETLPipeline:
    def test_run_csv_to_csv(self, sample_csv: Path, tmp_path: Path) -> None:
        loader = DataLoader(output_dir=tmp_path)
        pipeline = ETLPipeline(loader=loader)
        result = pipeline.run(
            source=sample_csv,
            source_type="csv",
            output_format="csv",
        )
        assert result.success
        assert result.rows_extracted == 3
        assert result.rows_loaded == 3
        assert result.output_path is not None

    def test_run_dict_to_json(self, tmp_path: Path) -> None:
        loader = DataLoader(output_dir=tmp_path)
        pipeline = ETLPipeline(loader=loader)
        data = [{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]
        result = pipeline.run(
            source=data,
            source_type="dict",
            output_format="json",
        )
        assert result.success
        assert result.rows_extracted == 2

    def test_custom_transform(self, sample_csv: Path, tmp_path: Path) -> None:
        loader = DataLoader(output_dir=tmp_path)
        pipeline = ETLPipeline(loader=loader)

        def add_flag(df: pd.DataFrame) -> pd.DataFrame:
            df["flag"] = True
            return df

        pipeline.add_transform(add_flag)
        result = pipeline.run(source=sample_csv, source_type="csv")
        assert result.success

    def test_pipeline_result_dataclass(self) -> None:
        r = PipelineResult(success=True, rows_extracted=10)
        assert r.success
        assert r.rows_extracted == 10
        assert r.errors == []

    def test_invalid_source_type(self, tmp_path: Path) -> None:
        loader = DataLoader(output_dir=tmp_path)
        pipeline = ETLPipeline(loader=loader)
        result = pipeline.run(source="test", source_type="invalid")
        assert not result.success
        assert len(result.errors) > 0
