# GenAI ETL Pipeline | Pipeline ETL com IA Generativa

![CI](https://github.com/galafis/python-genai-etl-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--3.5-orange)

**[English](#english)** | **[Portugues](#portugues)**

---

## English

### Overview

Production-ready ETL (Extract, Transform, Load) pipeline powered by OpenAI GPT for intelligent data enrichment. Built with Python, featuring modular architecture, comprehensive testing, Docker support, and CI/CD.

### Architecture

```
data/sample_users.csv --> [EXTRACT] --> [TRANSFORM + GenAI] --> [LOAD] --> output/
                          CSV/JSON/API   Clean + Enrich        CSV/JSON/Parquet/SQLite
```

### Key Features

- **Multi-source extraction**: CSV, JSON, REST API, SQLite
- **GenAI enrichment**: Sentiment analysis, text classification, summarization
- **Multiple output formats**: CSV, JSON, Parquet, SQLite, Excel
- **Pipeline orchestrator**: Coordinated ETL with logging and error handling
- **Production-ready**: Docker, CI/CD, type hints, comprehensive tests

### Quick Start

```bash
# Clone and install
git clone https://github.com/galafis/python-genai-etl-pipeline.git
cd python-genai-etl-pipeline
pip install -r requirements.txt

# Run without AI (uses sample data)
python main.py

# Run with AI enrichment
export OPENAI_API_KEY=your-key
python main.py --ai --ai-column name
```

### Project Structure

```
src/
  extract.py      # Multi-source data extraction
  transform.py    # GenAI-powered transformations
  load.py         # Multi-destination data loading
  pipeline.py     # ETL pipeline orchestrator
tests/
  test_pipeline.py  # Comprehensive unit tests
data/
  sample_users.csv  # Sample dataset
main.py             # CLI entry point
Dockerfile          # Container support
```

### Technologies

- Python 3.10+ | Pandas | OpenAI API
- pytest | flake8 | GitHub Actions CI
- Docker | Type Hints | Logging

---

## Portugues

### Visao Geral

Pipeline ETL (Extract, Transform, Load) pronto para producao, potencializado por OpenAI GPT para enriquecimento inteligente de dados. Construido com Python, com arquitetura modular, testes abrangentes, suporte Docker e CI/CD.

### Funcionalidades

- **Extracao multi-fonte**: CSV, JSON, REST API, SQLite
- **Enriquecimento com IA**: Analise de sentimento, classificacao de texto, resumos
- **Multiplos formatos de saida**: CSV, JSON, Parquet, SQLite, Excel
- **Orquestrador de pipeline**: ETL coordenado com logging e tratamento de erros
- **Pronto para producao**: Docker, CI/CD, type hints, testes abrangentes

### Inicio Rapido

```bash
git clone https://github.com/galafis/python-genai-etl-pipeline.git
cd python-genai-etl-pipeline
pip install -r requirements.txt
python main.py
```

### Docker

```bash
docker build -t genai-etl .
docker run genai-etl
docker run -e OPENAI_API_KEY=key genai-etl --ai
```

---

## Author | Autor

**Gabriel Demetrios Lafis**

- [LinkedIn](https://linkedin.com/in/gabriel-lafis)
- [GitHub](https://github.com/galafis)

## License

MIT License - see [LICENSE](LICENSE) for details.
