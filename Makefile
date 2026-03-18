.PHONY: install test lint run docker clean help

# Default target
help:
	@echo "GenAI ETL Pipeline - Available commands:"
	@echo "  make install    Install Python dependencies"
	@echo "  make test       Run test suite with pytest"
	@echo "  make lint       Run flake8 linter"
	@echo "  make run        Execute the ETL pipeline"
	@echo "  make run-ai     Execute with AI enrichment"
	@echo "  make docker     Build Docker image"
	@echo "  make compose    Run with Docker Compose"
	@echo "  make clean      Remove output files and caches"

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v --tb=short

lint:
	flake8 src/ tests/ main.py --max-line-length=120 --statistics

run:
	python main.py

run-ai:
	python main.py --ai --ai-column name

docker:
	docker build -t genai-etl .

compose:
	docker-compose up --build

clean:
	rm -rf output/ __pycache__ .pytest_cache
	rm -rf src/__pycache__ tests/__pycache__
	rm -f pipeline.log
	@echo "Cleaned output files and caches."
