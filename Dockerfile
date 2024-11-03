FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    redis-server \
    libgl1 \
    libglib2.0-0 \
    curl \
    wget \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /app

COPY pyproject.toml poetry.lock README.md ./
COPY .env.example .env

RUN poetry config virtualenvs.in-project true \
    && if [ "$CPU_ONLY" = "true" ]; then \
        poetry install --no-root --with cpu; \
    else \
        poetry install --no-root; \
    fi

ENV HF_HOME=/tmp/ \
    TORCH_HOME=/tmp/ \
    OMP_NUM_THREADS=4

RUN poetry run python -c 'from docling.document_converter import DocumentConverter; artifacts_path = DocumentConverter.download_models_hf(force=True);'

COPY . .

EXPOSE 8080

CMD ["poetry", "run", "uvicorn", "--port", "8080", "--host", "0.0.0.0", "main:app"]
