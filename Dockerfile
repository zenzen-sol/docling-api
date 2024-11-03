# Use a base image with CUDA support and the desired Python version
FROM python:3.12-slim-bookworm

ARG CPU_ONLY=false
WORKDIR /app

RUN apt-get update \
    && apt-get install -y redis-server libgl1 libglib2.0-0 curl wget git procps \
    && apt-get clean

# Install Poetry
RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN if [ "$CPU_ONLY" = "true" ]; then \
    pip install --no-cache-dir torch torchvision --extra-index-url https://download.pytorch.org/whl/cpu; \
    else \
    pip install --no-cache-dir torch torchvision; \
    fi

ENV HF_HOME=/tmp/ \
    TORCH_HOME=/tmp/ \
    OMP_NUM_THREADS=4

COPY . .

EXPOSE 8080

CMD ["poetry", "run", "uvicorn", "--port", "8080", "--host", "0.0.0.0", "main:app"]
