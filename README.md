# Document Converter Service

A high-performance document conversion service that transforms various document formats into Markdown. Built with FastAPI, Celery, and Redis, supporting both CPU and GPU processing modes.

## Features

- **Multiple Format Support**: Converts various document types including:
  - PDF files
  - Microsoft Word documents (DOCX)
  - PowerPoint presentations (PPTX)
  - HTML files
  - Images (JPG, PNG, TIFF, BMP)
  - AsciiDoc files
  - Markdown files

- **Conversion Capabilities**:
  - Text extraction and formatting
  - Table detection and conversion
  - Image extraction and processing
  - Multi-language OCR support (French, German, Spanish, English, Italian, Portuguese)
  - Configurable image resolution scaling

- **API Endpoints**:
  - Synchronous single document conversion
  - Synchronous batch document conversion
  - Asynchronous single document conversion with job tracking
  - Asynchronous batch conversion with job tracking

- **Processing Modes**:
  - CPU-only processing for standard deployments
  - GPU-accelerated processing for improved performance
  - Distributed task processing using Celery
  - Task monitoring through Flower dashboard

## Prerequisites

- Docker and Docker Compose
- NVIDIA GPU with CUDA support (for GPU mode)
- NVIDIA Container Toolkit (for GPU mode)

## Environment Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd document-converter
```

2. Create a `.env` file:
```bash
REDIS_HOST=redis://redis:6379/0
ENV=production
```

3. Start the service using CPU-only processing:
```bash
docker-compose -f docker-compose.cpu.yml up --build
```

### GPU Mode

Start the service with GPU acceleration:

```bash
docker-compose -f docker-compose.gpu.yml up --build
```

## Service Components

The service will start the following components:

- **API Server**: http://localhost:8080
- **Redis**: http://localhost:6379
- **Flower Dashboard**:
  - CPU mode: http://localhost:5556
  - GPU mode: http://localhost:5555

## API Usage

### Synchronous Conversion

Convert a single document immediately:

```bash
curl -X POST "http://localhost:8080/documents/convert" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "document=@/path/to/document.pdf" \
  -F "extract_tables_as_images=true" \
  -F "image_resolution_scale=4"
```

### Asynchronous Conversion

1. Submit a document for conversion:

```bash
curl -X POST "http://localhost:8080/conversion-jobs" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "document=@/path/to/document.pdf"
```

2. Check conversion status:

```bash
curl -X GET "http://localhost:8080/conversion-jobs/{job_id}" \
  -H "accept: application/json"
```

### Batch Processing

Convert multiple documents asynchronously:

```bash
curl -X POST "http://localhost:8080/batch-conversion-jobs" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "documents=@/path/to/document1.pdf" \
  -F "documents=@/path/to/document2.pdf"
```

## Configuration Options

- `image_resolution_scale`: Control the resolution of extracted images (1-4)
- `extract_tables_as_images`: Extract tables as images (true/false)
- `CPU_ONLY`: Build argument to switch between CPU/GPU modes

## Monitoring

- Access the Flower dashboard to monitor Celery tasks and workers
- View task status, success/failure rates, and worker performance
- Monitor resource usage and task queues

## Architecture

The service uses a distributed architecture with the following components:

1. FastAPI application serving the REST API
2. Celery workers for distributed task processing
3. Redis as message broker and result backend
4. Flower for task monitoring and management

## Performance Considerations

- GPU mode provides significantly faster processing for large documents
- CPU mode is suitable for smaller deployments or when GPU is not available
- Multiple workers can be scaled horizontally for increased throughput

## License

[Your License Here]
```

This README provides a comprehensive overview of your document conversion service, including setup instructions, features, and usage examples. The code references show that the service supports both synchronous and asynchronous processing modes, with endpoints for single and batch document conversion.
