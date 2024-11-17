from io import BytesIO
from multiprocessing.pool import AsyncResult
from typing import List, AsyncGenerator, Optional, Dict, Any
from fastapi import APIRouter, File, HTTPException, UploadFile, Query, Depends, Form
from fastapi.responses import StreamingResponse, JSONResponse
import os
import uuid
import json
from datetime import datetime
from document_converter.schema import (
    BatchConversionJobResult,
    ConversationJobResult,
    ConversionResult,
)
from document_converter.service import (
    DocumentConverterService,
    DoclingDocumentConversion,
)
from document_converter.utils import is_file_format_supported
from document_converter.rag_processor import RAGProcessor, ProcessingStatus, ProcessingStep
from worker.tasks import convert_document_task, convert_documents_task

router = APIRouter()

# Could be docling or another converter as long as it implements DocumentConversionBase
converter = DoclingDocumentConversion()
document_converter_service = DocumentConverterService(document_converter=converter)


# Initialize RAG processor with environment variables
def get_rag_processor():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise HTTPException(
            status_code=500,
            detail="Supabase configuration missing"
        )
    return RAGProcessor(supabase_url, supabase_key)

# Document direct conversion endpoints
@router.post(
    "/documents/convert",
    response_model=ConversionResult,
    response_model_exclude_unset=True,
    description="Convert a single document synchronously",
)
async def convert_single_document(
    document: UploadFile = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    # Debug logging
    print("Received file details:")
    print(f"Filename: {document.filename}")
    print(f"Content type: {document.content_type}")
    print(f"Headers: {document.headers}")

    file_bytes = await document.read()
    if not is_file_format_supported(file_bytes, document.filename):
        raise HTTPException(
            status_code=400, detail=f"Unsupported file format: {document.filename}"
        )

    return document_converter_service.convert_document(
        (document.filename, BytesIO(file_bytes)),
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )


@router.post(
    "/documents/convert-and-embed",
    description="Convert a document and generate embeddings with progress streaming"
)
async def convert_and_embed_document(
    document: UploadFile = File(...),
    creator_id: str = Form(None),
    contract_id: str = Form(None),
    source_id: int = Form(None),
    extract_tables_as_images: bool = Form(False),
    image_resolution_scale: int = Form(4, ge=1, le=4),
    rag_processor: RAGProcessor = Depends(get_rag_processor),
):
    try:
        # Initialize document processing
        document_id = await rag_processor.initialize_document_processing(
            filename=document.filename,
            content_type=document.content_type,
            creator_id=creator_id,
            contract_id=contract_id,
            source_id=source_id
        )

        # First convert the document
        conversion_result = await convert_single_document(
            document, 
            extract_tables_as_images, 
            image_resolution_scale
        )

        # Prepare metadata
        metadata = {
            "filename": document.filename,
            "content_type": document.content_type,
            "conversion_timestamp": datetime.utcnow().isoformat(),
            "has_images": len(conversion_result.images) > 0 if conversion_result.images else False,
            "creator_id": creator_id,
            "contract_id": contract_id,
            "source_id": source_id
        }

        # Process the document with streaming updates
        return StreamingResponse(
            rag_processor.process_document(conversion_result.markdown, document_id, metadata),
            media_type="application/x-ndjson"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/documents/{document_id}/status",
    description="Get the processing status of a document"
)
async def get_document_status(
    document_id: uuid.UUID,
    rag_processor: RAGProcessor = Depends(get_rag_processor)
):
    try:
        status = await rag_processor.get_processing_status(document_id)
        return JSONResponse(content=status)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/documents/batch-convert",
    response_model=List[ConversionResult],
    response_model_exclude_unset=True,
    description="Convert multiple documents synchronously",
)
async def convert_multiple_documents(
    documents: List[UploadFile] = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    doc_streams = []
    for document in documents:
        file_bytes = await document.read()
        if not is_file_format_supported(file_bytes, document.filename):
            raise HTTPException(
                status_code=400, detail=f"Unsupported file format: {document.filename}"
            )
        doc_streams.append((document.filename, BytesIO(file_bytes)))

    return document_converter_service.convert_documents(
        doc_streams,
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )


# Asynchronous conversion jobs endpoints
@router.post(
    "/conversion-jobs",
    response_model=ConversationJobResult,
    description="Create a conversion job for a single document",
)
async def create_single_document_conversion_job(
    document: UploadFile = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    file_bytes = await document.read()
    if not is_file_format_supported(file_bytes, document.filename):
        raise HTTPException(
            status_code=400, detail=f"Unsupported file format: {document.filename}"
        )

    task = convert_document_task.delay(
        (document.filename, file_bytes),
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )

    return ConversationJobResult(job_id=task.id, status="IN_PROGRESS")


@router.get(
    "/conversion-jobs/{job_id}",
    response_model=ConversationJobResult,
    description="Get the status of a single document conversion job",
    response_model_exclude_unset=True,
)
async def get_conversion_job_status(job_id: str):
    return document_converter_service.get_single_document_task_result(job_id)


@router.post(
    "/batch-conversion-jobs",
    response_model=BatchConversionJobResult,
    response_model_exclude_unset=True,
    description="Create a conversion job for multiple documents",
)
async def create_batch_conversion_job(
    documents: List[UploadFile] = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    """Create a batch conversion job for multiple documents."""
    doc_data = []
    for document in documents:
        file_bytes = await document.read()
        if not is_file_format_supported(file_bytes, document.filename):
            raise HTTPException(
                status_code=400, detail=f"Unsupported file format: {document.filename}"
            )
        doc_data.append((document.filename, file_bytes))

    task = convert_documents_task.delay(
        doc_data,
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )

    return BatchConversionJobResult(job_id=task.id, status="IN_PROGRESS")


@router.get(
    "/batch-conversion-jobs/{job_id}",
    response_model=BatchConversionJobResult,
    response_model_exclude_unset=True,
    description="Get the status of a batch conversion job",
)
async def get_batch_conversion_job_status(job_id: str):
    """Get the status and results of a batch conversion job."""
    return document_converter_service.get_batch_conversion_task_result(job_id)
