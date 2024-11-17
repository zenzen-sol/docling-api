from io import BytesIO
from multiprocessing.pool import AsyncResult
from typing import List, AsyncGenerator, Optional, Dict, Any
from fastapi import APIRouter, File, Form, HTTPException, Response, UploadFile, Query, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import os
import uuid
import json
from datetime import datetime
from supabase import create_client, Client

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
from starlette.datastructures import Headers

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
    supabase = create_client(supabase_url, supabase_key)
    return RAGProcessor(supabase)

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
    source_id: int = Form(..., description="ID of the source to process"),
    extract_tables_as_images: bool = Form(False),
    image_resolution_scale: int = Form(4, ge=1, le=4),
    rag_processor: RAGProcessor = Depends(get_rag_processor),
):
    print(f"Starting conversion for source_id: {source_id}")  
    
    async def process_stream():
        try:
            # Get source details
            source = rag_processor.supabase.table('sources').select(
                '*',
                count='exact'
            ).eq('id', source_id).execute()
            
            print(f"Source query result: {source}")  
            
            if not source.data:
                error = f"Source {source_id} not found"
                print(f"Error: {error}")  
                raise HTTPException(status_code=404, detail=error)
                
            source_record = source.data[0]
            print(f"Source record: {source_record}")  
            
            # Download file from storage
            file_path = source_record['file_name']
            try:
                print(f"Attempting to download file: {file_path}")  
                response = rag_processor.supabase.storage.from_('documents').download(file_path)
                if not response:
                    error = f"File not found in storage: {file_path}"
                    print(f"Error: {error}")  
                    raise HTTPException(
                        status_code=404,
                        detail=error
                    )
                print(f"File downloaded successfully, size: {len(response)} bytes")  
            except Exception as e:
                error = f"Failed to download file from storage: {str(e)}"
                print(f"Error: {error}")  
                raise HTTPException(
                    status_code=500,
                    detail=error
                )

            # Create UploadFile from downloaded content
            headers = Headers({
                "content-type": source_record['file_metadata'].get('contentType', 'application/octet-stream')
            })
            file = UploadFile(
                filename=file_path,
                file=BytesIO(response),
                headers=headers
            )
            print(f"Created UploadFile object with filename: {file.filename} and content_type: {file.content_type}")  

            # Convert the document
            print("Starting document conversion")  
            conversion_result = await convert_single_document(
                file, 
                extract_tables_as_images, 
                image_resolution_scale
            )
            print("Document conversion completed")  

            # Process the document and yield results
            print("Starting source processing")  
            async for update in rag_processor.process_source(
                source_id=source_id,
                markdown_content=conversion_result.markdown
            ):
                print(f"Processing update: {update}")  
                yield json.dumps(update) + "\n"

        except Exception as e:
            error_update = {
                "type": "error",
                "message": str(e),
                "step": ProcessingStep.CONVERSION.value,
                "source_id": source_id
            }
            print(f"Error during processing: {error_update}")  
            yield json.dumps(error_update) + "\n"
            raise HTTPException(status_code=500, detail=str(e))

    return StreamingResponse(
        process_stream(),
        media_type="application/x-ndjson"
    )


@router.get(
    "/sources/{source_id}/status",
    description="Get the processing status of a source"
)
async def get_source_status(
    source_id: int,
    rag_processor: RAGProcessor = Depends(get_rag_processor)
):
    try:
        # Get source details
        source = rag_processor.supabase.table('sources').select(
            '*',
            count='exact'
        ).eq('id', source_id).execute()
        
        if not source.data:
            raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
            
        # Get latest conversion if any
        conversion = rag_processor.supabase.table('conversions').select(
            '*',
            count='exact'
        ).eq('source_id', source_id).order('created_at.desc').limit(1).execute()
        
        return JSONResponse(content={
            "source": source.data[0],
            "conversion": conversion.data[0] if conversion.data else None
        })
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
