from typing import AsyncGenerator, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks
from fastapi.responses import JSONResponse
import json
import logging
from datetime import datetime
import jwt
import asyncio

from document_converter.rag_processor import RAGProcessor
from shared.dependencies import get_rag_processor, get_supabase_client
from .schema import FactsheetRequest, StreamingFactsheetResponse, FACTSHEET_QUESTIONS, GenerateFactsheetRequest
from .service import FactsheetService

router = APIRouter(prefix="/contracts/{contract_id}/factsheet")
logger = logging.getLogger(__name__)


def get_factsheet_service(
    rag_processor: RAGProcessor = Depends(get_rag_processor),
    supabase=Depends(get_supabase_client),
) -> FactsheetService:
    """Dependency to get the FactsheetService instance."""
    return FactsheetService(supabase, rag_processor)


async def get_user_id(
    authorization: str = Header(..., description="Bearer token from Supabase"),
) -> str:
    """Extract user ID from Supabase JWT token."""
    try:
        token = authorization.split(" ")[1]  # Remove 'Bearer ' prefix
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded.get("sub")
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid authorization token")


async def get_current_user_id(
    authorization: str = Header(..., description="Bearer token from Supabase"),
) -> str:
    """Extract user ID from Supabase JWT token."""
    try:
        token = authorization.split(" ")[1]  # Remove 'Bearer ' prefix
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded.get("sub")
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid authorization token")


async def generate_answers(
    service: FactsheetService,
    contract_id: str,
    question_keys: list[str],
    job_id: str,
    user_id: str,
    factsheet_id: str,
):
    """Generate answers in background and store updates in Redis."""
    try:
        logger.info(f"Starting answer generation for job {job_id}, factsheet {factsheet_id}")
        
        for question_key in question_keys:
            if question_key not in FACTSHEET_QUESTIONS:
                logger.warning(f"Skipping invalid question key: {question_key}")
                continue

            logger.info(f"Generating answer for question: {question_key}")
            full_answer = ""
            async for response in service.generate_answer(
                contract_id, question_key, factsheet_id
            ):
                current_chunk = response.answers[question_key]
                if len(current_chunk) > 0:
                    # Accumulate locally
                    if not full_answer:
                        full_answer = current_chunk
                    else:
                        full_answer += current_chunk
                    
                    # Store update in Redis
                    logger.info(f"Storing update for {question_key}, length: {len(full_answer)}")
                    await service.store_update(job_id, question_key, full_answer)
            
            # Store final answer in Supabase
            logger.info(f"Saving final answer for {question_key}, length: {len(full_answer)}")
            await service.save_answer(factsheet_id, question_key, full_answer)

        # Mark job as completed
        logger.info(f"Marking job {job_id} as completed")
        service.supabase.table("factsheet_jobs").update({"status": "completed"}).eq(
            "id", job_id
        ).execute()

    except Exception as e:
        logger.error(f"Error in generation: {str(e)}", exc_info=True)
        service.supabase.table("factsheet_jobs").update(
            {"status": "error", "error": str(e)}
        ).eq("id", job_id).execute()


@router.post("/generate")
async def generate_factsheet(
    contract_id: str,
    request: GenerateFactsheetRequest,
    background_tasks: BackgroundTasks,
    service: FactsheetService = Depends(get_factsheet_service),
    user_id: str = Depends(get_current_user_id),
):
    """Start factsheet generation in background."""
    try:
        job_id = request.job_id
        question_keys = request.question_keys
        
        logger.info(f"Starting generation for job_id: {job_id}")

        # Create factsheet first
        factsheet_id = await service.create_factsheet(contract_id, user_id)
        logger.info(f"Created factsheet with ID: {factsheet_id}")

        # Start generation in background
        background_tasks.add_task(
            generate_answers, service, contract_id, question_keys, job_id, user_id, factsheet_id
        )

        response_data = {
            "id": job_id,
            "factsheet_id": factsheet_id,
            "status": "processing"
        }
        logger.info(f"Returning response: {response_data}")
        return JSONResponse(response_data)
    except Exception as e:
        logger.error(f"Error starting generation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/updates")
async def get_updates(
    contract_id: str,
    job_id: str,
    cursor: int = 0,
    service: FactsheetService = Depends(get_factsheet_service),
):
    """Get incremental updates for a factsheet generation job."""
    try:
        # Get updates since cursor
        updates = await service.get_job_updates(job_id, cursor)
        
        # Get job status
        job = service.supabase.table("factsheet_jobs").select("status").eq("id", job_id).single().execute()
        
        return JSONResponse({
            "updates": updates,
            "next_cursor": cursor + len(updates),
            "status": job.data["status"]
        })
    except Exception as e:
        logger.error(f"Error getting updates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
