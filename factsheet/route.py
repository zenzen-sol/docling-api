from typing import AsyncGenerator, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
import json
import logging
from datetime import datetime
import jwt
import asyncio

from document_converter.rag_processor import RAGProcessor
from shared.dependencies import get_rag_processor, get_supabase_client
from .schema import FactsheetRequest, StreamingFactsheetResponse, FACTSHEET_QUESTIONS
from .service import FactsheetService

router = APIRouter()
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


async def stream_chunks(
    service: FactsheetService,
    contract_id: str,
    question_keys: list[str],
    job_id: str,
) -> None:
    """Asynchronously generate and store factsheet chunks."""
    try:
        logger.info(
            f"Starting stream_chunks for job {job_id} with questions {question_keys}"
        )

        # Update job status to processing
        service.supabase.table("factsheet_jobs").update({"status": "processing"}).eq(
            "id", job_id
        ).execute()
        logger.info(f"Updated job {job_id} status to processing")

        # Get user_id from job
        job = (
            service.supabase.table("factsheet_jobs")
            .select("user_id")
            .eq("id", job_id)
            .single()
            .execute()
        )
        user_id = job.data["user_id"]
        logger.info(f"Retrieved user_id {user_id} for job {job_id}")

        # Create factsheet first
        factsheet_id = await service.create_factsheet(contract_id, user_id)
        logger.info(f"Created factsheet {factsheet_id} for contract {contract_id}")

        for question_key in question_keys:
            if question_key not in FACTSHEET_QUESTIONS:
                logger.error(f"Invalid question key: {question_key}")
                service.supabase.table("factsheet_jobs").update(
                    {
                        "status": "error",
                        "error": f"Invalid question key: {question_key}",
                    }
                ).eq("id", job_id).execute()
                return

            logger.info(f"Processing question {question_key} for job {job_id}")
            full_answer = ""
            update_count = 0
            async for response in service.generate_answer(
                contract_id, question_key, factsheet_id
            ):
                current_answer = response.answers[question_key]
                # Only update if the answer has changed and is not empty
                if current_answer != full_answer and len(current_answer) > 0:
                    update_count += 1
                    logger.debug(f"Answer update #{update_count} for {question_key}: {len(current_answer)} chars")
                    
                    try:
                        # Update the answer in factsheet_answers
                        await service.save_answer(factsheet_id, question_key, current_answer)
                        logger.debug(f"Updated answer for {question_key} in factsheet_answers")
                    except Exception as e:
                        logger.error(f"Error updating answer: {str(e)}", exc_info=True)
                        raise
                    
                    full_answer = current_answer

        # Update factsheet timestamp at the end
        service.supabase.table("factsheets").update({
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", factsheet_id).execute()
        logger.info(f"Updated factsheet {factsheet_id} timestamp")

        logger.info(f"Marking job {job_id} as completed")
        # Mark job as completed
        service.supabase.table("factsheet_jobs").update({"status": "completed"}).eq(
            "id", job_id
        ).execute()

    except Exception as e:
        logger.error(f"Error in stream_chunks: {str(e)}", exc_info=True)
        # Update job with error status
        service.supabase.table("factsheet_jobs").update(
            {"status": "error", "error": str(e)}
        ).eq("id", job_id).execute()


@router.post("/contracts/{contract_id}/factsheet/generate")
async def generate_factsheet(
    contract_id: str,
    request: FactsheetRequest,
    background_tasks: BackgroundTasks,
    service: FactsheetService = Depends(get_factsheet_service),
    user_id: str = Depends(get_user_id),
):
    """Initialize factsheet generation job."""
    try:
        logger.info(f"Initializing factsheet generation for contract {contract_id}")
        logger.info(f"Request: {request.dict()}")

        # Verify contract access
        if not await service.verify_contract_access(contract_id, user_id):
            logger.error(f"Contract access verification failed for user {user_id}")
            raise HTTPException(status_code=404, detail="Contract not found")

        # Use job_id from request or fail
        job_id = request.job_id
        if not job_id:
            raise HTTPException(status_code=400, detail="job_id is required")

        # Start generation in background
        background_tasks.add_task(
            stream_chunks, service, contract_id, request.question_keys, job_id
        )

        return JSONResponse(
            {
                "id": job_id,
                "chunks": [],
                "cursor": "0",
                "done": False,
            }
        )

    except Exception as e:
        logger.error(
            f"Error initializing factsheet generation: {str(e)}", exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contracts/{contract_id}/factsheet/chunks")
async def get_chunks(
    contract_id: str,
    job_id: str,
    cursor: str = "0",
    service: FactsheetService = Depends(get_factsheet_service),
    user_id: str = Depends(get_user_id),
):
    """Get chunks for a factsheet generation job."""
    try:
        logger.info(f"Fetching chunks for job {job_id} with cursor {cursor}")

        # Verify contract access
        if not await service.verify_contract_access(contract_id, user_id):
            logger.error(f"Contract access verification failed for user {user_id}")
            raise HTTPException(status_code=404, detail="Contract not found")

        # Get job status
        job_result = (
            service.supabase.table("factsheet_jobs")
            .select("*")
            .eq("id", job_id)
            .eq("user_id", user_id)
            .execute()
        )
        logger.info(f"Job query result: {job_result.data}")

        if not job_result.data:
            logger.error(f"Job {job_id} not found for user {user_id}")
            raise HTTPException(status_code=404, detail="Job not found")

        job = job_result.data[0]
        logger.info(f"Job status: {job['status']}")

        # Get new chunks
        chunks_query = (
            service.supabase.table("factsheet_chunks")
            .select("*")
            .eq("job_id", job_id)
            .gt("sequence", int(cursor))
            .order("sequence", ascending=True)
            .limit(50)
        )
        logger.debug(f"Chunks query: {chunks_query}")
        chunks_result = chunks_query.execute()
        logger.info(f"Found {len(chunks_result.data or [])} new chunks")
        logger.debug(f"Chunks result: {chunks_result.data}")

        chunks = chunks_result.data or []
        new_cursor = str(chunks[-1]["sequence"]) if chunks else cursor
        logger.info(f"New cursor: {new_cursor}")

        response_data = {
            "id": job_id,
            "chunks": [chunk["content"] for chunk in chunks],
            "cursor": new_cursor,
            "done": job["status"] in ["completed", "error"],
            "error": job.get("error"),
        }
        logger.info(f"Returning response: {response_data}")
        return JSONResponse(response_data)

    except Exception as e:
        logger.error(f"Error fetching chunks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
