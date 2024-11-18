from typing import AsyncGenerator, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
import json
import logging
from datetime import datetime
import jwt
import asyncio
from uuid import uuid4

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
        # Update job status to processing
        service.supabase.table("factsheet_jobs").update({"status": "processing"}).eq(
            "id", job_id
        ).execute()

        # Get user_id from job
        job = service.supabase.table("factsheet_jobs").select("user_id").eq("id", job_id).single().execute()
        user_id = job.data["user_id"]

        # Create factsheet first
        factsheet_id = await service.create_factsheet(contract_id, user_id)

        sequence = 0
        for question_key in question_keys:
            if question_key not in FACTSHEET_QUESTIONS:
                service.supabase.table("factsheet_jobs").update(
                    {
                        "status": "error",
                        "error": f"Invalid question key: {question_key}",
                    }
                ).eq("id", job_id).execute()
                return

            # Collect the entire answer first
            full_answer = ""
            async for response in service.generate_answer(
                contract_id, question_key, factsheet_id
            ):
                full_answer = response.answers[question_key]
                if len(full_answer) > 0 and sequence % 5 == 0:  # Only store every 5th update
                    # Store chunk in database
                    service.supabase.table("factsheet_chunks").insert(
                        {
                            "job_id": job_id,
                            "sequence": sequence,
                            "content": json.dumps(
                                {
                                    "key": question_key,
                                    "content": full_answer,
                                }
                            ),
                        }
                    ).execute()
                    sequence += 1

            # Store final chunk if we haven't already
            if sequence == 0 or len(full_answer) > 0:
                service.supabase.table("factsheet_chunks").insert(
                    {
                        "job_id": job_id,
                        "sequence": sequence,
                        "content": json.dumps(
                            {
                                "key": question_key,
                                "content": full_answer,
                            }
                        ),
                    }
                ).execute()

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

        # Create new job
        job_id = str(uuid4())
        result = (
            service.supabase.table("factsheet_jobs")
            .insert(
                {
                    "id": job_id,
                    "user_id": user_id,
                    "contract_id": contract_id,
                    "question_keys": request.question_keys,
                    "status": "pending",
                }
            )
            .execute()
        )

        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create job")

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
        # Verify contract access
        if not await service.verify_contract_access(contract_id, user_id):
            raise HTTPException(status_code=404, detail="Contract not found")

        # Get job status
        job_result = (
            service.supabase.table("factsheet_jobs")
            .select("*")
            .eq("id", job_id)
            .eq("user_id", user_id)
            .execute()
        )

        if not job_result.data:
            raise HTTPException(status_code=404, detail="Job not found")

        job = job_result.data[0]

        # Get new chunks
        chunks_result = (
            service.supabase.table("factsheet_chunks")
            .select("*")
            .eq("job_id", job_id)
            .gt("sequence", int(cursor))
            .order("sequence", ascending=True)
            .limit(50)
            .execute()
        )

        chunks = chunks_result.data or []
        new_cursor = str(chunks[-1]["sequence"]) if chunks else cursor

        return JSONResponse(
            {
                "id": job_id,
                "chunks": [chunk["content"] for chunk in chunks],
                "cursor": new_cursor,
                "done": job["status"] in ["completed", "error"],
                "error": job.get("error"),
            }
        )

    except Exception as e:
        logger.error(f"Error fetching chunks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{contract_id}/factsheet/generate")
async def generate_factsheet(
    contract_id: str,
    request: FactsheetRequest,
    user_id: str = Depends(get_user_id),
    supabase=Depends(get_supabase_client),
    rag_processor=Depends(get_rag_processor),
):
    """Generate factsheet for a contract."""
    logger.info(f"Request: {request.model_dump()}")

    service = FactsheetService(supabase, rag_processor)

    # Verify contract access
    if not service.verify_contract_access(contract_id, user_id):
        raise HTTPException(status_code=403, detail="Access denied")

    try:
        # Create job record
        job = (
            service.supabase.table("factsheet_jobs")
            .insert(
                {
                    "contract_id": contract_id,
                    "user_id": user_id,
                    "status": "processing",
                    "created_at": datetime.utcnow().isoformat(),
                    "question_keys": request.question_keys,
                }
            )
            .execute()
        )

        if not job.data:
            raise HTTPException(status_code=500, detail="Failed to create job")

        job_id = job.data[0]["id"]
        return {"job_id": job_id}

    except Exception as e:
        logger.error(
            f"Error initializing factsheet generation: {str(e)}", exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{contract_id}/factsheet/chunks")
async def get_factsheet_chunks(
    contract_id: str,
    job_id: str,
    question_key: str,
    user_id: str = Depends(get_user_id),
    supabase=Depends(get_supabase_client),
    rag_processor=Depends(get_rag_processor),
) -> StreamingResponse:
    """Stream factsheet chunks for a specific question."""
    service = FactsheetService(supabase, rag_processor)

    # Verify contract access
    if not service.verify_contract_access(contract_id, user_id):
        raise HTTPException(status_code=403, detail="Access denied")

    async def generate() -> AsyncGenerator[str, None]:
        try:
            async for chunk in service.generate_answer(
                contract_id, question_key, job_id
            ):
                yield f"data: {json.dumps(chunk)}\n\n"
        except Exception as e:
            logger.error(f"Error generating answer: {str(e)}", exc_info=True)
            error_response = {"error": str(e)}
            yield f"data: {json.dumps(error_response)}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")
