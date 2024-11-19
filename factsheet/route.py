from typing import AsyncGenerator, Dict, Any
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Header,
    BackgroundTasks,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
import json
import logging
from datetime import datetime
import jwt
import asyncio
import uuid

from document_converter.rag_processor import RAGProcessor
from shared.dependencies import get_rag_processor, get_supabase_client
from supabase.client import Client
from .schema import (
    FactsheetRequest,
    StreamingFactsheetResponse,
    FACTSHEET_QUESTIONS,
    GenerateFactsheetRequest,
)
from .service import FactsheetService
from .websocket import manager

router = APIRouter(prefix="/contracts/{contract_id}/factsheet")
logger = logging.getLogger(__name__)


def get_factsheet_service(
    rag_processor: RAGProcessor = Depends(get_rag_processor),
    supabase_client: Client = Depends(get_supabase_client),
) -> FactsheetService:
    """Dependency to get the FactsheetService instance."""
    return FactsheetService(supabase_client, rag_processor)


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
        logger.info(
            f"Starting answer generation for job {job_id}, factsheet {factsheet_id}"
        )

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
                    # logger.info(
                    #     f"Storing update for {question_key}, length: {len(full_answer)}"
                    # )
                    await service.store_update(job_id, question_key, full_answer)

            # Store final answer in Supabase
            logger.info(
                f"Saving final answer for {question_key}, length: {len(full_answer)}"
            )
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


@router.websocket("/stream")
async def websocket_endpoint(
    websocket: WebSocket,
    contract_id: str,
    job_id: str,
    api_key: str,
    service: FactsheetService = Depends(get_factsheet_service),
):
    """WebSocket endpoint for streaming factsheet updates."""
    try:
        # Accept the connection
        await websocket.accept()
        logger.info(f"WebSocket connected for job {job_id}")

        # Verify API key and get user_id
        result = (
            service.supabase.from_("profiles")
            .select("id")
            .eq("api_key", api_key)
            .single()
            .execute()
        )
        
        if not result.data:
            logger.error("Invalid API key")
            await websocket.send_json({"type": "error", "message": "Invalid API key"})
            await websocket.close()
            return
            
        user_id = result.data["id"]
        
        # Verify contract access
        access_result = (
            service.supabase.from_("contracts")
            .select("id")
            .eq("id", contract_id)
            .eq("owner_id", user_id)
            .single()
            .execute()
        )
        
        if not access_result.data:
            logger.error(f"User {user_id} not authorized for contract {contract_id}")
            await websocket.send_json({"type": "error", "message": "Not authorized"})
            await websocket.close()
            return

        # Send connected message
        await websocket.send_json({"type": "connected"})

        # Start streaming updates
        try:
            async for update in service.subscribe_to_updates(job_id):
                try:
                    await websocket.send_json(update)
                    if update.get("type") in ["complete", "error"]:
                        break
                except Exception as e:
                    logger.error(f"Error sending update: {e}")
                    break

        except Exception as e:
            logger.error(f"Error in update stream: {e}")
            await websocket.send_json({
                "type": "error",
                "message": "Error streaming updates"
            })

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for job {job_id}")
    except Exception as e:
        logger.error(f"WebSocket error for job {job_id}: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": "Internal server error"
            })
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


@router.post("/generate")
async def generate_factsheet(
    request: GenerateFactsheetRequest,
    contract_id: str,
    background_tasks: BackgroundTasks,
    token: str = Depends(get_current_user_id),
    service: FactsheetService = Depends(get_factsheet_service),
):
    """Start factsheet generation and return connection info."""
    try:
        # Verify access
        if not await service.verify_contract_access(contract_id, token):
            raise HTTPException(status_code=403, detail="Not authorized")

        # Create factsheet record
        factsheet_id = await service.create_factsheet(contract_id, token)

        # Start answer generation in background
        background_tasks.add_task(
            generate_answers,
            service,
            contract_id,
            request.question_keys,
            request.job_id,
            token,
            factsheet_id,
        )

        return {
            "job_id": request.job_id,
            "factsheet_id": factsheet_id,
            "message": "Factsheet generation started"
        }

    except Exception as e:
        logger.error(f"Error starting factsheet generation: {e}")
        raise HTTPException(status_code=500, detail="Error starting factsheet generation")


@router.get("/updates")
async def get_updates(
    job_id: str,
    cursor: int = 0,
    service: FactsheetService = Depends(get_factsheet_service),
):
    """Get incremental updates for a factsheet generation job."""
    try:
        # Get updates since cursor
        all_updates = await service.get_job_updates(job_id, cursor)

        # Only send the latest update for each key
        latest_updates = {}
        for update in all_updates:
            latest_updates[update["key"]] = update["content"]

        # Convert back to list format
        updates = [{"key": k, "content": v} for k, v in latest_updates.items()]

        # Get job status
        job = (
            service.supabase.table("factsheet_jobs")
            .select("status")
            .eq("id", job_id)
            .single()
            .execute()
        )

        return JSONResponse(
            {
                "updates": updates,
                "next_cursor": cursor
                + len(all_updates),  # Still increment by total updates
                "status": job.data["status"],
            }
        )
    except Exception as e:
        logger.error(f"Error getting updates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
