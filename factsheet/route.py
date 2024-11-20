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
import jwt

from document_converter.rag_processor import RAGProcessor
from shared.dependencies import get_rag_processor, get_supabase_client
from supabase.client import Client
from .schema import (
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
        await service.initialize()  # Initialize Redis connection
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
                    # Store only the new chunk in Redis
                    # logger.info(
                    #     f"Publishing chunk for {question_key}, length: {len(current_chunk)}"
                    # )
                    await service.store_update(job_id, question_key, current_chunk)
                    # Accumulate locally for final storage
                    if not full_answer:
                        full_answer = current_chunk
                    else:
                        full_answer += current_chunk

            # Store final answer in Supabase
            logger.info(
                f"Saving final answer for {question_key}, length: {len(full_answer)}"
            )
            await service.save_answer(factsheet_id, question_key, full_answer)

        # Mark job as completed
        logger.info(f"Marking job {job_id} as completed")
        await service.redis.publish(
            f"factsheet:{job_id}",
            json.dumps({"type": "complete", "message": "All questions completed"}),
        )
        service.supabase.table("factsheet_jobs").update({"status": "completed"}).eq(
            "id", job_id
        ).execute()

    except Exception as e:
        logger.error(f"Error in generation: {str(e)}", exc_info=True)
        await service.redis.publish(
            f"factsheet:{job_id}", json.dumps({"type": "error", "message": str(e)})
        )
        service.supabase.table("factsheet_jobs").update(
            {"status": "error", "error": str(e)}
        ).eq("id", job_id).execute()
    finally:
        await service.cleanup()


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
        # Initialize Redis connection first
        await service.initialize()
        
        # Accept the connection
        await websocket.accept()
        
        # Add to connection manager
        await manager.connect(job_id, websocket)

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
            return

        # Send connected message
        await websocket.send_json({"type": "connected"})
        logger.info(f"Starting Redis subscription for job {job_id}")

        async for update in service.subscribe_to_updates(job_id):
            # logger.info(f"Sending update for job {job_id}: {update}")
            await websocket.send_json(update)

            if update.get("type") == "complete":
                logger.info("Received complete message, closing connection")
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for job {job_id}")
        await manager.disconnect(websocket, job_id)
        await service.cleanup()
    except Exception as e:
        logger.error(f"WebSocket error for job {job_id}: {e}")
        await manager.disconnect(websocket, job_id)
        await service.cleanup()
        raise
    finally:
        await manager.disconnect(websocket, job_id)
        await service.cleanup()


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
            "message": "Factsheet generation started",
        }

    except Exception as e:
        logger.error(f"Error starting factsheet generation: {e}")
        raise HTTPException(
            status_code=500, detail="Error starting factsheet generation"
        )


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
