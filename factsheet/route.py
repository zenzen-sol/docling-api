from typing import AsyncGenerator, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.responses import StreamingResponse
import json
import jwt
import logging

from document_converter.rag_processor import RAGProcessor
from shared.dependencies import get_rag_processor
from .schema import FactsheetRequest, StreamingFactsheetResponse, FACTSHEET_QUESTIONS
from .service import FactsheetService

router = APIRouter()
logger = logging.getLogger(__name__)

def get_factsheet_service(
    rag_processor: RAGProcessor = Depends(get_rag_processor)
) -> FactsheetService:
    """Dependency to get the FactsheetService instance."""
    return FactsheetService(rag_processor.supabase, rag_processor)

async def get_user_id(
    authorization: str = Header(..., description="Bearer token from Supabase")
) -> str:
    """Extract user ID from Supabase JWT token."""
    try:
        token = authorization.split(" ")[1]  # Remove 'Bearer ' prefix
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded.get("sub")
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid authorization token")

async def stream_generator(
    service: FactsheetService,
    contract_id: str,
    question_keys: list[str],
    factsheet_id: str
) -> AsyncGenerator[bytes, None]:
    """Generate streaming response for factsheet generation."""
    try:
        for question_key in question_keys:
            if question_key not in FACTSHEET_QUESTIONS:
                yield json.dumps({
                    "error": f"Invalid question key: {question_key}",
                    "is_complete": True
                }).encode() + b"\n"
                return

            async for response in service.generate_answer(
                contract_id,
                question_key,
                factsheet_id
            ):
                yield json.dumps(response.dict()).encode() + b"\n"

    except Exception as e:
        yield json.dumps({
            "error": str(e),
            "is_complete": True
        }).encode() + b"\n"

@router.post("/contracts/{contract_id}/factsheet/generate")
async def generate_factsheet(
    contract_id: str,
    request: FactsheetRequest,
    service: FactsheetService = Depends(get_factsheet_service),
    user_id: str = Depends(get_user_id)
):
    """Generate a factsheet for a contract with streaming response."""
    try:
        logger.info(f"Generating factsheet for contract {contract_id}")
        logger.info(f"Request: {request.dict()}")

        # Verify contract access
        if not service.verify_contract_access(contract_id, user_id):
            logger.error(f"Contract access verification failed for user {user_id}")
            raise HTTPException(status_code=404, detail="Contract not found")

        # Create or update factsheet
        factsheet_id = service.create_or_update_factsheet(contract_id, user_id)
        logger.info(f"Created/Updated factsheet {factsheet_id}")

        # Return streaming response
        return StreamingResponse(
            stream_generator(
                service,
                contract_id,
                request.question_keys,
                factsheet_id
            ),
            media_type="application/x-ndjson"
        )

    except Exception as e:
        logger.error(f"Error generating factsheet: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
