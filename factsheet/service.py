from typing import AsyncGenerator, Dict, Any, List, Optional
import uuid
from datetime import datetime
import logging
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from contextlib import asynccontextmanager
from supabase.client import Client
from document_converter.rag_processor import RAGProcessor
from .schema import FACTSHEET_QUESTIONS, StreamingFactsheetResponse
from postgrest import APIError
import os
import redis.asyncio as redis
import json

logger = logging.getLogger(__name__)


class FactsheetService:
    def __init__(self, supabase: Client, rag_processor: RAGProcessor):
        """Initialize the factsheet service."""
        self.supabase = supabase
        self.rag_processor = rag_processor
        # Initialize Redis client using the same connection as Celery
        self.redis = redis.from_url(
            os.environ.get("REDIS_HOST", "redis://localhost:6379/0")
        )

    async def cleanup(self):
        """Cleanup resources."""
        # No cleanup needed

    async def verify_contract_access(self, contract_id: str, user_id: str) -> bool:
        """Verify that the user has access to the specified contract."""
        result = (
            self.supabase.table("contracts")
            .select("id")
            .eq("id", contract_id)
            .eq("owner_id", user_id)
            .execute()
        )
        return bool(result.data)

    async def get_relevant_context(self, contract_id: str, question: str) -> List[str]:
        """Get relevant context for a question using RAG."""
        try:
            logger.info(f"Getting embedding for question: {question}")
            question_embedding = await self.rag_processor.get_embedding(question)
            logger.info("Querying citations for relevant context")

            # Use the match_citations function instead of direct vector query
            query = self.supabase.rpc(
                "match_citations",
                {
                    "query_embedding": question_embedding,
                    "match_count": 5,
                    "target_contract_id": contract_id,
                },
            )
            result = query.execute()

            if not result.data:
                logger.warning(
                    f"No relevant citations found for contract {contract_id}"
                )
                return []

            logger.info(f"Found {len(result.data)} relevant citations")
            return [item["content"] for item in result.data]
        except Exception as e:
            logger.error(f"Error getting relevant context: {str(e)}", exc_info=True)
            # Return empty context rather than failing completely
            return []

    async def create_or_update_factsheet(self, contract_id: str, user_id: str) -> str:
        """Create a new factsheet or update existing one and return its ID."""
        # Check for existing factsheet
        result = (
            self.supabase.table("factsheets")
            .select("id")
            .eq("contract_id", contract_id)
            .execute()
        )
        now = datetime.utcnow().isoformat()

        if result.data:
            # Update existing factsheet
            factsheet_id = result.data[0]["id"]
            self.supabase.table("factsheets").update(
                {
                    "updated_at": now,
                    "owner_id": user_id,  # Update owner in case it changed
                }
            ).eq("id", factsheet_id).execute()
        else:
            # Create new factsheet
            factsheet_id = str(uuid.uuid4())
            self.supabase.table("factsheets").insert(
                {
                    "id": factsheet_id,
                    "contract_id": contract_id,
                    "created_at": now,
                    "updated_at": now,
                    "created_by": user_id,
                    "owner_id": user_id,
                }
            ).execute()

        # Clear existing answers for this factsheet
        self.supabase.table("factsheet_answers").delete().eq(
            "factsheet_id", factsheet_id
        ).execute()

        return factsheet_id

    async def create_factsheet(self, contract_id: str, user_id: str) -> str:
        """Create a new factsheet or return existing one and update its timestamp."""
        try:
            # Try to create new factsheet
            factsheet_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat()
            self.supabase.table("factsheets").insert(
                {
                    "id": factsheet_id,
                    "contract_id": contract_id,
                    "created_at": now,
                    "updated_at": now,
                    "created_by": user_id,
                    "owner_id": user_id,
                }
            ).execute()
            return factsheet_id
        except APIError as e:
            if getattr(e, "code", None) == "23505":  # Unique violation
                # Get existing factsheet
                result = (
                    self.supabase.table("factsheets")
                    .select("id")
                    .eq("contract_id", contract_id)
                    .single()
                    .execute()
                )
                if result.data:
                    factsheet_id = result.data["id"]
                    # Update timestamp
                    self.supabase.table("factsheets").update(
                        {"updated_at": datetime.utcnow().isoformat()}
                    ).eq("id", factsheet_id).execute()
                    return factsheet_id
            raise  # Re-raise if it's not a unique violation or factsheet not found

    async def save_answer(self, factsheet_id: str, question_key: str, answer: str):
        """Save or update an answer for a factsheet question."""
        try:
            # Try to create new answer
            self.supabase.table("factsheet_answers").insert(
                {
                    "factsheet_id": factsheet_id,
                    "question_key": question_key,
                    "answer": answer,
                    "last_updated": datetime.utcnow().isoformat(),
                }
            ).execute()
        except APIError as e:
            if getattr(e, "code", None) == "23505":  # Unique violation
                # Update existing answer
                self.supabase.table("factsheet_answers").update(
                    {"answer": answer, "last_updated": datetime.utcnow().isoformat()}
                ).eq("factsheet_id", factsheet_id).eq(
                    "question_key", question_key
                ).execute()
            else:
                raise  # Re-raise if it's not a unique violation

    async def store_update(self, job_id: str, question_key: str, content: str) -> None:
        """Store a streaming update in Redis."""
        try:
            key = f"factsheet:updates:{job_id}"
            # logger.info(f"Storing update in Redis for key: {key}")

            # Get existing updates
            updates = await self.redis.get(key)
            if updates:
                updates = json.loads(updates)
            else:
                updates = []

            # Add new update
            updates.append({"key": question_key, "content": content})
            # logger.info(f"Total updates for job {job_id}: {len(updates)}")

            # Store with 1-hour TTL
            await self.redis.setex(key, 3600, json.dumps(updates))
        except Exception as e:
            logger.error(f"Error storing update in Redis: {str(e)}", exc_info=True)
            raise

    async def get_job_updates(self, job_id: str, cursor: int = 0) -> List[Dict]:
        """Get updates after the given cursor."""
        try:
            key = f"factsheet:updates:{job_id}"
            logger.info(f"Getting updates from Redis for key: {key}, cursor: {cursor}")

            # Get all updates
            updates = await self.redis.get(key)
            if not updates:
                logger.info(f"No updates found for job {job_id}")
                return []

            updates = json.loads(updates)
            # logger.info(f"Found {len(updates)} total updates, returning from cursor {cursor}")

            # Return updates after cursor
            return updates[cursor:]
        except Exception as e:
            logger.error(f"Error getting updates from Redis: {str(e)}", exc_info=True)
            return []

    async def generate_answer(
        self, contract_id: str, question_key: str, factsheet_id: str
    ) -> AsyncGenerator[StreamingFactsheetResponse, None]:
        """Generate an answer for a single question."""
        try:
            logger.info(f"Generating answer for question {question_key}")
            # Prepare context for the question
            context = await self.get_relevant_context(
                contract_id, FACTSHEET_QUESTIONS[question_key]["question"]
            )
            logger.debug(f"Context prepared for {question_key}: {len(context)} chars")

            # Stream the response
            async for chunk in self.rag_processor.stream(f"""
            {FACTSHEET_QUESTIONS[question_key]["prompt"]}
            
            Contract excerpts:
            {context}
            """):
                logger.debug(f"Received chunk for {question_key}: {len(chunk)} chars")
                yield StreamingFactsheetResponse(
                    answers={question_key: chunk}, is_complete=False
                )

            # Send final chunk with is_complete=True
            yield StreamingFactsheetResponse(
                answers={question_key: chunk}, is_complete=True
            )
        except Exception as e:
            logger.error(f"Error generating answer: {str(e)}", exc_info=True)
            raise
