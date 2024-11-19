from datetime import datetime
from enum import Enum
from functools import lru_cache
import re
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from uuid import UUID
import os
import asyncio
import json

import numpy as np
from sentence_transformers import SentenceTransformer
from postgrest import APIError
import httpx

class ValidationError(Exception):
    pass

class ProcessingError(Exception):
    pass

class ProcessingStep(Enum):
    """
    Corresponds to database enum type 'processing_step'
    CREATE TYPE processing_step AS ENUM ('conversion', 'embedding', 'storage')
    """
    CONVERSION = "conversion"  # Initial step: Converting document to processable format
    EMBEDDING = "embedding"    # Second step: Generating embeddings for chunks
    STORAGE = "storage"       # Final step: Storing embeddings in database

class ProcessingStatus(Enum):
    """
    Corresponds to database enum type 'processing_status'
    CREATE TYPE processing_status AS ENUM ('pending', 'running', 'ready', 'failed')
    
    State transitions:
    - PENDING -> RUNNING: When processing begins
    - RUNNING -> READY: When all steps complete successfully
    - RUNNING -> FAILED: If any step fails
    - FAILED -> PENDING: On retry
    """
    PENDING = "pending"       # Initial state, ready to process
    RUNNING = "running"      # Currently being processed
    READY = "ready"         # Successfully completed all steps
    FAILED = "failed"       # Failed during processing

class RAGProcessor:
    DEFAULT_MODEL = "BAAI/bge-small-en"  # Changed from "BAAI/bge-m3" to reduce memory usage
    OLLAMA_BASE_URL = "http://localhost:11434"
    LLM_MODEL = "llama3.1:8b"  # Using llama3.1 8B model
    
    def __init__(self, supabase_client, model_name: Optional[str] = None):
        """
        Initialize the RAG Processor
        
        Args:
            supabase_client: Initialized Supabase client
            model_name: Optional model name to use for embeddings. 
                       Defaults to BAAI/bge-m3
        """
        self.supabase = supabase_client
        self.model = SentenceTransformer(model_name or self.DEFAULT_MODEL)
        self.MAX_BATCH_SIZE = 10
        self.http_client = httpx.AsyncClient()

    async def get_source_details(self, source_id: int) -> Tuple[Dict, Dict, UUID]:
        """
        Get source details and validate relationships
        
        Args:
            source_id: ID of the source to process
            
        Returns:
            Tuple of (source record, contract record, creator_id)
            
        Raises:
            ValidationError: If source not found or invalid state
        """
        # Get source with contract details
        source = self.supabase.table('sources').select(
            '*',
            count='exact'
        ).eq('id', source_id).execute()
        
        if not source.data:
            raise ValidationError(f"Source {source_id} not found")
            
        source_record = source.data[0]
        
        # Check source status
        if source_record['status'] not in [ProcessingStatus.PENDING.value, ProcessingStatus.RUNNING.value]:
            raise ValidationError(f"Source {source_id} is not in a processable state (current state: {source_record['status']})")
            
        # Get contract
        contract = self.supabase.table('contracts').select(
            '*',
            count='exact'
        ).eq('id', source_record['contract_id']).execute()
        
        if not contract.data:
            raise ValidationError(f"Contract {source_record['contract_id']} not found")
            
        contract_record = contract.data[0]
        creator_id = UUID(source_record['owner_id'])
        
        return source_record, contract_record, creator_id

    async def initialize_processing(
        self,
        source_id: int,
    ) -> Tuple[UUID, Dict, Dict, UUID]:
        """
        Initialize processing for a source
        
        Args:
            source_id: ID of the source to process
            
        Returns:
            Tuple of (conversion_id, source record, contract record, creator_id)
            
        Raises:
            ValidationError: If initialization fails
        """
        try:
            # Get and validate source details
            source, contract, creator_id = await self.get_source_details(source_id)
            
            # Create conversion record
            processing_record = {
                "filename": source['file_name'],
                "content_type": source['file_metadata'].get('contentType', 'text/plain'),
                "creator_id": str(creator_id),
                "status": ProcessingStatus.PENDING.value,
                "current_step": ProcessingStep.CONVERSION.value,
                "total_chunks": 0,
                "processed_chunks": 0,
                "started_at": datetime.utcnow().isoformat(),
                "error_message": None
            }

            response = self.supabase.table('conversions').insert(processing_record).execute()
            if not response.data:
                raise ValidationError("Failed to initialize conversion record")
            
            conversion_id = UUID(response.data[0]['id'])
            
            # Update source status
            self.supabase.table('sources').update(
                {"status": ProcessingStatus.RUNNING.value}
            ).eq('id', source_id).execute()
            
            return conversion_id, source, contract, creator_id
            
        except Exception as e:
            # Ensure source status is reset on error
            try:
                self.supabase.table('sources').update(
                    {"status": "pending"}
                ).eq('id', source_id).execute()
            except:
                pass
            raise ProcessingError(f"Failed to initialize processing: {str(e)}")

    async def process_source(
        self,
        source_id: int,
        markdown_content: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a source document
        
        Args:
            source_id: ID of the source to process
            markdown_content: The markdown content to process
            
        Yields:
            Dict containing progress updates and status information
            
        Raises:
            ValidationError: If source validation fails
            ProcessingError: If processing fails
        """
        try:
            # Initialize processing
            conversion_id, source, contract, creator_id = await self.initialize_processing(source_id)
            
            # Process the document
            async for update in self.process_document(
                markdown_content=markdown_content,
                conversion_id=conversion_id,
                creator_id=creator_id,
                contract_id=UUID(contract['id']),
                source_id=source_id,
                metadata={
                    "source_name": source['file_name'],
                    "contract_name": contract['name'],
                    "file_metadata": source['file_metadata']
                }
            ):
                yield update
                
            # Update source status on success
            self.supabase.table('sources').update(
                {"status": ProcessingStatus.READY.value}
            ).eq('id', source_id).execute()
            
        except Exception as e:
            # Update source status on error
            try:
                self.supabase.table('sources').update(
                    {"status": ProcessingStatus.FAILED.value, "error_message": str(e)}
                ).eq('id', source_id).execute()
            except:
                pass
            raise ProcessingError(f"Failed to process source: {str(e)}")

    async def validate_relationships(
        self,
        creator_id: UUID,
        contract_id: UUID,
        source_id: int
    ) -> Tuple[Dict, Dict, Dict]:
        """
        Validate that:
        1. The creator exists in profiles
        2. The contract exists
        3. The source exists and belongs to the specified contract
        4. The creator has access to the contract/source
        
        Args:
            creator_id: UUID of the user
            contract_id: UUID of the contract
            source_id: ID of the source
            
        Returns:
            Tuple of (profile, contract, source) records
            
        Raises:
            ValidationError: If any validation fails
        """
        # Check profile exists
        profile = self.supabase.table('profiles').select('*').eq('id', str(creator_id)).execute()
        if not profile.data:
            raise ValidationError(f"Profile {creator_id} not found")
        
        # Check contract exists and user has access
        contract = self.supabase.table('contracts').select('*').eq('id', str(contract_id)).execute()
        if not contract.data:
            raise ValidationError(f"Contract {contract_id} not found")
            
        # Check source exists and belongs to contract
        source = self.supabase.table('sources').select('*').eq('id', source_id).execute()
        if not source.data:
            raise ValidationError(f"Source {source_id} not found")
        if str(source.data[0]['contract_id']) != str(contract_id):
            raise ValidationError(f"Source {source_id} does not belong to contract {contract_id}")
            
        return profile.data[0], contract.data[0], source.data[0]

    async def initialize_document_processing(
        self,
        filename: str,
        content_type: str,
        creator_id: UUID,
    ) -> UUID:
        """
        Initialize document processing and return conversion ID.
        
        Args:
            filename: Name of the file being processed
            content_type: MIME type of the file
            creator_id: UUID of the user creating this conversion
            
        Returns:
            UUID: The conversion ID
            
        Raises:
            ValidationError: If creator_id is invalid
            APIError: If database operation fails
        """
        # Validate creator exists
        profile = self.supabase.table('profiles').select('*').eq('id', str(creator_id)).execute()
        if not profile.data:
            raise ValidationError(f"Profile {creator_id} not found")

        processing_record = {
            "filename": filename,
            "content_type": content_type,
            "creator_id": str(creator_id),
            "status": ProcessingStatus.PENDING.value,
            "current_step": ProcessingStep.CONVERSION.value,
            "total_chunks": 0,
            "processed_chunks": 0,
            "started_at": datetime.utcnow().isoformat(),
            "error_message": None
        }

        try:
            response = self.supabase.table('conversions').insert(processing_record).execute()
            if not response.data:
                raise ValidationError("Failed to initialize conversion record")
            
            conversion_id = UUID(response.data[0]['id'])
            return conversion_id
        except APIError as e:
            raise ValidationError(f"Database error: {str(e)}")

    async def update_processing_status(
        self,
        conversion_id: UUID,  # This is the conversion ID, not document ID
        status: ProcessingStatus,
        step: Optional[ProcessingStep] = None,
        processed_chunks: Optional[int] = None,
        total_chunks: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> None:
        """
        Update processing status in database
        
        Args:
            conversion_id: ID of the conversion record (not document ID)
            status: New processing status
            step: Current processing step
            processed_chunks: Number of chunks processed
            total_chunks: Total number of chunks
            error_message: Optional error message
        """
        update_data = {"status": status.value}
        
        if step is not None:
            update_data["current_step"] = step.value
        if processed_chunks is not None:
            update_data["processed_chunks"] = processed_chunks
        if total_chunks is not None:
            update_data["total_chunks"] = total_chunks
        if error_message is not None:
            update_data["error_message"] = error_message
            
        response = self.supabase.table('conversions').update(update_data).eq("id", str(conversion_id)).execute()
        if not response.data:
            raise Exception(f"Failed to update conversion status for ID {conversion_id}")

    async def get_processing_status(self, conversion_id: UUID) -> Dict[str, Any]:
        """
        Get current processing status
        
        Args:
            conversion_id: ID of the conversion record (not document ID)
            
        Returns:
            Dict containing the conversion record
        """
        response = self.supabase.table('conversions').select("*").eq("id", str(conversion_id)).execute()
        if not response.data:
            raise Exception(f"No conversion record found for ID {conversion_id}")
        return response.data[0]

    def preprocess_legal_text(self, text: str) -> str:
        """
        Preprocess text specifically for legal documents
        """
        # Remove multiple spaces and normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Preserve common legal document structures
        text = re.sub(r'(?i)(section|article)\s+(\d+)', r'\n\1 \2', text)
        text = re.sub(r'(?i)(whereas|wherefore|now,\s+therefore)', r'\n\1', text)
        
        # Handle numbered lists and sublists
        text = re.sub(r'(\d+\.\d+|\d+\.)\s+', r'\n\1 ', text)
        
        # Preserve paragraph breaks
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        return text.strip()

    def split_text(self, text: str) -> List[str]:
        """
        Split text into chunks with overlap, optimized for legal documents and BGE-M3
        """
        # Preprocess the text
        text = self.preprocess_legal_text(text)
        
        # Split into semantic units (paragraphs, sections, etc.)
        units = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        chunks = []
        current_chunk = []
        current_size = 0
        
        for unit in units:
            unit_size = len(unit.split())
            
            # If unit itself is too large, split it further
            if unit_size > 750:
                sentences = re.split(r'(?<=[.!?])\s+', unit)
                for sentence in sentences:
                    sentence_size = len(sentence.split())
                    if current_size + sentence_size > 750 and current_chunk:
                        chunks.append(' '.join(current_chunk))
                        # Calculate overlap
                        overlap_tokens = []
                        overlap_size = 0
                        for token in reversed(current_chunk):
                            if overlap_size + len(token.split()) > 150:
                                break
                            overlap_tokens.insert(0, token)
                            overlap_size += len(token.split())
                        current_chunk = overlap_tokens
                        current_size = overlap_size
                    current_chunk.append(sentence)
                    current_size += sentence_size
            else:
                if current_size + unit_size > 750 and current_chunk:
                    chunks.append(' '.join(current_chunk))
                    # Calculate overlap
                    overlap_tokens = []
                    overlap_size = 0
                    for token in reversed(current_chunk):
                        if overlap_size + len(token.split()) > 150:
                            break
                        overlap_tokens.insert(0, token)
                        overlap_size += len(token.split())
                    current_chunk = overlap_tokens
                    current_size = overlap_size
                current_chunk.append(unit)
                current_size += unit_size

        # Add the last chunk if there's anything left
        if current_chunk:
            chunks.append(' '.join(current_chunk))

        return chunks

    @lru_cache(maxsize=1024)
    def _get_embedding(self, text: str) -> np.ndarray:
        """
        Generate and cache embeddings for a single text chunk
        """
        return self.model.encode(text)

    async def get_embedding(self, text: str) -> str:
        """
        Get embedding for text and return as a string for Supabase.
        """
        embedding = self._get_embedding(text)
        return f"[{','.join(str(float(x)) for x in embedding.flatten())}]"

    def _process_chunk_batch(self, chunks: List[str]) -> List[np.ndarray]:
        """
        Process a batch of chunks in parallel
        """
        prefixed_chunks = [f"Represent this legal text for retrieval: {chunk}" for chunk in chunks]
        embeddings = self.model.encode(prefixed_chunks, normalize_embeddings=True)
        # Ensure we always return a list of 1D arrays
        if len(chunks) == 1:
            return [embeddings]
        return [embedding for embedding in embeddings]

    async def process_document(
        self,
        markdown_content: str,
        conversion_id: UUID,
        creator_id: UUID,
        contract_id: UUID,
        source_id: int,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a document and store results directly in the database
        
        Args:
            markdown_content: The markdown content to process
            conversion_id: ID of the conversion record
            creator_id: UUID of the user creating these citations
            contract_id: UUID of the contract these citations belong to
            source_id: ID of the source these citations belong to
            metadata: Additional metadata to store with the citations
            
        Yields:
            Dict containing progress updates and status information
            
        Raises:
            ValidationError: If any relationships are invalid
            APIError: If database operations fail
        """
        try:
            # Validate all relationships first
            profile, contract, source = await self.validate_relationships(
                creator_id,
                contract_id,
                source_id
            )
            
            # Update status to processing
            await self.update_processing_status(
                conversion_id,
                ProcessingStatus.RUNNING,
                ProcessingStep.CONVERSION
            )
            
            # Delete existing citations for this source
            try:
                self.supabase.table('citations').delete().eq('source_id', source_id).execute()
            except Exception as e:
                raise ValidationError(f"Failed to clean up existing citations: {str(e)}")
            
            # Split the text into chunks
            chunks = self.split_text(markdown_content)
            total_chunks = len(chunks)
            
            # Update total chunks
            await self.update_processing_status(
                conversion_id,
                ProcessingStatus.RUNNING,
                ProcessingStep.EMBEDDING,
                total_chunks=total_chunks
            )
            
            yield {
                "type": "progress",
                "step": ProcessingStep.EMBEDDING.value,
                "processed": 0,
                "total": total_chunks,
                "conversion_id": str(conversion_id)
            }
            
            processed_chunks = 0
            
            # Process chunks in batches
            for i in range(0, len(chunks), self.MAX_BATCH_SIZE):
                batch = chunks[i:i + self.MAX_BATCH_SIZE]
                
                try:
                    # Generate embeddings
                    batch_embeddings = self._process_chunk_batch(batch)
                    
                    # Prepare citations
                    batch_citations = []
                    for chunk, embedding in zip(batch, batch_embeddings):
                        citation = {
                            "conversion_id": str(conversion_id),
                            "creator_id": str(creator_id),
                            "contract_id": str(contract_id),
                            "source_id": source_id,
                            "content": chunk,
                            "embedding": f"[{','.join(str(float(x)) for x in embedding.flatten())}]",
                            "metadata": {
                                **metadata,
                                "chunk_size": len(chunk.split()),
                                "chunk_number": processed_chunks + 1,
                                "total_chunks": total_chunks,
                                "source_name": source["file_name"],
                                "contract_name": contract["name"],
                                "processing_timestamp": datetime.utcnow().isoformat()
                            }
                        }
                        batch_citations.append(citation)
                    
                    # Store citations
                    try:
                        response = self.supabase.table('citations').insert(batch_citations).execute()
                        if not response.data:
                            raise ValidationError("Failed to store citations in database")
                    except APIError as e:
                        raise ValidationError(f"Database error storing citations: {str(e)}")
                    
                    processed_chunks += len(batch)
                    
                    # Update progress
                    await self.update_processing_status(
                        conversion_id,
                        ProcessingStatus.RUNNING,
                        ProcessingStep.STORAGE,
                        processed_chunks=processed_chunks
                    )
                    
                    yield {
                        "type": "progress",
                        "step": ProcessingStep.STORAGE.value,
                        "processed": processed_chunks,
                        "total": total_chunks,
                        "conversion_id": str(conversion_id)
                    }
                    
                except Exception as e:
                    error_msg = f"Error processing batch: {str(e)}"
                    await self.update_processing_status(
                        conversion_id,
                        ProcessingStatus.FAILED,
                        error_message=error_msg
                    )
                    yield {
                        "type": "error",
                        "message": error_msg,
                        "step": ProcessingStep.STORAGE.value,
                        "conversion_id": str(conversion_id)
                    }
                    raise
            
            # Mark as completed
            await self.update_processing_status(
                conversion_id,
                ProcessingStatus.READY,
                processed_chunks=processed_chunks
            )
            
            yield {
                "type": "complete",
                "conversion_id": str(conversion_id),
                "total_chunks": total_chunks,
                "metadata": metadata
            }
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            if 'conversion_id' in locals():
                await self.update_processing_status(
                    conversion_id,
                    ProcessingStatus.FAILED,
                    error_message=error_msg
                )
            yield {
                "type": "error",
                "message": error_msg,
                "step": ProcessingStep.STORAGE.value,
                "conversion_id": str(conversion_id) if 'conversion_id' in locals() else None
            }
            raise

    async def stream(self, prompt: str) -> AsyncGenerator[str, None]:
        """Stream responses from the LLM using Ollama."""
        try:
            async with self.http_client.stream(
                'POST',
                f"{self.OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": self.LLM_MODEL,
                    "prompt": prompt,
                    "stream": True
                },
                timeout=None
            ) as response:
                async for line in response.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        if "response" in data:
                            yield data["response"]
                        if data.get("done", False):
                            break
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            raise ValidationError(f"Error streaming from Ollama: {str(e)}")
