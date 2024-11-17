from typing import List, Dict, Any, Optional, Tuple, AsyncGenerator
import os
import json
import re
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from sentence_transformers import SentenceTransformer
import numpy as np
import asyncio
from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

class ProcessingStep(Enum):
    CONVERSION = "conversion"
    EMBEDDING = "embedding"
    STORAGE = "storage"

class ProcessingStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class RAGProcessor:
    DEFAULT_CHUNK_SIZE = 750
    DEFAULT_CHUNK_OVERLAP = 150
    MAX_BATCH_SIZE = 10

    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.model = SentenceTransformer('BAAI/bge-m3')
        self.chunk_size = self.DEFAULT_CHUNK_SIZE
        self.chunk_overlap = self.DEFAULT_CHUNK_OVERLAP
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def initialize_document_processing(
        self,
        filename: str,
        content_type: str,
        creator_id: Optional[str] = None,
        contract_id: Optional[str] = None,
        source_id: Optional[int] = None
    ) -> UUID:
        """Initialize document processing and create a processing record"""
        document_id = uuid4()
        
        processing_record = {
            "document_id": str(document_id),
            "filename": filename,
            "content_type": content_type,
            "creator_id": creator_id,
            "contract_id": contract_id,
            "source_id": source_id,
            "status": ProcessingStatus.PENDING.value,
            "current_step": ProcessingStep.CONVERSION.value,
            "total_chunks": 0,
            "processed_chunks": 0,
            "started_at": datetime.utcnow().isoformat(),
            "error_message": None
        }
        
        await self.supabase.table('document_processing').insert(processing_record).execute()
        return document_id

    async def update_processing_status(
        self,
        document_id: UUID,
        status: ProcessingStatus,
        step: Optional[ProcessingStep] = None,
        processed_chunks: Optional[int] = None,
        total_chunks: Optional[int] = None,
        error_message: Optional[str] = None
    ):
        """Update the processing status of a document"""
        update_data = {"status": status.value}
        if step:
            update_data["current_step"] = step.value
        if processed_chunks is not None:
            update_data["processed_chunks"] = processed_chunks
        if total_chunks is not None:
            update_data["total_chunks"] = total_chunks
        if error_message:
            update_data["error_message"] = error_message
        if status == ProcessingStatus.COMPLETED:
            update_data["completed_at"] = datetime.utcnow().isoformat()
        
        await self.supabase.table('document_processing').update(update_data).eq("document_id", str(document_id)).execute()

    async def get_processing_status(self, document_id: UUID) -> Dict[str, Any]:
        """Get the current processing status of a document"""
        response = await self.supabase.table('document_processing').select("*").eq("document_id", str(document_id)).execute()
        if not response.data:
            raise ValueError(f"No processing record found for document {document_id}")
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
            if unit_size > self.chunk_size:
                sentences = re.split(r'(?<=[.!?])\s+', unit)
                for sentence in sentences:
                    sentence_size = len(sentence.split())
                    if current_size + sentence_size > self.chunk_size and current_chunk:
                        chunks.append(' '.join(current_chunk))
                        # Calculate overlap
                        overlap_tokens = []
                        overlap_size = 0
                        for token in reversed(current_chunk):
                            if overlap_size + len(token.split()) > self.chunk_overlap:
                                break
                            overlap_tokens.insert(0, token)
                            overlap_size += len(token.split())
                        current_chunk = overlap_tokens
                        current_size = overlap_size
                    current_chunk.append(sentence)
                    current_size += sentence_size
            else:
                if current_size + unit_size > self.chunk_size and current_chunk:
                    chunks.append(' '.join(current_chunk))
                    # Calculate overlap
                    overlap_tokens = []
                    overlap_size = 0
                    for token in reversed(current_chunk):
                        if overlap_size + len(token.split()) > self.chunk_overlap:
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

    @lru_cache(maxsize=1000)
    def _get_embedding(self, text: str) -> np.ndarray:
        """
        Generate and cache embeddings for a single text chunk
        """
        prefixed_text = f"Represent this legal text for retrieval: {text}"
        return self.model.encode(prefixed_text, normalize_embeddings=True)

    def _process_chunk_batch(self, chunks: List[str]) -> List[np.ndarray]:
        """
        Process a batch of chunks in parallel
        """
        prefixed_chunks = [f"Represent this legal text for retrieval: {chunk}" for chunk in chunks]
        return self.model.encode(prefixed_chunks, normalize_embeddings=True)

    async def process_document(
        self,
        markdown_content: str,
        document_id: UUID,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a document and store results directly in the database"""
        try:
            # Update status to processing
            await self.update_processing_status(
                document_id,
                ProcessingStatus.PROCESSING,
                ProcessingStep.CONVERSION
            )
            
            # Split the text into chunks
            chunks = self.split_text(markdown_content)
            total_chunks = len(chunks)
            
            # Update total chunks
            await self.update_processing_status(
                document_id,
                ProcessingStatus.PROCESSING,
                ProcessingStep.EMBEDDING,
                total_chunks=total_chunks
            )
            
            yield {
                "type": "progress",
                "step": ProcessingStep.EMBEDDING.value,
                "processed": 0,
                "total": total_chunks,
                "document_id": str(document_id)
            }
            
            processed_chunks = 0
            
            # Process chunks in batches
            for i in range(0, len(chunks), self.MAX_BATCH_SIZE):
                batch = chunks[i:i + self.MAX_BATCH_SIZE]
                
                try:
                    # Generate embeddings
                    batch_embeddings = self._process_chunk_batch(batch)
                    
                    # Prepare snippets
                    batch_snippets = []
                    for chunk, embedding in zip(batch, batch_embeddings):
                        snippet = {
                            "document_id": str(document_id),
                            "content": chunk,
                            "embedding": embedding.tolist(),
                            "metadata": json.dumps({
                                **metadata,
                                "chunk_size": len(chunk.split()),
                                "chunk_number": processed_chunks + 1,
                                "total_chunks": total_chunks,
                                "processing_timestamp": datetime.utcnow().isoformat()
                            })
                        }
                        batch_snippets.append(snippet)
                    
                    # Store snippets
                    await self.supabase.table('snippets').insert(batch_snippets).execute()
                    
                    processed_chunks += len(batch)
                    
                    # Update progress
                    await self.update_processing_status(
                        document_id,
                        ProcessingStatus.PROCESSING,
                        ProcessingStep.STORAGE,
                        processed_chunks=processed_chunks
                    )
                    
                    yield {
                        "type": "progress",
                        "step": ProcessingStep.STORAGE.value,
                        "processed": processed_chunks,
                        "total": total_chunks,
                        "document_id": str(document_id)
                    }
                    
                except Exception as e:
                    error_msg = f"Error processing batch: {str(e)}"
                    await self.update_processing_status(
                        document_id,
                        ProcessingStatus.FAILED,
                        error_message=error_msg
                    )
                    yield {
                        "type": "error",
                        "message": error_msg,
                        "step": ProcessingStep.STORAGE.value,
                        "document_id": str(document_id)
                    }
                    raise
            
            # Mark as completed
            await self.update_processing_status(
                document_id,
                ProcessingStatus.COMPLETED,
                processed_chunks=processed_chunks
            )
            
            yield {
                "type": "complete",
                "document_id": str(document_id),
                "total_chunks": total_chunks,
                "metadata": metadata
            }
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            await self.update_processing_status(
                document_id,
                ProcessingStatus.FAILED,
                error_message=error_msg
            )
            yield {
                "type": "error",
                "message": error_msg,
                "step": ProcessingStep.STORAGE.value,
                "document_id": str(document_id)
            }
            raise
