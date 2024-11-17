from typing import List, Dict, Any, Optional
import os
import json
from supabase import create_client, Client
from sentence_transformers import SentenceTransformer

class RAGProcessor:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        # Use BGE-M3 for better semantic understanding and matching vector dimensions
        self.model = SentenceTransformer('BAAI/bge-m3')
        self.chunk_size = 500
        self.chunk_overlap = 50

    def split_text(self, text: str) -> List[str]:
        """
        Split text into chunks with overlap, optimized for BGE-M3 model
        which handles longer sequences better
        """
        # Split text into paragraphs first
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        chunks = []
        current_chunk = []
        current_size = 0

        for paragraph in paragraphs:
            # If adding this paragraph would exceed chunk size, save current chunk
            if current_size + len(paragraph.split()) > self.chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                # Keep last part for overlap
                overlap_size = 0
                overlap_chunk = []
                for p in reversed(current_chunk):
                    if overlap_size + len(p.split()) > self.chunk_overlap:
                        break
                    overlap_chunk.insert(0, p)
                    overlap_size += len(p.split())
                current_chunk = overlap_chunk
                current_size = overlap_size

            current_chunk.append(paragraph)
            current_size += len(paragraph.split())

        # Add the last chunk if there's anything left
        if current_chunk:
            chunks.append(' '.join(current_chunk))

        return chunks

    def process_markdown(self, 
                        markdown_content: str, 
                        metadata: Dict[str, Any],
                        creator_id: Optional[str] = None,
                        contract_id: Optional[str] = None,
                        source_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Process markdown content into chunks and generate embeddings using BGE-M3
        
        Args:
            markdown_content: The markdown text to process
            metadata: Additional metadata to store
            creator_id: ID of the user who created/uploaded the document
            contract_id: Associated contract ID if applicable
            source_id: Associated source ID if applicable
        """
        # Split the text into chunks
        chunks = self.split_text(markdown_content)
        
        # Generate embeddings for each chunk
        # BGE-M3 works best with a prefix for retrieval tasks
        chunks_with_prefix = [f"Represent this text for retrieval: {chunk}" for chunk in chunks]
        embeddings = self.model.encode(chunks_with_prefix, normalize_embeddings=True)
        
        # Prepare data for Supabase
        snippets = []
        for chunk, embedding in zip(chunks, embeddings):
            snippet = {
                "content": chunk,
                "embedding": embedding.tolist(),  # Convert numpy array to list
                "file_name": metadata.get("filename"),
                "creator_id": creator_id,
                "contract_id": contract_id,
                "source_id": source_id,
                "metadata": json.dumps(metadata)  # Store additional metadata as JSON
            }
            snippets.append(snippet)
            
        return snippets

    async def store_snippets(self, snippets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Store snippets with their embeddings in Supabase
        """
        # Insert all snippets into the 'snippets' table
        result = await self.supabase.table('snippets').insert(snippets).execute()
        return result
