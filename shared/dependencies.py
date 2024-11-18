import os
from fastapi import HTTPException
from supabase import create_client

from document_converter.rag_processor import RAGProcessor

def get_rag_processor():
    """Initialize RAG processor with environment variables."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise HTTPException(
            status_code=500,
            detail="Supabase configuration missing"
        )
    supabase = create_client(supabase_url, supabase_key)
    return RAGProcessor(supabase)
