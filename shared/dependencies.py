import os
from fastapi import HTTPException, Depends
from supabase.client import Client, create_client
from supabase.lib.client_options import ClientOptions

from document_converter.rag_processor import RAGProcessor

def get_supabase_client() -> Client:
    """Get Supabase client instance."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise HTTPException(
            status_code=500,
            detail="Supabase configuration missing"
        )
    options = ClientOptions(
        postgrest_client_timeout=30,
        storage_client_timeout=30
    )
    return create_client(supabase_url, supabase_key, options=options)

def get_rag_processor(supabase=Depends(get_supabase_client)):
    """Initialize RAG processor with environment variables."""
    return RAGProcessor(supabase)
