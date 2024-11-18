from typing import Dict, List, Optional
from pydantic import BaseModel, UUID4
from datetime import datetime


class FactsheetRequest(BaseModel):
    question_keys: List[str]
    job_id: str


class StreamingFactsheetResponse(BaseModel):
    answers: Dict[str, str]
    error: Optional[str] = None


class FactsheetAnswer(BaseModel):
    question_key: str
    answer: str
    last_updated: datetime


class Factsheet(BaseModel):
    id: UUID4
    contract_id: UUID4
    answers: List[FactsheetAnswer]
    created_at: datetime
    updated_at: datetime


class GenerateFactsheetRequest(BaseModel):
    question_keys: List[str]
    job_id: str


# Predefined questions and prompts
FACTSHEET_QUESTIONS = {
    "parties": {
        "question": "Who are the parties to this contract?",
        "prompt": """
        Based on the provided contract excerpts, identify all parties to the contract.
        Include both full legal names and any defined short names or abbreviations used.
        Format the response as a clear, concise list.
        """,
    },
    "title": {
        "question": "What is the title or type of this contract?",
        "prompt": """
        Based on the provided contract excerpts, what is the official title or type of this contract?
        Look for text at the beginning of the document or in header sections.
        Provide a concise response with just the title/type.
        """,
    },
}


class StreamingFactsheetResponse(BaseModel):
    answers: Dict[str, str]
    is_complete: bool
    error: Optional[str] = None
