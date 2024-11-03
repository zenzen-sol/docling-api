from pydantic import BaseModel, Field
from typing import List


class ConversionResult(BaseModel):
    filename: str = Field(None, description="The filename of the document")
    markdown: str = Field(None, description="The markdown content of the document")
    images: List[str] = Field(default_factory=list, description="The images in the document")
