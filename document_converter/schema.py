from pydantic import BaseModel, Field
from typing import List, Literal, Optional


class ImageData(BaseModel):
    type: Optional[Literal["table", "picture"]] = Field(None, description="The type of the image")
    filename: Optional[str] = Field(None, description="The filename of the image")
    image: Optional[bytes] = Field(None, description="The image data")


class ConversionResult(BaseModel):
    filename: str = Field(None, description="The filename of the document")
    markdown: str = Field(None, description="The markdown content of the document")
    images: List[ImageData] = Field(default_factory=list, description="The images in the document")
