from io import BytesIO
from typing import List
from fastapi import APIRouter, File, UploadFile, Query

from document_converter.schema import ConversionResult
from document_converter.service import DocumentConverterService, DoclingDocumentConversion

router = APIRouter()

# Could be docling or another converter as long as it implements DocumentConversionBase
converter = DoclingDocumentConversion()
document_converter_service = DocumentConverterService(document_converter=converter)


@router.post('/convert-document', response_model=ConversionResult, response_model_exclude_unset=True)
async def convert_document(
    document: UploadFile = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    file_bytes = await document.read()
    return document_converter_service.convert_document(
        (document.filename, BytesIO(file_bytes)),
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )


@router.post('/convert-documents', response_model=List[ConversionResult], response_model_exclude_unset=True)
async def convert_documents(
    documents: List[UploadFile] = File(...),
    extract_tables_as_images: bool = False,
    image_resolution_scale: int = Query(4, ge=1, le=4),
):
    return document_converter_service.convert_documents(
        [(document.filename, BytesIO(await document.read())) for document in documents],
        extract_tables=extract_tables_as_images,
        image_resolution_scale=image_resolution_scale,
    )
