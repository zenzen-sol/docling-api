from abc import ABC, abstractmethod
from io import BytesIO
import logging
from typing import List, Tuple

from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption, DocumentConverter
from docling_core.types.doc import ImageRefMode, TableItem, PictureItem
from fastapi import HTTPException

from document_converter.schema import ConversionResult, ImageData
import base64

logging.basicConfig(level=logging.INFO)
IMAGE_RESOLUTION_SCALE = 4


class DocumentConversionBase(ABC):
    @abstractmethod
    def convert(self, document: Tuple[str, BytesIO], **kwargs) -> ConversionResult:
        pass

    @abstractmethod
    def convert_batch(self, documents: List[Tuple[str, BytesIO]], **kwargs) -> List[ConversionResult]:
        pass


class DoclingDocumentConversion(DocumentConversionBase):
    def _setup_pipeline_options(self, extract_tables: bool, image_resolution_scale: int) -> PdfPipelineOptions:
        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = image_resolution_scale
        pipeline_options.generate_page_images = False
        pipeline_options.generate_table_images = extract_tables
        pipeline_options.generate_picture_images = True
        return pipeline_options

    def _process_document_images(self, conv_res) -> Tuple[str, List[ImageData]]:
        images = []
        table_counter = 0
        picture_counter = 0

        for element, _level in conv_res.document.iterate_items():
            if isinstance(element, (TableItem, PictureItem)):
                img_buffer = BytesIO()
                element.image.pil_image.save(img_buffer, format="PNG")

                if isinstance(element, TableItem):
                    table_counter += 1
                    image_name = f"table-{table_counter}.png"
                    image_type = "table"
                else:
                    picture_counter += 1
                    image_name = f"picture-{picture_counter}.png"
                    image_type = "picture"

                image_bytes = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
                images.append(ImageData(type=image_type, filename=image_name, image=image_bytes))

        content_md = conv_res.document.export_to_markdown(image_mode=ImageRefMode.PLACEHOLDER)
        return content_md, images

    def convert(
        self,
        document: Tuple[str, BytesIO],
        extract_tables: bool = False,
        image_resolution_scale: int = IMAGE_RESOLUTION_SCALE,
    ) -> ConversionResult:
        filename, file = document
        pipeline_options = self._setup_pipeline_options(extract_tables, image_resolution_scale)
        doc_converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

        conv_res = doc_converter.convert(DocumentStream(name=filename, stream=file), raises_on_error=False)
        doc_filename = conv_res.input.file.stem

        if conv_res.errors:
            logging.error(f"Failed to convert {filename}: {conv_res.errors[0].error_message}")
            return ConversionResult(filename=doc_filename, error=conv_res.errors[0].error_message)

        content_md, images = self._process_document_images(conv_res)
        return ConversionResult(filename=doc_filename, markdown=content_md, images=images)

    def convert_batch(
        self,
        documents: List[Tuple[str, BytesIO]],
        extract_tables: bool = False,
        image_resolution_scale: int = IMAGE_RESOLUTION_SCALE,
    ) -> List[ConversionResult]:
        pipeline_options = self._setup_pipeline_options(extract_tables, image_resolution_scale)
        doc_converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

        conv_results = doc_converter.convert_all(
            [DocumentStream(name=filename, stream=file) for filename, file in documents],
            raises_on_error=False,
        )

        results = []
        for conv_res in conv_results:
            doc_filename = conv_res.input.file.stem

            if conv_res.errors:
                logging.error(f"Failed to convert {conv_res.input.name}: {conv_res.errors[0].error_message}")
                results.append(ConversionResult(filename=conv_res.input.name, error=conv_res.errors[0].error_message))
                continue

            content_md, images = self._process_document_images(conv_res)
            results.append(ConversionResult(filename=doc_filename, markdown=content_md, images=images))

        return results


class DocumentConverterService:
    def __init__(self, document_converter: DocumentConversionBase):
        self.document_converter = document_converter

    def convert_document(self, document: Tuple[str, BytesIO], **kwargs) -> ConversionResult:
        result = self.document_converter.convert(document, **kwargs)
        if result.error:
            raise HTTPException(status_code=500, detail=result.error)
        return result

    def convert_documents(self, documents: List[Tuple[str, BytesIO]], **kwargs) -> List[ConversionResult]:
        return self.document_converter.convert_batch(documents, **kwargs)
