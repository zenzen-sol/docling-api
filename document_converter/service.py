from abc import ABC, abstractmethod
from io import BytesIO
import logging
from typing import List, Tuple

from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption, DocumentConverter
from docling_core.types.doc import ImageRefMode, TableItem, PictureItem

from document_converter.schema import ConversionResult, ImageData

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
    def convert(
        self,
        document: Tuple[str, BytesIO],
        extract_tables: bool = False,
        image_resolution_scale: int = IMAGE_RESOLUTION_SCALE,
    ) -> ConversionResult:
        filename, file = document
        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = image_resolution_scale
        pipeline_options.generate_page_images = False
        pipeline_options.generate_table_images = extract_tables
        pipeline_options.generate_picture_images = True

        doc_converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

        # Convert document
        conv_res = doc_converter.convert(DocumentStream(name=filename, stream=file))
        doc_filename = conv_res.input.file.stem
        images = []

        # Extract images from figures and tables
        table_counter = 0
        picture_counter = 0
        for element, _level in conv_res.document.iterate_items():
            try:
                if isinstance(element, TableItem):
                    table_counter += 1
                    img_buffer = BytesIO()
                    element.image.pil_image.save(img_buffer, format="PNG")
                    image_name = f"table-{table_counter}.png"
                    images.append(ImageData(type="table", filename=image_name, image=img_buffer.getvalue()))

                if isinstance(element, PictureItem):
                    picture_counter += 1
                    img_buffer = BytesIO()
                    element.image.pil_image.save(img_buffer, format="PNG")
                    image_name = f"picture-{picture_counter}.png"
                    images.append(ImageData(type="picture", filename=image_name, image=img_buffer.getvalue()))
            except Exception as e:
                logging.warning(f"Failed to process image in {filename}: {str(e)}")
                continue

        # Generate markdown with embedded pictures
        content_md = conv_res.document.export_to_markdown(image_mode=ImageRefMode.PLACEHOLDER)
        return ConversionResult(filename=doc_filename, markdown=content_md, images=images)

    def convert_batch(self, documents: List[Tuple[str, BytesIO]]) -> List[ConversionResult]:
        return documents


class DocumentConverterService:
    def __init__(self, document_converter: DocumentConversionBase):
        self.document_converter = document_converter

    def convert_document(self, document: Tuple[str, BytesIO]) -> ConversionResult:
        return self.document_converter.convert(document)

    def convert_documents(self, documents: List[Tuple[str, BytesIO]]) -> List[ConversionResult]:
        return self.document_converter.convert_batch(documents)
