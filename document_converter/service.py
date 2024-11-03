from abc import ABC, abstractmethod


class DocumentConversionBase(ABC):
    @abstractmethod
    def convert(self, document):
        pass

    @abstractmethod
    def convert_batch(self, documents):
        pass


class DoclingDocumentConversion(DocumentConversionBase):
    def convert(self, document):
        return document

    def convert_batch(self, documents):
        return documents


class DocumentConverterService:
    def __init__(self, document_converter: DocumentConversionBase):
        self.document_converter = document_converter

    def convert_document(self, document):
        return self.document_converter.convert(document)

    def convert_documents(self, documents):
        return self.document_converter.convert_batch(documents)
