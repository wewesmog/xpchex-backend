# Use langchain to chunk text into smaller chunks
# acceptable formats are:
# - plain text, markdown, html, pdf, docx, txt, csv, json, xml, yaml, ini, toml
# Required packages:
# - langchain, langchain-docling, docling

from pathlib import Path
from langchain_docling import DoclingLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from psycopg2.extras import execute_values

from ..shared_services.db import pooled_connection
from ..shared_services.llm import embed_texts
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

def chunk_documents(paths: list[str]):
    """
    Load one or more documents from disk and split them into chunks.
    Returns a list of LangChain Document objects.
    """
    # DoclingLoader can actually handle a list of paths directly
    loader = DoclingLoader(file_path=paths)
    docs = loader.load()

    # Using separators (plural) allows the splitter to fall back 
    # from paragraphs to sentences to words gracefully.
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=200, 
        separators=["\n\n", "\n", ".", " ", ""]
    )

    chunks = text_splitter.split_documents(docs)
    logger.info("Chunked %s chunks from %s documents", len(chunks), len(paths))
    return chunks

def save_chunks(chunks, document_id: int):
    texts = [
        chunk.page_content if hasattr(chunk, "page_content") else str(chunk)
        for chunk in chunks
    ]
    
    embeddings = embed_texts(texts)  # Calls your shared embedding service
    
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            values = [
                (document_id, idx, text, embeddings[idx])
                for idx, text in enumerate(texts)
            ]
            execute_values(
                cur,
                """
                INSERT INTO knowledge_chunks 
                    (document_id, chunk_index, chunk_text, embedding)
                VALUES %s
                """,
                values,
            )
        conn.commit()

    logger.info(
        "Saved %s chunks for document_id=%s",
        len(chunks),
        document_id
    )

def chunk_document_and_save(document_id: int, relative_filename: str):
    # Resolve the path to the uploads folder
    backend_root = Path(__file__).resolve().parents[2]  # .../backend
    file_path = backend_root / "uploads" / "knowledge" / relative_filename

    if not file_path.exists():
        raise FileNotFoundError(f"Expected file not found at: {file_path}")

    chunks = chunk_documents([str(file_path)])
    save_chunks(chunks, document_id=document_id)

if __name__ == "__main__":
    # Corrected indentation and removed redundant if check
    doc_id = 6
    filename = "a57d609ae4534f15a012e74d41e653bf.pdf"
    chunk_document_and_save(document_id=doc_id, relative_filename=filename)