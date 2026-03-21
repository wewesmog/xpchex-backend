# backend/app/routers/file_upload_router.py

"""
File upload router for the knowledge base.

- Accepts multipart/form-data: metadata (Form) + file (File).
- Saves file to backend/uploads/knowledge/ with a safe name.
- Returns upload response. Duplicate-name check and replace can be handled later via DB + update endpoint.
"""

import logging
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse

from app.models.document_upload_model import DocumentUploadResponse
from app.shared_services.db import pooled_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/file-upload", tags=["file-upload"])

BACKEND_ROOT = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR = BACKEND_ROOT / "uploads" / "knowledge"
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".doc"}
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


def _safe_filename(original: str) -> str:
    """Return a safe storage filename: uuid + original extension."""
    ext = Path(original).suffix.lower() or ".bin"
    if ext not in ALLOWED_EXTENSIONS:
        ext = ".bin"
    return f"{uuid4().hex}{ext}"


@router.post("/upload", response_model=DocumentUploadResponse, status_code=status.HTTP_200_OK)
async def upload_file(
    file: UploadFile = File(..., description="Document file (PDF, DOCX, TXT)"),
    app_id: str = Form(..., description="App ID"),
    user_id: str = Form(..., description="User ID (UUID)"),
    file_name: str = Form(..., description="Display name for the document"),
    file_type: str = Form(..., description="Document type: faq | policy | knowledge_base | other"),
    file_type_other: Optional[str] = Form(None, description="Required when file_type is 'other'"),
    file_description: Optional[str] = Form(None, description="Optional description"),
    is_active: bool = Form(True, description="Include in knowledge base"),
):
    """
    Upload a document to the knowledge base.
    File is stored under backend/uploads/knowledge/. Persist to DB when ready; duplicate/replace via update endpoint.
    """
    try:
        user_uuid = UUID(user_id.strip())
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid user_id; must be a valid UUID.",
        )

    if not file.filename or file.filename.strip() == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file provided or empty filename.",
        )

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Allowed types: {', '.join(ALLOWED_EXTENSIONS)}. Got: {ext or '(none)'}",
        )

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    storage_name = _safe_filename(file.filename)
    file_path = UPLOAD_DIR / storage_name

    try:
        content = await file.read()
        if len(content) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File size exceeds {MAX_FILE_SIZE_MB}MB limit.",
            )
        with open(file_path, "wb") as f:
            f.write(content)
    except HTTPException:
        raise
    except OSError as e:
        logger.exception("Failed to save uploaded file to %s", file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save file on server.",
        ) from e

    file_id = uuid4()

    ext_to_source_type = {
        ".pdf": "pdf",
        ".docx": "docx",
        ".doc": "docx",
        ".txt": "txt",
    }
    source_type = ext_to_source_type.get(ext, "txt")
    file_path_stored = f"uploads/knowledge/{storage_name}"

    try:
        with pooled_connection() as conn:
            with conn.cursor() as cursor:
                original_filename = file.filename or storage_name
                cursor.execute("""
                    INSERT INTO knowledge_documents (
                        file_id,
                        app_id,
                        title,
                        original_filename,
                        document_type,
                        document_type_other,
                        description,
                        source_type,
                        file_path,
                        is_active,
                        deleted,
                        created_by
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(file_id),
                    app_id,
                    file_name,
                    original_filename,
                    file_type,
                    file_type_other if file_type_other is not None else None,
                    file_description if file_description is not None else None,
                    source_type,
                    file_path_stored,
                    is_active,
                    False,
                    str(user_uuid),
                ))
                conn.commit()
        logger.info("Inserted knowledge_documents row for file_id=%s", file_id)
    except Exception as e:
        logger.exception("Failed to insert into knowledge_documents: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save document record.",
        ) from e

    payload = {
        "app_id": app_id,
        "user_id": str(user_uuid),
        "success": True,
        "message": "File uploaded successfully.",
        "file_id": str(file_id),
        "file_name": file_name,
        "original_filename": file.filename or "",
        "file_type": file_type,
        "file_type_other": file_type_other if file_type_other is not None else None,
    }
    return JSONResponse(status_code=200, content=payload)


@router.get("/documents/{file_id}/file", status_code=200)
async def download_document(file_id: str):
    """
    Download a knowledge document by file_id. Returns the file with original filename.
    """
    try:
        file_uuid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file_id.")

    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT file_path, original_filename FROM knowledge_documents WHERE file_id = %s",
                    (str(file_uuid),),
                )
                row = cursor.fetchone()
    except Exception as e:
        logger.exception("Failed to look up document: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to look up document.",
        ) from e

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found.")

    file_path_stored = row.get("file_path") or ""
    original_filename = row.get("original_filename") or "document"
    full_path = BACKEND_ROOT / file_path_stored

    if not full_path.is_file():
        logger.warning("File not found on disk: %s", full_path)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    return FileResponse(
        path=str(full_path),
        filename=original_filename,
        media_type="application/octet-stream",
    )


# Allowed sort columns and directions for get_all_documents (avoid SQL injection)
_ALLOWED_SORT_COLUMNS = {"id", "created_at", "updated_at", "title", "original_filename", "document_type", "source_type", "deleted"}
_ALLOWED_SORT_ORDERS = {"asc", "desc"}


# Get all documents, with pagination & filtering (only app_id required)
@router.get("/get-all-documents", status_code=status.HTTP_200_OK)
async def get_all_documents(
    app_id: str = Query(..., description="App ID (required)"),
    user_id: Optional[str] = Query(None, description="Filter by user ID (UUID)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Page size"),
    search: Optional[str] = Query(None, description="Search in title (ILIKE)"),
    document_type: Optional[str] = Query(None, description="Filter by document_type"),
    document_type_other: Optional[str] = Query(None, description="Filter by document_type_other"),
    description: Optional[str] = Query(None, description="Filter by description (ILIKE)"),
    source_type: Optional[str] = Query(None, description="Filter by source_type"),
    file_path: Optional[str] = Query(None, description="Filter by file_path (ILIKE)"),
    original_filename: Optional[str] = Query(None, description="Filter by original_filename (ILIKE)"),
    is_active: Optional[bool] = Query(None, description="Filter by is_active"),
    include_deleted: bool = Query(False, description="If true, include soft-deleted documents"),
    sort: Optional[str] = Query("created_at", description="Sort column"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc | desc"),
):
    """
    Get all documents from the knowledge base. Only app_id is required; all other params are optional filters.
    By default excludes soft-deleted documents (deleted = true). Use include_deleted=true to include them.
    """
    # Build WHERE clause: only app_id is compulsory
    where_parts = ["app_id = %s"]
    params: list = [app_id]

    if not include_deleted:
        where_parts.append("(deleted = false OR deleted IS NULL)")

    if user_id is not None and user_id.strip():
        where_parts.append("created_by = %s")
        params.append(user_id.strip())
    if search is not None and search.strip():
        where_parts.append("title ILIKE %s")
        params.append(f"%{search.strip()}%")
    if document_type is not None and document_type.strip():
        where_parts.append("document_type = %s")
        params.append(document_type.strip())
    if document_type_other is not None and document_type_other.strip():
        where_parts.append("document_type_other ILIKE %s")
        params.append(f"%{document_type_other.strip()}%")
    if description is not None and description.strip():
        where_parts.append("description ILIKE %s")
        params.append(f"%{description.strip()}%")
    if source_type is not None and source_type.strip():
        where_parts.append("source_type = %s")
        params.append(source_type.strip())
    if file_path is not None and file_path.strip():
        where_parts.append("file_path ILIKE %s")
        params.append(f"%{file_path.strip()}%")
    if original_filename is not None and original_filename.strip():
        where_parts.append("original_filename ILIKE %s")
        params.append(f"%{original_filename.strip()}%")
    if is_active is not None:
        where_parts.append("is_active = %s")
        params.append(is_active)

    where_sql = " AND ".join(where_parts)

    # Safe ORDER BY (whitelist)
    sort_col = sort if sort and sort.lower() in _ALLOWED_SORT_COLUMNS else "created_at"
    order = sort_order if sort_order and sort_order.lower() in _ALLOWED_SORT_ORDERS else "desc"
    order_sql = f"ORDER BY {sort_col} {order}"

    count_query = f"SELECT COUNT(*) AS total FROM knowledge_documents WHERE {where_sql}"
    params_with_pagination = params + [page_size, (page - 1) * page_size]
    data_query = f"""
        SELECT * FROM knowledge_documents
        WHERE {where_sql}
        {order_sql}
        LIMIT %s OFFSET %s
    """

    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(count_query, params)
                total = (cursor.fetchone() or {}).get("total") or 0
                cursor.execute(data_query, params_with_pagination)
                documents = cursor.fetchall()
        # Convert to JSON-serializable dicts (UUID, datetime)
        out = []
        for row in documents:
            d = dict(row)
            for k, v in d.items():
                if hasattr(v, "isoformat"):  # datetime/date
                    d[k] = v.isoformat()
                elif hasattr(v, "hex"):  # UUID
                    d[k] = str(v)
            out.append(d)
        total_pages = max(1, (total + page_size - 1) // page_size) if page_size else 1
        has_more = page * page_size < total
        return {
            "data": out,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "has_more": has_more,
            },
        }
    except Exception as e:
        logger.exception("Failed to get all documents: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list documents.",
        ) from e


@router.patch("/documents/{file_id}", status_code=status.HTTP_200_OK)
async def update_document(
    file_id: str,
    file: Optional[UploadFile] = File(None, description="Optional new file to replace the current one"),
    title: Optional[str] = Form(None),
    document_type: Optional[str] = Form(None),
    document_type_other: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    is_active: Optional[str] = Form(None, description="true | false"),
    deleted: Optional[str] = Form(None, description="true | false — soft-delete when true"),
):
    """
    Update a knowledge document by file_id. Send multipart/form-data with any of:
    title, document_type, document_type_other, description, is_active, deleted, and optionally file (to replace the stored file).
    """
    try:
        file_uuid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file_id.")

    updates = []
    params = []
    if title is not None:
        updates.append("title = %s")
        params.append(title)
    if document_type is not None:
        updates.append("document_type = %s")
        params.append(document_type)
    if document_type_other is not None:
        updates.append("document_type_other = %s")
        params.append(document_type_other)
    if description is not None:
        updates.append("description = %s")
        params.append(description)
    if is_active is not None and is_active.strip().lower() in ("true", "false", "1", "0", "yes", "no"):
        updates.append("is_active = %s")
        params.append(is_active.strip().lower() in ("true", "1", "yes"))
    if deleted is not None and deleted.strip().lower() in ("true", "false", "1", "0", "yes", "no"):
        updates.append("deleted = %s")
        params.append(deleted.strip().lower() in ("true", "1", "yes"))

    old_file_path = None
    new_file_path_stored = None
    new_original_filename = None
    new_source_type = None

    if file is not None and file.filename and file.filename.strip():
        ext = Path(file.filename).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Allowed types: {', '.join(ALLOWED_EXTENSIONS)}. Got: {ext or '(none)'}",
            )
        content = await file.read()
        if len(content) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File size exceeds {MAX_FILE_SIZE_MB}MB limit.",
            )
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        storage_name = _safe_filename(file.filename)
        new_path = UPLOAD_DIR / storage_name
        try:
            with open(new_path, "wb") as f:
                f.write(content)
        except OSError as e:
            logger.exception("Failed to save replacement file to %s", new_path)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to save file on server.",
            ) from e
        ext_to_source = {".pdf": "pdf", ".docx": "docx", ".doc": "docx", ".txt": "txt"}
        new_file_path_stored = f"uploads/knowledge/{storage_name}"
        new_original_filename = file.filename or storage_name
        new_source_type = ext_to_source.get(ext, "txt")

        updates.append("file_path = %s")
        params.append(new_file_path_stored)
        updates.append("original_filename = %s")
        params.append(new_original_filename)
        updates.append("source_type = %s")
        params.append(new_source_type)

    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide at least one field to update: title, document_type, document_type_other, description, is_active, deleted, or file.",
        )

    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if new_file_path_stored is not None:
                    cursor.execute(
                        "SELECT file_path FROM knowledge_documents WHERE file_id = %s",
                        (str(file_uuid),),
                    )
                    row = cursor.fetchone()
                    if row:
                        old_file_path = row.get("file_path")

                params.append(str(file_uuid))
                set_sql = ", ".join(updates)
                query = f"UPDATE knowledge_documents SET {set_sql}, updated_at = NOW() WHERE file_id = %s"
                cursor.execute(query, params)
                conn.commit()
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found.")

        if old_file_path and new_file_path_stored:
            old_full = BACKEND_ROOT / old_file_path
            if old_full.is_file():
                try:
                    old_full.unlink()
                except OSError as e:
                    logger.warning("Could not delete old file %s after replace: %s", old_full, e)

        return {"success": True, "message": "Document updated.", "file_id": file_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to update document: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update document.",
        ) from e