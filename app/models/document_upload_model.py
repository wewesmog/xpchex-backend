from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import UploadFile
from pydantic import BaseModel


class DocumentUploadModel(BaseModel):
    """Full document entity (e.g. for DB row or list responses)."""
    file_id: UUID
    file_name: str
    file_type: str
    file_type_other: Optional[str] = None
    file_description: Optional[str] = None
    is_active: bool
    file_size: int
    file_path: str
    file_url: str
    file_created_at: datetime
    file_created_by: UUID
    file_updated_by: Optional[UUID] = None
    file_deleted_at: Optional[datetime] = None
    file_deleted_by: Optional[UUID] = None
    file_deleted_reason: Optional[str] = None
    file_deleted_comment: Optional[str] = None


class DocumentUploadRequest(BaseModel):
    """Form fields for upload (file comes as UploadFile in multipart)."""
    app_id: str
    user_id: UUID
    file_name: str
    file_type: str  # faq | policy | knowledge_base | other
    file_type_other: Optional[str] = None  # required when file_type == "other"
    file_description: Optional[str] = None
    is_active: bool
    # file: UploadFile  # not in Pydantic model; use FastAPI File() in route


class DocumentUploadResponse(BaseModel):
    app_id: str
    user_id: UUID
    success: bool
    message: str
    file_id: UUID
    file_name: str
    file_type: str
    file_type_other: Optional[str] = None
