from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, List, Literal

class ExistingStatement(BaseModel):
    statement: str
    canonical_id: str

class CanonizationRequest(BaseModel):
    review_id: str
    review_section: str
    statement: str  # The new statement to canonize
    existing_pairs: List[ExistingStatement] = Field(
        default=[],  # Default to empty list
        description="Array of existing statements with their canonical IDs for context and matching. Can be empty for completely new statements."
    )

    @property
    def existing_statements(self) -> List[str]:
        """Get list of existing statements for easy reference"""
        return [pair.statement for pair in self.existing_pairs]

    @property
    def existing_canonical_ids(self) -> List[str]:
        """Get list of existing canonical IDs for validation"""
        return [pair.canonical_id for pair in self.existing_pairs]

    @property
    def has_existing_pairs(self) -> bool:
        """Check if there are any existing pairs"""
        return len(self.existing_pairs) > 0

class CanonizationLLMResponse(BaseModel):
    canonical_id: str  # The canonical id of the statement (can be existing or new)
    reasoning: Optional[str] = None  # The reasoning for the canonization
    error: Optional[str] = None  # The error message if any

    @property
    def is_successful(self) -> bool:
        """Whether the canonization was successful - determined by absence of error"""
        return self.error is None

class CanonizationState(BaseModel):
    canonization_request: Optional[CanonizationRequest] = None
    canonization_response: Optional[CanonizationLLMResponse] = None
    canonization_status: Literal["pending", "completed", "failed", "already_exists"] = "pending"
    current_step: Literal["canonization_requested", "canonization_completed", "canonization_failed", "canonization_skipped"] = "canonization_requested"
    canonization_attempt: int = 0
   

