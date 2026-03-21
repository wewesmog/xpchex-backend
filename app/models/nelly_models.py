
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime

class EvalScores(BaseModel):
    accuracy: int
    tone: int
    completeness: int

class ChunkSnapshot(BaseModel):
    source: Literal['response_history', 'knowledge', 'conversation_messages']
    chunk_id: int
    text: str
    similarity: float
    score: int
    recency_weight: float
    final_score: float
    published_at: Optional[datetime] = None
    document_title: Optional[str] = None

class ContextSnapshot(BaseModel):
    confidence: float
    eval: EvalScores
    chunks: List[ChunkSnapshot]

# Example instantiation (for illustration; can be omitted from production model file)
"""
example = ContextSnapshot(
    confidence=0.87,
    eval=EvalScores(accuracy=4, tone=5, completeness=4),
    chunks=[
        ChunkSnapshot(
            source='response_history',
            chunk_id=1042,
            text="Thank you for your patience...",
            similarity=0.91,
            score=5,
            recency_weight=0.95,
            final_score=0.864,
            published_at=datetime.fromisoformat("2025-12-01T10:00:00+00:00")
        ),
        ChunkSnapshot(
            source='knowledge',
            chunk_id=37,
            text="For login issues: ask user to clear cache...",
            similarity=0.78,
            score=4,
            recency_weight=1.0,
            final_score=0.624,
            document_title="App FAQ v3.pdf"
        )
    ]
)
"""