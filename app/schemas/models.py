from typing import List, Optional

from pydantic import BaseModel, Field


class ArtifactLinks(BaseModel):
    docx_url: str
    pdf_url: str
    change_report_url: Optional[str] = None


class ActionLinks(BaseModel):
    case_url: str
    feedback_upload_url: str
    next_iteration_url: str


class FeedbackStatus(BaseModel):
    uploaded: bool
    comments_count: int = 0


class IterationSummary(BaseModel):
    iteration: int
    status: str
    comments_count: int = 0
    feedback_uploaded: bool
    artifacts: ArtifactLinks


class CaseResponse(BaseModel):
    ok: bool = True
    radicado: str
    current_iteration: int
    status: str
    artifacts: ArtifactLinks
    actions: ActionLinks
    feedback: FeedbackStatus
    iterations: List[IterationSummary] = Field(default_factory=list)
    download_url: str
