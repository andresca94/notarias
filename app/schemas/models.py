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


class MaintenanceStatus(BaseModel):
    status: str
    message: Optional[str] = None
    run_id: Optional[str] = None
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class IterationSummary(BaseModel):
    iteration: int
    status: str
    comments_count: int = 0
    feedback_uploaded: bool
    maintenance_status: Optional[str] = None
    artifacts: ArtifactLinks


class CaseResponse(BaseModel):
    ok: bool = True
    radicado: str
    current_iteration: int
    status: str
    artifacts: ArtifactLinks
    actions: ActionLinks
    feedback: FeedbackStatus
    maintenance: Optional[MaintenanceStatus] = None
    iterations: List[IterationSummary] = Field(default_factory=list)
    download_url: str
