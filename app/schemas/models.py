from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class UploadResult(BaseModel):
    saved: List[str] = Field(default_factory=list)

class GenerateRequest(BaseModel):
    comentario: Optional[str] = "(Sin comentarios)"
    template_id: Optional[str] = None  # si no viene, usa env GOOGLE_TEMPLATE_DOC_ID

class GenerateResponse(BaseModel):
    run_id: str
    radicado: str
    case_folder_id: str
    output_doc_id: str
    output_pdf_file_id: str
    output_pdf_name: str
    debug: Dict[str, Any] = Field(default_factory=dict)
