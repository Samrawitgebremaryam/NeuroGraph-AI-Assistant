"""Pipeline API endpoints."""  
import os  
import tempfile  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  
  
router = APIRouter()  
orchestration_service = OrchestrationService()  
  
@router.post("/execute")  
async def execute_pipeline(  
    file: UploadFile = File(...),  
    config: str = Form(...),  
    schema_json: str = Form(...),  
    tenant_id: str = Form("default"),  
    session_id: str = Form(None)  
):  
    """Execute graph generation and mining pipeline."""  
    if not file.filename.endswith('.csv'):  
        raise HTTPException(status_code=400, detail="Only CSV files are supported")  
        
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:  
        content = await file.read()  
        tmp_file.write(content)  
        tmp_file_path = tmp_file.name  
        
    try:  
        result = await orchestration_service.process_graph_pipeline(  
            csv_file_path=tmp_file_path,  
            config=config,  
            schema_json=schema_json,  
            tenant_id=tenant_id,  
            session_id=session_id  
        )  
        return result  
    finally:  
        os.unlink(tmp_file_path)  
  
@router.post("/annotate")  
async def annotate_motif(  
    job_id: str = Form(...),  
    neo4j_job_id: str = Form(...),  
    selected_motif: dict = Form(...)  
):  
    """Annotate selected motif."""  
    result = await orchestration_service.annotate_selected_motif(  
        job_id=job_id,  
        neo4j_job_id=neo4j_job_id,  
        selected_motif=selected_motif  
    )  
    return result  
  
@router.get("/job/{job_id}")  
async def get_job_status(job_id: str):  
    """Get job status."""  
    # This would typically check a job store  
    return {"job_id": job_id, "status": "processing"}  
  
@router.get("/health")  
async def health_check():  
    """Health check endpoint."""  
    return {"status": "healthy", "service": "integration-service"}