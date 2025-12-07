"""Pipeline API endpoints."""  
import os  
import tempfile  
from typing import List  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  

class MiningConfig(BaseModel):
    job_id: str
    min_pattern_size: int = 5
    max_pattern_size: int = 10
    min_neighborhood_size: int = 5
    max_neighborhood_size: int = 10
    n_neighborhoods: int = 2000
    n_trials: int = 100
    graph_type: Optional[str] = None
    search_strategy: str = "greedy"
    sample_method: str = "tree"
    graph_output_format: str = "representative"  
  
router = APIRouter()  
orchestration_service = OrchestrationService()  
  
@router.post("/generate-graph")  
async def generate_graph(  
    files: List[UploadFile] = File(...),  
    config: str = Form(...),  
    schema_json: str = Form(...),  
    writer_type: str = Form("networkx"),
    graph_type: str = Form("directed")
):  
    """Generate NetworkX graph from CSV files."""  
    # Validate all files are CSV  
    for file in files:  
        if not file.filename.endswith('.csv'):  
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")  
      
    # Save uploaded files to temporary directory  
    temp_dir = tempfile.mkdtemp()  
    csv_file_paths = []  
      
    try:  
        for file in files:  
            file_path = os.path.join(temp_dir, file.filename)  
            with open(file_path, "wb") as f:  
                content = await file.read()  
                f.write(content)  
            csv_file_paths.append(file_path)  
          
        result = await orchestration_service.generate_networkx(
            csv_files=csv_file_paths,
            config=config,
            schema_json=schema_json,
            writer_type=writer_type,
            graph_type=graph_type,
            tenant_id="default"
        )
          
        return result
          
    finally:  
        import shutil  
        if os.path.exists(temp_dir):  
            shutil.rmtree(temp_dir)

@router.post("/mine-patterns")
async def mine_patterns(
    config: MiningConfig
):
    """ Mine patterns from NetworkX graph with custom configuration."""
    
    # Auto-detect graph_type from metadata if not provided
    if config.graph_type is None:
        config.graph_type = await orchestration_service.get_graph_type_from_metadata(config.job_id)
    
    mining_config = config.dict()
    print(f"DEBUG PIPELINE: Received config: {mining_config}", flush=True)
    
    result = await orchestration_service.mine_patterns(
        job_id=config.job_id,
        mining_config=mining_config
    )
    
    return result

@router.get("/download-result")
async def download_result(job_id: str, filename: str = None):
    try:
        if filename:
            # Download specific file
            file_path = orchestration_service.get_result_file_path(job_id, filename)
            return FileResponse(
                path=file_path,
                filename=os.path.basename(file_path),
                media_type='application/octet-stream'
            )
        else:
            # Download entire job as ZIP
            zip_path = orchestration_service.create_job_archive(job_id)
            return FileResponse(
                path=zip_path,
                filename=f"{job_id}.zip",
                media_type='application/zip'
            )
            
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))