"""Main orchestration service for pipeline coordination."""  
import os  
import uuid  
import httpx  
import tempfile  
import shutil  
import json  
import asyncio  
from typing import Dict, Any, Tuple  
from .miner_service import MinerService  
from .annotation_service import AnnotationService  
from ..config.settings import settings  
  
class OrchestrationService:  
    """Main pipeline orchestrator."""  
        
    def __init__(self):  
        self.miner_service = MinerService()  
        self.annotation_service = AnnotationService()  
        self.atomspace_url = settings.atomspace_url  
        self.timeout = settings.atomspace_timeout  
        
    async def process_graph_pipeline(  
        self,     
        csv_file_path: str,     
        config: str,    
        schema_json: str,    
        tenant_id: str = "default",    
        session_id: str = None    
    ) -> Dict[str, Any]:  
        """CSV → NetworkX → Parallel Neo4j + Mining → Return motifs."""  
        job_id = str(uuid.uuid4())  
            
        try:  
            # Step 1: Generate NetworkX first (sequential)  
            networkx_result = await self._generate_networkx(  
                csv_file_path, job_id, config, schema_json, tenant_id, session_id  
            )  
                
            #Parallel Neo4j generation and Neural mining  
            neo4j_task = self._generate_neo4j(  
                csv_file_path, job_id, config, schema_json, tenant_id, session_id  
            )  
            miner_task = self._mine_motifs(networkx_result['networkx_file'])  
                
            # Execute both tasks in parallel  
            neo4j_result, motifs_result = await asyncio.gather(  
                neo4j_task, miner_task, return_exceptions=True  
            )  
                
            # Handle exceptions  
            if isinstance(neo4j_result, Exception):  
                neo4j_result = {"job_id": job_id, "status": "error", "error": str(neo4j_result)}  
            if isinstance(motifs_result, Exception):  
                motifs_result = {"status": "error", "error": str(motifs_result)}  
                
            return {  
                "job_id": job_id,  
                "status": "success",  
                "motifs": motifs_result.get('motifs', []),  
                "statistics": motifs_result.get('statistics', {}),  
                "neo4j_job_id": neo4j_result.get('job_id'),  
                "neo4j_ready": neo4j_result.get('status') == 'success'  
            }  
                
        except Exception as e:  
            return {"job_id": job_id, "status": "error", "error": str(e)}  
        
    async def annotate_selected_motif(  
        self,  
        job_id: str,  
        neo4j_job_id: str,  
        selected_motif: Dict[str, Any]  
    ) -> Dict[str, Any]:  
        """Annotate selected motif using Neo4j data."""  
        try:  
            # Check if Neo4j is ready  
            if not await self._check_neo4j_ready(neo4j_job_id):  
                return {"status": "error", "error": "Neo4j data not ready"}  
                
            # Call annotation service  
            annotation_result = await self.annotation_service.annotate_motif(  
                neo4j_job_id, selected_motif  
            )  
                
            return {  
                "job_id": job_id,  
                "status": "success",  
                "annotation": annotation_result  
            }  
                
        except Exception as e:  
            return {"job_id": job_id, "status": "error", "error": str(e)}  
        
    async def _generate_networkx(  
        self,     
        csv_file_path: str,     
        job_id: str,    
        config: str,    
        schema_json: str,    
        tenant_id: str,    
        session_id: str    
    ) -> Dict[str, Any]:  
        """Generate NetworkX using AtomSpace Builder."""  
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:  
            config_file.write(config)  
            config_path = config_file.name  
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:  
            schema_file.write(schema_json)  
            schema_path = schema_file.name  
            
        try:  
            async with httpx.AsyncClient(timeout=self.timeout) as client:  
                with open(csv_file_path, 'rb') as csv_file:  
                    files = {  
                        'files': (os.path.basename(csv_file_path), csv_file, 'text/csv')  
                    }  
                    data = {  
                        'config': config,  
                        'schema_json': schema_json,  
                        'writer_type': 'networkx',  
                        'tenant_id': tenant_id,  
                        'session_id': session_id or str(uuid.uuid4())  
                    }  
                        
                    response = await client.post(  
                        f"{self.atomspace_url}/api/load",  
                        files=files,  
                        data=data  
                    )  
                        
                    if response.status_code != 200:  
                        raise RuntimeError(f"AtomSpace returned {response.status_code}: {response.text}")  
                        
                    result = response.json()  
                
            networkx_file = f"/shared/output/{result['job_id']}/graph.gpickle"  
                
            return {  
                "job_id": result['job_id'],  
                "networkx_file": networkx_file  
            }  
                
        finally:  
            os.unlink(config_path)  
            os.unlink(schema_path)  
        
    async def _generate_neo4j(  
        self,     
        csv_file_path: str,     
        job_id: str,    
        config: str,    
        schema_json: str,    
        tenant_id: str,    
        session_id: str    
    ) -> Dict[str, Any]:  
        """Generate Neo4j using AtomSpace Builder."""  
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:  
            config_file.write(config)  
            config_path = config_file.name  
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:  
            schema_file.write(schema_json)  
            schema_path = schema_file.name  
            
        try:  
            async with httpx.AsyncClient(timeout=self.timeout) as client:  
                with open(csv_file_path, 'rb') as csv_file:  
                    files = {  
                        'files': (os.path.basename(csv_file_path), csv_file, 'text/csv')  
                    }  
                    data = {  
                        'config': config,  
                        'schema_json': schema_json,  
                        'writer_type': 'neo4j',  
                        'tenant_id': tenant_id,  
                        'session_id': session_id or str(uuid.uuid4())  
                    }  
                        
                    response = await client.post(  
                        f"{self.atomspace_url}/api/load",  
                        files=files,  
                        data=data  
                    )  
                        
                    if response.status_code != 200:  
                        raise RuntimeError(f"AtomSpace returned {response.status_code}: {response.text}")  
                        
                    result = response.json()  
                
            return {  
                "job_id": result['job_id'],  
                "status": "success"  
            }  
                
        finally:  
            os.unlink(config_path)  
            os.unlink(schema_path)  
        
    async def _mine_motifs(self, networkx_file_path: str) -> Dict[str, Any]:  
        """Mine motifs using Neural Miner service."""  
        return await self.miner_service.mine_motifs(networkx_file_path)  
        
    async def _check_neo4j_ready(self, neo4j_job_id: str) -> bool:  
        """Check if Neo4j data is ready for annotation."""  
        try:  
            async with httpx.AsyncClient(timeout=30) as client:  
                response = await client.get(f"{self.atomspace_url}/api/job/{neo4j_job_id}")  
                if response.status_code == 200:  
                    job_data = response.json()  
                    return job_data.get('status') == 'completed'  
                return False  
        except:  
            return False