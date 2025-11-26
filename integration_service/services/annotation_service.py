"""Annotation service communication for Integration Service."""  
import httpx  
from typing import Dict, Any, Optional  
from ..config.settings import settings  
  
class AnnotationService:  
    """Service for communicating with Annotation Service."""  
      
    def __init__(self):  
        self.service_url = settings.annotation_url  
        self.timeout = settings.annotation_timeout  
      
    async def annotate_motif(self, job_id: str, motif: Dict[str, Any]) -> Optional[Dict[str, Any]]:  
        """Send selected motif to annotation service."""  
        if not self.service_url:  
            raise RuntimeError("Annotation service URL not configured")  
          
        # Use Neo4j job_id for annotation (cypher type)  
        payload = {  
            "folder_id": job_id,  
            "type": "cypher",  
            "motif": motif  
        }  
          
        try:  
            async with httpx.AsyncClient() as client:  
                response = await client.post(  
                    self.service_url,  
                    json=payload,  
                    timeout=self.timeout  
                )  
                  
                if response.status_code != 200:  
                    raise RuntimeError(f"Annotation service returned {response.status_code}: {response.text}")  
                  
                return response.json()  
                  
        except httpx.TimeoutException:  
            raise RuntimeError("Timeout connecting to annotation service")  
        except Exception as e:  
            raise RuntimeError(f"Failed to connect to annotation service: {str(e)}")  
  
# Global instance  
annotation_service = AnnotationService()