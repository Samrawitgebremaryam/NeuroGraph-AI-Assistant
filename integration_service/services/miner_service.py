"""Neural Miner communication service."""  
import httpx  
import os  
import asyncio  
from typing import Dict, Any  
from ..config.settings import settings  
  
class MinerService:  
    """Service for communicating with Neural Subgraph Miner."""  
      
    def __init__(self):  
        self.miner_url = settings.miner_url  
        self.timeout = settings.miner_timeout  
      
    async def mine_motifs(self, networkx_file_path: str) -> Dict[str, Any]:  
        """Send NetworkX file to miner and return discovered motifs."""  
        if not os.path.exists(networkx_file_path):  
            raise FileNotFoundError(f"NetworkX file not found: {networkx_file_path}")  
          
        async with httpx.AsyncClient(timeout=self.timeout) as client:  
            with open(networkx_file_path, 'rb') as f:  
                files = {'file': f}  
                response = await client.post(f"{self.miner_url}/mine", files=files)  
                
            if response.status_code != 200:  
                raise RuntimeError(f"Miner returned {response.status_code}: {response.text}")  
                
            result = response.json()  
                
        # Validate response structure  
        if not self._validate_motif_output(result):  
            raise ValueError("Invalid motif output structure from miner")  
                
        return result  
        
    def _validate_motif_output(self, output: Dict[str, Any]) -> bool:  
        """Validate miner output structure."""  
        required_keys = ['motifs', 'statistics']  
        if not all(key in output for key in required_keys):  
            return False  
            
        if not isinstance(output['motifs'], list):  
            return False  
                
        return True  
  
# Global service instance  
miner_service = MinerService()