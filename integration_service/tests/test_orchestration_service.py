"""Tests for orchestration service."""  
import pytest  
import asyncio  
from unittest.mock import AsyncMock, patch  
from ..services.orchestration_service import OrchestrationService  
  
@pytest.mark.asyncio  
async def test_execute_mining_pipeline():  
    """Test complete pipeline execution."""  
    service = OrchestrationService()  
      
    # Mock data  
    csv_content = "id,name,type\n1,NodeA,Person\n2,NodeB,Organization"  
    config = '{"name": "test", "description": "test config"}'  
    schema_json = '{"nodes": [{"id": "id", "label": "name", "type": "type"}]}'  
      
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:  
        f.write(csv_content)  
        csv_path = f.name  
      
    try:  
        # Mock AtomSpace Builder response  
        mock_atomspace_response = {  
            "job_id": "test-job-123",  
            "status": "success"  
        }  
          
        # Mock miner response  
        mock_miner_response = {  
            "motifs": [{"id": 1, "pattern": "test"}],  
            "statistics": {"total_motifs": 1}  
        }  
          
        with patch('httpx.AsyncClient') as mock_client:  
            mock_client.return_value.__aenter__.return_value.post.return_value.json.return_value = mock_atomspace_response  
            mock_client.return_value.__aenter__.return_value.post.return_value.status_code = 200  
              
            with patch.object(service.miner_service, 'mine_motifs', return_value=mock_miner_response):  
                result = await service.execute_mining_pipeline(  
                    csv_path, config, schema_json, "tenant123", "session456"  
                )  
                  
                assert result["status"] == "success"  
                assert "job_id" in result  
                assert "motifs" in result  
                assert len(result["motifs"]) == 1  
      
    finally:  
        os.unlink(csv_path)