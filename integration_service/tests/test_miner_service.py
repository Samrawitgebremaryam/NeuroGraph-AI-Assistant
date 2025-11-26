"""Tests for Miner Service."""  
import pytest  
import asyncio  
from services.miner_service import MinerService  
  
@pytest.mark.asyncio  
async def test_validate_motif_output():  
    """Test motif output validation."""  
    miner_service = MinerService()  
      
    valid_output = {  
        "motifs": [{"id": 1, "pattern": "..."}],  
        "statistics": {"total_motifs": 5}  
    }  
      
    assert miner_service.validate_motif_output(valid_output) == True  
      
    invalid_output = {"motifs": []}  
    assert miner_service.validate_motif_output(invalid_output) == False