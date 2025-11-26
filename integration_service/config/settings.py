"""Configuration management for Integration Service."""  
import os  
from dotenv import load_dotenv  
  
load_dotenv()  
  
class Settings:  
    """Integration Service settings"""  
      
    def __init__(self):  
        # Service URLs  
        self.atomspace_url = os.getenv('ATOMSPACE_API_URL', 'http://atomspace-api:8000')  
        self.miner_url = os.getenv('NEURAL_MINER_URL', 'http://neural-miner:5000')  
        self.annotation_url = os.getenv('ANNOTATION_SERVICE_URL', 'http://annotation-service:6000')  
          
        # Timeouts 
        self.atomspace_timeout = int(os.getenv('ATOMSPACE_TIMEOUT', '1800'))  
        self.miner_timeout = int(os.getenv('MINER_TIMEOUT', '600'))  
        self.annotation_timeout = int(os.getenv('ANNOTATION_TIMEOUT', '300'))  
          
        # Shared volume paths  
        self.shared_output_path = '/shared/output'  
        self.csv_cache_path = '/tmp/csv_cache'  
        self.job_states_path = '/tmp/job_states'  
  
settings = Settings()