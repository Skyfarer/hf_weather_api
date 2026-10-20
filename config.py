import os

class Config:
    """Application configuration"""
    DEBUG = True
    PORT = 5000
    
    # Forecast grid file written by the ingest job
    GRID_PATH = os.environ.get('HF_GRID_PATH', '/data/forecast.grid')
