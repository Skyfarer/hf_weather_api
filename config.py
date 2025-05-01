import os

class Config:
    """Application configuration settings."""
    
    # Flask settings
    DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'
    PORT = int(os.environ.get('PORT', 5000))
    
    # Valkey settings
    VALKEY_HOST = os.environ.get('VALKEY_HOST', 'localhost')
    VALKEY_PORT = int(os.environ.get('VALKEY_PORT', 6379))
    VALKEY_PASSWORD = os.environ.get('VALKEY_PASSWORD', None)
