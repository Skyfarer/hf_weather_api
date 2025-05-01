import os

class Config:
    """Application configuration"""
    DEBUG = True
    PORT = 5000
    
    # Valkey configuration
    VALKEY_HOST = 'valkey'
    VALKEY_PORT = 6379
    VALKEY_PASSWORD = None
