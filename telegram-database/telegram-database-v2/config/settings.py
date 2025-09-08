"""
Configuration settings for Telegram Job Scraper V2
"""
import os
from typing import Dict, Any

class Settings:
    """Application settings"""
    
    # Database settings
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///telegram_jobs_v2.db")
    
    # Account limits
    MAX_GROUPS_PER_ACCOUNT = 1000
    DAILY_JOIN_LIMIT = 10
    DAILY_LEAVE_LIMIT = 0
    
    # Human behavior simulation
    JOIN_PROBABILITY = 0.7
    LEAVE_PROBABILITY = 0.1
    PRIORITY_WEIGHTS = {
        'high': 0.9,
        'medium': 0.7,
        'low': 0.5
    }
    
    # Telegram settings
    CRAWL_DELAY = 2  # seconds between actions
    MESSAGE_FETCH_LIMIT = 100
    
    # Logging
    LOG_LEVEL = "INFO"
    LOG_FILE = "logs/telegram_scraper_v2.log"
    
    # Current accounts from original system
    ACCOUNTS = [
        {
            'id': 'account_1',
            'name': 'Account 1',
            'phone': '+919794670665',
            'api_id': 24242582,
            'api_hash': 'd8a500dd4f6956793a0be40ac48c1669',
            'session_name': 'session_account1'
        },
        {
            'id': 'account_2',
            'name': 'Account 2',
            'phone': '+917398227455',
            'api_id': 23717746,
            'api_hash': '23f3b527b36bf24d95435d245e73b270',
            'session_name': 'session_account2'
        },
        {
            'id': 'account_3',
            'name': 'Account 3',
            'phone': '+919140057096',
            'api_id': 29261262,
            'api_hash': '884a43e2719d86d9023d9a82bc61db58',
            'session_name': 'session_account3'
        },
        {
            'id': 'account_4',
            'name': 'Account 4',
            'phone': '+917828629905',
            'api_id': 29761042,
            'api_hash': 'c140669550a74b751993c941b2ab0aa7',
            'session_name': 'session_account4'
        }
    ]
    
    # Universal groups file
    UNIVERSAL_GROUPS_FILE = "data/universal_groups.json"
    
    @classmethod
    def get_account_by_id(cls, account_id: str) -> Dict[str, Any]:
        """Get account configuration by ID"""
        for account in cls.ACCOUNTS:
            if account['id'] == account_id:
                return account
        raise ValueError(f"Account {account_id} not found")
    
    @classmethod
    def get_all_account_ids(cls) -> list:
        """Get all account IDs"""
        return [account['id'] for account in cls.ACCOUNTS]

# Global settings instance
settings = Settings()
