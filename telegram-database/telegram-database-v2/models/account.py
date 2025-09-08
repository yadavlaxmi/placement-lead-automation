"""
Account data model
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class Account:
    """Account data model"""
    id: str
    name: str
    phone: str
    api_id: int
    api_hash: str
    session_name: str
    status: str = 'active'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

@dataclass
class AccountState:
    """Account state information"""
    account_id: str
    active_groups: List[str]  # List of group IDs
    total_messages: int = 0
    last_activity: Optional[datetime] = None
    join_history: List[dict] = None
    
    def __post_init__(self):
        if self.join_history is None:
            self.join_history = []

@dataclass
class AccountState:
    """Account state for human behavior simulation"""
    account_id: str
    active_groups: List[str]
    total_messages: int
    last_activity: datetime
