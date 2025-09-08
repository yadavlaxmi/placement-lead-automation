"""
Assignment data model
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Assignment:
    """Persistent assignment between account and group"""
    id: str
    account_id: str
    group_id: str
    assigned_at: datetime
    status: str = 'active'  # active, inactive, left
    last_message_fetch: Optional[datetime] = None
    total_messages_fetched: int = 0
    
    def __post_init__(self):
        if self.assigned_at is None:
            self.assigned_at = datetime.now()

@dataclass
class AssignmentHistory:
    """Assignment history tracking"""
    id: str
    account_id: str
    group_id: str
    action: str  # joined, left, reassigned
    timestamp: datetime
    reason: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

@dataclass
class AssignmentHistory:
    """Assignment history record"""
    id: str
    account_id: str
    group_id: str
    action: str  # joined, left, reassigned
    timestamp: datetime
    reason: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
