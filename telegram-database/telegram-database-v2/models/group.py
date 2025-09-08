"""
Group data model
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Group:
    """Group data model"""
    id: str
    name: str
    link: str
    category: str
    priority: str
    credibility_score: float = 0.0
    total_members: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

@dataclass
class GroupStats:
    """Group statistics"""
    group_id: str
    total_messages: int = 0
    job_messages: int = 0
    last_message_at: Optional[datetime] = None
    avg_messages_per_day: float = 0.0
    activity_score: float = 0.0
