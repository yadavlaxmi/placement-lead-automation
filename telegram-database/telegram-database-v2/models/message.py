"""
Message data model
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Message:
    """Message data model"""
    id: str
    message_text: str
    timestamp: datetime
    is_job_message: bool = False
    job_score: float = 0.0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
