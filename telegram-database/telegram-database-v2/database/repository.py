"""
Database Repository - Handles all database operations
"""
import sqlite3
import uuid
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

from models.account import Account
from models.group import Group
from models.assignment import Assignment, AssignmentHistory
from models.message import Message

class DatabaseRepository:
    """Repository for database operations"""
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or "sqlite:///telegram_jobs_v2.db"
        self.logger = logging.getLogger(__name__)
        
        # Extract database path from URL
        if self.db_url.startswith("sqlite:///"):
            self.db_path = self.db_url.replace("sqlite:///", "")
        else:
            self.db_path = "telegram_jobs_v2.db"
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context manager"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
        finally:
            conn.close()
    
    async def initialize(self):
        """Initialize database with schema"""
        self.logger.info("Initializing database...")
        
        with self.get_connection() as conn:
            with open('database/schema.sql', 'r') as f:
                schema_sql = f.read()
            
            conn.executescript(schema_sql)
            conn.commit()
        
        self.logger.info("Database initialized successfully!")
    
    # Account operations
    async def create_account(self, account_data: Dict[str, Any]) -> bool:
        """Create new account"""
        try:
            with self.get_connection() as conn:
                group_id = group_data.get("id", str(uuid.uuid4()))
                conn.execute("""
                    INSERT INTO accounts (id, name, phone, api_id, api_hash, session_name, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    account_data['id'],
                    account_data['name'],
                    account_data['phone'],
                    account_data['api_id'],
                    account_data['api_hash'],
                    account_data['session_name'],
                    'active'
                ))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error creating account: {e}")
            return False
    
    def get_account_by_id(self, account_id: str) -> Optional[Account]:
        """Get account by ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM accounts WHERE id = ?", (account_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    return Account(
                        id=row['id'],
                        name=row['name'],
                        phone=row['phone'],
                        api_id=row['api_id'],
                        api_hash=row['api_hash'],
                        session_name=row['session_name'],
                        status=row['status'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    )
                return None
        except Exception as e:
            self.logger.error(f"Error getting account {account_id}: {e}")
            return None
    
    def get_all_accounts(self) -> List[Account]:
        """Get all accounts"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT * FROM accounts WHERE status = 'active'")
                rows = cursor.fetchall()
                
                accounts = []
                for row in rows:
                    accounts.append(Account(
                        id=row['id'],
                        name=row['name'],
                        phone=row['phone'],
                        api_id=row['api_id'],
                        api_hash=row['api_hash'],
                        session_name=row['session_name'],
                        status=row['status'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    ))
                return accounts
        except Exception as e:
            self.logger.error(f"Error getting all accounts: {e}")
            return []
    
    def count_accounts(self) -> int:
        """Count total accounts"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error counting accounts: {e}")
            return 0
    
    # Group operations
    async def create_group(self, group_data: Dict[str, Any]) -> str:
        """Create new group"""
        try:
            with self.get_connection() as conn:
                group_id = group_data.get("id", str(uuid.uuid4()))
                conn.execute("""
                    INSERT INTO groups (id, name, link, category, priority, credibility_score, total_members)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    group_data['id'],
                    group_data['name'],
                    group_data['link'],
                    group_data['category'],
                    group_data['priority'],
                    group_data.get('credibility_score', 0.0),
                    group_data.get('total_members', 0)
                ))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error creating group: {e}")
            return False
    
    def get_group_by_id(self, group_id: str) -> Optional[Group]:
        """Get group by ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM groups WHERE id = ?", (group_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    return Group(
                        id=row['id'],
                        name=row['name'],
                        link=row['link'],
                        category=row['category'],
                        priority=row['priority'],
                        credibility_score=row['credibility_score'],
                        total_members=row['total_members'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    )
                return None
        except Exception as e:
            self.logger.error(f"Error getting group {group_id}: {e}")
            return None
    
    def get_all_groups(self) -> List[Group]:
        """Get all groups"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT * FROM groups")
                rows = cursor.fetchall()
                
                groups = []
                for row in rows:
                    groups.append(Group(
                        id=row['id'],
                        name=row['name'],
                        link=row['link'],
                        category=row['category'],
                        priority=row['priority'],
                        credibility_score=row['credibility_score'],
                        total_members=row['total_members'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    ))
                return groups
        except Exception as e:
            self.logger.error(f"Error getting all groups: {e}")
            return []
    
    def count_groups(self) -> int:
        """Count total groups"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM groups")
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error counting groups: {e}")
            return 0
    
    # Assignment operations
    def create_assignment(self, assignment: Assignment) -> bool:
        """Create new assignment"""
        try:
            with self.get_connection() as conn:
                group_id = group_data.get("id", str(uuid.uuid4()))
                conn.execute("""
                    INSERT INTO persistent_assignments (id, account_id, group_id, assigned_at, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    assignment.id,
                    assignment.account_id,
                    assignment.group_id,
                    assignment.assigned_at,
                    assignment.status
                ))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error creating assignment: {e}")
            return False
    
    def get_assignment(self, account_id: str, group_id: str) -> Optional[Assignment]:
        """Get assignment by account and group"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM persistent_assignments WHERE account_id = ? AND group_id = ?",
                    (account_id, group_id)
                )
                row = cursor.fetchone()
                
                if row:
                    return Assignment(
                        id=row['id'],
                        account_id=row['account_id'],
                        group_id=row['group_id'],
                        assigned_at=row['assigned_at'],
                        status=row['status'],
                        last_message_fetch=row['last_message_fetch'],
                        total_messages_fetched=row['total_messages_fetched']
                    )
                return None
        except Exception as e:
            self.logger.error(f"Error getting assignment: {e}")
            return None
    
    def get_active_assignments_by_account(self, account_id: str) -> List[Assignment]:
        """Get active assignments for account"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM persistent_assignments WHERE account_id = ? AND status = 'active'",
                    (account_id,)
                )
                rows = cursor.fetchall()
                
                assignments = []
                for row in rows:
                    assignments.append(Assignment(
                        id=row['id'],
                        account_id=row['account_id'],
                        group_id=row['group_id'],
                        assigned_at=row['assigned_at'],
                        status=row['status'],
                        last_message_fetch=row['last_message_fetch'],
                        total_messages_fetched=row['total_messages_fetched']
                    ))
                return assignments
        except Exception as e:
            self.logger.error(f"Error getting active assignments: {e}")
            return []
    
    def get_active_assignment_by_group(self, group_id: str) -> Optional[Assignment]:
        """Get active assignment for group"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM persistent_assignments WHERE group_id = ? AND status = 'active'",
                    (group_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    return Assignment(
                        id=row['id'],
                        account_id=row['account_id'],
                        group_id=row['group_id'],
                        assigned_at=row['assigned_at'],
                        status=row['status'],
                        last_message_fetch=row['last_message_fetch'],
                        total_messages_fetched=row['total_messages_fetched']
                    )
                return None
        except Exception as e:
            self.logger.error(f"Error getting active assignment: {e}")
            return None
    
    def update_assignment_status(self, account_id: str, group_id: str, status: str) -> bool:
        """Update assignment status"""
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE persistent_assignments SET status = ?, updated_at = ? WHERE account_id = ? AND group_id = ?",
                    (status, datetime.now(), account_id, group_id)
                )
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error updating assignment status: {e}")
            return False
    
    def get_all_assigned_group_ids(self) -> List[str]:
        """Get all assigned group IDs"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT group_id FROM persistent_assignments WHERE status = 'active'"
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting assigned group IDs: {e}")
            return []
    
    def count_active_assignments(self) -> int:
        """Count active assignments"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM persistent_assignments WHERE status = 'active'")
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error counting active assignments: {e}")
            return 0
    
    # Assignment history operations
    def create_assignment_history(self, history: AssignmentHistory) -> bool:
        """Create assignment history record"""
        try:
            with self.get_connection() as conn:
                group_id = group_data.get("id", str(uuid.uuid4()))
                conn.execute("""
                    INSERT INTO assignment_history (id, account_id, group_id, action, timestamp, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    history.id,
                    history.account_id,
                    history.group_id,
                    history.action,
                    history.timestamp,
                    history.reason
                ))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error creating assignment history: {e}")
            return False
    
    # Message operations
    async def store_message(self, message: Message, account_id: str, group_id: str) -> bool:
        """Store message"""
        try:
            with self.get_connection() as conn:
                group_id = group_data.get("id", str(uuid.uuid4()))
                conn.execute("""
                    INSERT INTO messages (id, group_id, account_id, message_text, timestamp, is_job_message, job_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    message.id,
                    group_id,
                    account_id,
                    message.message_text,
                    message.timestamp,
                    message.is_job_message,
                    message.job_score
                ))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error storing message: {e}")
            return False
    
    async def get_message_count_by_account(self, account_id: str) -> int:
        """Get message count for account"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE account_id = ?",
                    (account_id,)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting message count: {e}")
            return 0
