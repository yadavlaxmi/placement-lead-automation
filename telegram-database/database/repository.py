"""
Database Repository - Data Access Layer
"""
import sqlite3
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from models.account import Account
from models.group import Group
from models.assignment import Assignment, AssignmentHistory
from config.settings import settings

class DatabaseRepository:
    """Database repository for all data operations"""
    
    def __init__(self):
        self.db_path = settings.DATABASE_URL.replace("sqlite:///", "")
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self):
        """Initialize database and create tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Read and execute schema
            with open('database/schema.sql', 'r') as f:
                schema = f.read()
                cursor.executescript(schema)
            
            conn.commit()
            conn.close()
            
            self.logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    # Account operations
    async def create_account(self, account_config: Dict[str, Any]) -> bool:
        """Create new account"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO accounts 
                (id, name, phone, api_id, api_hash, session_name, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account_config['id'],
                account_config['name'],
                account_config['phone'],
                account_config['api_id'],
                account_config['api_hash'],
                account_config['session_name'],
                'active',
                datetime.now(),
                datetime.now()
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Created account: {account_config['name']}")
            return group_data["id"]
            
        except Exception as e:
            self.logger.error(f"Error creating account: {e}")
            return False
    
    def get_account_by_id(self, account_id: str) -> Optional[Account]:
        """Get account by ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM accounts WHERE id = ?", (account_id,))
            row = cursor.fetchone()
            
            conn.close()
            
            if row:
                return Account(
                    id=row[0], name=row[1], phone=row[2],
                    api_id=row[3], api_hash=row[4], session_name=row[5],
                    status=row[6], created_at=row[7], updated_at=row[8]
                )
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting account: {e}")
            return None
    
    def get_all_accounts(self) -> List[Account]:
        """Get all accounts"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM accounts WHERE status = 'active'")
            rows = cursor.fetchall()
            
            conn.close()
            
            accounts = []
            for row in rows:
                accounts.append(Account(
                    id=row[0], name=row[1], phone=row[2],
                    api_id=row[3], api_hash=row[4], session_name=row[5],
                    status=row[6], created_at=row[7], updated_at=row[8]
                ))
            
            return accounts
            
        except Exception as e:
            self.logger.error(f"Error getting accounts: {e}")
            return []
    
    # Group operations
    async def create_group(self, group_data: Dict[str, Any]) -> str:
        """Create new group"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            group_id = str(uuid.uuid4())
            
            cursor.execute("""
                INSERT OR REPLACE INTO groups 
                (id, name, link, category, priority, credibility_score, total_members, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                group_id,
                group_data['name'],
                group_data['link'],
                group_data['category'],
                group_data['priority'],
                group_data.get('credibility_score', 0.0),
                group_data.get('total_members', 0),
                datetime.now(),
                datetime.now()
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Created group: {group_data['name']}")
            return group_id
            
        except Exception as e:
            self.logger.error(f"Error creating group: {e}")
            return None
    
    def get_group_by_id(self, group_id: str) -> Optional[Group]:
        """Get group by ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM groups WHERE id = ?", (group_id,))
            row = cursor.fetchone()
            
            conn.close()
            
            if row:
                return Group(
                    id=row[0], name=row[1], link=row[2],
                    category=row[3], priority=row[4], credibility_score=row[5],
                    total_members=row[6], created_at=row[7], updated_at=row[8]
                )
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting group: {e}")
            return None
    
    def get_all_groups(self) -> List[Group]:
        """Get all groups"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM groups")
            rows = cursor.fetchall()
            
            conn.close()
            
            groups = []
            for row in rows:
                groups.append(Group(
                    id=row[0], name=row[1], link=row[2],
                    category=row[3], priority=row[4], credibility_score=row[5],
                    total_members=row[6], created_at=row[7], updated_at=row[8]
                ))
            
            return groups
            
        except Exception as e:
            self.logger.error(f"Error getting groups: {e}")
            return []
    
    # Assignment operations
    def create_assignment(self, assignment: Assignment) -> bool:
        """Create assignment"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO persistent_assignments 
                (id, account_id, group_id, assigned_at, status, last_message_fetch, total_messages_fetched)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                assignment.id,
                assignment.account_id,
                assignment.group_id,
                assignment.assigned_at,
                assignment.status,
                assignment.last_message_fetch,
                assignment.total_messages_fetched
            ))
            
            conn.commit()
            conn.close()
            
            return group_data["id"]
            
        except Exception as e:
            self.logger.error(f"Error creating assignment: {e}")
            return False
    
    def get_assignment(self, account_id: str, group_id: str) -> Optional[Assignment]:
        """Get assignment"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM persistent_assignments 
                WHERE account_id = ? AND group_id = ?
            """, (account_id, group_id))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return Assignment(
                    id=row[0], account_id=row[1], group_id=row[2],
                    assigned_at=row[3], status=row[4], last_message_fetch=row[5],
                    total_messages_fetched=row[6]
                )
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting assignment: {e}")
            return None
    
    def get_active_assignments_by_account(self, account_id: str) -> List[Assignment]:
        """Get active assignments by account"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM persistent_assignments 
                WHERE account_id = ? AND status = 'active'
            """, (account_id,))
            
            rows = cursor.fetchall()
            conn.close()
            
            assignments = []
            for row in rows:
                assignments.append(Assignment(
                    id=row[0], account_id=row[1], group_id=row[2],
                    assigned_at=row[3], status=row[4], last_message_fetch=row[5],
                    total_messages_fetched=row[6]
                ))
            
            return assignments
            
        except Exception as e:
            self.logger.error(f"Error getting assignments: {e}")
            return []
    
    def get_active_assignment_by_group(self, group_id: str) -> Optional[Assignment]:
        """Get active assignment by group"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM persistent_assignments 
                WHERE group_id = ? AND status = 'active'
            """, (group_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return Assignment(
                    id=row[0], account_id=row[1], group_id=row[2],
                    assigned_at=row[3], status=row[4], last_message_fetch=row[5],
                    total_messages_fetched=row[6]
                )
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting assignment: {e}")
            return None
    
    def update_assignment_status(self, account_id: str, group_id: str, status: str) -> bool:
        """Update assignment status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE persistent_assignments 
                SET status = ?, updated_at = ?
                WHERE account_id = ? AND group_id = ?
            """, (status, datetime.now(), account_id, group_id))
            
            conn.commit()
            conn.close()
            
            return group_data["id"]
            
        except Exception as e:
            self.logger.error(f"Error updating assignment: {e}")
            return False
    
    def get_all_assigned_group_ids(self) -> List[str]:
        """Get all assigned group IDs"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT group_id FROM persistent_assignments WHERE status = 'active'")
            rows = cursor.fetchall()
            
            conn.close()
            
            return [row[0] for row in rows]
            
        except Exception as e:
            self.logger.error(f"Error getting assigned groups: {e}")
            return []
    
    # Assignment history
    def create_assignment_history(self, history: AssignmentHistory) -> bool:
        """Create assignment history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO assignment_history 
                (id, account_id, group_id, action, timestamp, reason)
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
            conn.close()
            
            return group_data["id"]
            
        except Exception as e:
            self.logger.error(f"Error creating history: {e}")
            return False
    
    # Message operations
    async def store_message(self, message_data: Dict[str, Any], account_id: str, group_id: str) -> bool:
        """Store message"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            message_id = str(uuid.uuid4())
            
            cursor.execute("""
                INSERT OR REPLACE INTO messages 
                (id, group_id, account_id, message_text, timestamp, is_job_message, job_score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                message_id,
                group_id,
                account_id,
                message_data.get('text', ''),
                message_data.get('timestamp', datetime.now()),
                message_data.get('is_job_message', False),
                message_data.get('job_score', 0.0)
            ))
            
            conn.commit()
            conn.close()
            
            return group_data["id"]
            
        except Exception as e:
            self.logger.error(f"Error storing message: {e}")
            return False
    
    async def get_message_count_by_account(self, account_id: str) -> int:
        """Get message count by account"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM messages WHERE account_id = ?", (account_id,))
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error getting message count: {e}")
            return 0
    
    # Statistics
    def count_active_assignments(self) -> int:
        """Count active assignments"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM persistent_assignments WHERE status = 'active'")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error counting assignments: {e}")
            return 0
    
    def count_groups(self) -> int:
        """Count groups"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM groups")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error counting groups: {e}")
            return 0
    
    def count_accounts(self) -> int:
        """Count accounts"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error counting accounts: {e}")
            return 0
