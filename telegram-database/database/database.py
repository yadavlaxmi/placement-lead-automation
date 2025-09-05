import sqlite3
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import config

class DatabaseManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DATABASE_PATH
        self.init_database()
        
    def init_database(self):
        """Initialize the database with schema"""
        try:
            with open('database/schema.sql', 'r') as f:
                schema = f.read()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.executescript(schema)
                conn.commit()
            logging.info("Database initialized successfully")
        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            raise
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def insert_city(self, name: str, state: str = None, country: str = "India") -> int:
        """Insert a new city and return its ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO cities (name, state, country) VALUES (?, ?, ?)",
                (name, state, country)
            )
            conn.commit()
            return cursor.lastrowid
    
    def insert_programming_group(self, group_data: Dict[str, Any]) -> int:
        """Insert a new programming group and return its ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO programming_groups 
                (group_name, group_link, group_id, city_id, source_type, credibility_score, joined_by_account)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                group_data['group_name'],
                group_data['group_link'],
                group_data.get('group_id'),
                group_data.get('city_id'),
                group_data.get('source_type', 'telegram'),
                group_data.get('credibility_score', 0.0),
                group_data.get('joined_by_account')
            ))
            conn.commit()
            return cursor.lastrowid
    
    def insert_account_group_assignment(self, account_name: str, group_id: int, assignment_date: str = None) -> int:
        """Insert account-group assignment"""
        if assignment_date is None:
            assignment_date = datetime.now().date().isoformat()
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO account_group_assignments 
                (account_name, group_id, assignment_date)
                VALUES (?, ?, ?)
            """, (account_name, group_id, assignment_date))
            conn.commit()
            return cursor.lastrowid
    
    def get_groups_by_account(self, account_name: str, date: str = None) -> List[Dict[str, Any]]:
        """Get all groups joined by a specific account"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if date:
                cursor.execute("""
                    SELECT pg.*, aga.assignment_date
                    FROM programming_groups pg
                    JOIN account_group_assignments aga ON pg.id = aga.group_id
                    WHERE aga.account_name = ? AND aga.assignment_date = ?
                    ORDER BY aga.assignment_date DESC
                """, (account_name, date))
            else:
                cursor.execute("""
                    SELECT pg.*, aga.assignment_date
                    FROM programming_groups pg
                    JOIN account_group_assignments aga ON pg.id = aga.group_id
                    WHERE aga.account_name = ?
                    ORDER BY aga.assignment_date DESC
                """, (account_name,))
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_account_group_summary(self) -> Dict[str, Any]:
        """Get summary of which account joined which groups"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    aga.account_name,
                    COUNT(DISTINCT aga.group_id) as total_groups,
                    COUNT(DISTINCT aga.assignment_date) as active_days,
                    MAX(aga.assignment_date) as last_assignment
                FROM account_group_assignments aga
                GROUP BY aga.account_name
                ORDER BY aga.account_name
            """)
            
            columns = [description[0] for description in cursor.description]
            summary = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Get detailed breakdown
            cursor.execute("""
                SELECT 
                    aga.account_name,
                    aga.assignment_date,
                    COUNT(aga.group_id) as groups_joined
                FROM account_group_assignments aga
                GROUP BY aga.account_name, aga.assignment_date
                ORDER BY aga.account_name, aga.assignment_date DESC
            """)
            
            columns = [description[0] for description in cursor.description]
            daily_breakdown = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return {
                "summary": summary,
                "daily_breakdown": daily_breakdown
            }
    
    def get_unique_groups_per_account(self, date: str = None) -> Dict[str, List[str]]:
        """Get unique groups assigned to each account"""
        if date is None:
            date = datetime.now().date().isoformat()
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    aga.account_name,
                    GROUP_CONCAT(pg.group_name, ', ') as group_names,
                    GROUP_CONCAT(pg.group_link, ', ') as group_links
                FROM account_group_assignments aga
                JOIN programming_groups pg ON aga.group_id = pg.id
                WHERE aga.assignment_date = ?
                GROUP BY aga.account_name
            """, (date,))
            
            result = {}
            for row in cursor.fetchall():
                account_name, group_names, group_links = row
                result[account_name] = {
                    "groups": group_names.split(', ') if group_names else [],
                    "links": group_links.split(', ') if group_links else []
                }
            
            return result
    
    def insert_message(self, message_data: Dict[str, Any]) -> int:
        """Insert a new message and return its ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO messages 
                (group_id, message_id, sender_id, sender_name, message_text, timestamp, is_job_post, fetched_by_account)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message_data['group_id'],
                message_data['message_id'],
                message_data.get('sender_id'),
                message_data.get('sender_name'),
                message_data['message_text'],
                message_data.get('timestamp'),
                message_data.get('is_job_post', False),
                message_data.get('fetched_by_account')
            ))
            conn.commit()
            return cursor.lastrowid
    
    def insert_job_score(self, job_score_data: Dict[str, Any]) -> int:
        """Insert job score data and return its ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            tags_json = json.dumps(job_score_data.get('tags', []))
            cursor.execute("""
                INSERT INTO job_scores 
                (message_id, salary_score, contact_score, website_score, name_score, 
                 skill_score, experience_score, location_score, remote_score, 
                 fresher_friendly_score, overall_score, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_score_data['message_id'],
                job_score_data.get('salary_score', 0.0),
                job_score_data.get('contact_score', 0.0),
                job_score_data.get('website_score', 0.0),
                job_score_data.get('name_score', 0.0),
                job_score_data.get('skill_score', 0.0),
                job_score_data.get('experience_score', 0.0),
                job_score_data.get('location_score', 0.0),
                job_score_data.get('remote_score', 0.0),
                job_score_data.get('fresher_friendly_score', 0.0),
                job_score_data.get('overall_score', 0.0),
                tags_json
            ))
            conn.commit()
            return cursor.lastrowid
    
    def get_cities(self) -> List[Dict[str, Any]]:
        """Get all cities"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cities ORDER BY name")
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_programming_groups(self, city_id: int = None, limit: int = None, account_name: str = None) -> List[Dict[str, Any]]:
        """Get programming groups, optionally filtered by city or account"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if account_name:
                cursor.execute("""
                    SELECT pg.* FROM programming_groups pg
                    JOIN account_group_assignments aga ON pg.id = aga.group_id
                    WHERE aga.account_name = ? AND pg.is_active = 1
                    ORDER BY pg.credibility_score DESC
                """, (account_name,))
            elif city_id:
                cursor.execute("""
                    SELECT * FROM programming_groups 
                    WHERE city_id = ? AND is_active = 1
                    ORDER BY credibility_score DESC
                """, (city_id,))
            else:
                cursor.execute("""
                    SELECT * FROM programming_groups 
                    WHERE is_active = 1
                    ORDER BY credibility_score DESC
                """)
            
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            if limit:
                rows = rows[:limit]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_messages(self, group_id: int, limit: int = 200, account_name: str = None) -> List[Dict[str, Any]]:
        """Get messages from a specific group, optionally filtered by account"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if account_name:
                cursor.execute("""
                    SELECT * FROM messages 
                    WHERE group_id = ? AND fetched_by_account = ?
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (group_id, account_name, limit))
            else:
                cursor.execute("""
                    SELECT * FROM messages 
                    WHERE group_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (group_id, limit))
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def update_group_credibility(self, group_id: int, credibility_score: float):
        """Update the credibility score of a group"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE programming_groups 
                SET credibility_score = ? 
                WHERE id = ?
            """, (credibility_score, group_id))
            conn.commit()
    
    def update_group_message_count(self, group_id: int, count: int):
        """Update the total message count of a group"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE programming_groups 
                SET total_messages = ? 
                WHERE id = ?
            """, (count, group_id))
            conn.commit()
    
    def get_high_score_groups(self, threshold: float = 7.0) -> List[Dict[str, Any]]:
        """Get groups with high credibility scores"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM programming_groups 
                WHERE credibility_score >= ? AND is_active = 1
                ORDER BY credibility_score DESC
            """, (threshold,))
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_fresher_friendly_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get fresher-friendly jobs with high scores"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT m.*, js.*, pg.group_name, pg.group_link
                FROM messages m
                JOIN job_scores js ON m.id = js.message_id
                JOIN programming_groups pg ON m.group_id = pg.id
                WHERE js.fresher_friendly_score >= 7.0
                AND js.overall_score >= 7.0
                ORDER BY js.overall_score DESC
                LIMIT ?
            """, (limit,))
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_messages_by_account(self, account_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all messages fetched by a specific account"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT m.*, pg.group_name, pg.group_link
                FROM messages m
                JOIN programming_groups pg ON m.group_id = pg.id
                WHERE m.fetched_by_account = ?
                ORDER BY m.timestamp DESC
                LIMIT ?
            """, (account_name, limit))
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()] 