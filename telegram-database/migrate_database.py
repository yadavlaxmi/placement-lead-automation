#!/usr/bin/env python3
"""
Database Migration Script - Add new columns for account tracking
"""

import sqlite3
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def migrate_database():
    """Migrate the existing database to add new columns"""
    db_path = "telegram_jobs.db"
    
    if not os.path.exists(db_path):
        logging.info("Database file not found, will be created with new schema")
        return
    
    logging.info("Starting database migration...")
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if columns already exist
            cursor.execute("PRAGMA table_info(programming_groups)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Add joined_by_account column if it doesn't exist
            if 'joined_by_account' not in columns:
                logging.info("Adding joined_by_account column to programming_groups table...")
                cursor.execute("ALTER TABLE programming_groups ADD COLUMN joined_by_account TEXT")
                logging.info("✅ Added joined_by_account column")
            
            # Add joined_date column if it doesn't exist
            if 'joined_date' not in columns:
                logging.info("Adding joined_date column to programming_groups table...")
                cursor.execute("ALTER TABLE programming_groups ADD COLUMN joined_date TEXT")
                # Set default value for existing rows
                current_time = datetime.now().isoformat()
                cursor.execute("UPDATE programming_groups SET joined_date = ? WHERE joined_date IS NULL", (current_time,))
                logging.info("✅ Added joined_date column")
            
            # Check if account_group_assignments table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='account_group_assignments'")
            if not cursor.fetchone():
                logging.info("Creating account_group_assignments table...")
                cursor.execute("""
                    CREATE TABLE account_group_assignments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT NOT NULL,
                        group_id INTEGER NOT NULL,
                        assignment_date DATE NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (group_id) REFERENCES programming_groups (id),
                        UNIQUE(account_name, group_id, assignment_date)
                    )
                """)
                logging.info("✅ Created account_group_assignments table")
            
            # Check messages table columns
            cursor.execute("PRAGMA table_info(messages)")
            msg_columns = [column[1] for column in cursor.fetchall()]
            
            # Add fetched_by_account column if it doesn't exist
            if 'fetched_by_account' not in msg_columns:
                logging.info("Adding fetched_by_account column to messages table...")
                cursor.execute("ALTER TABLE messages ADD COLUMN fetched_by_account TEXT")
                logging.info("✅ Added fetched_by_account column")
            
            # Check crawler_status table columns
            cursor.execute("PRAGMA table_info(crawler_status)")
            crawler_columns = [column[1] for column in cursor.fetchall()]
            
            # Add crawled_by_account column if it doesn't exist
            if 'crawled_by_account' not in crawler_columns:
                logging.info("Adding crawled_by_account column to crawler_status table...")
                cursor.execute("ALTER TABLE crawler_status ADD COLUMN crawled_by_account TEXT")
                logging.info("✅ Added crawled_by_account column")
            
            # Create indexes if they don't exist
            logging.info("Creating indexes...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_programming_groups_account ON programming_groups(joined_by_account)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_account ON messages(fetched_by_account)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_group_assignments ON account_group_assignments(account_name, assignment_date)")
            logging.info("✅ Created indexes")
            
            conn.commit()
            logging.info("🎉 Database migration completed successfully!")
            
    except Exception as e:
        logging.error(f"❌ Database migration failed: {e}")
        raise

if __name__ == "__main__":
    migrate_database() 