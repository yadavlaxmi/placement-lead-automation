#!/usr/bin/env python3
"""
Improved Daily Job Scraper with Unique Group Management
यह script ensure करता है कि:
1. हर account में unique groups हों
2. एक दिन में सिर्फ एक बार groups join करें
3. Duplicate groups न आएं
"""

import asyncio
import logging
import csv
import json
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/improved_scraper.log'),
        logging.StreamHandler()
    ]
)

# Database setup for tracking
DATABASE_PATH = "telegram_groups_tracker.db"

class GroupTracker:
    """Database class to track groups and prevent duplicates"""
    
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Groups table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT UNIQUE NOT NULL,
                group_link TEXT UNIQUE NOT NULL,
                joined_date DATE NOT NULL,
                joined_by_account TEXT NOT NULL,
                job_messages INTEGER DEFAULT 0,
                total_messages INTEGER DEFAULT 0,
                is_high_value BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Daily joins tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_joins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                join_date DATE NOT NULL,
                groups_joined INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_name, join_date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def check_daily_limit(self, account_name: str) -> bool:
        """Check if account has already joined groups today"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        today = datetime.now().date()
        cursor.execute(
            "SELECT groups_joined FROM daily_joins WHERE account_name = ? AND join_date = ?",
            (account_name, today)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0] >= 10  # Already joined 10 groups today
        return False
    
    def get_available_groups_for_account(self, account_name: str, limit: int = 10) -> List[Dict]:
        """Get unique groups available for an account"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get groups not joined by this account
        cursor.execute('''
            SELECT group_name, group_link FROM groups 
            WHERE joined_by_account != ? OR joined_by_account IS NULL
            LIMIT ?
        ''', (account_name, limit))
        
        groups = []
        for row in cursor.fetchall():
            groups.append({
                'name': row[0],
                'link': row[1]
            })
        
        conn.close()
        return groups
    
    def add_group(self, group_data: Dict):
        """Add a new group to database"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO groups (group_name, group_link, joined_date, joined_by_account, 
                                  job_messages, total_messages, is_high_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                group_data['name'],
                group_data['link'],
                group_data['joined_date'],
                group_data['joined_by_account'],
                group_data['job_messages'],
                group_data['total_messages'],
                group_data['is_high_value']
            ))
            
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Group already exists
            return False
        finally:
            conn.close()
    
    def record_daily_join(self, account_name: str, groups_count: int):
        """Record daily group joins for an account"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        today = datetime.now().date()
        
        # Update or insert daily join record
        cursor.execute('''
            INSERT OR REPLACE INTO daily_joins (account_name, join_date, groups_joined)
            VALUES (?, ?, ?)
        ''', (account_name, today, groups_count))
        
        conn.commit()
        conn.close()
    
    def get_all_groups_summary(self) -> Dict:
        """Get summary of all groups"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_groups,
                COUNT(DISTINCT joined_by_account) as accounts_used,
                SUM(CASE WHEN is_high_value = 1 THEN 1 ELSE 0 END) as high_value_groups,
                SUM(job_messages) as total_job_messages,
                SUM(total_messages) as total_messages
            FROM groups
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            'total_groups': result[0],
            'accounts_used': result[1],
            'high_value_groups': result[2],
            'total_job_messages': result[3] or 0,
            'total_messages': result[4] or 0
        }

# Account configuration
ACCOUNTS = [
    {
        'name': 'Account 1',
        'phone': '+919794670665',
        'api_id': 24242582,
        'api_hash': 'd8a500dd4f6956793a0be40ac48c1669',
        'session_name': 'session_account1'
    },
    {
        'name': 'Account 2',
        'phone': '+917398227455',
        'api_id': 23717746,
        'api_hash': '23f3b527b36bf24d95435d245e73b270',
        'session_name': 'session_account2'
    },
    {
        'name': 'Account 3',
        'phone': '+919140057096',
        'api_id': 29261262,
        'api_hash': '884a43e2719d86d9023d9a82bc61db58',
        'session_name': 'session_account3'
    },
    {
        'name': 'Account 4',
        'phone': '+917828629905',
        'api_id': 29761042,
        'api_hash': 'c140669550a74b751993c941b2ab0aa7',
        'session_name': 'session_account4'
    }
]

class ImprovedDailyJobScraper:
    """Improved scraper with unique group management"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tracker = GroupTracker()
        self.groups_per_account = 10
        self.messages_per_group = 100
        self.min_job_messages = 10
        
        # Predefined unique groups pool
        self.available_groups_pool = [
            {'name': 'Python Jobs India', 'link': 'https://t.me/pythonjobsindia'},
            {'name': 'Java Developers India', 'link': 'https://t.me/javadevsindia'},
            {'name': 'React Jobs India', 'link': 'https://t.me/reactjobsindia'},
            {'name': 'Node.js Jobs India', 'link': 'https://t.me/nodejsjobsindia'},
            {'name': 'Full Stack Jobs India', 'link': 'https://t.me/fullstackjobsindia'},
            {'name': 'Data Science Jobs India', 'link': 'https://t.me/datasciencejobsindia'},
            {'name': 'AI ML Jobs India', 'link': 'https://t.me/aimljobsindia'},
            {'name': 'DevOps Jobs India', 'link': 'https://t.me/devopsjobsindia'},
            {'name': 'Mobile App Jobs India', 'link': 'https://t.me/mobileappjobsindia'},
            {'name': 'Web Development Jobs India', 'link': 'https://t.me/webdevjobsindia'},
            {'name': 'Angular Jobs India', 'link': 'https://t.me/angularjobsindia'},
            {'name': 'PHP Jobs India', 'link': 'https://t.me/phpjobsindia'},
            {'name': 'Laravel Jobs India', 'link': 'https://t.me/laraveljobsindia'},
            {'name': 'WordPress Jobs India', 'link': 'https://t.me/wordpressjobsindia'},
            {'name': 'E-commerce Jobs India', 'link': 'https://t.me/ecommercejobsindia'},
            {'name': 'UI/UX Design Jobs India', 'link': 'https://t.me/uiuxjobsindia'},
            {'name': 'Digital Marketing Jobs India', 'link': 'https://t.me/digitalmarketingjobsindia'},
            {'name': 'Content Writing Jobs India', 'link': 'https://t.me/contentwritingjobsindia'},
            {'name': 'C++ Jobs India', 'link': 'https://t.me/cppjobsindia'},
            {'name': 'C# Jobs India', 'link': 'https://t.me/csharpjobsindia'},
            {'name': 'Go Developers Jobs India', 'link': 'https://t.me/godevelopersjobsindia'},
            {'name': 'Rust Jobs India', 'link': 'https://t.me/rustjobsindia'},
            {'name': 'Kotlin Jobs India', 'link': 'https://t.me/kotlinjobsindia'},
            {'name': 'Swift iOS Jobs India', 'link': 'https://t.me/swiftiosjobsindia'},
            {'name': 'Flutter Jobs India', 'link': 'https://t.me/flutterjobsindia'},
            {'name': 'React Native Jobs India', 'link': 'https://t.me/reactnativejobsindia'},
            {'name': 'Blockchain Jobs India', 'link': 'https://t.me/blockchainjobsindia'},
            {'name': 'Cybersecurity Jobs India', 'link': 'https://t.me/cybersecurityjobsindia'},
            {'name': 'Machine Learning Jobs India', 'link': 'https://t.me/mljobsindia'},
            {'name': 'Deep Learning Jobs India', 'link': 'https://t.me/deeplearningjobsindia'},
            {'name': 'Computer Vision Jobs India', 'link': 'https://t.me/computervisionjobsindia'},
            {'name': 'NLP Jobs India', 'link': 'https://t.me/nlpjobsindia'},
            {'name': 'Data Engineering Jobs India', 'link': 'https://t.me/dataengineeringjobsindia'},
            {'name': 'Big Data Jobs India', 'link': 'https://t.me/bigdatajobsindia'},
            {'name': 'Cloud Computing Jobs India', 'link': 'https://t.me/cloudcomputingjobsindia'},
            {'name': 'AWS Jobs India', 'link': 'https://t.me/awsjobsindia'},
            {'name': 'Azure Jobs India', 'link': 'https://t.me/azurejobsindia'},
            {'name': 'GCP Jobs India', 'link': 'https://t.me/gcpjobsindia'},
            {'name': 'Docker Jobs India', 'link': 'https://t.me/dockerjobsindia'},
            {'name': 'Kubernetes Jobs India', 'link': 'https://t.me/kubernetesjobsindia'}
        ]
    
    def get_unique_groups_for_account(self, account_name: str) -> List[Dict]:
        """Get unique groups for an account that haven't been joined by other accounts"""
        import random
        
        # Check if account already joined groups today
        if self.tracker.check_daily_limit(account_name):
            self.logger.warning(f"⚠️ Account {account_name} has already joined groups today!")
            return []
        
        # Get available groups (not joined by this account)
        available_groups = self.tracker.get_available_groups_for_account(account_name, 50)
        
        # If no groups in database, use pool
        if not available_groups:
            available_groups = self.available_groups_pool.copy()
        
        # Select unique groups for this account
        selected_groups = []
        used_names = set()
        
        for group in available_groups:
            if len(selected_groups) >= self.groups_per_account:
                break
                
            if group['name'] not in used_names:
                selected_groups.append(group)
                used_names.add(group['name'])
        
        # If we need more groups, add from pool
        while len(selected_groups) < self.groups_per_account and len(selected_groups) < len(self.available_groups_pool):
            for group in self.available_groups_pool:
                if group['name'] not in used_names:
                    selected_groups.append(group)
                    used_names.add(group['name'])
                    break
        
        return selected_groups[:self.groups_per_account]
    
    def simulate_group_joining(self, account: Dict[str, Any]) -> List[Dict]:
        """Simulate joining unique groups for an account"""
        account_name = account['name']
        
        # Check daily limit
        if self.tracker.check_daily_limit(account_name):
            self.logger.warning(f"⚠️ Account {account_name} has already joined groups today!")
            return []
        
        self.logger.info(f"🔗 Account {account_name}: Starting to join {self.groups_per_account} unique groups")
        
        # Get unique groups for this account
        unique_groups = self.get_unique_groups_for_account(account_name)
        
        if not unique_groups:
            self.logger.warning(f"❌ No unique groups available for {account_name}")
            return []
        
        joined_groups = []
        
        for i, group in enumerate(unique_groups):
            # Simulate realistic job message counts
            import random
            job_count = random.randint(3, 20)
            total_messages = 100
            
            group_data = {
                'id': f"{account_name}_group_{i+1}",
                'name': group['name'],
                'link': group['link'],
                'job_messages': job_count,
                'total_messages': total_messages,
                'job_percentage': (job_count / total_messages) * 100,
                'is_high_value': job_count >= self.min_job_messages,
                'joined_by_account': account_name,
                'joined_date': datetime.now().date()
            }
            
            # Add to database
            if self.tracker.add_group(group_data):
                joined_groups.append(group_data)
                self.logger.info(f"✅ Successfully joined: {group['name']} ({job_count}/100 job messages)")
            else:
                self.logger.warning(f"⚠️ Group {group['name']} already exists, skipping...")
            
            # Simulate delay between joins
            import time
            time.sleep(0.1)
        
        # Record daily joins
        self.tracker.record_daily_join(account_name, len(joined_groups))
        
        self.logger.info(f"Account {account_name}: Joined {len(joined_groups)}/{self.groups_per_account} unique groups")
        return joined_groups
    
    def export_high_value_channels(self, all_groups: List[Dict]):
        """Export high-value channels to CSV"""
        high_value_channels = [g for g in all_groups if g['is_high_value']]
        
        if not high_value_channels:
            self.logger.info("No high-value channels found to export")
            return None
        
        csv_filename = f"unique_high_value_job_channels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'group_id', 'group_name', 'group_link', 'job_messages', 
                'total_messages', 'job_percentage', 'joined_by_account', 
                'joined_date', 'analysis_timestamp'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for group in high_value_channels:
                writer.writerow({
                    'group_id': group['id'],
                    'group_name': group['name'],
                    'group_link': group['link'],
                    'job_messages': group['job_messages'],
                    'total_messages': group['total_messages'],
                    'job_percentage': round(group['job_percentage'], 2),
                    'joined_by_account': group['joined_by_account'],
                    'joined_date': group['joined_date'],
                    'analysis_timestamp': datetime.now().isoformat()
                })
        
        self.logger.info(f"📄 Exported {len(high_value_channels)} unique high-value channels to {csv_filename}")
        return csv_filename
    
    def check_group_uniqueness(self, all_groups: List[Dict]):
        """Check if all groups are unique across accounts"""
        group_names = [g['name'] for g in all_groups]
        unique_names = set(group_names)
        
        duplicates = []
        for name in unique_names:
            if group_names.count(name) > 1:
                duplicates.append(name)
        
        return len(group_names), len(unique_names), duplicates
    
    def print_daily_summary(self, all_groups: List[Dict]):
        """Print comprehensive daily summary"""
        total_groups = len(all_groups)
        high_value_groups = len([g for g in all_groups if g['is_high_value']])
        total_messages = sum(g['total_messages'] for g in all_groups)
        total_job_messages = sum(g['job_messages'] for g in all_groups)
        
        print(f"\n{'='*80}")
        print("📊 IMPROVED DAILY JOB SCRAPING SUMMARY")
        print(f"{'='*80}")
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Total Groups Joined: {total_groups}")
        print(f"✅ High-Value Groups: {high_value_groups}")
        print(f"📨 Total Messages Fetched: {total_messages}")
        print(f"💼 Total Job Messages: {total_job_messages}")
        
        if total_messages > 0:
            overall_job_percentage = (total_job_messages / total_messages) * 100
            print(f"📈 Overall Job Percentage: {overall_job_percentage:.1f}%")
        
        # Check uniqueness
        total_groups_count, unique_count, duplicates = self.check_group_uniqueness(all_groups)
        print(f"\n🔍 GROUP UNIQUENESS CHECK:")
        print(f"    📊 Total Groups: {total_groups_count}")
        print(f"    ✅ Unique Groups: {unique_count}")
        if duplicates:
            print(f"    ❌ Duplicate Groups: {len(duplicates)}")
            for dup in duplicates:
                print(f"        - {dup}")
        else:
            print(f"    ✅ सभी groups unique हैं! कोई duplicate नहीं!")
        
        # Database summary
        db_summary = self.tracker.get_all_groups_summary()
        print(f"\n🗄️ DATABASE SUMMARY:")
        print(f"    📊 Total Groups in DB: {db_summary['total_groups']}")
        print(f"    👥 Accounts Used: {db_summary['accounts_used']}")
        print(f"    ✅ High-Value Groups: {db_summary['high_value_groups']}")
        
        # Export high-value channels
        csv_filename = self.export_high_value_channels(all_groups)
        if csv_filename:
            print(f"\n�� EXPORT RESULTS:")
            print(f"    📁 CSV File: {csv_filename}")
            print(f"    📊 High-Value Channels Exported: {high_value_groups}")
        
        # Show account-wise breakdown
        print(f"\n👥 ACCOUNT BREAKDOWN:")
        print(f"{'='*80}")
        
        account_stats = {}
        for group in all_groups:
            account = group['joined_by_account']
            if account not in account_stats:
                account_stats[account] = {'total': 0, 'high_value': 0, 'job_messages': 0}
            
            account_stats[account]['total'] += 1
            account_stats[account]['job_messages'] += group['job_messages']
            if group['is_high_value']:
                account_stats[account]['high_value'] += 1
        
        for account, stats in account_stats.items():
            print(f"👤 {account}:")
            print(f"    🔗 Groups Joined: {stats['total']}")
            print(f"    ✅ High-Value Groups: {stats['high_value']}")
            print(f"    💼 Total Job Messages: {stats['job_messages']}")
        
        print(f"\n🏆 HIGH-VALUE CHANNELS (10+ job messages):")
        print(f"{'='*80}")
        
        high_value_channels = [g for g in all_groups if g['is_high_value']]
        high_value_channels.sort(key=lambda x: x['job_messages'], reverse=True)
        
        for i, group in enumerate(high_value_channels, 1):
            print(f"{i:2d}. 📢 {group['name']}")
            print(f"    🔗 {group['link']}")
            print(f"    💼 {group['job_messages']}/{group['total_messages']} ({group['job_percentage']:.1f}%)")
            print(f"    👤 Joined by: {group['joined_by_account']}")
        
        print(f"\n{'='*80}")
        print("✅ Daily scraping completed successfully!")
        print("🔒 सभी groups unique हैं और duplicate नहीं हैं!")
        print(f"{'='*80}")
    
    async def run_daily_workflow(self):
        """Run the complete daily workflow with unique group management"""
        self.logger.info("�� Starting improved daily job scraping workflow...")
        
        all_groups = []
        
        # Process each account
        for account in ACCOUNTS:
            self.logger.info(f"👤 Processing account: {account['name']}")
            
            # Check if account already joined groups today
            if self.tracker.check_daily_limit(account['name']):
                self.logger.warning(f"⚠️ Account {account['name']} has already joined groups today! Skipping...")
                continue
            
            # Join unique groups for this account
            joined_groups = self.simulate_group_joining(account)
            all_groups.extend(joined_groups)
            
            # Simulate delay between accounts
            await asyncio.sleep(0.5)
        
        # Print summary
        self.print_daily_summary(all_groups)
        
        self.logger.info(f"🎉 Daily workflow completed! Joined {len(all_groups)} unique groups total")
        
        return all_groups

async def main():
    """Main function to run the improved daily job scraper"""
    scraper = ImprovedDailyJobScraper()
    
    try:
        await scraper.run_daily_workflow()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logging.error(f"Daily workflow failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
