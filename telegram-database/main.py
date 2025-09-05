#!/usr/bin/env python3
"""
Telegram Job Scraper - Main Runner
Handles multiple Telegram accounts for crawling programming job groups
"""

import asyncio
import logging
import schedule
import time
import os
from datetime import datetime
from crawler import JobCrawler
from email_notifier import EmailNotifier
from universal_group_manager import UniversalGroupManager
import config

# Create necessary directories first
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)
os.makedirs('models', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/telegram_scraper.log'),
        logging.StreamHandler()
    ]
)

# Your Telegram accounts configuration (without individual groups)
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
        'phone': '+917828629905',  # Add your 4th account details
        'api_id': 29761042,
        'api_hash': 'c140669550a74b751993c941b2ab0aa7',
        'session_name': 'session_account4'
    }
]

class TelegramJobScraper:
    def __init__(self):
        self.crawler = JobCrawler(ACCOUNTS)
        self.email_notifier = EmailNotifier()
        self.universal_group_manager = UniversalGroupManager()
        self.is_running = False
        
    async def start_system(self):
        """Start the entire system"""
        logging.info("🚀 Starting Telegram Job Scraper System...")
        
        # Start the crawler
        self.is_running = True
        await self.crawler.start_crawling()
    
    def stop_system(self):
        """Stop the system"""
        logging.info("🛑 Stopping Telegram Job Scraper System...")
        self.is_running = False
        self.crawler.stop_crawling()
    
    def schedule_daily_email(self):
        """Schedule daily email reports"""
        schedule.every().day.at("09:00").do(self.email_notifier.send_daily_job_report)
        schedule.every().day.at("18:00").do(self.email_notifier.send_daily_job_report)
        logging.info("📧 Scheduled daily email reports at 9:00 AM and 6:00 PM")
    
    def schedule_daily_reset(self):
        """Schedule daily reset of join tracking"""
        schedule.every().day.at("00:00").do(self.universal_group_manager.reset_daily_joins)
        logging.info("🔄 Scheduled daily reset of join tracking at midnight")
    
    def run_scheduler(self):
        """Run the email scheduler"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def show_account_status(self):
        """Show current status of all accounts"""
        logging.info("📊 Account Status Report:")
        for account in ACCOUNTS:
            status = self.universal_group_manager.get_account_status(account['name'])
            logging.info(f"  {account['name']}: Joined {status['joined_today']}/10 today, {status['remaining']} remaining")
        
        # Show universal group stats
        stats = self.universal_group_manager.get_universal_stats()
        logging.info(f"📈 Universal Groups: {stats['total_groups']} total groups available")
    
    def show_unique_group_assignments(self):
        """Show which account joined which unique groups"""
        logging.info("🎯 Unique Group Assignments:")
        
        # Get today's assignments
        summary = self.universal_group_manager.get_all_accounts_groups_summary()
        
        if not summary:
            logging.info("  No group assignments found for today")
            return
        
        for account_name, groups in summary.items():
            logging.info(f"  {account_name}:")
            if not groups:
                logging.info("    No groups assigned today")
            else:
                for i, group in enumerate(groups, 1):
                    priority_emoji = {
                        'high': '🔴',
                        'medium': '🟡', 
                        'low': '🟢'
                    }.get(group.get('priority', 'unknown'), '⚪')
                    
                    logging.info(f"    {i:2d}. {priority_emoji} {group['name']} ({group['link']})")
                    logging.info(f"        Category: {group.get('category', 'unknown')} | Priority: {group.get('priority', 'unknown')}")
    
    def show_database_summary(self):
        """Show database summary of group assignments"""
        logging.info("🗄️ Database Group Assignment Summary:")
        
        from database.database import DatabaseManager
        db = DatabaseManager()
        db_summary = db.get_account_group_summary()
        
        if not db_summary['summary']:
            logging.info("  No group assignments found in database")
            return
        
        for account_summary in db_summary['summary']:
            logging.info(f"  {account_summary['account_name']}:")
            logging.info(f"    Total Groups: {account_summary['total_groups']}")
            logging.info(f"    Active Days: {account_summary['active_days']}")
            logging.info(f"    Last Assignment: {account_summary['last_assignment']}")
    
    async def get_account_group_report(self):
        """Get detailed account-group report"""
        return await self.crawler.get_account_group_report()

async def main():
    """Main function"""
    scraper = TelegramJobScraper()
    
    try:
        # Schedule daily tasks
        scraper.schedule_daily_email()
        scraper.schedule_daily_reset()
        
        # Show initial status
        scraper.show_account_status()
        scraper.show_unique_group_assignments()
        scraper.show_database_summary()
        
        # Start the system
        await scraper.start_system()
        
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, shutting down...")
        scraper.stop_system()
    except Exception as e:
        logging.error(f"System error: {e}")
        scraper.stop_system()

def run_simple_crawler():
    """Run a simple version for testing"""
    logging.info("🧪 Running simple crawler test...")
    
    scraper = TelegramJobScraper()
    
    # Test database connection
    try:
        from database.database import DatabaseManager
        db = DatabaseManager()
        cities = db.get_cities()
        logging.info(f"✅ Database connection successful. Found {len(cities)} cities.")
        
        # Test search engine
        from search_engine import SearchEngine
        search_engine = SearchEngine()
        logging.info("✅ Search engine initialized successfully.")
        
        # Test ML pipeline
        from ml_pipeline import MLPipeline
        ml_pipeline = MLPipeline()
        logging.info("✅ ML pipeline initialized successfully.")
        
        # Test universal group manager
        universal_manager = UniversalGroupManager()
        stats = universal_manager.get_universal_stats()
        logging.info(f"✅ Universal group manager initialized. {stats['total_groups']} groups available.")
        
        # Show account status
        scraper.show_account_status()
        scraper.show_unique_group_assignments()
        
        logging.info("🎉 All components initialized successfully!")
        
    except Exception as e:
        logging.error(f"❌ Component test failed: {e}")

def run_account_report():
    """Run account-group assignment report"""
    logging.info("📊 Running account-group assignment report...")
    
    scraper = TelegramJobScraper()
    
    try:
        scraper.show_account_status()
        scraper.show_unique_group_assignments()
        scraper.show_database_summary()
        
        logging.info("✅ Account report completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Account report failed: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            run_simple_crawler()
        elif sys.argv[1] == "report":
            run_account_report()
        else:
            print("Usage: python main.py [test|report]")
            print("  test  - Run component tests")
            print("  report - Show account-group assignments")
    else:
        # Run the full system
        asyncio.run(main()) 