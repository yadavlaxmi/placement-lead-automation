#!/usr/bin/env python3
"""
Telegram Job Scraper - Main Runner (Commented Version)
=======================================================

This is the main entry point for the Telegram Job Scraper system.
The system handles multiple Telegram accounts for crawling programming job groups
without getting banned by implementing smart joining strategies and message fetching.

Key Features:
- Multi-account management to distribute load
- Safe joining/leaving of Telegram channels
- Message fetching and storage in database
- Job post identification using ML pipeline
- Email notifications for job reports
- Search engine API for finding new groups

Architecture Overview:
- Uses multiple Telegram accounts to avoid rate limits
- Implements universal group manager for unique assignments
- Stores messages in SQLite database with job classification
- Uses ML pipeline to identify job posts from regular messages
"""

# Standard library imports
import asyncio          # For asynchronous programming
import logging         # For application logging
import schedule        # For scheduling daily tasks
import time           # For time operations
import os             # For file/directory operations
from datetime import datetime  # For date/time handling

# Local module imports
from crawler import JobCrawler                    # Main crawler for fetching messages
from email_notifier import EmailNotifier         # Email notification system
from universal_group_manager import UniversalGroupManager  # Group assignment manager
import config                                     # Configuration settings

# ==========================================
# DIRECTORY SETUP
# ==========================================
# Create necessary directories if they don't exist
# This ensures the application has required folders for operation
os.makedirs('logs', exist_ok=True)      # For log files
os.makedirs('data', exist_ok=True)      # For data storage
os.makedirs('models', exist_ok=True)    # For ML model files

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
# Configure logging to write to both file and console
# This helps in debugging and monitoring the application
logging.basicConfig(
    level=logging.INFO,                          # Log level (INFO and above)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format with timestamp
    handlers=[
        logging.FileHandler('logs/telegram_scraper.log'),   # Write to file
        logging.StreamHandler()                              # Write to console
    ]
)

# ==========================================
# TELEGRAM ACCOUNTS CONFIGURATION
# ==========================================
# Multiple Telegram accounts are used to distribute the load
# This prevents any single account from being banned due to excessive activity
# Each account can join up to 10 groups per day to stay within Telegram limits
ACCOUNTS = [
    {
        'name': 'Account 1',                    # Human-readable account name
        'phone': '+919794670665',               # Phone number for authentication
        'api_id': 24242582,                     # Telegram API ID (from my.telegram.org)
        'api_hash': 'd8a500dd4f6956793a0be40ac48c1669',  # Telegram API hash
        'session_name': 'session_account1'      # Session file name for persistence
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

# ==========================================
# MAIN APPLICATION CLASS
# ==========================================
class TelegramJobScraper:
    """
    Main application class that orchestrates all components
    
    This class manages:
    - Job crawler for fetching messages from groups
    - Email notifier for sending daily reports
    - Universal group manager for smart group assignments
    - System lifecycle (start/stop operations)
    """
    
    def __init__(self):
        """Initialize all system components"""
        # Initialize the job crawler with all accounts
        # The crawler handles joining groups and fetching messages
        self.crawler = JobCrawler(ACCOUNTS)
        
        # Initialize email notification system
        # Sends daily job reports to configured recipients
        self.email_notifier = EmailNotifier()
        
        # Initialize universal group manager
        # Ensures each group is assigned to only one account per day
        # This prevents duplicate message fetching and reduces ban risk
        self.universal_group_manager = UniversalGroupManager()
        
        # System running state flag
        self.is_running = False
        
    async def start_system(self):
        """
        Start the entire Telegram job scraping system
        
        This method:
        1. Sets the system running flag
        2. Starts the crawler which begins joining groups and fetching messages
        3. The crawler runs continuously until stopped
        """
        logging.info("🚀 Starting Telegram Job Scraper System...")
        
        # Set system as running
        self.is_running = True
        
        # Start the main crawler - this is the core of the system
        # The crawler will:
        # - Join new groups (up to daily limits)
        # - Fetch historical messages from joined groups
        # - Store messages in database
        # - Run ML pipeline to identify job posts
        await self.crawler.start_crawling()
    
    def stop_system(self):
        """
        Gracefully stop the system
        
        This ensures all operations are completed before shutdown
        """
        logging.info("🛑 Stopping Telegram Job Scraper System...")
        self.is_running = False
        self.crawler.stop_crawling()
    
    def schedule_daily_email(self):
        """
        Schedule automated daily email reports
        
        Sends job reports twice a day:
        - 9:00 AM: Morning report with overnight jobs
        - 6:00 PM: Evening report with day's jobs
        """
        # Schedule morning report
        schedule.every().day.at("09:00").do(self.email_notifier.send_daily_job_report)
        
        # Schedule evening report
        schedule.every().day.at("18:00").do(self.email_notifier.send_daily_job_report)
        
        logging.info("📧 Scheduled daily email reports at 9:00 AM and 6:00 PM")
    
    def schedule_daily_reset(self):
        """
        Schedule daily reset of group join tracking
        
        At midnight each day:
        - Reset daily join counters for all accounts
        - This allows accounts to join new groups the next day
        """
        schedule.every().day.at("00:00").do(self.universal_group_manager.reset_daily_joins)
        logging.info("🔄 Scheduled daily reset of join tracking at midnight")
    
    def run_scheduler(self):
        """
        Run the task scheduler
        
        This runs in a loop checking for scheduled tasks every minute
        Handles email reports and daily resets
        """
        while self.is_running:
            schedule.run_pending()    # Execute any due scheduled tasks
            time.sleep(60)           # Check every minute
    
    def show_account_status(self):
        """
        Display current status of all Telegram accounts
        
        Shows:
        - How many groups each account joined today
        - How many more groups each account can join
        - Total available groups in the system
        """
        logging.info("📊 Account Status Report:")
        
        # Loop through all configured accounts
        for account in ACCOUNTS:
            # Get current status from group manager
            status = self.universal_group_manager.get_account_status(account['name'])
            
            # Display join status (current/limit)
            logging.info(f"  {account['name']}: Joined {status['joined_today']}/10 today, {status['remaining']} remaining")
        
        # Show overall system statistics
        stats = self.universal_group_manager.get_universal_stats()
        logging.info(f"📈 Universal Groups: {stats['total_groups']} total groups available")

    def show_unique_group_assignments(self):
        """
        Display which account is assigned to which groups today
        
        This helps ensure:
        - No duplicate assignments
        - Even distribution across accounts
        - Priority groups are handled first
        """
        logging.info("🎯 Unique Group Assignments:")
        
        # Get today's group assignments summary
        summary = self.universal_group_manager.get_all_accounts_groups_summary()
        
        if not summary:
            logging.info("  No group assignments found for today")
            return
        
        # Display assignments for each account
        for account_name, groups in summary.items():
            logging.info(f"  {account_name}:")
            
            if not groups:
                logging.info("    No groups assigned today")
            else:
                # Display each assigned group with priority indicator
                for i, group in enumerate(groups, 1):
                    # Color-code by priority
                    priority_emoji = {
                        'high': '🔴',      # High priority (urgent jobs)
                        'medium': '🟡',    # Medium priority (regular jobs)
                        'low': '🟢'        # Low priority (general groups)
                    }.get(group.get('priority', 'unknown'), '⚪')
                    
                    # Display group info
                    logging.info(f"    {i:2d}. {priority_emoji} {group['name']} ({group['link']})")
                    logging.info(f"        Category: {group.get('category', 'unknown')} | Priority: {group.get('priority', 'unknown')}")
    
    def show_database_summary(self):
        """
        Display database summary of group assignments and activity
        
        Shows historical data about:
        - Total groups per account
        - Active days for each account
        - Last assignment dates
        """
        logging.info("🗄️ Database Group Assignment Summary:")
        
        # Import database manager
        from database.database import DatabaseManager
        db = DatabaseManager()
        
        # Get summary from database
        db_summary = db.get_account_group_summary()
        
        if not db_summary['summary']:
            logging.info("  No group assignments found in database")
            return
        
        # Display summary for each account
        for account_summary in db_summary['summary']:
            logging.info(f"  {account_summary['account_name']}:")
            logging.info(f"    Total Groups: {account_summary['total_groups']}")
            logging.info(f"    Active Days: {account_summary['active_days']}")
            logging.info(f"    Last Assignment: {account_summary['last_assignment']}")
    
    async def get_account_group_report(self):
        """
        Get detailed account-group assignment report
        
        Returns comprehensive data about which accounts are assigned to which groups
        Used for monitoring and debugging the assignment system
        """
        return await self.crawler.get_account_group_report()

# ==========================================
# MAIN EXECUTION FUNCTIONS
# ==========================================

async def main():
    """
    Main application entry point
    
    This function:
    1. Creates the main scraper instance
    2. Schedules daily tasks (emails, resets)
    3. Shows initial system status
    4. Starts the main crawling system
    5. Handles graceful shutdown on interruption
    """
    # Create main scraper instance
    scraper = TelegramJobScraper()
    
    try:
        # Schedule automated daily tasks
        scraper.schedule_daily_email()    # Email reports
        scraper.schedule_daily_reset()    # Reset join counters
        
        # Show initial system status for monitoring
        scraper.show_account_status()           # Account join status
        scraper.show_unique_group_assignments() # Group assignments
        scraper.show_database_summary()         # Database statistics
        
        # Start the main system - this runs indefinitely
        await scraper.start_system()
        
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        logging.info("Received interrupt signal, shutting down...")
        scraper.stop_system()
    except Exception as e:
        # Handle any unexpected errors
        logging.error(f"System error: {e}")
        scraper.stop_system()

def run_simple_crawler():
    """
    Run a simplified version for testing system components
    
    This function tests:
    - Database connectivity
    - Search engine initialization
    - ML pipeline setup
    - Group manager functionality
    
    Used for debugging and system health checks
    """
    logging.info("🧪 Running simple crawler test...")
    
    scraper = TelegramJobScraper()
    
    try:
        # Test database connection
        from database.database import DatabaseManager
        db = DatabaseManager()
        cities = db.get_cities()
        logging.info(f"✅ Database connection successful. Found {len(cities)} cities.")
        
        # Test search engine (for finding new groups)
        from search_engine import SearchEngine
        search_engine = SearchEngine()
        logging.info("✅ Search engine initialized successfully.")
        
        # Test ML pipeline (for job classification)
        from ml_pipeline import MLPipeline
        ml_pipeline = MLPipeline()
        logging.info("✅ ML pipeline initialized successfully.")
        
        # Test universal group manager
        universal_manager = UniversalGroupManager()
        stats = universal_manager.get_universal_stats()
        logging.info(f"✅ Universal group manager initialized. {stats['total_groups']} groups available.")
        
        # Show current system status
        scraper.show_account_status()
        scraper.show_unique_group_assignments()
        
        logging.info("🎉 All components initialized successfully!")
        
    except Exception as e:
        logging.error(f"❌ Component test failed: {e}")

def run_account_report():
    """
    Generate and display account assignment reports
    
    Shows:
    - Current account status
    - Group assignments
    - Database statistics
    
    Used for monitoring system performance and assignments
    """
    logging.info("📊 Running account-group assignment report...")
    
    scraper = TelegramJobScraper()
    
    try:
        # Display all status reports
        scraper.show_account_status()           # Current join status
        scraper.show_unique_group_assignments() # Today's assignments
        scraper.show_database_summary()         # Historical data
        
        logging.info("✅ Account report completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Account report failed: {e}")

# ==========================================
# SCRIPT ENTRY POINT
# ==========================================
if __name__ == "__main__":
    import sys
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Run component tests
            run_simple_crawler()
        elif sys.argv[1] == "report":
            # Show account reports
            run_account_report()
        else:
            # Show usage information
            print("Usage: python main.py [test|report]")
            print("  test  - Run component tests")
            print("  report - Show account-group assignments")
    else:
        # Run the full system (default behavior)
        asyncio.run(main())
