#!/usr/bin/env python3
"""
Quick fix script to get telegram-database v2 working for group joining and message fetching
"""
import asyncio
import logging
import sys
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from services.telegram_service import TelegramService
from database.repository import DatabaseRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/telegram_scraper_v2.log'),
        logging.StreamHandler()
    ]
)

class SimpleTelegramJobScraper:
    """Simplified version for immediate use"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseRepository()
        self.telegram_service = TelegramService()
        
    async def initialize(self):
        """Initialize the system"""
        self.logger.info("🚀 Initializing Simple Telegram Job Scraper...")
        
        # Initialize database
        await self.db.initialize()
        
        # Load universal groups
        await self.load_universal_groups()
        
        # Initialize accounts
        await self.initialize_accounts()
        
        self.logger.info("✅ System initialized successfully!")
    
    async def load_universal_groups(self):
        """Load universal groups from file"""
        try:
            import json
            with open(settings.UNIVERSAL_GROUPS_FILE, 'r') as f:
                groups_data = json.load(f)
            
            self.logger.info(f"📁 Loaded {len(groups_data)} groups from file")
            
            # Load groups into database
            for group_data in groups_data:
                # Generate ID if not present
                if 'id' not in group_data:
                    group_data['id'] = str(uuid.uuid4())
                
                # Create group in database
                await self.db.create_group(group_data)
            
            self.logger.info("✅ All groups loaded into database")
            
        except Exception as e:
            self.logger.error(f"Error loading universal groups: {e}")
            raise
    
    async def initialize_accounts(self):
        """Initialize accounts in database"""
        for account_config in settings.ACCOUNTS:
            account = self.db.get_account_by_id(account_config['id'])
            if not account:
                self.logger.info(f"Creating account: {account_config['name']}")
                await self.db.create_account(account_config)
            else:
                self.logger.info(f"Account already exists: {account_config['name']}")
    
    async def join_groups_and_fetch_messages(self):
        """Join groups and fetch messages"""
        self.logger.info("🔄 Starting group joining and message fetching...")
        
        # Get all accounts
        accounts = self.db.get_all_accounts()
        
        # Get all groups
        groups = self.db.get_all_groups()
        
        self.logger.info(f"Found {len(accounts)} accounts and {len(groups)} groups")
        
        # For each account, join groups and fetch messages
        for account in accounts:
            await self.process_account_groups(account, groups)
    
    async def process_account_groups(self, account, groups):
        """Process groups for an account"""
        self.logger.info(f"👤 Processing account: {account.name}")
        
        # Join first few groups (limit to avoid rate limits)
        groups_to_join = groups[:5]  # Join first 5 groups per account
        
        for group in groups_to_join:
            await self.join_group_and_fetch_messages(account, group)
    
    async def join_group_and_fetch_messages(self, account, group):
        """Join group and fetch messages"""
        self.logger.info(f"🔗 Account {account.name} joining group: {group.name}")
        
        try:
            # Join group via Telegram
            success = await self.telegram_service.join_group(account, group.link)
            
            if success:
                # Fetch message history
                messages = await self.telegram_service.get_group_messages(
                    account, group.link, limit=settings.MESSAGE_FETCH_LIMIT
                )
                
                # Store messages
                job_messages = []
                for message_data in messages:
                    # Create message object
                    from models.message import Message
                    message = Message(
                        id=str(uuid.uuid4()),
                        message_text=message_data['text'],
                        timestamp=message_data['timestamp'],
                        is_job_message=message_data['is_job_message'],
                        job_score=message_data['job_score']
                    )
                    
                    # Store in database
                    await self.db.store_message(message, account.id, group.id)
                    
                    # Collect job messages
                    if message_data['is_job_message']:
                        job_messages.append(message_data['text'])
                
                self.logger.info(f"✅ Joined {group.name} and fetched {len(messages)} messages")
                self.logger.info(f"📋 Found {len(job_messages)} job messages in {group.name}")
                
                # Show sample job messages
                if job_messages:
                    self.logger.info(f"📝 Sample job messages from {group.name}:")
                    for i, msg in enumerate(job_messages[:3]):  # Show first 3
                        self.logger.info(f"  {i+1}. {msg[:100]}...")
            else:
                self.logger.warning(f"❌ Failed to join group: {group.name}")
                
        except Exception as e:
            self.logger.error(f"Error processing group {group.name}: {e}")
    
    async def show_groups_with_job_messages(self):
        """Show groups that have job messages"""
        self.logger.info("📊 Groups with Job Messages:")
        
        try:
            # This would query the database for groups with job messages
            # For now, we'll show a summary
            groups = self.db.get_all_groups()
            self.logger.info(f"Total groups available: {len(groups)}")
            
            # Show group list
            for group in groups:
                self.logger.info(f"  - {group.name} ({group.priority} priority)")
                
        except Exception as e:
            self.logger.error(f"Error showing groups: {e}")
    
    async def run(self):
        """Main run method"""
        try:
            await self.initialize()
            await self.join_groups_and_fetch_messages()
            await self.show_groups_with_job_messages()
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.error(f"System error: {e}")
            raise

async def main():
    """Main function"""
    scraper = SimpleTelegramJobScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
