#!/usr/bin/env python3
"""
Telegram Job Scraper V2 - Professional Architecture
Main application with persistent group assignment
"""
import asyncio
import logging
import sys
import os
from datetime import datetime
from typing import List, Dict, Any

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from core.assignment_engine import PersistentAssignmentEngine
from core.human_behavior_simulator import HumanBehaviorSimulator
from services.telegram_service import TelegramService
from services.group_manager import GroupManager
from database.repository import DatabaseRepository

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler()
    ]
)

class TelegramJobScraperV2:
    """Main application class"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseRepository()
        self.assignment_engine = PersistentAssignmentEngine()
        self.human_simulator = HumanBehaviorSimulator()
        self.telegram_service = TelegramService()
        self.group_manager = GroupManager()
        
    async def initialize(self):
        """Initialize the system"""
        self.logger.info("🚀 Initializing Telegram Job Scraper V2...")
        
        # Initialize database
        await self.db.initialize()
        
        # Load universal groups
        await self.group_manager.load_universal_groups()
        
        # Initialize accounts
        await self.initialize_accounts()
        
        self.logger.info("✅ System initialized successfully!")
    
    async def initialize_accounts(self):
        """Initialize accounts in database"""
        for account_config in settings.ACCOUNTS:
            account = self.db.get_account_by_id(account_config['id'])
            if not account:
                self.logger.info(f"Creating account: {account_config['name']}")
                await self.db.create_account(account_config)
            else:
                self.logger.info(f"Account already exists: {account_config['name']}")
    
    async def run_daily_cycle(self):
        """Run daily cycle for all accounts"""
        self.logger.info("🔄 Starting daily cycle...")
        
        # Get all active accounts
        accounts = await self.db.get_all_accounts()
        
        for account in accounts:
            await self.process_account(account)
        
        # Show summary
        await self.show_daily_summary()
    
    async def process_account(self, account):
        """Process individual account"""
        self.logger.info(f"👤 Processing account: {account.name}")
        
        # Get current account state
        account_state = await self.get_account_state(account.id)
        
        # Simulate human behavior - decide to leave groups
        groups_to_leave = self.human_simulator.simulate_leave_decision(account, account_state)
        
        # Leave groups if decided
        for group_id in groups_to_leave:
            await self.leave_group(account, group_id, "Human behavior simulation")
        
        # Get available groups
        available_groups = self.assignment_engine.get_available_groups()
        
        # Select groups to join based on human behavior
        groups_to_join = self.human_simulator.select_groups_to_join(
            account, available_groups, account_state
        )
        
        # Join new groups
        for group in groups_to_join:
            await self.join_group(account, group)
    
    async def join_group(self, account, group):
        """Join group and fetch message history"""
        self.logger.info(f"🔗 Account {account.name} joining group: {group.name}")
        
        try:
            # Join group via Telegram
            success = await self.telegram_service.join_group(account, group.link)
            
            if success:
                # Create persistent assignment
                self.assignment_engine.assign_group_to_account(account.id, group.id)
                
                # Fetch message history
                await self.fetch_message_history(account, group)
                
                self.logger.info(f"✅ Successfully joined group: {group.name}")
            else:
                self.logger.warning(f"❌ Failed to join group: {group.name}")
                
        except Exception as e:
            self.logger.error(f"Error joining group {group.name}: {e}")
    
    async def leave_group(self, account, group_id, reason="Manual leave"):
        """Leave group"""
        self.logger.info(f"🚪 Account {account.name} leaving group: {group_id}")
        
        try:
            # Leave group via Telegram
            success = await self.telegram_service.leave_group(account, group_id)
            
            if success:
                # Remove assignment
                self.assignment_engine.unassign_group_from_account(account.id, group_id, reason)
                self.logger.info(f"✅ Successfully left group: {group_id}")
            else:
                self.logger.warning(f"❌ Failed to leave group: {group_id}")
                
        except Exception as e:
            self.logger.error(f"Error leaving group {group_id}: {e}")
    
    async def fetch_message_history(self, account, group):
        """Fetch historical messages from group"""
        self.logger.info(f"📥 Fetching message history for group: {group.name}")
        
        try:
            # Fetch messages
            messages = await self.telegram_service.get_group_messages(
                account, group.link, limit=settings.MESSAGE_FETCH_LIMIT
            )
            
            # Store messages
            for message in messages:
                await self.db.store_message(message, account.id, group.id)
            
            self.logger.info(f"✅ Fetched {len(messages)} messages from {group.name}")
            
        except Exception as e:
            self.logger.error(f"Error fetching messages from {group.name}: {e}")
    
    async def get_account_state(self, account_id: str):
        """Get current account state"""
        groups = self.assignment_engine.get_account_groups(account_id)
        total_messages = await self.db.get_message_count_by_account(account_id)
        
        return {
            'account_id': account_id,
            'active_groups': [group.id for group in groups],
            'total_messages': total_messages,
            'last_activity': datetime.now()
        }
    
    async def show_daily_summary(self):
        """Show daily summary"""
        self.logger.info("📊 Daily Summary:")
        
        summary = self.assignment_engine.get_assignment_summary()
        
        self.logger.info(f"  Total Assignments: {summary.get('total_assignments', 0)}")
        self.logger.info(f"  Total Groups: {summary.get('total_groups', 0)}")
        self.logger.info(f"  Total Accounts: {summary.get('total_accounts', 0)}")
        self.logger.info(f"  Available Groups: {summary.get('available_groups', 0)}")
        
        # Show account stats
        for account_name, stats in summary.get('account_stats', {}).items():
            self.logger.info(f"  {account_name}: {stats['total_groups']} groups")
    
    async def show_assignment_report(self):
        """Show detailed assignment report"""
        self.logger.info("📋 Assignment Report:")
        
        summary = self.assignment_engine.get_assignment_summary()
        
        for account_name, stats in summary.get('account_stats', {}).items():
            self.logger.info(f"  {account_name}:")
            for group_name in stats['groups']:
                self.logger.info(f"    - {group_name}")
    
    async def run(self):
        """Main run method"""
        try:
            await self.initialize()
            await self.run_daily_cycle()
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.error(f"System error: {e}")
            raise

async def main():
    """Main function"""
    scraper = TelegramJobScraperV2()
    await scraper.run()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        # Show report only
        scraper = TelegramJobScraperV2()
        asyncio.run(scraper.show_assignment_report())
    else:
        # Run full system
        asyncio.run(main())
