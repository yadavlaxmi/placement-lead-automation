#!/usr/bin/env python3
"""
Real Daily Job Scraper - Actual Telegram API Version
====================================================

यह script हर बार run करने पर:
1. हर account के लिए unique groups join करती है
2. 100 messages fetch करती है हर group से
3. Job messages analyze करती है
4. 10+ job messages वाले groups को CSV में export करती है
5. सभी messages database में store करती है

Features:
- Real Telegram API integration
- Unique group assignment per account
- Actual message fetching
- Smart job detection
- CSV export for high-value channels
"""

import asyncio
import logging
import csv
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import os
import sys
import random

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from universal_group_manager import UniversalGroupManager
from database.database import DatabaseManager
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/real_daily_scraper.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class GroupAnalysisResult:
    """Result of analyzing a group for job content"""
    group_id: int
    group_name: str
    group_link: str
    total_messages: int
    job_messages: int
    job_percentage: float
    is_high_value: bool
    joined_by_account: str
    analysis_timestamp: str

class RealDailyJobScraper:
    """
    Real daily job scraper with actual Telegram API integration
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.group_manager = UniversalGroupManager()
        self.db = DatabaseManager()
        
        # Job detection keywords
        self.job_keywords = {
            'roles': [
                'developer', 'engineer', 'programmer', 'coder', 'architect',
                'analyst', 'designer', 'manager', 'lead', 'senior', 'junior',
                'intern', 'fresher', 'experienced', 'fullstack', 'frontend',
                'backend', 'devops', 'qa', 'tester', 'product manager',
                'project manager', 'scrum master', 'tech lead', 'team lead'
            ],
            'technologies': [
                'python', 'java', 'javascript', 'react', 'angular', 'vue',
                'node', 'django', 'flask', 'spring', 'html', 'css', 'sql',
                'mongodb', 'postgresql', 'mysql', 'aws', 'azure', 'docker',
                'kubernetes', 'git', 'linux', 'android', 'ios', 'flutter',
                'react native', 'machine learning', 'data science', 'ai'
            ],
            'job_indicators': [
                'hiring', 'job', 'vacancy', 'position', 'opening', 'opportunity',
                'career', 'employment', 'work', 'salary', 'ctc', 'lpa',
                'experience', 'years', 'apply', 'resume', 'cv', 'interview',
                'recruitment', 'hr', 'human resource', 'join our team',
                'we are hiring', 'job alert', 'job opportunity'
            ],
            'location_indicators': [
                'bangalore', 'mumbai', 'delhi', 'hyderabad', 'pune', 'chennai',
                'kolkata', 'gurgaon', 'noida', 'remote', 'work from home',
                'wfh', 'onsite', 'hybrid', 'office', 'location'
            ]
        }
        
        # Daily targets
        self.groups_per_account = 10
        self.messages_per_group = 100
        self.min_job_messages = 10
        
        # Telegram client instances for each account
        self.telegram_clients = {}
        
    def is_job_message(self, message_text: str) -> Tuple[bool, float]:
        """
        Analyze if a message is job-related using keyword matching
        
        Args:
            message_text: The message content to analyze
            
        Returns:
            Tuple of (is_job_message, confidence_score)
        """
        if not message_text:
            return False, 0.0
        
        # Convert to lowercase for case-insensitive matching
        text_lower = message_text.lower()
        
        # Count matches for each keyword category
        category_matches = {}
        
        for category, keywords in self.job_keywords.items():
            matches = sum(1 for keyword in keywords if keyword in text_lower)
            category_matches[category] = matches
        
        # Calculate weighted score
        weighted_score = (
            category_matches['job_indicators'] * 3 +  # High weight
            category_matches['roles'] * 2 +           # Medium-high weight
            category_matches['technologies'] * 1.5 +   # Medium weight
            category_matches['location_indicators'] * 1 # Low weight
        )
        
        # Normalize score (0-1 range)
        max_possible_score = (
            len(self.job_keywords['job_indicators']) * 3 +
            len(self.job_keywords['roles']) * 2 +
            len(self.job_keywords['technologies']) * 1.5 +
            len(self.job_keywords['location_indicators']) * 1
        )
        
        confidence = min(weighted_score / max_possible_score, 1.0)
        
        # Consider it a job message if confidence > 0.1 (10%)
        # and at least one job indicator is present
        is_job = (confidence > 0.1 and category_matches['job_indicators'] > 0) or \
                (confidence > 0.15)  # Or high overall confidence
        
        return is_job, confidence
    
    async def initialize_telegram_clients(self):
        """Initialize Telegram clients for all accounts"""
        self.logger.info("🔧 Initializing Telegram clients...")
        
        try:
            from telegram_client import TelegramClient
            
            for account in config.ACCOUNTS:
                self.logger.info(f"Initializing client for {account['name']}")
                
                client = TelegramClient(
                    api_id=account['api_id'],
                    api_hash=account['api_hash'],
                    session_name=account['session_name']
                )
                
                await client.start()
                self.telegram_clients[account['name']] = client
                
                self.logger.info(f"✅ Client initialized for {account['name']}")
                
        except Exception as e:
            self.logger.error(f"Error initializing Telegram clients: {e}")
            raise
    
    async def get_unique_groups_for_account(self, account_name: str) -> List[Dict]:
        """
        Get unique groups for a specific account (not joined by other accounts today)
        
        Args:
            account_name: Name of the account
            
        Returns:
            List of unique groups for this account
        """
        self.logger.info(f"🎯 Getting unique groups for {account_name}")
        
        # Get all available groups
        all_groups = self.group_manager.get_all_groups()
        
        # Get groups already joined today by other accounts
        today = datetime.now().date().isoformat()
        joined_today = self.db.get_groups_joined_today(today)
        
        # Filter out groups already joined today
        available_groups = []
        for group in all_groups:
            if group['id'] not in [g['group_id'] for g in joined_today]:
                available_groups.append(group)
        
        # Shuffle to get random selection
        random.shuffle(available_groups)
        
        # Select up to 10 groups for this account
        selected_groups = available_groups[:self.groups_per_account]
        
        self.logger.info(f"Found {len(selected_groups)} unique groups for {account_name}")
        return selected_groups
    
    async def join_group_with_account(self, account_name: str, group: Dict) -> bool:
        """
        Join a group using the specific account's Telegram client
        
        Args:
            account_name: Name of the account
            group: Group information dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self.telegram_clients[account_name]
            
            self.logger.info(f"🔗 {account_name}: Joining {group['name']}")
            
            # Join the group
            await client.join_group(group['link'])
            
            # Store group in database
            group_data = {
                'group_name': group['name'],
                'group_link': group['link'],
                'group_id': group.get('group_id'),
                'joined_by_account': account_name,
                'source_type': 'telegram',
                'credibility_score': group.get('credibility_score', 0.0)
            }
            
            group_db_id = self.db.insert_programming_group(group_data)
            
            # Record assignment
            self.db.insert_account_group_assignment(account_name, group_db_id)
            
            self.logger.info(f"✅ {account_name}: Successfully joined {group['name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {account_name}: Failed to join {group['name']}: {e}")
            return False
    
    async def fetch_messages_from_group(self, account_name: str, group: Dict) -> List[Dict]:
        """
        Fetch messages from a group using the specific account
        
        Args:
            account_name: Name of the account
            group: Group information dictionary
            
        Returns:
            List of messages
        """
        try:
            client = self.telegram_clients[account_name]
            
            self.logger.info(f"📥 {account_name}: Fetching messages from {group['name']}")
            
            # Fetch messages from the group
            messages = await client.get_group_messages(group['link'], limit=self.messages_per_group)
            
            self.logger.info(f"✅ {account_name}: Fetched {len(messages)} messages from {group['name']}")
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ {account_name}: Error fetching messages from {group['name']}: {e}")
            return []
    
    def analyze_group_messages(self, group: Dict, account_name: str, messages: List[Dict]) -> GroupAnalysisResult:
        """
        Analyze messages from a group for job content
        
        Args:
            group: Group information dictionary
            account_name: Name of the account
            messages: List of messages to analyze
            
        Returns:
            GroupAnalysisResult with analysis data
        """
        self.logger.info(f"📊 Analyzing messages from: {group['name']}")
        
        # Analyze messages for job content
        job_count = 0
        stored_messages = []
        
        for message in messages:
            message_text = message.get('text', '') or message.get('message', '')
            
            if message_text:
                # Check if it's a job message
                is_job, confidence = self.is_job_message(message_text)
                
                # Store message in database
                message_data = {
                    'group_id': group['id'],
                    'message_id': message.get('id', ''),
                    'sender_id': message.get('sender_id', ''),
                    'sender_name': message.get('sender', '') or message.get('sender_name', ''),
                    'message_text': message_text,
                    'timestamp': message.get('date', datetime.now().isoformat()),
                    'is_job_post': is_job,
                    'job_score': confidence,
                    'fetched_by_account': account_name
                }
                
                # Insert message into database
                self.db.insert_message(message_data)
                stored_messages.append(message_data)
                
                if is_job:
                    job_count += 1
        
        # Calculate job percentage
        total_messages = len(messages)
        job_percentage = (job_count / total_messages * 100) if total_messages > 0 else 0.0
        
        # Determine if group is high-value
        is_high_value = job_count >= self.min_job_messages
        
        result = GroupAnalysisResult(
            group_id=group['id'],
            group_name=group['name'],
            group_link=group['link'],
            total_messages=total_messages,
            job_messages=job_count,
            job_percentage=job_percentage,
            is_high_value=is_high_value,
            joined_by_account=account_name,
            analysis_timestamp=datetime.now().isoformat()
        )
        
        self.logger.info(f"📊 {group['name']}: {job_count}/{total_messages} job messages "
                       f"({job_percentage:.1f}%) - {'✅ HIGH VALUE' if is_high_value else '❌ Low value'}")
        
        return result
    
    def export_high_value_channels(self, results: List[GroupAnalysisResult]):
        """
        Export high-value channels to CSV file
        
        Args:
            results: List of group analysis results
        """
        high_value_channels = [r for r in results if r.is_high_value]
        
        if not high_value_channels:
            self.logger.info("No high-value channels found to export")
            return
        
        csv_filename = f"high_value_job_channels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'group_id', 'group_name', 'group_link', 'job_messages', 
                'total_messages', 'job_percentage', 'joined_by_account', 
                'analysis_timestamp'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in high_value_channels:
                writer.writerow({
                    'group_id': result.group_id,
                    'group_name': result.group_name,
                    'group_link': result.group_link,
                    'job_messages': result.job_messages,
                    'total_messages': result.total_messages,
                    'job_percentage': round(result.job_percentage, 2),
                    'joined_by_account': result.joined_by_account,
                    'analysis_timestamp': result.analysis_timestamp
                })
        
        self.logger.info(f"📄 Exported {len(high_value_channels)} high-value channels to {csv_filename}")
    
    def print_daily_summary(self, results: List[GroupAnalysisResult]):
        """
        Print daily summary report
        
        Args:
            results: List of group analysis results
        """
        total_groups = len(results)
        high_value_groups = len([r for r in results if r.is_high_value])
        total_messages = sum(r.total_messages for r in results)
        total_job_messages = sum(r.job_messages for r in results)
        
        print(f"\n{'='*80}")
        print("📊 REAL DAILY JOB SCRAPING SUMMARY")
        print(f"{'='*80}")
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Total Groups Joined: {total_groups}")
        print(f"✅ High-Value Groups: {high_value_groups}")
        print(f"📨 Total Messages Fetched: {total_messages}")
        print(f"💼 Total Job Messages: {total_job_messages}")
        
        if total_messages > 0:
            overall_job_percentage = (total_job_messages / total_messages) * 100
            print(f"📈 Overall Job Percentage: {overall_job_percentage:.1f}%")
        
        print(f"\n🏆 HIGH-VALUE CHANNELS ({self.min_job_messages}+ job messages):")
        print(f"{'='*80}")
        
        high_value_channels = [r for r in results if r.is_high_value]
        for i, result in enumerate(high_value_channels, 1):
            print(f"{i:2d}. 📢 {result.group_name}")
            print(f"    🔗 {result.group_link}")
            print(f"    💼 {result.job_messages}/{result.total_messages} ({result.job_percentage:.1f}%)")
            print(f"    👤 Joined by: {result.joined_by_account}")
        
        print(f"\n{'='*80}")
        print("✅ Real daily scraping completed successfully!")
        print(f"{'='*80}")
    
    async def run_real_workflow(self):
        """
        Run the real workflow with actual Telegram API:
        1. Initialize Telegram clients for all accounts
        2. Get unique groups for each account
        3. Join groups using respective accounts
        4. Fetch messages from joined groups
        5. Analyze for job content
        6. Export high-value channels to CSV
        7. Store all messages in database
        """
        self.logger.info("🚀 Starting REAL daily job scraping workflow...")
        
        # Initialize Telegram clients
        await self.initialize_telegram_clients()
        
        all_results = []
        total_joined_groups = 0
        
        # Process each account
        for account in config.ACCOUNTS:
            account_name = account['name']
            self.logger.info(f"👤 Processing account: {account_name}")
            
            # Get unique groups for this account
            unique_groups = await self.get_unique_groups_for_account(account_name)
            
            if not unique_groups:
                self.logger.warning(f"No unique groups available for {account_name}")
                continue
            
            # Join groups for this account
            joined_groups = []
            for group in unique_groups:
                success = await self.join_group_with_account(account_name, group)
                if success:
                    joined_groups.append(group)
                    total_joined_groups += 1
                    
                    # Add delay between joins to avoid rate limiting
                    await asyncio.sleep(2)
            
            self.logger.info(f"Account {account_name}: Joined {len(joined_groups)} groups")
            
            # Fetch and analyze messages from each joined group
            for group in joined_groups:
                # Fetch messages
                messages = await self.fetch_messages_from_group(account_name, group)
                
                if messages:
                    # Analyze messages
                    result = self.analyze_group_messages(group, account_name, messages)
                    all_results.append(result)
                
                # Add delay between groups to avoid rate limiting
                await asyncio.sleep(1)
        
        # Export high-value channels to CSV
        self.export_high_value_channels(all_results)
        
        # Print summary
        self.print_daily_summary(all_results)
        
        self.logger.info(f"🎉 Real workflow completed! Joined {total_joined_groups} groups, "
                        f"analyzed {len(all_results)} groups")
        
        return all_results

async def main():
    """Main function to run the real daily job scraper"""
    scraper = RealDailyJobScraper()
    
    try:
        await scraper.run_real_workflow()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logging.error(f"Real workflow failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
