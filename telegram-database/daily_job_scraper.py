#!/usr/bin/env python3
"""
Daily Job Scraper - Enhanced Version
====================================

This script implements the daily workflow:
1. Join 10 groups per account (40 total daily)
2. Fetch 100 messages from each group
3. Analyze for job content (10+ jobs per 100 messages)
4. Add high-value channels to CSV
5. Store all messages in database

Key Features:
- Multi-account group joining (10 per account)
- Message fetching and analysis
- Automatic CSV export for high-value channels
- Complete message storage in database
- Real-time progress tracking
"""

import asyncio
import logging
import csv
import json
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import os
import sys

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawler import JobCrawler
from universal_group_manager import UniversalGroupManager
from database.database import DatabaseManager
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/daily_job_scraper.log'),
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

class DailyJobScraper:
    """
    Enhanced daily job scraper that implements the complete workflow
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.crawler = JobCrawler(config.ACCOUNTS)
        self.group_manager = UniversalGroupManager()
        self.db = DatabaseManager()
        
        # Job detection keywords (same as job_analyzer.py)
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
    
    async def join_groups_for_account(self, account: Dict[str, Any]) -> List[Dict]:
        """
        Join 10 groups for a specific account
        
        Args:
            account: Account configuration dictionary
            
        Returns:
            List of successfully joined groups
        """
        self.logger.info(f"🔗 Account {account['name']}: Starting to join {self.groups_per_account} groups")
        
        # Get available groups for this account
        available_groups = self.group_manager.get_available_groups_for_account(account['name'])
        
        if not available_groups:
            self.logger.warning(f"No available groups for {account['name']}")
            return []
        
        # Select groups to join (up to 10)
        groups_to_join = available_groups[:self.groups_per_account]
        joined_groups = []
        
        for group in groups_to_join:
            try:
                self.logger.info(f"Joining group: {group['name']}")
                
                # Join the group via Telegram
                success = await self.crawler.join_group(account, group['link'])
                
                if success:
                    # Mark group as assigned to this account
                    self.group_manager.assign_group_to_account(account['name'], group['id'])
                    
                    # Store group in database
                    group_data = {
                        'group_name': group['name'],
                        'group_link': group['link'],
                        'group_id': group.get('group_id'),
                        'joined_by_account': account['name'],
                        'source_type': 'telegram',
                        'credibility_score': group.get('credibility_score', 0.0)
                    }
                    
                    group_db_id = self.db.insert_programming_group(group_data)
                    
                    # Record assignment
                    self.db.insert_account_group_assignment(account['name'], group_db_id)
                    
                    joined_groups.append({
                        'id': group_db_id,
                        'name': group['name'],
                        'link': group['link'],
                        'account': account['name']
                    })
                    
                    self.logger.info(f"✅ Successfully joined: {group['name']}")
                    
                    # Add delay between joins to avoid rate limiting
                    await asyncio.sleep(2)
                    
                else:
                    self.logger.warning(f"❌ Failed to join: {group['name']}")
                    
            except Exception as e:
                self.logger.error(f"Error joining group {group['name']}: {e}")
                continue
        
        self.logger.info(f"Account {account['name']}: Joined {len(joined_groups)}/{self.groups_per_account} groups")
        return joined_groups
    
    async def fetch_and_analyze_group_messages(self, group: Dict, account: Dict) -> GroupAnalysisResult:
        """
        Fetch 100 messages from a group and analyze for job content
        
        Args:
            group: Group information dictionary
            account: Account information dictionary
            
        Returns:
            GroupAnalysisResult with analysis data
        """
        self.logger.info(f"📥 Fetching messages from: {group['name']}")
        
        try:
            # Fetch messages from the group
            messages = await self.crawler.get_group_messages(
                account, group['link'], limit=self.messages_per_group
            )
            
            if not messages:
                self.logger.warning(f"No messages fetched from {group['name']}")
                return GroupAnalysisResult(
                    group_id=group['id'],
                    group_name=group['name'],
                    group_link=group['link'],
                    total_messages=0,
                    job_messages=0,
                    job_percentage=0.0,
                    is_high_value=False,
                    joined_by_account=account['name'],
                    analysis_timestamp=datetime.now().isoformat()
                )
            
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
                        'fetched_by_account': account['name']
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
                joined_by_account=account['name'],
                analysis_timestamp=datetime.now().isoformat()
            )
            
            self.logger.info(f"📊 {group['name']}: {job_count}/{total_messages} job messages "
                           f"({job_percentage:.1f}%) - {'✅ HIGH VALUE' if is_high_value else '❌ Low value'}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing group {group['name']}: {e}")
            return GroupAnalysisResult(
                group_id=group['id'],
                group_name=group['name'],
                group_link=group['link'],
                total_messages=0,
                job_messages=0,
                job_percentage=0.0,
                is_high_value=False,
                joined_by_account=account['name'],
                analysis_timestamp=datetime.now().isoformat()
            )
    
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
        print("📊 DAILY JOB SCRAPING SUMMARY")
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
        print("✅ Daily scraping completed successfully!")
        print(f"{'='*80}")
    
    async def run_daily_workflow(self):
        """
        Run the complete daily workflow:
        1. Join 10 groups per account (40 total)
        2. Fetch 100 messages from each group
        3. Analyze for job content
        4. Export high-value channels to CSV
        5. Store all messages in database
        """
        self.logger.info("🚀 Starting daily job scraping workflow...")
        
        all_results = []
        total_joined_groups = 0
        
        # Process each account
        for account in config.ACCOUNTS:
            self.logger.info(f"�� Processing account: {account['name']}")
            
            # Join 10 groups for this account
            joined_groups = await self.join_groups_for_account(account)
            total_joined_groups += len(joined_groups)
            
            # Fetch and analyze messages from each joined group
            for group in joined_groups:
                result = await self.fetch_and_analyze_group_messages(group, account)
                all_results.append(result)
                
                # Add delay between groups to avoid rate limiting
                await asyncio.sleep(1)
        
        # Export high-value channels to CSV
        self.export_high_value_channels(all_results)
        
        # Print summary
        self.print_daily_summary(all_results)
        
        self.logger.info(f"🎉 Daily workflow completed! Joined {total_joined_groups} groups, "
                        f"analyzed {len(all_results)} groups")
        
        return all_results

async def main():
    """Main function to run the daily job scraper"""
    scraper = DailyJobScraper()
    
    try:
        await scraper.run_daily_workflow()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logging.error(f"Daily workflow failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
