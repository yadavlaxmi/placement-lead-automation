#!/usr/bin/env python3
"""
Job Message Analyzer for Telegram Channels
==========================================

This script analyzes Telegram channels to identify those with high job posting activity.
It examines the last 100 messages from each channel and identifies channels that have
at least 10 job-related messages, making them valuable for job seekers.

Key Features:
- Analyzes message content using ML pipeline for job classification
- Identifies channels with high job posting frequency (10+ jobs per 100 messages)
- Generates reports of high-value job channels
- Supports filtering by date range and channel categories
- Exports results to JSON and CSV formats

Usage:
- python job_analyzer.py                    # Analyze all channels
- python job_analyzer.py --min-jobs 15      # Channels with 15+ jobs per 100 messages
- python job_analyzer.py --export-csv       # Export results to CSV
- python job_analyzer.py --days 7           # Analyze last 7 days only
"""

import sqlite3
import json
import csv
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class ChannelJobStats:
    """Data class to hold channel job statistics"""
    channel_id: int
    channel_name: str
    channel_link: str
    total_messages: int
    job_messages: int
    job_percentage: float
    last_activity: str
    joined_by_account: str
    sample_job_messages: List[str]

class JobMessageAnalyzer:
    """
    Analyzer for identifying job messages in Telegram channels
    
    This class provides functionality to:
    - Connect to the Telegram jobs database
    - Analyze message content for job-related keywords
    - Calculate job message ratios for channels
    - Generate reports of high-value job channels
    """
    
    def __init__(self, db_path: str = "telegram_jobs.db"):
        """
        Initialize the job analyzer
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        
        # Job-related keywords for message classification
        # These keywords help identify job postings in messages
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
    
    def connect_db(self) -> sqlite3.Connection:
        """
        Create database connection
        
        Returns:
            SQLite database connection
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
    
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
        total_keywords = 0
        
        for category, keywords in self.job_keywords.items():
            matches = sum(1 for keyword in keywords if keyword in text_lower)
            category_matches[category] = matches
            total_keywords += len(keywords)
        
        # Calculate weighted score
        # Job indicators and roles have higher weight
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
    
    def get_channel_messages(self, channel_id: int, limit: int = 100, days: int = None) -> List[Dict]:
        """
        Get recent messages from a specific channel
        
        Args:
            channel_id: ID of the channel to analyze
            limit: Maximum number of messages to retrieve
            days: If specified, only get messages from last N days
            
        Returns:
            List of message dictionaries
        """
        conn = self.connect_db()
        
        try:
            query = """
                SELECT message_id, message_text, timestamp, sender_name
                FROM messages 
                WHERE group_id = ?
            """
            params = [channel_id]
            
            # Add date filter if specified
            if days:
                cutoff_date = datetime.now() - timedelta(days=days)
                query += " AND timestamp >= ?"
                params.append(cutoff_date.isoformat())
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            messages = [dict(row) for row in cursor.fetchall()]
            
            self.logger.info(f"Retrieved {len(messages)} messages from channel {channel_id}")
            return messages
            
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving messages for channel {channel_id}: {e}")
            return []
        finally:
            conn.close()
    
    def analyze_channel(self, channel_id: int, channel_name: str, channel_link: str, 
                       joined_by_account: str, limit: int = 100, days: int = None) -> ChannelJobStats:
        """
        Analyze a single channel for job message frequency
        
        Args:
            channel_id: Channel ID to analyze
            channel_name: Channel name
            channel_link: Channel link
            joined_by_account: Account that joined this channel
            limit: Number of recent messages to analyze
            days: Analyze only last N days if specified
            
        Returns:
            ChannelJobStats object with analysis results
        """
        self.logger.info(f"Analyzing channel: {channel_name}")
        
        # Get recent messages
        messages = self.get_channel_messages(channel_id, limit, days)
        
        if not messages:
            return ChannelJobStats(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_link=channel_link,
                total_messages=0,
                job_messages=0,
                job_percentage=0.0,
                last_activity="No messages",
                joined_by_account=joined_by_account,
                sample_job_messages=[]
            )
        
        # Analyze each message for job content
        job_messages = []
        job_count = 0
        sample_job_messages = []
        
        for message in messages:
            message_text = message.get('message_text', '')
            if message_text:
                is_job, confidence = self.is_job_message(message_text)
                if is_job:
                    job_count += 1
                    job_messages.append({
                        'message_id': message['message_id'],
                        'text': message_text[:200] + '...' if len(message_text) > 200 else message_text,
                        'confidence': confidence,
                        'timestamp': message['timestamp']
                    })
                    
                    # Keep sample job messages for reporting
                    if len(sample_job_messages) < 3:
                        sample_job_messages.append(message_text[:150] + '...' if len(message_text) > 150 else message_text)
        
        # Calculate job percentage
        total_messages = len(messages)
        job_percentage = (job_count / total_messages * 100) if total_messages > 0 else 0.0
        
        # Get last activity timestamp
        last_activity = messages[0]['timestamp'] if messages else "No activity"
        
        return ChannelJobStats(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_link=channel_link,
            total_messages=total_messages,
            job_messages=job_count,
            job_percentage=job_percentage,
            last_activity=last_activity,
            joined_by_account=joined_by_account,
            sample_job_messages=sample_job_messages
        )
    
    def get_all_channels(self) -> List[Dict]:
        """
        Get all channels from the database
        
        Returns:
            List of channel information dictionaries
        """
        conn = self.connect_db()
        
        try:
            query = """
                SELECT id, group_name, group_link, joined_by_account, last_activity
                FROM programming_groups 
                WHERE is_active = 1
                ORDER BY group_name
            """
            
            cursor = conn.execute(query)
            channels = [dict(row) for row in cursor.fetchall()]
            
            self.logger.info(f"Found {len(channels)} active channels in database")
            return channels
            
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving channels: {e}")
            return []
        finally:
            conn.close()
    
    def analyze_all_channels(self, min_job_messages: int = 10, limit: int = 100, 
                           days: int = None) -> List[ChannelJobStats]:
        """
        Analyze all channels and return those with high job message frequency
        
        Args:
            min_job_messages: Minimum number of job messages required
            limit: Number of recent messages to analyze per channel
            days: Analyze only last N days if specified
            
        Returns:
            List of ChannelJobStats for channels meeting criteria
        """
        self.logger.info(f"Starting analysis of all channels (min {min_job_messages} job messages)")
        
        channels = self.get_all_channels()
        high_value_channels = []
        
        for channel in channels:
            try:
                stats = self.analyze_channel(
                    channel_id=channel['id'],
                    channel_name=channel['group_name'],
                    channel_link=channel['group_link'],
                    joined_by_account=channel['joined_by_account'] or 'Unknown',
                    limit=limit,
                    days=days
                )
                
                # Check if channel meets criteria
                if stats.job_messages >= min_job_messages:
                    high_value_channels.append(stats)
                    self.logger.info(f"✅ {stats.channel_name}: {stats.job_messages}/{stats.total_messages} "
                                   f"({stats.job_percentage:.1f}%) job messages")
                else:
                    self.logger.info(f"❌ {stats.channel_name}: {stats.job_messages}/{stats.total_messages} "
                                   f"({stats.job_percentage:.1f}%) job messages (below threshold)")
                    
            except Exception as e:
                self.logger.error(f"Error analyzing channel {channel['group_name']}: {e}")
                continue
        
        # Sort by job message count (descending)
        high_value_channels.sort(key=lambda x: x.job_messages, reverse=True)
        
        self.logger.info(f"Found {len(high_value_channels)} channels with {min_job_messages}+ job messages")
        return high_value_channels
    
    def generate_report(self, channel_stats: List[ChannelJobStats], 
                       output_file: str = None) -> Dict:
        """
        Generate a comprehensive report of high-value job channels
        
        Args:
            channel_stats: List of channel statistics
            output_file: Optional file to save JSON report
            
        Returns:
            Report dictionary
        """
        report = {
            'analysis_date': datetime.now().isoformat(),
            'total_channels_analyzed': len(channel_stats),
            'summary': {
                'total_job_messages': sum(stats.job_messages for stats in channel_stats),
                'total_messages': sum(stats.total_messages for stats in channel_stats),
                'average_job_percentage': sum(stats.job_percentage for stats in channel_stats) / len(channel_stats) if channel_stats else 0
            },
            'top_channels': []
        }
        
        # Add detailed channel information
        for i, stats in enumerate(channel_stats, 1):
            channel_info = {
                'rank': i,
                'channel_name': stats.channel_name,
                'channel_link': stats.channel_link,
                'job_messages': stats.job_messages,
                'total_messages': stats.total_messages,
                'job_percentage': round(stats.job_percentage, 2),
                'last_activity': stats.last_activity,
                'joined_by_account': stats.joined_by_account,
                'sample_job_messages': stats.sample_job_messages
            }
            report['top_channels'].append(channel_info)
        
        # Save to file if specified
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Report saved to {output_file}")
        
        return report
    
    def export_to_csv(self, channel_stats: List[ChannelJobStats], 
                     output_file: str = "high_value_job_channels.csv"):
        """
        Export channel statistics to CSV file
        
        Args:
            channel_stats: List of channel statistics
            output_file: CSV file name
        """
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'rank', 'channel_name', 'channel_link', 'job_messages', 
                'total_messages', 'job_percentage', 'last_activity', 
                'joined_by_account'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, stats in enumerate(channel_stats, 1):
                writer.writerow({
                    'rank': i,
                    'channel_name': stats.channel_name,
                    'channel_link': stats.channel_link,
                    'job_messages': stats.job_messages,
                    'total_messages': stats.total_messages,
                    'job_percentage': round(stats.job_percentage, 2),
                    'last_activity': stats.last_activity,
                    'joined_by_account': stats.joined_by_account
                })
        
        self.logger.info(f"CSV report exported to {output_file}")
    
    def print_summary_report(self, channel_stats: List[ChannelJobStats]):
        """
        Print a formatted summary report to console
        
        Args:
            channel_stats: List of channel statistics
        """
        if not channel_stats:
            print("\n❌ No channels found meeting the criteria.")
            return
        
        print(f"\n{'='*80}")
        print("🎯 HIGH-VALUE JOB CHANNELS REPORT")
        print(f"{'='*80}")
        print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Total Channels Found: {len(channel_stats)}")
        print(f"💼 Total Job Messages: {sum(stats.job_messages for stats in channel_stats)}")
        print(f"📨 Total Messages Analyzed: {sum(stats.total_messages for stats in channel_stats)}")
        
        avg_job_percentage = sum(stats.job_percentage for stats in channel_stats) / len(channel_stats)
        print(f"📈 Average Job Percentage: {avg_job_percentage:.1f}%")
        
        print(f"\n{'='*80}")
        print("🏆 TOP CHANNELS BY JOB MESSAGE COUNT")
        print(f"{'='*80}")
        
        for i, stats in enumerate(channel_stats[:20], 1):  # Show top 20
            print(f"\n{i:2d}. 📢 {stats.channel_name}")
            print(f"    🔗 Link: {stats.channel_link}")
            print(f"    💼 Job Messages: {stats.job_messages}/{stats.total_messages} ({stats.job_percentage:.1f}%)")
            print(f"    👤 Joined by: {stats.joined_by_account}")
            print(f"    🕐 Last Activity: {stats.last_activity}")
            
            if stats.sample_job_messages:
                print(f"    📝 Sample Job Messages:")
                for j, sample in enumerate(stats.sample_job_messages[:2], 1):
                    print(f"       {j}. {sample}")
        
        print(f"\n{'='*80}")
        print("✅ Analysis Complete!")
        print(f"{'='*80}")

def main():
    """Main function to run the job analyzer"""
    parser = argparse.ArgumentParser(description='Analyze Telegram channels for job posting frequency')
    parser.add_argument('--min-jobs', type=int, default=10, 
                       help='Minimum number of job messages required (default: 10)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Number of recent messages to analyze per channel (default: 100)')
    parser.add_argument('--days', type=int, default=None,
                       help='Analyze only messages from last N days')
    parser.add_argument('--export-json', type=str, default=None,
                       help='Export detailed report to JSON file')
    parser.add_argument('--export-csv', action='store_true',
                       help='Export results to CSV file')
    parser.add_argument('--db-path', type=str, default='telegram_jobs.db',
                       help='Path to SQLite database file')
    
    args = parser.parse_args()
    
    # Create analyzer instance
    analyzer = JobMessageAnalyzer(args.db_path)
    
    try:
        # Analyze all channels
        channel_stats = analyzer.analyze_all_channels(
            min_job_messages=args.min_jobs,
            limit=args.limit,
            days=args.days
        )
        
        # Print summary report
        analyzer.print_summary_report(channel_stats)
        
        # Export to JSON if requested
        if args.export_json:
            analyzer.generate_report(channel_stats, args.export_json)
        
        # Export to CSV if requested
        if args.export_csv:
            analyzer.export_to_csv(channel_stats)
        
    except Exception as e:
        logging.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
