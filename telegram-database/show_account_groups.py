#!/usr/bin/env python3
"""
Show Account Groups - Display which account joined which groups
"""

import asyncio
import logging
from datetime import datetime
from database.database import DatabaseManager
from universal_group_manager import UniversalGroupManager
from crawler import JobCrawler
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class AccountGroupTracker:
    def __init__(self):
        self.db = DatabaseManager()
        self.universal_group_manager = UniversalGroupManager()
        
    def show_today_assignments(self):
        """Show today's unique group assignments"""
        print("\n" + "="*80)
        print("📊 TODAY'S UNIQUE GROUP ASSIGNMENTS")
        print("="*80)
        
        # Get universal group manager summary
        summary = self.universal_group_manager.get_all_accounts_groups_summary()
        
        if not summary:
            print("❌ No group assignments found for today")
            return
        
        for account_name, groups in summary.items():
            print(f"\n🔹 {account_name}:")
            if not groups:
                print("   No groups assigned today")
            else:
                for i, group in enumerate(groups, 1):
                    priority_emoji = {
                        'high': '🔴',
                        'medium': '🟡', 
                        'low': '🟢'
                    }.get(group.get('priority', 'unknown'), '⚪')
                    
                    print(f"   {i:2d}. {priority_emoji} {group['name']}")
                    print(f"       📍 {group['link']}")
                    print(f"       🏷️  Category: {group.get('category', 'unknown')}")
                    print(f"       ⭐ Priority: {group.get('priority', 'unknown')}")
    
    def show_database_assignments(self):
        """Show database-stored group assignments"""
        print("\n" + "="*80)
        print("🗄️ DATABASE GROUP ASSIGNMENTS")
        print("="*80)
        
        # Get database summary
        db_summary = self.db.get_account_group_summary()
        
        if not db_summary['summary']:
            print("❌ No group assignments found in database")
            return
        
        print("\n📈 Account Summary:")
        for account_summary in db_summary['summary']:
            print(f"   🔹 {account_summary['account_name']}:")
            print(f"      📊 Total Groups: {account_summary['total_groups']}")
            print(f"      📅 Active Days: {account_summary['active_days']}")
            print(f"      🕐 Last Assignment: {account_summary['last_assignment']}")
        
        print("\n📅 Daily Breakdown:")
        for daily in db_summary['daily_breakdown']:
            print(f"   🔹 {daily['account_name']} - {daily['assignment_date']}: {daily['groups_joined']} groups")
    
    def show_unique_groups_per_account(self):
        """Show unique groups assigned to each account today"""
        print("\n" + "="*80)
        print("🎯 UNIQUE GROUPS PER ACCOUNT (TODAY)")
        print("="*80)
        
        unique_groups = self.db.get_unique_groups_per_account()
        
        if not unique_groups:
            print("❌ No unique group assignments found for today")
            return
        
        for account_name, data in unique_groups.items():
            print(f"\n🔹 {account_name}:")
            if not data['groups']:
                print("   No groups assigned today")
            else:
                for i, (group_name, group_link) in enumerate(zip(data['groups'], data['links']), 1):
                    print(f"   {i:2d}. 📢 {group_name}")
                    print(f"       🔗 {group_link}")
    
    def show_messages_by_account(self):
        """Show messages fetched by each account"""
        print("\n" + "="*80)
        print("💬 MESSAGES BY ACCOUNT")
        print("="*80)
        
        # Get all accounts from config
        accounts = ["Account 1", "Account 2", "Account 3", "Account 4"]
        
        for account_name in accounts:
            messages = self.db.get_messages_by_account(account_name, limit=10)
            print(f"\n🔹 {account_name}:")
            if not messages:
                print("   No messages fetched")
            else:
                print(f"   📊 Total messages fetched: {len(messages)}")
                for i, msg in enumerate(messages[:5], 1):  # Show first 5 messages
                    print(f"   {i}. 📝 {msg['message_text'][:100]}...")
                    print(f"      📅 {msg['timestamp']} | 📢 {msg['group_name']}")
    
    def show_group_credibility_scores(self):
        """Show group credibility scores with account info"""
        print("\n" + "="*80)
        print("⭐ GROUP CREDIBILITY SCORES")
        print("="*80)
        
        groups = self.db.get_programming_groups()
        
        if not groups:
            print("❌ No groups found in database")
            return
        
        # Sort by credibility score
        groups.sort(key=lambda x: x.get('credibility_score', 0), reverse=True)
        
        for group in groups[:20]:  # Show top 20 groups
            score = group.get('credibility_score', 0)
            account = group.get('joined_by_account', 'Unknown')
            
            # Score emoji
            if score >= 8:
                score_emoji = "🟢"
            elif score >= 6:
                score_emoji = "🟡"
            elif score >= 4:
                score_emoji = "🟠"
            else:
                score_emoji = "🔴"
            
            print(f"   {score_emoji} {group['group_name']}")
            print(f"      📊 Score: {score:.2f}/10")
            print(f"      👤 Joined by: {account}")
            print(f"      📍 {group['group_link']}")
            print(f"      💬 Messages: {group.get('total_messages', 0)}")
            print()
    
    def show_account_status(self):
        """Show current account status"""
        print("\n" + "="*80)
        print("📊 ACCOUNT STATUS")
        print("="*80)
        
        accounts = ["Account 1", "Account 2", "Account 3", "Account 4"]
        
        for account_name in accounts:
            status = self.universal_group_manager.get_account_status(account_name)
            print(f"\n🔹 {account_name}:")
            print(f"   📈 Joined today: {status['joined_today']}/10")
            print(f"   ⏳ Remaining: {status['remaining']}")
            print(f"   🌐 Total available: {status['total_available']}")
            
            # Progress bar
            progress = (status['joined_today'] / 10) * 100
            bar_length = 20
            filled = int((progress / 100) * bar_length)
            bar = "█" * filled + "░" * (bar_length - filled)
            print(f"   📊 Progress: [{bar}] {progress:.1f}%")
    
    def show_universal_stats(self):
        """Show universal group statistics"""
        print("\n" + "="*80)
        print("🌐 UNIVERSAL GROUP STATISTICS")
        print("="*80)
        
        stats = self.universal_group_manager.get_universal_stats()
        
        print(f"📊 Total Groups: {stats['total_groups']}")
        print(f"🕐 Last Updated: {stats['last_updated']}")
        
        print("\n📂 Categories:")
        for category, count in stats['categories'].items():
            print(f"   🔹 {category}: {count}")
        
        print("\n⭐ Priorities:")
        for priority, count in stats['priorities'].items():
            priority_emoji = {
                'high': '🔴',
                'medium': '🟡',
                'low': '🟢'
            }.get(priority, '⚪')
            print(f"   {priority_emoji} {priority}: {count}")
    
    def run_full_report(self):
        """Run complete account-group report"""
        print("🚀 ACCOUNT-GROUP TRACKING REPORT")
        print("Generated at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        self.show_today_assignments()
        self.show_database_assignments()
        self.show_unique_groups_per_account()
        self.show_messages_by_account()
        self.show_group_credibility_scores()
        self.show_account_status()
        self.show_universal_stats()
        
        print("\n" + "="*80)
        print("✅ REPORT COMPLETE")
        print("="*80)

async def main():
    """Main function"""
    tracker = AccountGroupTracker()
    
    try:
        tracker.run_full_report()
    except Exception as e:
        logging.error(f"Error generating report: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 