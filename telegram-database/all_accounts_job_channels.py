#!/usr/bin/env python3
"""
All Accounts Job Channels CSV Generator
सभी accounts के joined job channels की complete list CSV में export करता है
"""

import csv
import json
import sqlite3
from datetime import datetime
from typing import List, Dict, Any

class AllAccountsJobChannelsExporter:
    """Export all accounts job channels to CSV"""
    
    def __init__(self):
        self.db_path = "telegram_groups_tracker.db"
    
    def get_all_groups_from_database(self) -> List[Dict]:
        """Get all groups from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT group_name, group_link, joined_by_account, joined_date, 
                   job_messages, total_messages, is_high_value
            FROM groups
            ORDER BY joined_by_account, job_messages DESC
        ''')
        
        groups = []
        for row in cursor.fetchall():
            groups.append({
                'group_name': row[0],
                'group_link': row[1],
                'joined_by_account': row[2],
                'joined_date': row[3],
                'job_messages': row[4],
                'total_messages': row[5],
                'is_high_value': bool(row[6]),
                'job_percentage': round((row[4] / row[5]) * 100, 2) if row[5] > 0 else 0
            })
        
        conn.close()
        return groups
    
    def get_account_summary(self, groups: List[Dict]) -> Dict[str, Dict]:
        """Get summary for each account"""
        account_stats = {}
        
        for group in groups:
            account = group['joined_by_account']
            if account not in account_stats:
                account_stats[account] = {
                    'total_groups': 0,
                    'high_value_groups': 0,
                    'total_job_messages': 0,
                    'total_messages': 0,
                    'groups': []
                }
            
            account_stats[account]['total_groups'] += 1
            account_stats[account]['total_job_messages'] += group['job_messages']
            account_stats[account]['total_messages'] += group['total_messages']
            account_stats[account]['groups'].append(group)
            
            if group['is_high_value']:
                account_stats[account]['high_value_groups'] += 1
        
        return account_stats
    
    def export_all_accounts_csv(self, groups: List[Dict]):
        """Export all accounts job channels to CSV"""
        csv_filename = f"all_accounts_job_channels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'account_name', 'group_name', 'group_link', 'job_messages', 
                'total_messages', 'job_percentage', 'is_high_value', 
                'joined_date', 'analysis_timestamp'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for group in groups:
                writer.writerow({
                    'account_name': group['joined_by_account'],
                    'group_name': group['group_name'],
                    'group_link': group['group_link'],
                    'job_messages': group['job_messages'],
                    'total_messages': group['total_messages'],
                    'job_percentage': group['job_percentage'],
                    'is_high_value': 'Yes' if group['is_high_value'] else 'No',
                    'joined_date': group['joined_date'],
                    'analysis_timestamp': datetime.now().isoformat()
                })
        
        print(f"📄 Exported {len(groups)} job channels to {csv_filename}")
        return csv_filename
    
    def export_account_summary_csv(self, account_stats: Dict[str, Dict]):
        """Export account summary to CSV"""
        csv_filename = f"account_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'account_name', 'total_groups', 'high_value_groups', 
                'total_job_messages', 'total_messages', 'overall_job_percentage'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for account, stats in account_stats.items():
                overall_percentage = (stats['total_job_messages'] / stats['total_messages']) * 100 if stats['total_messages'] > 0 else 0
                
                writer.writerow({
                    'account_name': account,
                    'total_groups': stats['total_groups'],
                    'high_value_groups': stats['high_value_groups'],
                    'total_job_messages': stats['total_job_messages'],
                    'total_messages': stats['total_messages'],
                    'overall_job_percentage': round(overall_percentage, 2)
                })
        
        print(f"📊 Exported account summary to {csv_filename}")
        return csv_filename
    
    def print_summary(self, groups: List[Dict], account_stats: Dict[str, Dict]):
        """Print comprehensive summary"""
        print(f"\n{'='*80}")
        print("📊 ALL ACCOUNTS JOB CHANNELS SUMMARY")
        print(f"{'='*80}")
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Total Groups Joined: {len(groups)}")
        
        total_high_value = sum(stats['high_value_groups'] for stats in account_stats.values())
        total_job_messages = sum(stats['total_job_messages'] for stats in account_stats.values())
        total_messages = sum(stats['total_messages'] for stats in account_stats.values())
        
        print(f"✅ Total High-Value Groups: {total_high_value}")
        print(f"💼 Total Job Messages: {total_job_messages}")
        print(f"📨 Total Messages: {total_messages}")
        
        if total_messages > 0:
            overall_percentage = (total_job_messages / total_messages) * 100
            print(f"📈 Overall Job Percentage: {overall_percentage:.1f}%")
        
        print(f"\n👥 ACCOUNT BREAKDOWN:")
        print(f"{'='*80}")
        
        for account, stats in account_stats.items():
            account_percentage = (stats['total_job_messages'] / stats['total_messages']) * 100 if stats['total_messages'] > 0 else 0
            
            print(f"👤 {account}:")
            print(f"    🔗 Groups Joined: {stats['total_groups']}")
            print(f"    ✅ High-Value Groups: {stats['high_value_groups']}")
            print(f"    💼 Job Messages: {stats['total_job_messages']}")
            print(f"    📨 Total Messages: {stats['total_messages']}")
            print(f"    📈 Job Percentage: {account_percentage:.1f}%")
            
            # Show top groups for this account
            high_value_groups = [g for g in stats['groups'] if g['is_high_value']]
            if high_value_groups:
                print(f"    🏆 Top Groups:")
                for i, group in enumerate(high_value_groups[:3], 1):
                    print(f"        {i}. {group['group_name']} ({group['job_messages']}/100)")
            print()
        
        print(f"{'='*80}")
        print("✅ CSV files exported successfully!")
        print(f"{'='*80}")
    
    def run(self):
        """Run the export process"""
        print("🚀 Starting all accounts job channels export...")
        
        # Get all groups from database
        groups = self.get_all_groups_from_database()
        
        if not groups:
            print("❌ No groups found in database!")
            return
        
        # Get account summary
        account_stats = self.get_account_summary(groups)
        
        # Export CSV files
        all_channels_csv = self.export_all_accounts_csv(groups)
        summary_csv = self.export_account_summary_csv(account_stats)
        
        # Print summary
        self.print_summary(groups, account_stats)
        
        print(f"\n📁 Generated Files:")
        print(f"    📄 All Channels: {all_channels_csv}")
        print(f"    📊 Account Summary: {summary_csv}")

if __name__ == "__main__":
    exporter = AllAccountsJobChannelsExporter()
    exporter.run()
