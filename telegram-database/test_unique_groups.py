#!/usr/bin/env python3
"""
Test Unique Group Assignment - Verify that each account gets unique groups
"""

import asyncio
import logging
from universal_group_manager import UniversalGroupManager
from database.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class UniqueGroupTester:
    def __init__(self):
        self.universal_group_manager = UniversalGroupManager()
        self.db = DatabaseManager()
        
    def test_unique_assignments(self):
        """Test that each account gets unique groups"""
        print("🧪 Testing Unique Group Assignments")
        print("="*50)
        
        # Test accounts
        accounts = ["Account 1", "Account 2", "Account 3", "Account 4"]
        
        # Get groups for each account
        all_assigned_groups = set()
        account_assignments = {}
        
        for account in accounts:
            groups = self.universal_group_manager.get_groups_for_account(account, limit=5)
            account_assignments[account] = groups
            
            print(f"\n🔹 {account}:")
            for i, group in enumerate(groups, 1):
                print(f"   {i}. {group['name']} ({group['link']})")
                all_assigned_groups.add(group['link'])
        
        # Check for uniqueness
        total_assigned = len(all_assigned_groups)
        expected_total = sum(len(groups) for groups in account_assignments.values())
        
        print(f"\n📊 Assignment Summary:")
        print(f"   Total unique groups assigned: {total_assigned}")
        print(f"   Expected total: {expected_total}")
        
        if total_assigned == expected_total:
            print("   ✅ All assignments are unique!")
        else:
            print("   ❌ Found duplicate assignments!")
            
        # Show which groups are assigned to which accounts
        print(f"\n🎯 Group Assignment Details:")
        for account, groups in account_assignments.items():
            print(f"   {account}: {len(groups)} groups")
            for group in groups:
                print(f"     - {group['name']}")
    
    def test_database_tracking(self):
        """Test database tracking of group assignments"""
        print("\n🗄️ Testing Database Tracking")
        print("="*50)
        
        # Get database summary
        db_summary = self.db.get_account_group_summary()
        
        if db_summary['summary']:
            print("📈 Database Summary:")
            for account_summary in db_summary['summary']:
                print(f"   {account_summary['account_name']}: {account_summary['total_groups']} groups")
        else:
            print("   No data in database yet")
    
    def test_universal_stats(self):
        """Test universal group statistics"""
        print("\n🌐 Testing Universal Group Stats")
        print("="*50)
        
        stats = self.universal_group_manager.get_universal_stats()
        
        print(f"📊 Total Groups: {stats['total_groups']}")
        print(f"🕐 Last Updated: {stats['last_updated']}")
        
        print("\n📂 Categories:")
        for category, count in stats['categories'].items():
            print(f"   {category}: {count}")
        
        print("\n⭐ Priorities:")
        for priority, count in stats['priorities'].items():
            print(f"   {priority}: {count}")
    
    def run_all_tests(self):
        """Run all tests"""
        print("🚀 Starting Unique Group Assignment Tests")
        print("="*60)
        
        try:
            self.test_unique_assignments()
            self.test_database_tracking()
            self.test_universal_stats()
            
            print("\n" + "="*60)
            print("✅ All tests completed successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            logging.error(f"Test error: {e}")

async def main():
    """Main function"""
    tester = UniqueGroupTester()
    tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main()) 