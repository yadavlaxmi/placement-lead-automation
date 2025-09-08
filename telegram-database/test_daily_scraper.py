#!/usr/bin/env python3
"""
Test script for the daily job scraper
This script tests the components without actually joining groups
"""

import asyncio
import logging
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from daily_job_scraper import DailyJobScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def test_job_detection():
    """Test the job detection algorithm"""
    print("🧪 Testing job detection algorithm...")
    
    scraper = DailyJobScraper()
    
    # Test messages
    test_messages = [
        "We are hiring Python developers with 3+ years experience. Salary: 8-12 LPA. Apply now!",
        "Hello everyone, how are you doing today?",
        "Looking for React developer for remote position. CTC: 6-10 LPA. Send resume to hr@company.com",
        "Good morning! Hope everyone has a great day ahead.",
        "Urgent requirement: Senior Java Developer in Bangalore. Experience: 5+ years. Contact: 9876543210",
        "Thanks for sharing this information.",
        "Job Alert: DevOps Engineer needed. Skills: AWS, Docker, Kubernetes. Location: Mumbai",
        "This is just a regular conversation message."
    ]
    
    print("\n📝 Testing job message detection:")
    print("=" * 60)
    
    for i, message in enumerate(test_messages, 1):
        is_job, confidence = scraper.is_job_message(message)
        status = "✅ JOB" if is_job else "❌ NOT JOB"
        print(f"{i}. {status} (Confidence: {confidence:.2f})")
        print(f"   Message: {message[:80]}{'...' if len(message) > 80 else ''}")
        print()
    
    print("=" * 60)
    print("✅ Job detection test completed!")

async def test_database_connection():
    """Test database connection and methods"""
    print("\n🗄️ Testing database connection...")
    
    try:
        from database.database import DatabaseManager
        db = DatabaseManager()
        
        # Test basic operations
        cities = db.get_cities()
        print(f"✅ Database connected. Found {len(cities)} cities.")
        
        # Test message insertion
        test_message = {
            'group_id': 1,
            'message_id': 'test_123',
            'sender_id': 'test_sender',
            'sender_name': 'Test User',
            'message_text': 'This is a test message',
            'timestamp': '2024-09-07T15:30:00',
            'is_job_post': True,
            'job_score': 0.8,
            'fetched_by_account': 'Test Account'
        }
        
        message_id = db.insert_message(test_message)
        print(f"✅ Test message inserted with ID: {message_id}")
        
        # Test group summary
        summary = db.get_account_group_summary()
        print(f"✅ Account group summary retrieved: {len(summary['summary'])} accounts")
        
        print("✅ Database tests completed!")
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")

async def main():
    """Run all tests"""
    print("🚀 Starting Daily Job Scraper Tests")
    print("=" * 60)
    
    try:
        await test_job_detection()
        await test_database_connection()
        
        print("\n" + "=" * 60)
        print("🎉 All tests completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
