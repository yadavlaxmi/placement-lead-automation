"""
Telegram Service - Handles Telegram API operations
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from telethon import TelegramClient
from telethon.tl.types import Message
from models.account import Account
from config.settings import settings

class TelegramService:
    """Service for Telegram operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.clients = {}  # Cache for Telegram clients
    
    def get_client(self, account: Account) -> TelegramClient:
        """Get or create Telegram client for account"""
        if account.id not in self.clients:
            client = TelegramClient(
                account.session_name,
                account.api_id,
                account.api_hash
            )
            self.clients[account.id] = client
        
        return self.clients[account.id]
    
    async def join_group(self, account: Account, group_link: str) -> bool:
        """Join Telegram group"""
        try:
            client = self.get_client(account)
            
            # Connect if not connected
            if not client.is_connected():
                await client.start(phone=account.phone)
            
            # Extract username from link
            username = group_link.replace('https://t.me/', '').replace('@', '')
            
            # Join group
            await client.join_chat(username)
            
            self.logger.info(f"✅ Account {account.name} joined group: {username}")
            
            # Rate limiting
            await asyncio.sleep(settings.CRAWL_DELAY)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to join group {group_link} with account {account.name}: {e}")
            return False
    
    async def leave_group(self, account: Account, group_id: str) -> bool:
        """Leave Telegram group"""
        try:
            client = self.get_client(account)
            
            # Connect if not connected
            if not client.is_connected():
                await client.start(phone=account.phone)
            
            # Get group entity
            group = await client.get_entity(group_id)
            
            # Leave group
            await client.delete_dialog(group)
            
            self.logger.info(f"✅ Account {account.name} left group: {group_id}")
            
            # Rate limiting
            await asyncio.sleep(settings.CRAWL_DELAY)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to leave group {group_id} with account {account.name}: {e}")
            return False
    
    async def get_group_messages(self, account: Account, group_link: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get messages from Telegram group"""
        try:
            client = self.get_client(account)
            
            # Connect if not connected
            if not client.is_connected():
                await client.start(phone=account.phone)
            
            # Extract username from link
            username = group_link.replace('https://t.me/', '').replace('@', '')
            
            # Get group entity
            group = await client.get_entity(username)
            
            # Fetch messages
            messages = []
            async for message in client.iter_messages(group, limit=limit):
                if message.text:  # Only text messages
                    messages.append({
                        'text': message.text,
                        'timestamp': message.date,
                        'message_id': message.id,
                        'is_job_message': self._is_job_message(message.text),
                        'job_score': self._calculate_job_score(message.text)
                    })
            
            self.logger.info(f"📥 Fetched {len(messages)} messages from {username}")
            
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch messages from {group_link}: {e}")
            return []
    
    async def get_new_messages(self, account: Account, group_link: str, last_message_id: int = None) -> List[Dict[str, Any]]:
        """Get new messages since last fetch"""
        try:
            client = self.get_client(account)
            
            # Connect if not connected
            if not client.is_connected():
                await client.start(phone=account.phone)
            
            # Extract username from link
            username = group_link.replace('https://t.me/', '').replace('@', '')
            
            # Get group entity
            group = await client.get_entity(username)
            
            # Fetch new messages
            messages = []
            async for message in client.iter_messages(group, min_id=last_message_id):
                if message.text and message.id > (last_message_id or 0):
                    messages.append({
                        'text': message.text,
                        'timestamp': message.date,
                        'message_id': message.id,
                        'is_job_message': self._is_job_message(message.text),
                        'job_score': self._calculate_job_score(message.text)
                    })
            
            self.logger.info(f"📥 Fetched {len(messages)} new messages from {username}")
            
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch new messages from {group_link}: {e}")
            return []
    
    def _is_job_message(self, text: str) -> bool:
        """Check if message is job-related"""
        job_keywords = [
            'job', 'hiring', 'recruitment', 'position', 'vacancy',
            'developer', 'programmer', 'engineer', 'remote', 'work',
            'salary', 'experience', 'skills', 'requirements',
            'apply', 'cv', 'resume', 'interview'
        ]
        
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in job_keywords)
    
    def _calculate_job_score(self, text: str) -> float:
        """Calculate job relevance score"""
        job_keywords = {
            'job': 0.8, 'hiring': 0.9, 'recruitment': 0.9, 'position': 0.7,
            'vacancy': 0.8, 'developer': 0.8, 'programmer': 0.8,
            'engineer': 0.7, 'remote': 0.6, 'work': 0.5,
            'salary': 0.7, 'experience': 0.6, 'skills': 0.6,
            'requirements': 0.6, 'apply': 0.8, 'cv': 0.7,
            'resume': 0.7, 'interview': 0.8
        }
        
        text_lower = text.lower()
        score = 0.0
        
        for keyword, weight in job_keywords.items():
            if keyword in text_lower:
                score += weight
        
        # Normalize score
        return min(score / 5.0, 1.0)
    
    async def close_all_clients(self):
        """Close all Telegram clients"""
        for client in self.clients.values():
            if client.is_connected():
                await client.disconnect()
        
        self.clients.clear()
        self.logger.info("🔌 All Telegram clients closed")
