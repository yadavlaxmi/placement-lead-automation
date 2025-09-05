import asyncio
import logging
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, User
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError, ChatAdminRequiredError, ChannelPrivateError
import time
from typing import List, Dict, Any, Optional
import config
from database.database import DatabaseManager

class TelegramAccount:
    def __init__(self, account_config: Dict[str, Any]):
        self.name = account_config['name']
        self.phone = account_config['phone']
        self.api_id = account_config['api_id']
        self.api_hash = account_config['api_hash']
        self.session_name = account_config['session_name']
        self.client = None
        self.is_connected = False
        
    async def connect(self):
        """Connect to Telegram"""
        try:
            self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
            await self.client.start(phone=self.phone)
            self.is_connected = True
            logging.info(f"Connected to Telegram with account: {self.name}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect account {self.name}: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Telegram"""
        if self.client and self.is_connected:
            await self.client.disconnect()
            self.is_connected = False
            logging.info(f"Disconnected account: {self.name}")

class TelegramManager:
    def __init__(self, accounts_config: List[Dict[str, Any]]):
        self.accounts = [TelegramAccount(acc) for acc in accounts_config]
        self.db = DatabaseManager()
        self.current_account_index = 0
        
    async def connect_all_accounts(self):
        """Connect all Telegram accounts"""
        for account in self.accounts:
            await account.connect()
            await asyncio.sleep(2)  # Avoid rate limiting
    
    async def disconnect_all_accounts(self):
        """Disconnect all Telegram accounts"""
        for account in self.accounts:
            await account.disconnect()
    
    def get_next_account(self) -> TelegramAccount:
        """Get next available account (round-robin)"""
        account = self.accounts[self.current_account_index]
        self.current_account_index = (self.current_account_index + 1) % len(self.accounts)
        return account
    
    async def join_group_with_account(self, account: TelegramAccount, group_link: str) -> bool:
        """Join a Telegram group using a specific account"""
        if not account.is_connected:
            logging.warning(f"Account {account.name} not connected")
            return False
        
        try:
            # Extract username from link
            if 't.me/' in group_link:
                username = group_link.split('t.me/')[-1].split('/')[0]
            elif 'telegram.me/' in group_link:
                username = group_link.split('telegram.me/')[-1].split('/')[0]
            else:
                username = group_link
            
            # Remove @ if present
            username = username.replace('@', '')
            
            # Try to join the group using the correct method
            try:
                entity = await account.client.get_entity(username)
                if isinstance(entity, (Channel, Chat)):
                    # Use the correct method for joining
                    await account.client(JoinChannelRequest(channel=entity))
                    logging.info(f"Successfully joined group: {username} with account: {account.name}")
                    return True
                else:
                    logging.warning(f"Entity {username} is not a group/channel")
                    return False
            except AttributeError:
                # Fallback method for older Telethon versions
                try:
                    await account.client.join_chat(username)
                    logging.info(f"Successfully joined group: {username} with account: {account.name}")
                    return True
                except Exception as e2:
                    logging.error(f"Fallback join method failed: {e2}")
                    return False
                
        except FloodWaitError as e:
            logging.warning(f"Rate limited for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return False
        except ChannelPrivateError:
            logging.warning(f"Group {group_link} is private")
            return False
        except Exception as e:
            logging.error(f"Failed to join group {group_link}: {e}")
            return False
    
    async def join_group(self, group_link: str) -> bool:
        """Join a Telegram group using available account"""
        account = self.get_next_account()
        return await self.join_group_with_account(account, group_link)
    
    async def fetch_messages_for_group(self, group_username: str, limit: int = 200, account_name: str = None) -> List[Dict[str, Any]]:
        """Fetch messages from a Telegram group using a specific account"""
        # Find the account that joined this group
        if account_name:
            account = next((acc for acc in self.accounts if acc.name == account_name), None)
            if not account:
                logging.warning(f"Account {account_name} not found")
                return []
        else:
            account = self.get_next_account()
        
        if not account.is_connected:
            return []
        
        try:
            # Get entity
            if group_username.startswith('@'):
                group_username = group_username[1:]
            
            entity = await account.client.get_entity(group_username)
            if not isinstance(entity, (Channel, Chat)):
                return []
            
            # Fetch messages
            messages = []
            async for message in account.client.iter_messages(entity, limit=limit):
                if message.text:  # Only text messages
                    msg_data = {
                        'message_id': str(message.id),
                        'sender_id': str(message.sender_id) if message.sender_id else None,
                        'sender_name': None,
                        'message_text': message.text,
                        'timestamp': message.date.isoformat() if message.date else None,
                        'is_job_post': False  # Will be determined by ML pipeline
                    }
                    
                    # Get sender name if available
                    if message.sender_id:
                        try:
                            sender = await account.client.get_entity(message.sender_id)
                            if isinstance(sender, User):
                                msg_data['sender_name'] = sender.first_name or sender.username or "Unknown"
                        except:
                            pass
                    
                    messages.append(msg_data)
            
            logging.info(f"Fetched {len(messages)} messages from {group_username} using account: {account.name}")
            return messages
            
        except Exception as e:
            logging.error(f"Failed to fetch messages from {group_username}: {e}")
            return []
    
    async def fetch_messages(self, group_username: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Fetch messages from a Telegram group (legacy method)"""
        return await self.fetch_messages_for_group(group_username, limit)
    
    async def get_group_info(self, group_username: str) -> Optional[Dict[str, Any]]:
        """Get information about a Telegram group"""
        account = self.get_next_account()
        if not account.is_connected:
            return None
        
        try:
            if group_username.startswith('@'):
                group_username = group_username[1:]
            
            entity = await account.client.get_entity(group_username)
            if isinstance(entity, (Channel, Chat)):
                return {
                    'group_name': entity.title,
                    'group_id': str(entity.id),
                    'member_count': getattr(entity, 'participants_count', 0),
                    'description': getattr(entity, 'about', ''),
                    'is_verified': getattr(entity, 'verified', False)
                }
        except Exception as e:
            logging.error(f"Failed to get group info for {group_username}: {e}")
        
        return None
    
    async def verify_group_activity(self, group_username: str) -> bool:
        """Verify if a group is active and has recent messages"""
        try:
            messages = await self.fetch_messages(group_username, limit=10)
            if len(messages) >= 5:  # At least 5 recent messages
                return True
            return False
        except:
            return False 