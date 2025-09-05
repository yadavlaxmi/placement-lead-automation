import asyncio
import logging
import time
from typing import List, Dict, Any
from database.database import DatabaseManager
from telegram_client import TelegramManager
from search_engine import SearchEngine
from ml_pipeline import MLPipeline
from universal_group_manager import UniversalGroupManager
import config

class JobCrawler:
    def __init__(self, accounts_config: List[Dict[str, Any]]):
        self.db = DatabaseManager()
        self.telegram_manager = TelegramManager(accounts_config)
        self.search_engine = SearchEngine()
        self.ml_pipeline = MLPipeline()
        self.universal_group_manager = UniversalGroupManager()
        self.is_running = False
        
    async def start_crawling(self):
        """Start the main crawling process"""
        self.is_running = True
        logging.info("Starting job crawler...")
        
        try:
            # Connect all Telegram accounts
            await self.telegram_manager.connect_all_accounts()
            
            # Initialize cities if not exists
            await self._initialize_cities()
            
            # Phase 1: Search and discover groups
            await self._discover_programming_groups()
            
            # Phase 2: Join unique groups using universal manager
            await self._join_unique_groups_from_universal_list()
            
            # Phase 3: Fetch messages from unique groups per account
            await self._fetch_messages_from_unique_groups()
            
            # Phase 4: Score groups and identify high-quality sources
            await self._score_and_filter_groups()
            
            # Phase 5: Continuous crawling of high-score groups
            await self._continuous_crawling()
            
        except Exception as e:
            logging.error(f"Crawler error: {e}")
        finally:
            await self.telegram_manager.disconnect_all_accounts()
            self.is_running = False
    
    async def _initialize_cities(self):
        """Initialize cities in database"""
        cities = [
            "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata",
            "Pune", "Ahmedabad", "Surat", "Jaipur", "Lucknow", "Kanpur",
            "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Pimpri-Chinchwad",
            "Patna", "Vadodara", "Ghaziabad", "Ludhiana", "Agra", "Nashik",
            "Faridabad", "Meerut", "Rajkot", "Kalyan-Dombivali", "Vasai-Virar",
            "Varanasi", "Srinagar", "Aurangabad", "Navi Mumbai", "Solapur",
            "Vijayawada", "Kolhapur", "Amritsar", "Gwalior", "Bhubaneswar",
            "Bikaner", "Bhavnagar", "Jodhpur", "Raipur", "Kota", "Jabalpur",
            "Guntur", "Chandigarh", "Mysore", "Bareilly", "Moradabad", "Aligarh",
            "Jhansi", "Warangal", "Raebareli", "Gorakhpur", "Bhiwandi", "Amravati",
            "Noida", "Jamshedpur", "Bhilai", "Cuttack", "Firozabad", "Kochi",
            "Bhavnagar", "Dehradun", "Durgapur", "Asansol", "Nanded", "Kolhapur",
            "Ajmer", "Gulbarga", "Jamnagar", "Ujjain", "Loni", "Siliguri",
            "Jhansi", "Ulhasnagar", "Jammu", "Mangalore", "Erode", "Belgaum",
            "Ambattur", "Tirunelveli", "Malegaon", "Gaya", "Jalgaon", "Udaipur",
            "Maheshtala", "Tirupur", "Davanagere", "Kozhikode", "Akola", "Kurnool",
            "Rajpur Sonarpur", "Bokaro", "South Dumdum", "Bellary", "Patiala",
            "Gopalpur", "Agartala", "Bhagalpur", "Muzaffarnagar", "Bhatpara",
            "Panihati", "Latur", "Dhule", "Rohtak", "Korba", "Bhilwara",
            "Brahmapur", "Muzaffarpur", "Ahmednagar", "Mathura", "Kollam",
            "Avadi", "Rajahmundry", "Lakhnow", "Bilaspur", "Thiruvananthapuram",
            "Kamarhati", "Sambalpur", "Shahjahanpur", "Satara", "Bijapur",
            "Rampur", "Shimoga", "Chandrapur", "Junagadh", "Thrissur",
            "Alwar", "Bardhaman", "Kulti", "Kakinada", "Nizamabad",
            "Parbhani", "Tumkur", "Hisar", "Ozhukarai", "Bihar Sharif",
            "Panipat", "Darbhanga", "Bally", "Aizawl", "Dewas", "Ichalkaranji",
            "Tirupati", "Karnal", "Bathinda", "Rampur", "Shahjahanpur",
            "Satara", "Bijapur", "Rampur", "Shimoga", "Chandrapur",
            "Junagadh", "Thrissur", "Alwar", "Bardhaman", "Kulti",
            "Kakinada", "Nizamabad", "Parbhani", "Tumkur", "Hisar"
        ]
        
        for city in cities:
            self.db.insert_city(city)
        logging.info(f"Initialized {len(cities)} cities")
    
    async def _discover_programming_groups(self):
        """Discover programming groups using search engines"""
        logging.info("Starting group discovery...")
        cities = self.db.get_cities()
        
        # Focus on major cities first for better results
        major_cities = [
            "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata",
            "Pune", "Ahmedabad", "Surat", "Jaipur", "Lucknow", "Kanpur",
            "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Noida",
            "Gurgaon", "Faridabad", "Ghaziabad", "Vadodara", "Ludhiana"
        ]
        
        discovered_groups = []
        
        # Search in major cities first (better job opportunities)
        for city in major_cities:
            if city in [c['name'] for c in cities]:
                logging.info(f"Searching groups in major city: {city}")
                
                # Search for groups in this city with fewer programming languages
                for language in config.PROGRAMMING_LANGUAGES[:5]:  # Top 5 languages
                    try:
                        groups = self.search_engine.search_programming_groups(city, language)
                        
                        for group in groups:
                            group_data = {
                                'name': group.get('title', 'Unknown'),
                                'link': group['url'],
                                'category': 'programming',
                                'priority': 'high',  # Major cities get high priority
                                'city': city,
                                'language': language
                            }
                            discovered_groups.append(group_data)
                            logging.info(f"Discovered: {group['url']} in {city} for {language}")
                        
                        # Rate limiting between searches
                        await asyncio.sleep(5)  # Increased delay for major cities
                        
                    except Exception as e:
                        logging.error(f"Error searching {city} for {language}: {e}")
                        continue
        
        # Add discovered groups to universal list
        if discovered_groups:
            self.universal_group_manager.add_discovered_groups(discovered_groups)
            logging.info(f"Added {len(discovered_groups)} new groups to universal list")
        else:
            logging.warning("No new groups discovered by search engines")
    
    async def _join_unique_groups_from_universal_list(self):
        """Join unique groups using universal group manager"""
        logging.info("Starting unique group joining from universal list...")
        
        # Get unique groups for each account
        for account in self.telegram_manager.accounts:
            account_name = account.name
            groups_to_join = self.universal_group_manager.get_groups_for_account(account_name, limit=10)
            
            logging.info(f"Account {account_name} will join {len(groups_to_join)} unique groups today")
            
            for group in groups_to_join:
                try:
                    success = await self.telegram_manager.join_group_with_account(account, group['link'])
                    
                    if success:
                        # Store group in database with account tracking
                        group_data = {
                            'group_name': group['name'],
                            'group_link': group['link'],
                            'city_id': None,  # Will be updated later
                            'source_type': 'telegram',
                            'credibility_score': 0.0,
                            'joined_by_account': account_name
                        }
                        
                        group_id = self.db.insert_programming_group(group_data)
                        
                        # Track account-group assignment
                        self.db.insert_account_group_assignment(account_name, group_id)
                        
                        logging.info(f"Successfully joined unique group: {group['name']} with account: {account_name}")
                        
                        # Rate limiting between joins
                        await asyncio.sleep(config.CRAWL_DELAY)
                    else:
                        logging.warning(f"Failed to join group: {group['name']} with account: {account_name}")
                        
                except Exception as e:
                    logging.error(f"Error joining group {group['name']}: {e}")
                    continue
        
        # Show summary of unique group assignments
        await self._show_unique_group_summary()
    
    async def _show_unique_group_summary(self):
        """Show summary of which account joined which unique groups"""
        logging.info("📊 Unique Group Assignment Summary:")
        
        summary = self.universal_group_manager.get_all_accounts_groups_summary()
        
        for account_name, groups in summary.items():
            logging.info(f"  {account_name}:")
            for group in groups:
                logging.info(f"    - {group['name']} ({group['link']}) - Priority: {group.get('priority', 'unknown')}")
        
        # Show database summary
        db_summary = self.db.get_account_group_summary()
        logging.info("📈 Database Group Assignment Summary:")
        for account_summary in db_summary['summary']:
            logging.info(f"  {account_summary['account_name']}: {account_summary['total_groups']} groups, {account_summary['active_days']} active days")
    
    async def _fetch_messages_from_unique_groups(self):
        """Fetch messages from unique groups assigned to each account"""
        logging.info("Fetching messages from unique groups per account...")
        
        # Get unique groups for each account
        for account in self.telegram_manager.accounts:
            account_name = account.name
            unique_groups = self.universal_group_manager.get_account_unique_groups(account_name)
            
            logging.info(f"Fetching messages for account {account_name} from {len(unique_groups)} unique groups")
            
            for group in unique_groups:
                try:
                    # Fetch messages using the specific account
                    messages = await self.telegram_manager.fetch_messages_for_group(
                        group['link'], 
                        limit=config.MESSAGES_PER_GROUP,
                        account_name=account_name
                    )
                    
                    if messages:
                        # Get group from database
                        db_groups = self.db.get_programming_groups(account_name=account_name)
                        group_id = None
                        for db_group in db_groups:
                            if db_group['group_link'] == group['link']:
                                group_id = db_group['id']
                                break
                        
                        if group_id:
                            # Store messages with account tracking
                            for msg in messages:
                                msg_data = {
                                    'group_id': group_id,
                                    'message_id': msg['message_id'],
                                    'sender_id': msg['sender_id'],
                                    'sender_name': msg['sender_name'],
                                    'message_text': msg['message_text'],
                                    'timestamp': msg['timestamp'],
                                    'is_job_post': False,
                                    'fetched_by_account': account_name
                                }
                                
                                message_id = self.db.insert_message(msg_data)
                                
                                # Process through ML pipeline
                                if msg['message_text']:
                                    self.ml_pipeline.process_message(message_id, msg['message_text'])
                            
                            # Update group message count
                            self.db.update_group_message_count(group_id, len(messages))
                            logging.info(f"Account {account_name} processed {len(messages)} messages from {group['name']}")
                    
                    # Rate limiting
                    await asyncio.sleep(config.CRAWL_DELAY)
                    
                except Exception as e:
                    logging.error(f"Error processing group {group['name']} for account {account_name}: {e}")
                    continue
    
    async def _score_and_filter_groups(self):
        """Score groups and identify high-quality sources"""
        logging.info("Scoring and filtering groups...")
        groups = self.db.get_programming_groups()
        
        for group in groups:
            # Get messages for this group
            messages = self.db.get_messages(group['id'], limit=100)
            
            if len(messages) >= config.MIN_MESSAGES_FOR_SCORING:
                # Calculate group credibility based on message quality
                total_score = 0
                job_messages = 0
                
                for msg in messages:
                    # Get job score for this message
                    if any(keyword in msg['message_text'].lower() for keyword in ['hiring', 'job', 'position']):
                        job_messages += 1
                        total_score += 5  # Basic score for job-related messages
                
                # Calculate credibility score
                if job_messages > 0:
                    credibility_score = min(10.0, (total_score / len(messages)) + (job_messages / len(messages) * 3))
                else:
                    credibility_score = 0.0
                
                # Update group credibility
                self.db.update_group_credibility(group['id'], credibility_score)
                logging.info(f"Group {group['group_name']} (joined by {group.get('joined_by_account', 'unknown')}) scored: {credibility_score:.2f}")
    
    async def _continuous_crawling(self):
        """Continuous crawling of high-score groups"""
        logging.info("Starting continuous crawling...")
        
        while self.is_running:
            try:
                # Get high-score groups
                high_score_groups = self.db.get_high_score_groups(threshold=config.JOB_SCORE_THRESHOLD)
                
                for group in high_score_groups:
                    if not self.is_running:
                        break
                    
                    # Get the account that joined this group
                    joined_by_account = group.get('joined_by_account')
                    if not joined_by_account:
                        continue
                    
                    # Fetch new messages using the same account
                    messages = await self.telegram_manager.fetch_messages_for_group(
                        group['group_link'], 
                        limit=50,  # Fetch fewer messages for continuous crawling
                        account_name=joined_by_account
                    )
                    
                    # Process new messages
                    for msg in messages:
                        # Check if message already exists
                        existing_messages = self.db.get_messages(group['id'], limit=1000)
                        if not any(existing['message_id'] == msg['message_id'] for existing in existing_messages):
                            # Store new message
                            msg_data = {
                                'group_id': group['id'],
                                'message_id': msg['message_id'],
                                'sender_id': msg['sender_id'],
                                'sender_name': msg['sender_name'],
                                'message_text': msg['message_text'],
                                'timestamp': msg['timestamp'],
                                'is_job_post': False,
                                'fetched_by_account': joined_by_account
                            }
                            
                            message_id = self.db.insert_message(msg_data)
                            
                            # Process through ML pipeline
                            if msg['message_text']:
                                self.ml_pipeline.process_message(message_id, msg['message_text'])
                    
                    # Rate limiting
                    await asyncio.sleep(config.CRAWL_DELAY)
                
                # Wait before next cycle
                await asyncio.sleep(300)  # 5 minutes
                
            except Exception as e:
                logging.error(f"Continuous crawling error: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def stop_crawling(self):
        """Stop the crawler"""
        self.is_running = False
        logging.info("Stopping crawler...")
    
    async def get_fresher_friendly_jobs(self) -> List[Dict[str, Any]]:
        """Get fresher-friendly jobs for email notifications"""
        return self.db.get_fresher_friendly_jobs(limit=50)
    
    async def get_account_group_report(self) -> Dict[str, Any]:
        """Get detailed report of which account joined which groups"""
        return {
            "universal_summary": self.universal_group_manager.get_all_accounts_groups_summary(),
            "database_summary": self.db.get_account_group_summary(),
            "unique_groups_per_account": self.db.get_unique_groups_per_account()
        } 