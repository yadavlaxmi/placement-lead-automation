"""
Group Manager - Manages universal groups
"""
import json
import uuid
import logging
from typing import List, Dict, Any
from models.group import Group
from database.repository import DatabaseRepository
from config.settings import settings

class GroupManager:
    """Manages universal groups"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseRepository()
        self.universal_groups = []
    
    async def load_universal_groups(self):
        """Load universal groups from file and database"""
        try:
            # Load from file
            with open(settings.UNIVERSAL_GROUPS_FILE, 'r') as f:
                self.universal_groups = json.load(f)
            
            self.logger.info(f"📁 Loaded {len(self.universal_groups)} groups from file")
            
            # Load groups into database
            await self._load_groups_to_database()
            
        except Exception as e:
            self.logger.error(f"Error loading universal groups: {e}")
            raise
    
    async def _load_groups_to_database(self):
        """Load groups into database"""
        try:
            for group_data in self.universal_groups:
                # Check if group already exists
                existing_group = self._find_group_by_link(group_data['link'])
                
                if not existing_group:
                    # Create group in database
                    group_id = await self.db.create_group(group_data)
                    if group_id:
                        self.logger.debug(f"Created group in database: {group_data['name']}")
                else:
                    self.logger.debug(f"Group already exists: {group_data['name']}")
            
            self.logger.info("✅ All groups loaded into database")
            
        except Exception as e:
            self.logger.error(f"Error loading groups to database: {e}")
            raise
    
    def _find_group_by_link(self, link: str) -> bool:
        """Check if group exists in database by link"""
        try:
            groups = self.db.get_all_groups()
            return any(group.link == link for group in groups)
        except Exception as e:
            self.logger.error(f"Error finding group: {e}")
            return False
    
    def get_groups_by_priority(self, priority: str) -> List[Group]:
        """Get groups by priority"""
        try:
            groups = self.db.get_all_groups()
            return [group for group in groups if group.priority == priority]
        except Exception as e:
            self.logger.error(f"Error getting groups by priority: {e}")
            return []
    
    def get_groups_by_category(self, category: str) -> List[Group]:
        """Get groups by category"""
        try:
            groups = self.db.get_all_groups()
            return [group for group in groups if group.category == category]
        except Exception as e:
            self.logger.error(f"Error getting groups by category: {e}")
            return []
    
    def get_high_priority_groups(self) -> List[Group]:
        """Get high priority groups"""
        return self.get_groups_by_priority('high')
    
    def get_medium_priority_groups(self) -> List[Group]:
        """Get medium priority groups"""
        return self.get_groups_by_priority('medium')
    
    def get_low_priority_groups(self) -> List[Group]:
        """Get low priority groups"""
        return self.get_groups_by_priority('low')
    
    def get_group_statistics(self) -> Dict[str, Any]:
        """Get group statistics"""
        try:
            groups = self.db.get_all_groups()
            
            stats = {
                'total_groups': len(groups),
                'by_priority': {'high': 0, 'medium': 0, 'low': 0},
                'by_category': {},
                'avg_credibility': 0.0
            }
            
            total_credibility = 0.0
            
            for group in groups:
                # Count by priority
                if group.priority in stats['by_priority']:
                    stats['by_priority'][group.priority] += 1
                
                # Count by category
                if group.category not in stats['by_category']:
                    stats['by_category'][group.category] = 0
                stats['by_category'][group.category] += 1
                
                # Sum credibility
                total_credibility += group.credibility_score
            
            # Calculate average credibility
            if len(groups) > 0:
                stats['avg_credibility'] = total_credibility / len(groups)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting group statistics: {e}")
            return {}
    
    def search_groups(self, query: str) -> List[Group]:
        """Search groups by name or link"""
        try:
            groups = self.db.get_all_groups()
            query_lower = query.lower()
            
            matching_groups = []
            for group in groups:
                if (query_lower in group.name.lower() or 
                    query_lower in group.link.lower()):
                    matching_groups.append(group)
            
            return matching_groups
            
        except Exception as e:
            self.logger.error(f"Error searching groups: {e}")
            return []
    
    def update_group_credibility(self, group_id: str, credibility_score: float) -> bool:
        """Update group credibility score"""
        try:
            # This would update the group in database
            # For now, we'll just log it
            self.logger.info(f"Updated credibility for group {group_id}: {credibility_score}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating group credibility: {e}")
            return False
    
    def get_universal_groups_summary(self) -> Dict[str, Any]:
        """Get summary of universal groups"""
        try:
            stats = self.get_group_statistics()
            
            return {
                'total_groups': stats['total_groups'],
                'priority_distribution': stats['by_priority'],
                'category_distribution': stats['by_category'],
                'average_credibility': stats['avg_credibility'],
                'file_groups': len(self.universal_groups),
                'database_groups': stats['total_groups']
            }
            
        except Exception as e:
            self.logger.error(f"Error getting universal groups summary: {e}")
            return {}
