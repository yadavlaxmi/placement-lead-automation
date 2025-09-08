"""
Persistent Assignment Engine - Core of the system
This ensures groups are permanently assigned to accounts
"""
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from models.account import Account
from models.group import Group
from models.assignment import Assignment, AssignmentHistory
from database.repository import DatabaseRepository

class PersistentAssignmentEngine:
    """Engine for managing persistent group assignments"""
    
    def __init__(self):
        self.db = DatabaseRepository()
        self.logger = logging.getLogger(__name__)
    
    def assign_group_to_account(self, account_id: str, group_id: str) -> bool:
        """Permanently assign group to account"""
        try:
            # Check if assignment already exists
            existing = self.db.get_assignment(account_id, group_id)
            if existing:
                self.logger.warning(f"Assignment already exists: {account_id} -> {group_id}")
                return True
            
            # Create new assignment
            assignment = Assignment(
                id=str(uuid.uuid4()),
                account_id=account_id,
                group_id=group_id,
                assigned_at=datetime.now(),
                status='active'
            )
            
            # Save to database
            success = self.db.create_assignment(assignment)
            
            if success:
                # Log assignment history
                self.log_assignment_action(account_id, group_id, 'joined', 'Initial assignment')
                self.logger.info(f"Successfully assigned group {group_id} to account {account_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error assigning group {group_id} to account {account_id}: {e}")
            return False
    
    def unassign_group_from_account(self, account_id: str, group_id: str, reason: str = "Manual unassignment") -> bool:
        """Remove assignment between account and group"""
        try:
            # Update assignment status
            success = self.db.update_assignment_status(account_id, group_id, 'left')
            
            if success:
                # Log assignment history
                self.log_assignment_action(account_id, group_id, 'left', reason)
                self.logger.info(f"Successfully unassigned group {group_id} from account {account_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error unassigning group {group_id} from account {account_id}: {e}")
            return False
    
    def get_account_groups(self, account_id: str) -> List[Group]:
        """Get all groups assigned to account"""
        try:
            assignments = self.db.get_active_assignments_by_account(account_id)
            groups = []
            
            for assignment in assignments:
                group = self.db.get_group_by_id(assignment.group_id)
                if group:
                    groups.append(group)
            
            return groups
            
        except Exception as e:
            self.logger.error(f"Error getting groups for account {account_id}: {e}")
            return []
    
    def get_group_account(self, group_id: str) -> Optional[Account]:
        """Get account assigned to group"""
        try:
            assignment = self.db.get_active_assignment_by_group(group_id)
            if assignment:
                return self.db.get_account_by_id(assignment.account_id)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting account for group {group_id}: {e}")
            return None
    
    def is_group_available(self, group_id: str) -> bool:
        """Check if group is available for assignment"""
        try:
            assignment = self.db.get_active_assignment_by_group(group_id)
            return assignment is None
            
        except Exception as e:
            self.logger.error(f"Error checking group availability {group_id}: {e}")
            return False
    
    def get_available_groups(self) -> List[Group]:
        """Get all groups that are not assigned to any account"""
        try:
            all_groups = self.db.get_all_groups()
            assigned_group_ids = self.db.get_all_assigned_group_ids()
            
            available_groups = [
                group for group in all_groups 
                if group.id not in assigned_group_ids
            ]
            
            return available_groups
            
        except Exception as e:
            self.logger.error(f"Error getting available groups: {e}")
            return []
    
    def get_assignment_summary(self) -> Dict[str, Any]:
        """Get summary of all assignments"""
        try:
            total_assignments = self.db.count_active_assignments()
            total_groups = self.db.count_groups()
            total_accounts = self.db.count_accounts()
            
            account_stats = {}
            for account in self.db.get_all_accounts():
                groups = self.get_account_groups(account.id)
                account_stats[account.name] = {
                    'account_id': account.id,
                    'total_groups': len(groups),
                    'groups': [group.name for group in groups]
                }
            
            return {
                'total_assignments': total_assignments,
                'total_groups': total_groups,
                'total_accounts': total_accounts,
                'account_stats': account_stats,
                'available_groups': len(self.get_available_groups())
            }
            
        except Exception as e:
            self.logger.error(f"Error getting assignment summary: {e}")
            return {}
    
    def log_assignment_action(self, account_id: str, group_id: str, action: str, reason: str = None):
        """Log assignment action to history"""
        try:
            history = AssignmentHistory(
                id=str(uuid.uuid4()),
                account_id=account_id,
                group_id=group_id,
                action=action,
                timestamp=datetime.now(),
                reason=reason
            )
            
            self.db.create_assignment_history(history)
            
        except Exception as e:
            self.logger.error(f"Error logging assignment action: {e}")
