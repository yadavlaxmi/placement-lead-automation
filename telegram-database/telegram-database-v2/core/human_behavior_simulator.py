"""
Human Behavior Simulator - Makes the system behave like humans
"""
import random
import logging
from typing import List, Dict, Any
from models.account import Account, AccountState
from models.group import Group
from config.settings import settings

class HumanBehaviorSimulator:
    """Simulates human-like behavior for group joining/leaving"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.join_probability = settings.JOIN_PROBABILITY
        self.leave_probability = settings.LEAVE_PROBABILITY
        self.priority_weights = settings.PRIORITY_WEIGHTS
        self.daily_join_limit = settings.DAILY_JOIN_LIMIT
        self.daily_leave_limit = settings.DAILY_LEAVE_LIMIT
    
    def should_join_group(self, account: Account, group: Group, account_state: AccountState) -> bool:
        """Simulate human decision to join group"""
        try:
            # Check daily limits
            if len(account_state.active_groups) >= settings.MAX_GROUPS_PER_ACCOUNT:
                self.logger.info(f"Account {account.name} has reached max groups limit")
                return False
            
            # Check if already joined today (simulate daily limits)
            # This would be tracked in a daily activity table in real implementation
            
            # Higher priority groups more likely to join
            base_probability = self.priority_weights.get(group.priority, 0.5)
            
            # Adjust probability based on current group count
            group_count_factor = 1.0 - (len(account_state.active_groups) / settings.MAX_GROUPS_PER_ACCOUNT) * 0.3
            
            # Final probability
            final_probability = base_probability * self.join_probability * group_count_factor
            
            # Add some randomness
            random_factor = random.uniform(0.8, 1.2)
            final_probability *= random_factor
            
            decision = random.random() < final_probability
            
            self.logger.info(f"Account {account.name} decision to join {group.name}: {decision} "
                           f"(probability: {final_probability:.2f})")
            
            return decision
            
        except Exception as e:
            self.logger.error(f"Error in should_join_group: {e}")
            return False
    
    def simulate_leave_decision(self, account: Account, account_state: AccountState) -> List[str]:
        """Simulate human decision to leave groups"""
        try:
            groups_to_leave = []
            
            # Only consider leaving if we have enough groups
            if len(account_state.active_groups) <= 2:
                return groups_to_leave
            
            # Randomly decide to leave some groups
            for group_id in account_state.active_groups:
                if random.random() < self.leave_probability:
                    groups_to_leave.append(group_id)
                    
                    # Limit daily leaves
                    if len(groups_to_leave) >= self.daily_leave_limit:
                        break
            
            if groups_to_leave:
                self.logger.info(f"Account {account.name} decided to leave {len(groups_to_leave)} groups")
            
            return groups_to_leave
            
        except Exception as e:
            self.logger.error(f"Error in simulate_leave_decision: {e}")
            return []
    
    def select_groups_to_join(self, account: Account, available_groups: List[Group], 
                            account_state: AccountState) -> List[Group]:
        """Select which groups to join based on human behavior"""
        try:
            selected_groups = []
            
            # Sort groups by priority and credibility
            sorted_groups = sorted(available_groups, 
                                key=lambda g: (
                                    self.priority_weights.get(g.priority, 0.5),
                                    g.credibility_score
                                ), reverse=True)
            
            # Select groups based on human behavior
            for group in sorted_groups:
                if len(selected_groups) >= self.daily_join_limit:
                    break
                
                if self.should_join_group(account, group, account_state):
                    selected_groups.append(group)
            
            return selected_groups
            
        except Exception as e:
            self.logger.error(f"Error in select_groups_to_join: {e}")
            return []
    
    def simulate_activity_pattern(self, account: Account) -> Dict[str, Any]:
        """Simulate human activity patterns"""
        try:
            # Simulate different activity levels
            activity_levels = ['high', 'medium', 'low']
            weights = [0.2, 0.5, 0.3]  # Most accounts are medium activity
            
            activity_level = random.choices(activity_levels, weights=weights)[0]
            
            # Activity affects join/leave probabilities
            if activity_level == 'high':
                join_multiplier = 1.2
                leave_multiplier = 1.1
            elif activity_level == 'medium':
                join_multiplier = 1.0
                leave_multiplier = 1.0
            else:  # low
                join_multiplier = 0.8
                leave_multiplier = 0.9
            
            return {
                'activity_level': activity_level,
                'join_multiplier': join_multiplier,
                'leave_multiplier': leave_multiplier,
                'preferred_group_size': random.randint(3, 8),
                'join_frequency': random.uniform(0.3, 0.8)
            }
            
        except Exception as e:
            self.logger.error(f"Error in simulate_activity_pattern: {e}")
            return {
                'activity_level': 'medium',
                'join_multiplier': 1.0,
                'leave_multiplier': 1.0,
                'preferred_group_size': 5,
                'join_frequency': 0.5
            }
    
    def get_human_behavior_summary(self) -> Dict[str, Any]:
        """Get summary of human behavior simulation settings"""
        return {
            'join_probability': self.join_probability,
            'leave_probability': self.leave_probability,
            'priority_weights': self.priority_weights,
            'daily_join_limit': self.daily_join_limit,
            'daily_leave_limit': self.daily_leave_limit,
            'max_groups_per_account': settings.MAX_GROUPS_PER_ACCOUNT
        }
