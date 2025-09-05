import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
from database.database import DatabaseManager
import config

class UniversalGroupManager:
    def __init__(self):
        self.db = DatabaseManager()
        self.universal_groups_file = "data/universal_groups.json"
        self.daily_join_limit = 10  # Each account joins 10 groups per day
        self.accounts_daily_joins = {}  # Track daily joins per account
        self.assigned_groups = {}  # Track which groups are assigned to which accounts
        
    def load_universal_groups(self) -> List[Dict[str, Any]]:
        """Load universal groups from file"""
        try:
            with open(self.universal_groups_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Initialize with default groups
            default_groups = self._get_default_groups()
            self.save_universal_groups(default_groups)
            return default_groups
    
    def save_universal_groups(self, groups: List[Dict[str, Any]]):
        """Save universal groups to file"""
        os.makedirs('data', exist_ok=True)
        with open(self.universal_groups_file, 'w') as f:
            json.dump(groups, f, indent=2)
    
    def _get_default_groups(self) -> List[Dict[str, Any]]:
        """Get default universal groups"""
        return [
            # Programming Jobs
            {"name": "TechJobsOccean", "link": "https://t.me/TechJobsOccean", "category": "programming", "priority": "high"},
            {"name": "fullremoteit", "link": "https://t.me/fullremoteit", "category": "programming", "priority": "high"},
            {"name": "Tech_Jobs_Ind", "link": "https://t.me/Tech_Jobs_Ind", "category": "programming", "priority": "high"},
            {"name": "linkedinremotejobs", "link": "https://t.me/linkedinremotejobs", "category": "programming", "priority": "high"},
            {"name": "devitjobs", "link": "https://t.me/devitjobs", "category": "programming", "priority": "high"},
            {"name": "nqeeddev", "link": "https://t.me/nqeeddev", "category": "programming", "priority": "high"},
            {"name": "it_outsource", "link": "https://t.me/it_outsource", "category": "programming", "priority": "high"},
            {"name": "dot_aware", "link": "https://t.me/dot_aware", "category": "programming", "priority": "high"},
            {"name": "HiTech_Jobs_In_Israel", "link": "https://t.me/HiTech_Jobs_In_Israel", "category": "programming", "priority": "high"},
            {"name": "hightechforolims", "link": "https://t.me/hightechforolims", "category": "programming", "priority": "high"},
            {"name": "viet688_highpaid_IT_jobs", "link": "https://t.me/viet688_highpaid_IT_jobs", "category": "programming", "priority": "high"},
            {"name": "rust_community", "link": "https://t.me/rust_community", "category": "programming", "priority": "medium"},
            {"name": "STEMJobsLATAM", "link": "https://t.me/STEMJobsLATAM", "category": "programming", "priority": "medium"},
            {"name": "josad_software", "link": "https://t.me/josad_software", "category": "programming", "priority": "medium"},
            {"name": "forchiefs", "link": "https://t.me/forchiefs", "category": "programming", "priority": "medium"},
            {"name": "alljvmjobs", "link": "https://t.me/alljvmjobs", "category": "programming", "priority": "medium"},
            {"name": "talentboxjob", "link": "https://t.me/talentboxjob", "category": "programming", "priority": "medium"},
            {"name": "remotegeekjob", "link": "https://t.me/remotegeekjob", "category": "programming", "priority": "medium"},
            {"name": "forpython", "link": "https://t.me/forpython", "category": "programming", "priority": "high"},
            {"name": "techjobsworld", "link": "https://t.me/techjobsworld", "category": "programming", "priority": "medium"},
            {"name": "frelapayfreelancers", "link": "https://t.me/frelapayfreelancers", "category": "freelance", "priority": "medium"},
            {"name": "forcsharp", "link": "https://t.me/forcsharp", "category": "programming", "priority": "medium"},
            {"name": "remotejobscol", "link": "https://t.me/remotejobscol", "category": "remote", "priority": "high"},
            {"name": "IT_jobs_coop", "link": "https://t.me/IT_jobs_coop", "category": "programming", "priority": "medium"},
            {"name": "forgoandrust", "link": "https://t.me/forgoandrust", "category": "programming", "priority": "medium"},
            {"name": "shoghol_jobs_lb", "link": "https://t.me/shoghol_jobs_lb", "category": "programming", "priority": "low"},
            {"name": "symfony_careers", "link": "https://t.me/symfony_careers", "category": "programming", "priority": "medium"},
            {"name": "forfrontend", "link": "https://t.me/forfrontend", "category": "frontend", "priority": "high"},
            {"name": "JOBITT_Ukraine", "link": "https://t.me/JOBITT_Ukraine", "category": "programming", "priority": "medium"},
            {"name": "MegaManEgypt", "link": "https://t.me/MegaManEgypt", "category": "programming", "priority": "low"},
            {"name": "CloudLinux_Careers", "link": "https://t.me/CloudLinux_Careers", "category": "programming", "priority": "medium"},
            {"name": "futurehelp", "link": "https://t.me/futurehelp", "category": "general", "priority": "low"},
            {"name": "HireRiteServices", "link": "https://t.me/HireRiteServices", "category": "services", "priority": "medium"},
            {"name": "placementducatindia", "link": "https://t.me/placementducatindia", "category": "placement", "priority": "high"},
            {"name": "opusjobs", "link": "https://t.me/opusjobs", "category": "general", "priority": "medium"},
            {"name": "Step_Up_Agency", "link": "https://t.me/Step_Up_Agency", "category": "agency", "priority": "medium"},
            {"name": "embeddedduniya", "link": "https://t.me/embeddedduniya", "category": "embedded", "priority": "medium"},
            {"name": "sysadmin_jobs", "link": "https://t.me/sysadmin_jobs", "category": "sysadmin", "priority": "medium"},
            {"name": "ITInfraJobsBharat", "link": "https://t.me/ITInfraJobsBharat", "category": "infrastructure", "priority": "medium"},
            {"name": "nwopenings", "link": "https://t.me/nwopenings", "category": "general", "priority": "medium"},
            {"name": "placementsbox", "link": "https://t.me/placementsbox", "category": "placement", "priority": "high"},
            {"name": "Networkat20", "link": "https://t.me/Networkat20", "category": "networking", "priority": "medium"},
            {"name": "embeddedshiksha", "link": "https://t.me/embeddedshiksha", "category": "embedded", "priority": "medium"},
            {"name": "CyberSecurityJobs", "link": "https://t.me/CyberSecurityJobs", "category": "cybersecurity", "priority": "medium"},
            {"name": "datascience_courses", "link": "https://t.me/datascience_courses", "category": "datascience", "priority": "medium"},
            {"name": "daily_jobupdates", "link": "https://t.me/daily_jobupdates", "category": "general", "priority": "high"},
            {"name": "relocateme", "link": "https://t.me/relocateme", "category": "relocation", "priority": "medium"},
            {"name": "fresherearth", "link": "https://t.me/fresherearth", "category": "fresher", "priority": "high"},
            {"name": "EmpoweringIN", "link": "https://t.me/EmpoweringIN", "category": "general", "priority": "medium"},
            {"name": "nakcepatdapatkerja", "link": "https://t.me/nakcepatdapatkerja", "category": "general", "priority": "low"},
            {"name": "coachteddydiego", "link": "https://t.me/coachteddydiego", "category": "coaching", "priority": "low"},
            {"name": "Chennai_Jobs", "link": "https://t.me/Chennai_Jobs", "category": "location", "priority": "medium"},
            {"name": "tekpillar", "link": "https://t.me/tekpillar", "category": "general", "priority": "medium"},
            {"name": "vlsi_jobs", "link": "https://t.me/vlsi_jobs", "category": "vlsi", "priority": "medium"},
            {"name": "DevOpsBuddys", "link": "https://t.me/DevOpsBuddys", "category": "devops", "priority": "high"},
            {"name": "UgochiObi3", "link": "https://t.me/UgochiObi3", "category": "general", "priority": "low"},
            {"name": "networkfirewallsecurity", "link": "https://t.me/networkfirewallsecurity", "category": "security", "priority": "medium"},
            {"name": "jobsmukyambigilu", "link": "https://t.me/jobsmukyambigilu", "category": "general", "priority": "low"},
            {"name": "jrpindia01", "link": "https://t.me/jrpindia01", "category": "placement", "priority": "medium"},
            {"name": "ethansalumini", "link": "https://t.me/ethansalumini", "category": "general", "priority": "low"},
            {"name": "itasplacementassistance", "link": "https://t.me/itasplacementassistance", "category": "placement", "priority": "medium"},
            {"name": "r0_jobs", "link": "https://t.me/r0_jobs", "category": "general", "priority": "low"},
            {"name": "forcpp", "link": "https://t.me/forcpp", "category": "programming", "priority": "medium"},
            {"name": "TechUprise_Updates", "link": "https://t.me/TechUprise_Updates", "category": "updates", "priority": "medium"},
            {"name": "jobhunters_channel", "link": "https://t.me/jobhunters_channel", "category": "general", "priority": "medium"},
            {"name": "forallqa", "link": "https://t.me/forallqa", "category": "qa", "priority": "medium"},
            {"name": "krdjobs", "link": "https://t.me/krdjobs", "category": "general", "priority": "low"},
            {"name": "zohointerviewpreparation", "link": "https://t.me/zohointerviewpreparation", "category": "interview", "priority": "medium"},
            {"name": "gulfcareerschannel", "link": "https://t.me/gulfcareerschannel", "category": "gulf", "priority": "medium"},
            {"name": "jobsinsoftwaretesting", "link": "https://t.me/jobsinsoftwaretesting", "category": "testing", "priority": "medium"},
            {"name": "Engineering_Jobs19", "link": "https://t.me/Engineering_Jobs19", "category": "engineering", "priority": "medium"},
            {"name": "CE_Job", "link": "https://t.me/CE_Job", "category": "engineering", "priority": "medium"},
            {"name": "jrpindia02", "link": "https://t.me/jrpindia02", "category": "placement", "priority": "medium"},
            {"name": "remotejobsenglish", "link": "https://t.me/remotejobsenglish", "category": "remote", "priority": "high"},
            {"name": "Remote_Software_Developer_Jobs", "link": "https://t.me/Remote_Software_Developer_Jobs", "category": "remote", "priority": "high"},
            {"name": "remotedatajobsenglish", "link": "https://t.me/remotedatajobsenglish", "category": "remote", "priority": "high"},
            {"name": "RemotiveJobs_QA", "link": "https://t.me/RemotiveJobs_QA", "category": "remote", "priority": "medium"},
            {"name": "remotedevopsandsysadminjobs", "link": "https://t.me/remotedevopsandsysadminjobs", "category": "remote", "priority": "medium"},
            {"name": "RemotiveJobs_CustomerService", "link": "https://t.me/RemotiveJobs_CustomerService", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_Design", "link": "https://t.me/RemotiveJobs_Design", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_Marketing", "link": "https://t.me/RemotiveJobs_Marketing", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_Sales", "link": "https://t.me/RemotiveJobs_Sales", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_Business", "link": "https://t.me/RemotiveJobs_Business", "category": "remote", "priority": "low"},
            {"name": "remotelegalfinancejobs", "link": "https://t.me/remotelegalfinancejobs", "category": "remote", "priority": "low"},
            {"name": "remoteHRjobs", "link": "https://t.me/remoteHRjobs", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_Writing", "link": "https://t.me/RemotiveJobs_Writing", "category": "remote", "priority": "low"},
            {"name": "RemotiveJobs_All_Others", "link": "https://t.me/RemotiveJobs_All_Others", "category": "remote", "priority": "low"},
            {"name": "rem0te", "link": "https://t.me/rem0te", "category": "remote", "priority": "medium"},
            {"name": "jobs_us_uk", "link": "https://t.me/jobs_us_uk", "category": "international", "priority": "medium"},
            {"name": "sapremotejobs", "link": "https://t.me/sapremotejobs", "category": "sap", "priority": "medium"},
            {"name": "Maroset", "link": "https://t.me/Maroset", "category": "general", "priority": "low"},
            {"name": "remote_jobs_today", "link": "https://t.me/remote_jobs_today", "category": "remote", "priority": "high"},
            {"name": "remotejobss", "link": "https://t.me/remotejobss", "category": "remote", "priority": "medium"},
            {"name": "maxmojobs", "link": "https://t.me/maxmojobs", "category": "general", "priority": "low"},
            {"name": "REMOTESAPJOBS", "link": "https://t.me/REMOTESAPJOBS", "category": "sap", "priority": "medium"},
            {"name": "RemoteJobNg", "link": "https://t.me/RemoteJobNg", "category": "remote", "priority": "medium"},
            {"name": "dataremotejobs", "link": "https://t.me/dataremotejobs", "category": "data", "priority": "medium"},
            {"name": "UpworkJobs123", "link": "https://t.me/UpworkJobs123", "category": "freelance", "priority": "medium"},
            {"name": "thehire", "link": "https://t.me/thehire", "category": "general", "priority": "low"}
        ]
    
    def add_discovered_groups(self, new_groups: List[Dict[str, Any]]):
        """Add new groups discovered by search engines to universal list"""
        current_groups = self.load_universal_groups()
        existing_links = {group['link'] for group in current_groups}
        
        for group in new_groups:
            if group['link'] not in existing_links:
                group['discovered_date'] = datetime.now().isoformat()
                group['source'] = 'search_engine'
                current_groups.append(group)
                logging.info(f"Added new group to universal list: {group['name']}")
        
        self.save_universal_groups(current_groups)
    
    def get_groups_for_account(self, account_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get unique groups for an account to join today"""
        today = datetime.now().date()
        
        # Initialize daily tracking if not exists
        if today not in self.accounts_daily_joins:
            self.accounts_daily_joins[today] = {}
        
        if account_name not in self.accounts_daily_joins[today]:
            self.accounts_daily_joins[today][account_name] = []
        
        # Get already joined groups today
        already_joined = set(self.accounts_daily_joins[today][account_name])
        
        # Get all universal groups
        all_groups = self.load_universal_groups()
        
        # Get groups already assigned to other accounts today
        other_accounts_joined = set()
        for other_account, groups in self.accounts_daily_joins[today].items():
            if other_account != account_name:
                other_accounts_joined.update(groups)
        
        # Filter out already joined groups and groups assigned to other accounts
        available_groups = [
            group for group in all_groups 
            if group['link'] not in already_joined and group['link'] not in other_accounts_joined
        ]
        
        # Sort by priority (high -> medium -> low)
        priority_order = {'high': 3, 'medium': 2, 'low': 1}
        available_groups.sort(key=lambda x: priority_order.get(x.get('priority', 'low'), 1), reverse=True)
        
        # Return limited groups
        selected_groups = available_groups[:limit]
        
        # Mark these groups as joined for today
        for group in selected_groups:
            self.accounts_daily_joins[today][account_name].append(group['link'])
        
        return selected_groups
    
    def get_account_unique_groups(self, account_name: str) -> List[Dict[str, Any]]:
        """Get all unique groups assigned to a specific account"""
        today = datetime.now().date()
        
        if today not in self.accounts_daily_joins:
            return []
        
        if account_name not in self.accounts_daily_joins[today]:
            return []
        
        # Get all universal groups
        all_groups = self.load_universal_groups()
        
        # Filter groups assigned to this account today
        account_groups = []
        for group_link in self.accounts_daily_joins[today][account_name]:
            for group in all_groups:
                if group['link'] == group_link:
                    account_groups.append(group)
                    break
        
        return account_groups
    
    def get_all_accounts_groups_summary(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get summary of all accounts and their assigned groups"""
        today = datetime.now().date()
        
        if today not in self.accounts_daily_joins:
            return {}
        
        summary = {}
        all_groups = self.load_universal_groups()
        
        for account_name, group_links in self.accounts_daily_joins[today].items():
            account_groups = []
            for group_link in group_links:
                for group in all_groups:
                    if group['link'] == group_link:
                        account_groups.append(group)
                        break
            summary[account_name] = account_groups
        
        return summary
    
    def reset_daily_joins(self):
        """Reset daily join tracking (call this daily)"""
        today = datetime.now().date()
        if today in self.accounts_daily_joins:
            del self.accounts_daily_joins[today]
        logging.info("Reset daily join tracking")
    
    def get_account_status(self, account_name: str) -> Dict[str, Any]:
        """Get account's daily join status"""
        today = datetime.now().date()
        
        if today not in self.accounts_daily_joins:
            return {"joined_today": 0, "remaining": 10, "total_available": len(self.load_universal_groups())}
        
        joined_today = len(self.accounts_daily_joins[today].get(account_name, []))
        remaining = max(0, 10 - joined_today)
        
        return {
            "joined_today": joined_today,
            "remaining": remaining,
            "total_available": len(self.load_universal_groups())
        }
    
    def get_universal_stats(self) -> Dict[str, Any]:
        """Get statistics about universal groups"""
        groups = self.load_universal_groups()
        
        categories = {}
        priorities = {}
        
        for group in groups:
            category = group.get('category', 'unknown')
            priority = group.get('priority', 'unknown')
            
            categories[category] = categories.get(category, 0) + 1
            priorities[priority] = priorities.get(priority, 0) + 1
        
        return {
            "total_groups": len(groups),
            "categories": categories,
            "priorities": priorities,
            "last_updated": datetime.now().isoformat()
        } 