# Unique Group Assignment System

## Overview

This system ensures that each Telegram account joins unique groups from the universal group list, preventing overlap and maximizing coverage. The system tracks which account joined which group and stores all messages in SQLite database with account tracking.

## Key Features

### 1. Unique Group Assignment
- Each account gets assigned unique groups from the universal list
- No overlap between accounts for the same day
- Priority-based assignment (high → medium → low)
- Daily limit of 10 groups per account

### 2. Account Tracking
- Track which account joined which group
- Track which account fetched which messages
- Database storage with account attribution
- Daily reset of join tracking

### 3. Database Schema Updates

#### New Columns Added:
- `programming_groups.joined_by_account` - Which account joined this group
- `programming_groups.joined_date` - When the group was joined
- `messages.fetched_by_account` - Which account fetched this message
- `crawler_status.crawled_by_account` - Which account crawled this group

#### New Table:
- `account_group_assignments` - Track daily group assignments per account

### 4. Universal Group Manager Enhancements

#### New Methods:
- `get_account_unique_groups()` - Get unique groups for specific account
- `get_all_accounts_groups_summary()` - Get summary of all account assignments
- Enhanced `get_groups_for_account()` - Ensures uniqueness across accounts

### 5. Database Manager Enhancements

#### New Methods:
- `insert_account_group_assignment()` - Track account-group assignments
- `get_groups_by_account()` - Get groups joined by specific account
- `get_account_group_summary()` - Get comprehensive account summary
- `get_unique_groups_per_account()` - Get today's unique assignments
- `get_messages_by_account()` - Get messages fetched by specific account

## Usage

### 1. Run the System
```bash
# Start the full system
python3 main.py

# Run tests
python3 main.py test

# Show account-group report
python3 main.py report
```

### 2. View Account-Group Assignments
```bash
# Show detailed report
python3 show_account_groups.py

# Test unique assignments
python3 test_unique_groups.py
```

### 3. Database Migration
```bash
# Run migration if needed
python3 migrate_database.py
```

## How It Works

### 1. Group Assignment Process
1. System loads universal groups from `data/universal_groups.json`
2. For each account, gets unique groups not assigned to other accounts
3. Sorts by priority (high → medium → low)
4. Assigns up to 10 groups per account per day
5. Tracks assignments in memory and database

### 2. Message Fetching Process
1. Each account fetches messages from its assigned unique groups
2. Messages are stored with `fetched_by_account` tracking
3. ML pipeline processes messages for job scoring
4. Group credibility scores are calculated

### 3. Continuous Crawling
1. High-score groups are continuously monitored
2. Same account that joined the group fetches new messages
3. New messages are processed and stored with account tracking

## Example Output

### Unique Group Assignment Test:
```
🔹 Account 1:
   1. TechJobsOccean (https://t.me/TechJobsOccean)
   2. fullremoteit (https://t.me/fullremoteit)
   3. Tech_Jobs_Ind (https://t.me/Tech_Jobs_Ind)
   4. linkedinremotejobs (https://t.me/linkedinremotejobs)
   5. devitjobs (https://t.me/devitjobs)

🔹 Account 2:
   1. nqeeddev (https://t.me/nqeeddev)
   2. it_outsource (https://t.me/it_outsource)
   3. dot_aware (https://t.me/dot_aware)
   4. HiTech_Jobs_In_Israel (https://t.me/HiTech_Jobs_In_Israel)
   5. hightechforolims (https://t.me/hightechforolims)
```

### Account Status:
```
🔹 Account 1:
   📈 Joined today: 5/10
   ⏳ Remaining: 5
   🌐 Total available: 6572
   📊 Progress: [██████████░░░░░░░░░░] 50.0%
```

## Database Queries

### Get Groups by Account:
```sql
SELECT pg.*, aga.assignment_date
FROM programming_groups pg
JOIN account_group_assignments aga ON pg.id = aga.group_id
WHERE aga.account_name = 'Account 1'
ORDER BY aga.assignment_date DESC
```

### Get Messages by Account:
```sql
SELECT m.*, pg.group_name, pg.group_link
FROM messages m
JOIN programming_groups pg ON m.group_id = pg.id
WHERE m.fetched_by_account = 'Account 1'
ORDER BY m.timestamp DESC
```

### Account Summary:
```sql
SELECT 
    aga.account_name,
    COUNT(DISTINCT aga.group_id) as total_groups,
    COUNT(DISTINCT aga.assignment_date) as active_days,
    MAX(aga.assignment_date) as last_assignment
FROM account_group_assignments aga
GROUP BY aga.account_name
ORDER BY aga.account_name
```

## Benefits

1. **No Overlap**: Each account works on unique groups
2. **Maximum Coverage**: All accounts cover different groups
3. **Account Tracking**: Know which account did what
4. **Scalable**: Easy to add more accounts
5. **Transparent**: Clear reporting of assignments
6. **Efficient**: Prevents duplicate work

## Configuration

### Account Configuration (main.py):
```python
ACCOUNTS = [
    {
        'name': 'Account 1',
        'phone': '+919794670665',
        'api_id': 24242582,
        'api_hash': 'd8a500dd4f6956793a0be40ac48c1669',
        'session_name': 'session_account1'
    },
    # ... more accounts
]
```

### Daily Limits (universal_group_manager.py):
```python
self.daily_join_limit = 10  # Each account joins 10 groups per day
```

## Monitoring

### Log Files:
- `logs/telegram_scraper.log` - Main system logs
- Account-group assignments are logged with details

### Reports:
- `show_account_groups.py` - Comprehensive account-group report
- `test_unique_groups.py` - Test unique assignment functionality

## Future Enhancements

1. **Dynamic Assignment**: Adjust assignments based on group activity
2. **Account Performance**: Track which accounts perform better
3. **Group Rotation**: Rotate groups between accounts over time
4. **Load Balancing**: Distribute high-priority groups evenly
5. **Analytics Dashboard**: Web interface for monitoring

## Troubleshooting

### Common Issues:

1. **Database Migration Errors**: Run `python3 migrate_database.py`
2. **No Unique Groups**: Check universal groups file exists
3. **Account Assignment Issues**: Verify account names match
4. **Message Tracking Issues**: Check database schema

### Debug Commands:
```bash
# Check database schema
sqlite3 telegram_jobs.db ".schema"

# Check account assignments
sqlite3 telegram_jobs.db "SELECT * FROM account_group_assignments;"

# Check group assignments
sqlite3 telegram_jobs.db "SELECT group_name, joined_by_account FROM programming_groups;"
```

## Conclusion

The unique group assignment system ensures efficient distribution of work across multiple Telegram accounts while maintaining complete tracking and transparency. Each account operates on unique groups, maximizing coverage and preventing duplicate efforts. 