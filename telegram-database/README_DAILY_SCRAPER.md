# Daily Job Scraper - Complete Workflow Implementation

## Overview

The `daily_job_scraper.py` implements your exact requirements:
- **Join 10 groups per account daily** (40 total groups with 4 accounts)
- **Fetch 100 messages** from each joined group
- **Analyze for job content** (10+ job messages per 100 messages)
- **Export high-value channels** to CSV file
- **Store all messages** in the database

## 🎯 Daily Workflow

### Step 1: Group Joining (40 Groups Total)
- **Account 1**: Joins 10 groups
- **Account 2**: Joins 10 groups  
- **Account 3**: Joins 10 groups
- **Account 4**: Joins 10 groups
- **Total**: 40 groups joined daily

### Step 2: Message Fetching
- Fetches **100 recent messages** from each joined group
- **Total**: Up to 4,000 messages fetched daily

### Step 3: Job Analysis
- Analyzes each message for job content using keyword matching
- Identifies channels with **10+ job messages** out of 100
- Calculates job percentage for each channel

### Step 4: Data Storage
- **All 100 messages** stored in database for each group
- Job classification and confidence scores saved
- Account attribution tracked

### Step 5: High-Value Channel Export
- Channels with 10+ job messages exported to CSV
- File format: `high_value_job_channels_YYYYMMDD_HHMMSS.csv`

## 🚀 Usage

### Run Daily Scraper
```bash
# Run the complete daily workflow
python3 daily_job_scraper.py
```

### Test Components (Safe)
```bash
# Test job detection and database without joining groups
python3 test_daily_scraper.py
```

## 📊 Output Files

### 1. CSV Export: `high_value_job_channels_YYYYMMDD_HHMMSS.csv`
```csv
group_id,group_name,group_link,job_messages,total_messages,job_percentage,joined_by_account,analysis_timestamp
1,"Programming Jobs India","https://t.me/programmingjobsindia",15,100,15.0,"Account 1","2024-09-07T15:30:45"
2,"Tech Jobs Bangalore","https://t.me/techjobsbangalore",12,100,12.0,"Account 2","2024-09-07T15:32:10"
```

### 2. Console Output
```
================================================================================
📊 DAILY JOB SCRAPING SUMMARY
================================================================================
📅 Date: 2024-09-07 15:30:45
🔗 Total Groups Joined: 40
✅ High-Value Groups: 8
📨 Total Messages Fetched: 3,850
💼 Total Job Messages: 487
📈 Overall Job Percentage: 12.6%

🏆 HIGH-VALUE CHANNELS (10+ job messages):
================================================================================
 1. 📢 Programming Jobs India
    🔗 https://t.me/programmingjobsindia
    💼 15/100 (15.0%)
    👤 Joined by: Account 1

 2. 📢 Tech Jobs Bangalore
    🔗 https://t.me/techjobsbangalore
    💼 12/100 (12.0%)
    👤 Joined by: Account 2
```

### 3. Database Storage
All messages stored in `messages` table with:
- `group_id`: Reference to the group
- `message_text`: Full message content
- `is_job_post`: Boolean (True/False)
- `job_score`: Confidence score (0.0-1.0)
- `fetched_by_account`: Which account fetched it
- `timestamp`: When message was posted

## 🔧 Configuration

### Daily Targets (Configurable)
```python
# In daily_job_scraper.py
self.groups_per_account = 10      # Groups per account
self.messages_per_group = 100     # Messages per group
self.min_job_messages = 10       # Minimum job messages for high-value
```

### Job Detection Keywords
The system uses 4 categories of keywords:

1. **Job Indicators** (High Weight): hiring, job, vacancy, position, opening, opportunity, career, employment, work, salary, ctc, lpa, experience, years, apply, resume, cv, interview, recruitment, hr, human resource, join our team, we are hiring, job alert, job opportunity

2. **Roles** (Medium-High Weight): developer, engineer, programmer, coder, architect, analyst, designer, manager, lead, senior, junior, intern, fresher, experienced, fullstack, frontend, backend, devops, qa, tester, product manager, project manager, scrum master, tech lead, team lead

3. **Technologies** (Medium Weight): python, java, javascript, react, angular, vue, node, django, flask, spring, html, css, sql, mongodb, postgresql, mysql, aws, azure, docker, kubernetes, git, linux, android, ios, flutter, react native, machine learning, data science, ai

4. **Locations** (Low Weight): bangalore, mumbai, delhi, hyderabad, pune, chennai, kolkata, gurgaon, noida, remote, work from home, wfh, onsite, hybrid, office, location

## 🛡️ Safety Features

### Rate Limiting
- **2-second delay** between group joins
- **1-second delay** between message fetching
- **Daily limits**: 10 groups per account maximum

### Account Protection
- **Multi-account distribution**: Load spread across 4 accounts
- **Unique assignments**: No duplicate group assignments
- **Human-like behavior**: Realistic joining patterns

### Error Handling
- **Graceful failures**: Continues if one group fails
- **Detailed logging**: All operations logged
- **Database transactions**: Safe data storage

## 📈 Performance Metrics

### Expected Daily Output
- **Groups Joined**: 40 (10 per account)
- **Messages Fetched**: ~4,000 (100 per group)
- **High-Value Channels**: 5-15 (depending on quality)
- **Job Messages**: 400-800 (10-20% of total)

### Processing Time
- **Group Joining**: ~2-3 minutes (with delays)
- **Message Fetching**: ~5-10 minutes
- **Analysis**: ~1-2 minutes
- **Total Time**: ~10-15 minutes

## �� Daily Schedule

### Recommended Schedule
```bash
# Run daily at 9:00 AM
0 9 * * * cd /path/to/telegram-database && python3 daily_job_scraper.py

# Or run manually
python3 daily_job_scraper.py
```

### Monitoring
- Check `logs/daily_job_scraper.log` for detailed logs
- Monitor CSV exports for high-value channels
- Review database for message storage

## 🎯 Success Criteria

A successful daily run should:
1. ✅ Join 40 groups (10 per account)
2. ✅ Fetch 100 messages from each group
3. ✅ Identify 5-15 high-value channels
4. ✅ Export results to CSV
5. ✅ Store all messages in database
6. ✅ Complete within 15 minutes

## 🚨 Troubleshooting

### Common Issues

1. **No groups available**
   - Check `universal_group_manager.py` for group list
   - Ensure groups are marked as available

2. **Database errors**
   - Verify database file exists and is writable
   - Check database schema is up to date

3. **Account authentication issues**
   - Verify session files exist
   - Check API credentials in config

4. **Rate limiting**
   - Increase delays between operations
   - Reduce groups per account if needed

### Debug Mode
```bash
# Run with detailed logging
python3 daily_job_scraper.py 2>&1 | tee daily_scraper.log
```

## 📋 Daily Checklist

- [ ] Run `python3 daily_job_scraper.py`
- [ ] Check console output for summary
- [ ] Verify CSV file created with high-value channels
- [ ] Check database for new messages
- [ ] Review logs for any errors
- [ ] Monitor account health

This system will automatically discover and catalog the most valuable job channels while safely managing multiple Telegram accounts to avoid bans.
