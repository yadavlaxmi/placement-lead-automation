# Telegram Job Scraper System

A comprehensive system for scraping programming job opportunities from Telegram groups using multiple accounts, AI-powered classification, and automated email notifications.

## 🚀 Features

- **Multi-Account Support**: Uses 4 Telegram accounts for efficient crawling
- **AI-Powered Job Classification**: OpenAI-based message analysis and scoring
- **10-Parameter Job Scoring**: Comprehensive evaluation of job posts
- **City-Wise Group Discovery**: Searches across 200+ Indian cities
- **Automated Email Reports**: Daily job updates to placement team and students
- **SQLite Database**: Efficient data storage and retrieval
- **Rate Limiting**: Respects Telegram's API limits
- **AWS Integration**: S3 backup, CloudWatch monitoring, and health checks
- **Docker Support**: Containerized deployment
- **Health Monitoring**: Automated health checks and metrics

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Search APIs   │    │  Telegram API   │    │   ML Pipeline   │
│  (Exa, Tavily, │    │  (4 Accounts)   │    │   (OpenAI)      │
│    Lookup)      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Group         │    │   Message       │    │   Job Scoring   │
│  Discovery      │    │   Fetching      │    │   & Filtering   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SQLite        │    │   Email         │    │   AWS Services  │
│   Database      │    │   Notifications │    │  (S3, CloudWatch)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📋 Requirements

- Python 3.8+
- Telegram API credentials (4 accounts)
- OpenAI API key
- Search API keys (Exa.ai, Lookup.so, Tavily)
- SMTP email configuration
- AWS credentials (optional, for S3 backup and CloudWatch)

## 🛠️ Installation

### Local Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd telegram-database
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp env_example.txt .env
   # Edit .env with your actual API keys
   ```

4. **Configure Telegram accounts**
   - Edit `main.py` with your 4 Telegram account details
   - Get API credentials from [my.telegram.org](https://my.telegram.org)

### Docker Installation

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up -d
   ```

2. **Or build manually**
   ```bash
   docker build -t telegram-scraper .
   docker run -d --name telegram-scraper telegram-scraper
   ```

## 🚀 AWS Deployment

### Quick AWS Setup

1. **Use the deployment script**
   ```bash
   chmod +x deploy_to_aws.sh
   ./deploy_to_aws.sh
   ```

2. **Manual AWS setup**
   - Follow the detailed guide in `aws_setup_guide.md`
   - Use the provided systemd service configuration
   - Set up monitoring and backup scripts

### AWS Services Used

- **EC2**: Application hosting
- **S3**: Database and log backups
- **CloudWatch**: Monitoring and metrics
- **IAM**: Access management
- **Load Balancer**: Health checks (optional)

### Environment Variables for AWS

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
S3_BUCKET_NAME=your-s3-bucket-name

# AWS Features
ENABLE_S3_BACKUP=true
ENABLE_CLOUDWATCH_LOGGING=true
ENABLE_HEALTH_CHECK=true

# Performance Settings
MAX_CONCURRENT_REQUESTS=10
REQUEST_TIMEOUT=30
RETRY_ATTEMPTS=3
```

## ⚙️ Configuration

### Telegram Accounts Setup

Edit the `ACCOUNTS` list in `main.py`:

```python
ACCOUNTS = [
    {
        'name': 'Account 1',
        'phone': '+919794670665',
        'api_id': 24242582,
        'api_hash': 'd8a500dd4f6956793a0be40ac48c1669',
        'session_name': 'session_account1',
        'groups': ['group1', 'group2']
    },
    # Add more accounts...
]
```

### API Keys

Set these environment variables:

```bash
# Search APIs
EXA_API_KEY=your_exa_key
LOOKUP_API_KEY=your_lookup_key
TAVILY_API_KEY=your_tavily_key

# OpenAI
OPENAI_API_KEY=your_openai_key

# Email
SMTP_USERNAME=your_email
SMTP_PASSWORD=your_app_password
```

## 🚀 Usage

### Test the System

```bash
python main.py test
```

### Run Full System

```bash
python main.py
```

### Health Check

```bash
python main.py health
```

### Run Backup

```bash
python main.py backup
```

### Health Check Server

```bash
python health_check.py
```

### Stop the System

Press `Ctrl+C` to gracefully stop the crawler.

## 📊 Database Schema

The system uses SQLite with the following tables:

- **cities**: 200+ Indian cities
- **programming_groups**: Discovered Telegram groups
- **messages**: Fetched messages from groups
- **job_scores**: ML pipeline scoring results
- **ml_pipeline_results**: AI classification results
- **crawler_status**: Crawling progress tracking
- **email_notifications**: Email delivery status

## 🔍 Job Scoring Parameters

Each job message is scored on 10 parameters (0-10 scale):

1. **Salary Score**: Salary information completeness
2. **Contact Score**: Contact details availability
3. **Website Score**: Application website/link
4. **Name Score**: Company/job title clarity
5. **Skill Score**: Required skills specification
6. **Experience Score**: Experience requirements
7. **Location Score**: Location information
8. **Remote Score**: Remote work availability
9. **Fresher Friendly Score**: Entry-level suitability
10. **Overall Score**: Composite score

## 📧 Email Notifications

### Daily Reports
- Sent at 9:00 AM and 6:00 PM
- Contains top fresher-friendly jobs
- Includes job scores and group information

### Urgent Alerts
- Sent for high-quality job posts (score ≥ 8.0)
- Immediate notification to placement team

## 🔄 Crawling Process

1. **Discovery Phase**: Search APIs find programming groups
2. **Joining Phase**: Multiple accounts join groups
3. **Initial Fetch**: Get 200 messages per group
4. **ML Processing**: AI classification and scoring
5. **Group Scoring**: Evaluate group credibility
6. **Continuous Crawling**: Monitor high-score groups
7. **Email Reports**: Daily job summaries

## 📈 Performance Features

- **Rate Limiting**: Respects API limits
- **Account Rotation**: Distributes load across accounts
- **Efficient Storage**: SQLite with proper indexing
- **Error Handling**: Graceful failure recovery
- **Logging**: Comprehensive activity tracking
- **AWS Integration**: S3 backup and CloudWatch monitoring
- **Health Checks**: Automated system monitoring

## 🚨 Important Notes

- **API Limits**: Respect search API rate limits
- **Telegram Terms**: Follow Telegram's terms of service
- **Data Privacy**: Handle user data responsibly
- **Account Security**: Keep API credentials secure
- **AWS Costs**: Monitor S3 and CloudWatch usage

## 🐛 Troubleshooting

### Common Issues

1. **Telegram Connection Failed**
   - Verify API credentials
   - Check phone number format
   - Ensure account is not banned

2. **Search API Errors**
   - Verify API keys
   - Check rate limits
   - Ensure sufficient credits

3. **Database Errors**
   - Check file permissions
   - Verify SQLite installation
   - Check disk space

4. **AWS Integration Issues**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Ensure CloudWatch access

### Logs

Check `logs/telegram_scraper.log` for detailed error information.

### Health Checks

```bash
# Check system health
python main.py health

# View health endpoint
curl http://localhost:8080/health

# View detailed status
curl http://localhost:8080/status
```

## 📝 License

This project is for educational and institutional use. Please respect API terms of service and data privacy regulations.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📞 Support

For issues and questions:
- Check the logs
- Review configuration
- Ensure all dependencies are installed
- Verify API credentials
- Check AWS setup guide

---

**⚠️ Disclaimer**: This tool is for legitimate job discovery purposes. Users are responsible for complying with all applicable laws and terms of service. 