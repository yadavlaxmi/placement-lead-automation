import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DATABASE_PATH = "telegram_jobs.db"

# Telegram API configuration
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Search API configurations
EXA_API_KEY = "6B203F0A-59EE-469C-A202-954248166350"
LOOKUP_API_KEY = "80ef8ec5-cf72-440c-8dff-c0437d39c51f"
TAVILY_API_KEY = "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"
OPENAI_API_KEY = None

# Email configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Recipient emails
PLACEMENT_TEAM_EMAIL = os.getenv("PLACEMENT_TEAM_EMAIL", "placement@example.com")
STUDENTS_EMAIL = os.getenv("STUDENTS_EMAIL", "students@example.com")

# Search configuration
CITIES_FILE = "data/cities.txt"
PROGRAMMING_LANGUAGES = [
    "Python", "JavaScript", "Java", "C++", "C#", "PHP", "Ruby", "Go", "Rust", "Swift",
    "Kotlin", "TypeScript", "HTML", "CSS", "React", "Angular", "Vue", "Node.js", "Django", "Flask"
]

SEARCH_KEYWORDS = [
    "programming jobs", "developer jobs", "software engineer", "web developer",
    "frontend developer", "backend developer", "full stack developer", "coding jobs",
    "tech jobs", "IT jobs", "software jobs", "programmer jobs"
]

# ML Pipeline configuration
MIN_MESSAGES_FOR_SCORING = 100
JOB_SCORE_THRESHOLD = 7.0
FRESHER_FRIENDLY_KEYWORDS = ["fresher", "entry level", "junior", "0-1 years", "internship"]

# Crawler configuration
MESSAGES_PER_GROUP = 100  # Changed from 200 to 100 as requested
CRAWL_DELAY = 5  # seconds between requests
MAX_GROUPS_PER_CITY = 50

# File paths
LOGS_DIR = "logs"
DATA_DIR = "data"
MODELS_DIR = "models" 