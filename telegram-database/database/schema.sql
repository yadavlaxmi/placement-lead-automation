-- Database schema for Telegram Job Scraping System

-- Cities table to store 200 cities
CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    state TEXT,
    country TEXT DEFAULT 'India',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Programming groups table
CREATE TABLE IF NOT EXISTS programming_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_link TEXT NOT NULL UNIQUE,
    group_id TEXT,
    city_id INTEGER,
    source_type TEXT DEFAULT 'telegram', -- telegram, whatsapp, etc.
    credibility_score REAL DEFAULT 0.0,
    total_messages INTEGER DEFAULT 0,
    last_activity TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    joined_by_account TEXT, -- Track which account joined this group
    joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (city_id) REFERENCES cities (id)
);

-- Account group assignments table to track unique group assignments
CREATE TABLE IF NOT EXISTS account_group_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    assignment_date DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES programming_groups (id),
    UNIQUE(account_name, group_id, assignment_date)
);

-- Messages table to store fetched messages
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER,
    message_id TEXT NOT NULL,
    sender_id TEXT,
    sender_name TEXT,
    message_text TEXT,
    timestamp TIMESTAMP,
    is_job_post BOOLEAN DEFAULT FALSE,
    job_score REAL DEFAULT 0.0,
    fetched_by_account TEXT, -- Track which account fetched this message
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES programming_groups (id)
);

-- Job scores table for ML pipeline results
CREATE TABLE IF NOT EXISTS job_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER,
    salary_score REAL DEFAULT 0.0,
    contact_score REAL DEFAULT 0.0,
    website_score REAL DEFAULT 0.0,
    name_score REAL DEFAULT 0.0,
    skill_score REAL DEFAULT 0.0,
    experience_score REAL DEFAULT 0.0,
    location_score REAL DEFAULT 0.0,
    remote_score REAL DEFAULT 0.0,
    fresher_friendly_score REAL DEFAULT 0.0,
    overall_score REAL DEFAULT 0.0,
    tags TEXT, -- JSON string of tags
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (message_id) REFERENCES messages (id)
);

-- ML pipeline results table
CREATE TABLE IF NOT EXISTS ml_pipeline_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER,
    classification TEXT,
    confidence REAL,
    extracted_data TEXT, -- JSON string of extracted information
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (message_id) REFERENCES messages (id)
);

-- Crawler status table
CREATE TABLE IF NOT EXISTS crawler_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER,
    last_crawled TIMESTAMP,
    status TEXT DEFAULT 'pending', -- pending, active, completed, failed
    messages_fetched INTEGER DEFAULT 0,
    crawled_by_account TEXT, -- Track which account crawled this group
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES programming_groups (id)
);

-- Email notifications table
CREATE TABLE IF NOT EXISTS email_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_email TEXT NOT NULL,
    subject TEXT,
    content TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending' -- pending, sent, failed
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_programming_groups_city ON programming_groups(city_id);
CREATE INDEX IF NOT EXISTS idx_programming_groups_account ON programming_groups(joined_by_account);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id);
CREATE INDEX IF NOT EXISTS idx_messages_account ON messages(fetched_by_account);
CREATE INDEX IF NOT EXISTS idx_job_scores_message ON job_scores(message_id);
CREATE INDEX IF NOT EXISTS idx_crawler_status_group ON crawler_status(group_id); 
CREATE INDEX IF NOT EXISTS idx_account_group_assignments ON account_group_assignments(account_name, assignment_date); 