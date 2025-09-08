-- Professional Database Schema for Telegram Job Scraper V2

-- Accounts table
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    phone TEXT UNIQUE NOT NULL,
    api_id INTEGER NOT NULL,
    api_hash TEXT NOT NULL,
    session_name TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Groups table
CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    link TEXT UNIQUE NOT NULL,
    category TEXT NOT NULL,
    priority TEXT NOT NULL,
    credibility_score REAL DEFAULT 0.0,
    total_members INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Persistent assignments (KEY FEATURE!)
CREATE TABLE IF NOT EXISTS persistent_assignments (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'active', -- active, inactive, left
    last_message_fetch TIMESTAMP,
    total_messages_fetched INTEGER DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (group_id) REFERENCES groups(id),
    UNIQUE(account_id, group_id)
);

-- Assignment history
CREATE TABLE IF NOT EXISTS assignment_history (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    action TEXT NOT NULL, -- joined, left, reassigned
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

-- Messages table
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    message_text TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_job_message BOOLEAN DEFAULT FALSE,
    job_score REAL DEFAULT 0.0,
    FOREIGN KEY (group_id) REFERENCES groups(id),
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_persistent_assignments_account ON persistent_assignments(account_id);
CREATE INDEX IF NOT EXISTS idx_persistent_assignments_group ON persistent_assignments(group_id);
CREATE INDEX IF NOT EXISTS idx_persistent_assignments_status ON persistent_assignments(status);
CREATE INDEX IF NOT EXISTS idx_assignment_history_account ON assignment_history(account_id);
CREATE INDEX IF NOT EXISTS idx_assignment_history_group ON assignment_history(group_id);
CREATE INDEX IF NOT EXISTS idx_assignment_history_timestamp ON assignment_history(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id);
CREATE INDEX IF NOT EXISTS idx_messages_account ON messages(account_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_groups_priority ON groups(priority);
CREATE INDEX IF NOT EXISTS idx_groups_category ON groups(category);
