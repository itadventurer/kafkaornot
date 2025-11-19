CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY,
    results JSONB DEFAULT '{}'::jsonb,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at);