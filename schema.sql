CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY,
    results JSONB DEFAULT '{}'::jsonb,
    final_result VARCHAR(255),
    name TEXT,
    email TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at);
CREATE INDEX IF NOT EXISTS idx_sessions_final_result ON sessions(final_result);