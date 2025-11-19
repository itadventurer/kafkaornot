import os
import yaml
import uuid
import json
import psycopg2
from psycopg2.extras import Json
from flask import Flask, render_template, request, redirect, url_for, abort
from dotenv import load_dotenv

app = Flask(__name__)

# Load Configuration
def load_questions():
    with open('questions.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_questions()

# Database Connection
def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'kafkaornot'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD')
    )
    return conn

# --- ROUTES ---

@app.route('/')
def index():
    """Landing page. Generates a new session ID."""
    new_session_id = str(uuid.uuid4())
    
    # Initialize DB entry
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (session_id) VALUES (%s)",
        (new_session_id,)
    )
    conn.commit()
    cur.close()
    conn.close()
    
    return render_template(
        'index.html', 
        meta=config['meta'], 
        session_id=new_session_id
    )

@app.route('/q/<node_id>')
def node(node_id):
    """Handles both Questions and Results based on the ID."""
    session_id = request.args.get('session_id')
    
    # 1. Security Check: Does session exist?
    if not session_id:
        return redirect(url_for('index'))

    # 2. Process incoming answer (if any)
    # Format: ?ans=prev_question_id:answer_key
    incoming_answer = request.args.get('ans')
    
    if incoming_answer:
        try:
            prev_q_id, answer_key = incoming_answer.split(':')
            
            # VALIDATION: Ensure this question/answer pair exists in YAML
            if prev_q_id in config['questions'] and answer_key in config['questions'][prev_q_id]['answers']:
                
                # Save to DB
                conn = get_db_connection()
                cur = conn.cursor()
                # Postgres JSONB update to merge new key/value
                cur.execute(
                    "UPDATE sessions SET results = results || %s WHERE session_id = %s",
                    (json.dumps({prev_q_id: answer_key}), session_id)
                )
                conn.commit()
                cur.close()
                conn.close()
            else:
                # Spam/Hacking attempt detected on parameters
                abort(400, "Invalid question or answer key provided.")
                
        except ValueError:
            abort(400, "Malformed answer format.")

    # 3. Determine if node is a Question or a Result
    
    # CASE A: It's a Result
    if node_id in config['results']:
        result_data = config['results'][node_id]
        return render_template(
            'result.html', 
            result=result_data, 
            session_id=session_id
        )
    
    # CASE B: It's a Question
    elif node_id in config['questions']:
        question_data = config['questions'][node_id]
        return render_template(
            'question.html', 
            question=question_data, 
            q_id=node_id, 
            session_id=session_id
        )
    
    # CASE C: 404
    else:
        abort(404)

@app.route('/capture-lead', methods=['POST'])
def capture_lead():
    """Saves email/name from the result page."""
    session_id = request.form.get('session_id')
    name = request.form.get('name')
    email = request.form.get('email')
    
    if session_id and email:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sessions SET name = %s, email = %s WHERE session_id = %s",
            (name, email, session_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        
    return render_template('result.html', 
                           message="Thanks! I'll be in touch.", 
                           hide_form=True,
                           # Re-render result data (quick hack, better to redirect)
                           session_id=session_id)

@app.route('/admin')
def admin():
    """Simple protected stats page."""
    pwd = request.args.get('pwd')
    admin_pwd = os.environ.get('ADMIN_PASSWORD', 'secret123')
    
    if pwd != admin_pwd:
        abort(403)
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get simplistic stats
    cur.execute("SELECT count(*) FROM sessions")
    total_sessions = cur.fetchone()[0]
    
    cur.execute("SELECT count(*) FROM sessions WHERE email IS NOT NULL")
    leads = cur.fetchone()[0]
    
    cur.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 50")
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('admin.html', total=total_sessions, leads=leads, rows=rows)
# Run schema.sql before starting the app, if DB is empty (sessions table does not exist).
def ensure_schema():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'sessions'
        );
    """)
    exists = cur.fetchone()[0]
    cur.close()
    conn.close()
    if not exists:
        with open('schema.sql', 'r') as f:
            sql = f.read()
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
if __name__ == '__main__':
    load_dotenv()
    print(os.environ.get('DB_USER', 'kafkaornot'))
    ensure_schema()
    app.run(debug=True, port=5000)