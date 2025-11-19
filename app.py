import os
import yaml
import uuid
import json
import psycopg2
from psycopg2.extras import Json
from flask import Flask, render_template, request, redirect, url_for, abort, make_response
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
    new_session_id = str(uuid.uuid4())
    stats_display = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Initialize new session
        cur.execute("INSERT INTO sessions (session_id) VALUES (%s)", (new_session_id,))
        
        # 2. Get Statistics for Landing Page
        cur.execute("""
            SELECT final_result, COUNT(*) 
            FROM sessions 
            WHERE final_result IS NOT NULL 
            GROUP BY final_result
        """)
        stats_rows = cur.fetchall()
        
        conn.commit()
        cur.close()
        conn.close()

        # 3. Calculate Percentages
        total_votes = sum(row[1] for row in stats_rows)
        if total_votes > 0:
            for result_key, count in stats_rows:
                if result_key in config.get('results', {}):
                    # Clean up title (remove emojis if desired, or keep them)
                    title = config['results'][result_key]['title']
                    percentage = int((count / total_votes) * 100)
                    stats_display.append({'title': title, 'percent': percentage, 'key': result_key})
            
            # Sort by popularity
            stats_display.sort(key=lambda x: x['percent'], reverse=True)
            
    except Exception as e:
        print(f"DB Error: {e}")
        pass
    
    response = make_response(render_template(
        'index.html', 
        meta=config.get('meta', {}),
        stats=stats_display
    ))
    response.set_cookie('session_id', new_session_id)
    return response 

@app.route('/q/<node_id>')
def node(node_id):
    """Handles both Questions and Results based on the ID."""
    session_id = request.cookies.get('session_id')
    
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
        # Save the final result to DB
        if session_id:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE sessions SET final_result = %s WHERE session_id = %s",
                (node_id, session_id)
            )
            conn.commit()
            cur.close()
            conn.close()

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
    session_id = request.cookies.get('session_id')
    name = request.form.get('name')
    email = request.form.get('email')
    
    conn = get_db_connection()
    cur = conn.cursor()
    if session_id and email:
        cur.execute(
            "UPDATE sessions SET name = %s, email = %s WHERE session_id = %s",
            (name, email, session_id)
        )
        conn.commit()
    
    cur.execute("SELECT final_result FROM sessions WHERE session_id = %s", (session_id,))
    result = cur.fetchone()
    if result:
        result = config['results'][result[0]]
    else:
        result = None


    cur.close()
    conn.close()
        
    return render_template('result.html', 
                           message="Thanks! I'll be in touch.", 
                           hide_form=True,
                           result=result,
                           # Re-render result data (quick hack, better to redirect)
                           session_id=session_id)

@app.route('/admin')
def admin():
    """Calculates stats and renders the new dashboard."""
    pwd = request.args.get('pwd')
    admin_pwd = os.environ.get('ADMIN_PASSWORD', 'secret123')
    
    if pwd != admin_pwd:
        abort(403)
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Fetch all data needed for analytics
    cur.execute("SELECT results, created_at, name, email FROM sessions ORDER BY created_at DESC")
    all_rows = cur.fetchall()
    
    cur.close()
    conn.close()

    # --- Analytics Logic ---
    stats = {
        'total_sessions': len(all_rows),
        'leads_count': sum(1 for r in all_rows if r[3]), # r[3] is email
        'results_tally': {},
        'answers_tally': {},
        'leads': []
    }

    for row in all_rows:
        user_results_json = row[0] # e.g., {'q_volume': 'big_data', ...}
        created_at = row[1]
        name = row[2]
        email = row[3]

        # 1. Collect Lead
        if email:
            stats['leads'].append({
                'date': created_at.strftime('%Y-%m-%d %H:%M'),
                'name': name or 'Anonymous',
                'email': email
            })

        # 2. Tally Questions & Results
        if user_results_json:
            for q_id, answer_key in user_results_json.items():
                
                # A) Answer Distribution
                if q_id in config['questions']:
                    q_text = config['questions'][q_id]['text']
                    
                    if q_text not in stats['answers_tally']:
                        stats['answers_tally'][q_text] = {}
                    
                    # Get human readable answer text
                    if answer_key in config['questions'][q_id]['answers']:
                        a_text = config['questions'][q_id]['answers'][answer_key]['text']
                        # Truncate long answers for chart legibility
                        short_a_text = (a_text[:40] + '...') if len(a_text) > 40 else a_text
                        
                        stats['answers_tally'][q_text][short_a_text] = stats['answers_tally'][q_text].get(short_a_text, 0) + 1

                        # B) Result Distribution (Inferred)
                        # Check if this specific answer leads to a result node
                        next_node_id = config['questions'][q_id]['answers'][answer_key]['next_id']
                        if next_node_id.startswith('result_') and next_node_id in config['results']:
                            r_title = config['results'][next_node_id]['title']
                            stats['results_tally'][r_title] = stats['results_tally'].get(r_title, 0) + 1

    return render_template('admin.html', stats=stats)
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