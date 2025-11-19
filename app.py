import os
import yaml
import uuid
import json
from psycopg2 import pool
from psycopg2.extras import Json
from flask import Flask, render_template, request, redirect, url_for, abort, make_response
from dotenv import load_dotenv
from contextlib import contextmanager
load_dotenv()
app = Flask(__name__)

# Load Configuration
def load_questions():
        with open('questions.yaml', 'r') as file:
            return yaml.safe_load(file)

config = load_questions()

# Database Connection Pool
try:
    db_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'kafkaornot'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD')
    )
except Exception as e:
    print(f"Error initializing DB pool: {e}")
    db_pool = None

@contextmanager
def get_db_connection():
    if not db_pool:
        raise Exception("DB Pool not initialized")
    
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)


# --- ROUTES ---


@app.route('/')
def index():
    new_session_id = str(uuid.uuid4())
    stats_display = []

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get Statistics for Landing Page
                cur.execute("""
                    SELECT final_result, COUNT(*) 
                    FROM sessions 
                    WHERE final_result IS NOT NULL 
                    GROUP BY final_result
                """)
                stats_rows = cur.fetchall()

        # Calculate Percentages for Social Proof
        total_votes = sum(row[1] for row in stats_rows)
        if total_votes > 0:
            for result_key, count in stats_rows:
                if result_key in config.get('results', {}):
                    title = config['results'][result_key]['title']
                    percentage = int((count / total_votes) * 100)
                    stats_display.append({'title': title, 'percent': percentage, 'key': result_key})
            
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

    # 1. Handle Incoming Answers (Lazy Write / Upsert)
    incoming_answer = request.args.get('ans')
    if incoming_answer:
        try:
            prev_q_id, answer_key = incoming_answer.split(':')
            if prev_q_id in config['questions'] and answer_key in config['questions'][prev_q_id]['answers']:
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            # UPSERT LOGIC:
                            # If session doesn't exist -> Insert it with this result
                            # If session exists -> Append this result to existing JSONB
                            new_data = json.dumps({prev_q_id: answer_key})
                            
                            cur.execute("""
                                INSERT INTO sessions (session_id, results) 
                                VALUES (%s, %s::jsonb) 
                                ON CONFLICT (session_id) 
                                DO UPDATE SET results = sessions.results || EXCLUDED.results, updated_at = CURRENT_TIMESTAMP
                            """, (session_id, new_data))
                            
                            conn.commit()
                except Exception as e:
                    print(f"DB Error saving answer: {e}")
        except ValueError:
            pass

    # 2. Render Node (Question or Result)
    if node_id in config.get('results', {}):
        # SAVE FINAL RESULT TO DB (Upsert ensures row exists even if they skipped questions somehow)
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO sessions (session_id, final_result)
                        VALUES (%s, %s)
                        ON CONFLICT (session_id)
                        DO UPDATE SET final_result = EXCLUDED.final_result, updated_at = CURRENT_TIMESTAMP
                    """, (session_id, node_id))
                    conn.commit()
        except Exception as e:
            print(f"DB Error updating final result: {e}")

        result_data = config['results'][node_id]
        return render_template('result.html', result=result_data)
    
    elif node_id in config.get('questions', {}):
        question_data = config['questions'][node_id]
        return render_template('question.html', question=question_data, q_id=node_id)
    
    else:
        abort(404)

@app.route('/capture-lead', methods=['POST'])
def capture_lead():
    session_id = request.cookies.get('session_id')
    name = request.form.get('name')
    email = request.form.get('email')
    
    if session_id and email:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # We use UPDATE here because a user must have reached a verdict 
                    # (and thus created a row) to see this form.
                    cur.execute(
                        "UPDATE sessions SET name = %s, email = %s, updated_at = CURRENT_TIMESTAMP WHERE session_id = %s",
                        (name, email, session_id)
                    )
                    conn.commit()
        except Exception as e:
            print(f"DB Error capturing lead: {e}")
        
    return render_template('result.html', 
                           result={'title': 'Thank You', 'verdict': 'Saved', 'description': '', 'recommendation': ''},
                           message="Thanks! I'll be in touch.", 
                           hide_form=True)

@app.route('/admin')
def admin():
    pwd = request.args.get('pwd')
    admin_pwd = os.environ.get('ADMIN_PASSWORD', 'secret123')
    if pwd != admin_pwd: abort(403)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Added final_result to query
                cur.execute("SELECT results, created_at, name, email, final_result FROM sessions ORDER BY created_at DESC")
                all_rows = cur.fetchall()
    except Exception as e:
        print(e)
        all_rows = []

    stats = {
        'total_sessions': len(all_rows),
        'leads_count': sum(1 for r in all_rows if r[3]),
        'results_tally': {},
        'answers_tally': {},
        'leads': []
    }

    for row in all_rows:
        user_results_json = row[0]
        created_at = row[1]
        name = row[2]
        email = row[3]
        final_res = row[4]

        if email:
            stats['leads'].append({'date': created_at.strftime('%Y-%m-%d %H:%M'), 'name': name or 'Anon', 'email': email})

        # Tally Final Results
        if final_res and final_res in config['results']:
            title = config['results'][final_res]['title']
            stats['results_tally'][title] = stats['results_tally'].get(title, 0) + 1

        # Tally Answers
        if user_results_json:
            for q_id, answer_key in user_results_json.items():
                if q_id in config['questions']:
                    q_text = config['questions'][q_id]['text']
                    if q_text not in stats['answers_tally']:
                        stats['answers_tally'][q_text] = {}
                    
                    if answer_key in config['questions'][q_id]['answers']:
                        a_text = config['questions'][q_id]['answers'][answer_key]['text']
                        short_a_text = (a_text[:40] + '...') if len(a_text) > 40 else a_text
                        stats['answers_tally'][q_text][short_a_text] = stats['answers_tally'][q_text].get(short_a_text, 0) + 1

    return render_template('admin.html', stats=stats)

if __name__ == '__main__':
    app.run(debug=True, port=5005)