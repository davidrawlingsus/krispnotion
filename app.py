import json
import os
import re
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from psycopg import connect
from psycopg.rows import dict_row

app = Flask(__name__)

# Database connection function
def get_db_connection():
    """Get database connection using DATABASE_PUBLIC_URL (external) or DATABASE_URL (internal)"""
    # Prefer DATABASE_PUBLIC_URL for Railway external connections
    db_url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    
    if not db_url:
        raise Exception("No database URL found in environment variables")
    
    return connect(db_url)


def init_db():
    """Initialize database tables if they don't exist"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create payloads table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS payloads (
                        id SERIAL PRIMARY KEY,
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        payload_data JSONB NOT NULL
                    )
                """)
                
                # Create index on received_at for faster queries
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_payloads_received_at 
                    ON payloads(received_at DESC)
                """)
                
                # Create sent_tasks table to track tasks sent to Zapier
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sent_tasks (
                        id SERIAL PRIMARY KEY,
                        payload_id INTEGER REFERENCES payloads(id),
                        task TEXT NOT NULL,
                        owner VARCHAR(255) NOT NULL,
                        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        zapier_response TEXT,
                        success BOOLEAN NOT NULL
                    )
                """)
                
                # Create indexes for sent_tasks
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sent_tasks_sent_at 
                    ON sent_tasks(sent_at DESC)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sent_tasks_payload_id 
                    ON sent_tasks(payload_id)
                """)
                
                conn.commit()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        # Don't fail startup - connection might be temporary
        pass


# Initialize database on startup
init_db()


def parse_tasks_from_payload(payload):
    """
    Parse tasks from payload. Handles both string and dict formats.
    Returns list of dicts with 'task' and 'owner' keys.
    """
    tasks = []
    
    # If payload is a string, try to parse it as JSON first
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            # If not JSON, treat as plain text
            pass
    
    # If payload is a dict, look for common keys that might contain the task text
    if isinstance(payload, dict):
        # Check for common keys like 'text', 'content', 'body', 'message', 'data'
        text_content = (
            payload.get('text') or 
            payload.get('content') or 
            payload.get('body') or 
            payload.get('message') or 
            payload.get('data') or
            str(payload)
        )
    else:
        text_content = str(payload)
    
    # Pattern to match "Task: ... Owner: ..." entries
    # This handles multi-line tasks and various formats
    # Matches "Task: ..." followed by "Owner: ..." with optional whitespace/newlines
    pattern = r'Task:\s*(.+?)\s+Owner:\s*(\w+)'
    
    matches = re.finditer(pattern, text_content, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        task_text = match.group(1).strip()
        owner = match.group(2).strip()
        tasks.append({
            'task': task_text,
            'owner': owner
        })
    
    return tasks


def clean_task_text(task_text, owner):
    """
    Remove owner prefixes like "Anthony to...", "David to..." from task text.
    """
    # Pattern to match "Owner to" or "Owner:" at the start
    patterns = [
        rf'^{re.escape(owner)}\s+to\s+',
        rf'^{re.escape(owner)}:\s*',
        rf'^{re.escape(owner)}\s+',
    ]
    
    cleaned = task_text
    for pattern in patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip()


def post_to_zapier(task_data):
    """
    Post a single task to Zapier webhook.
    Returns (success: bool, response_text: str)
    """
    zapier_url = os.environ.get("ZAPIER_WEBHOOK_URL")
    
    if not zapier_url:
        return False, "ZAPIER_WEBHOOK_URL not found in environment variables"
    
    try:
        response = requests.post(
            zapier_url,
            json=task_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        return True, response.text
    except requests.exceptions.RequestException as e:
        return False, str(e)


@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        with get_db_connection() as conn:
            pass
        return jsonify({
            "status": "ok",
            "message": "Webhook receiver is running",
            "database": "connected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "ok",
            "message": "Webhook receiver is running",
            "database": f"error: {str(e)}"
        }), 200


@app.route("/latest", methods=["GET"])
def get_latest():
    """Get the most recent payload received"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute("""
                    SELECT id, received_at, payload_data
                    FROM payloads
                    ORDER BY received_at DESC
                    LIMIT 1
                """)
                
                result = cursor.fetchone()
        
        if not result:
            return jsonify({"error": "No payloads found"}), 404
        
        return jsonify({
            "id": result["id"],
            "received_at": result["received_at"].isoformat(),
            "payload": result["payload_data"]
        }), 200
        
    except Exception as e:
        print(f"Error retrieving latest payload: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/payloads", methods=["GET"])
def list_payloads():
    """List all received payloads"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute("""
                    SELECT id, received_at
                    FROM payloads
                    ORDER BY received_at DESC
                """)
                
                results = cursor.fetchall()
        
        payloads = [
            {
                "id": row["id"],
                "received_at": row["received_at"].isoformat()
            }
            for row in results
        ]
        
        return jsonify({
            "count": len(payloads),
            "payloads": payloads
        }), 200
        
    except Exception as e:
        print(f"Error listing payloads: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/payload/<int:payload_id>", methods=["GET"])
def get_payload(payload_id):
    """Get a specific payload by ID"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute("""
                    SELECT id, received_at, payload_data
                    FROM payloads
                    WHERE id = %s
                """, (payload_id,))
                
                result = cursor.fetchone()
        
        if not result:
            return jsonify({"error": "Payload not found"}), 404
        
        return jsonify({
            "id": result["id"],
            "received_at": result["received_at"].isoformat(),
            "payload": result["payload_data"]
        }), 200
        
    except Exception as e:
        print(f"Error retrieving payload: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Endpoint to receive JSON payload from Zapier, parse tasks, and forward to Zapier"""
    try:
        # Get JSON payload from request
        payload = request.get_json(force=True) if request.is_json else request.form.to_dict()
        
        if not payload:
            return jsonify({"error": "No payload received"}), 400
        
        # Save to database
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO payloads (payload_data)
                    VALUES (%s)
                    RETURNING id, received_at
                """, (json.dumps(payload),))
                
                result = cursor.fetchone()
                conn.commit()
        
        payload_id, received_at = result
        
        print(f"Received payload and saved to database with ID {payload_id}")
        
        # Parse tasks from payload
        tasks = parse_tasks_from_payload(payload)
        
        if not tasks:
            print("No tasks found in payload")
            return jsonify({
                "status": "success",
                "message": "Payload received and saved, but no tasks found",
                "id": payload_id,
                "received_at": received_at.isoformat(),
                "tasks_processed": 0
            }), 200
        
        # Process and forward each task to Zapier
        results = []
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for task_data in tasks:
                    # Clean the task text
                    cleaned_task = clean_task_text(task_data['task'], task_data['owner'])
                    
                    # Prepare data for Zapier
                    zapier_data = {
                        'task': cleaned_task,
                        'owner': task_data['owner']
                    }
                    
                    # Post to Zapier
                    success, response_text = post_to_zapier(zapier_data)
                    
                    # Save to database
                    cursor.execute("""
                        INSERT INTO sent_tasks (payload_id, task, owner, zapier_response, success)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (payload_id, cleaned_task, task_data['owner'], response_text, success))
                    
                    results.append({
                        'task': cleaned_task,
                        'owner': task_data['owner'],
                        'success': success,
                        'response': response_text
                    })
                    
                    if success:
                        print(f"Successfully posted task to Zapier: {cleaned_task[:50]}... (Owner: {task_data['owner']})")
                    else:
                        print(f"Failed to post task to Zapier: {response_text}")
                
                conn.commit()
        
        successful_count = sum(1 for r in results if r['success'])
        
        return jsonify({
            "status": "success",
            "message": f"Payload received and saved. Processed {len(tasks)} task(s), {successful_count} successfully posted to Zapier",
            "id": payload_id,
            "received_at": received_at.isoformat(),
            "tasks_processed": len(tasks),
            "tasks_successful": successful_count,
            "results": results
        }), 200
        
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/sent-tasks", methods=["GET"])
def list_sent_tasks():
    """List all tasks sent to Zapier"""
    try:
        limit = request.args.get('limit', default=50, type=int)
        offset = request.args.get('offset', default=0, type=int)
        
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Get total count
                cursor.execute("SELECT COUNT(*) as total FROM sent_tasks")
                total = cursor.fetchone()["total"]
                
                # Get tasks
                cursor.execute("""
                    SELECT id, payload_id, task, owner, sent_at, zapier_response, success
                    FROM sent_tasks
                    ORDER BY sent_at DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                
                results = cursor.fetchall()
        
        tasks = [
            {
                "id": row["id"],
                "payload_id": row["payload_id"],
                "task": row["task"],
                "owner": row["owner"],
                "sent_at": row["sent_at"].isoformat(),
                "success": row["success"],
                "zapier_response": row["zapier_response"]
            }
            for row in results
        ]
        
        return jsonify({
            "count": len(tasks),
            "total": total,
            "limit": limit,
            "offset": offset,
            "tasks": tasks
        }), 200
        
    except Exception as e:
        print(f"Error listing sent tasks: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/sent-tasks/<int:task_id>", methods=["GET"])
def get_sent_task(task_id):
    """Get a specific sent task by ID"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute("""
                    SELECT id, payload_id, task, owner, sent_at, zapier_response, success
                    FROM sent_tasks
                    WHERE id = %s
                """, (task_id,))
                
                result = cursor.fetchone()
        
        if not result:
            return jsonify({"error": "Task not found"}), 404
        
        return jsonify({
            "id": result["id"],
            "payload_id": result["payload_id"],
            "task": result["task"],
            "owner": result["owner"],
            "sent_at": result["sent_at"].isoformat(),
            "success": result["success"],
            "zapier_response": result["zapier_response"]
        }), 200
        
    except Exception as e:
        print(f"Error retrieving sent task: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/payload/<int:payload_id>/tasks", methods=["GET"])
def get_payload_tasks(payload_id):
    """Get all tasks sent to Zapier for a specific payload"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute("""
                    SELECT id, task, owner, sent_at, zapier_response, success
                    FROM sent_tasks
                    WHERE payload_id = %s
                    ORDER BY sent_at DESC
                """, (payload_id,))
                
                results = cursor.fetchall()
        
        tasks = [
            {
                "id": row["id"],
                "task": row["task"],
                "owner": row["owner"],
                "sent_at": row["sent_at"].isoformat(),
                "success": row["success"],
                "zapier_response": row["zapier_response"]
            }
            for row in results
        ]
        
        return jsonify({
            "payload_id": payload_id,
            "count": len(tasks),
            "tasks": tasks
        }), 200
        
    except Exception as e:
        print(f"Error retrieving payload tasks: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
