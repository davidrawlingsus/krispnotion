import json
import os
import re
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from psycopg import connect
from psycopg.rows import dict_row

# Webhook receiver for Krisp to Notion integration
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
                
                # Add meeting_name and meeting_date columns if they don't exist
                # Check if columns exist before adding them
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='sent_tasks' AND column_name='meeting_name'
                """)
                if not cursor.fetchone():
                    cursor.execute("""
                        ALTER TABLE sent_tasks 
                        ADD COLUMN meeting_name VARCHAR(255)
                    """)
                
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='sent_tasks' AND column_name='meeting_date'
                """)
                if not cursor.fetchone():
                    cursor.execute("""
                        ALTER TABLE sent_tasks 
                        ADD COLUMN meeting_date VARCHAR(255)
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
    Parse tasks from payload. Handles multiple formats:
    - Markdown task list: "- [ ] Owner to task description"
    - Task/Owner format: "Task: ... Owner: ..."
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
    
    # Handle list format (e.g., [{'krisp_blob': '...'}])
    if isinstance(payload, list) and len(payload) > 0:
        if isinstance(payload[0], dict) and 'krisp_blob' in payload[0]:
            text_content = payload[0]['krisp_blob']
            print(f"Found krisp_blob in list payload, length: {len(text_content)}")
        else:
            # Try to extract text from list items
            text_content = '\n'.join(str(item) for item in payload)
            print(f"Payload is list, converted to text, length: {len(text_content)}")
    # If payload is a dict, look for common keys that might contain the task text
    elif isinstance(payload, dict):
        # Check for krisp_blob first, then other common keys
        text_content = (
            payload.get('krisp_blob') or
            payload.get('text') or 
            payload.get('content') or 
            payload.get('body') or 
            payload.get('message') or 
            payload.get('data') or
            str(payload)
        )
        print(f"Payload is dict, extracted text_content length: {len(str(text_content))}")
    else:
        text_content = str(payload)
        print(f"Payload is not dict/list, text_content length: {len(text_content)}")
    
    # First, try to parse markdown task list format: "- [ ] Owner to task description"
    # Pattern matches: "- [ ]" followed by owner name, "to", and task description
    # Stops at next task item or end of string
    markdown_pattern = r'-\s*\[\s*\]\s+(\w+)\s+to\s+(.+?)(?=\n\s*-\s*\[|$)'
    markdown_matches = re.finditer(markdown_pattern, text_content, re.MULTILINE | re.DOTALL | re.IGNORECASE)
    
    match_count = 0
    for match in markdown_matches:
        match_count += 1
        owner = match.group(1).strip()
        task_text = match.group(2).strip().replace('\n', ' ').strip()  # Remove newlines and extra spaces
        print(f"Found markdown task {match_count}: owner={owner}, task_length={len(task_text)}")
        tasks.append({
            'task': task_text,
            'owner': owner
        })
    
    # If no markdown tasks found, try the "Task: ... Owner: ..." format
    if match_count == 0:
        pattern = r'Task:\s*(.+?)\s+Owner:\s*(\w+)'
        matches = re.finditer(pattern, text_content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            match_count += 1
            task_text = match.group(1).strip()
            owner = match.group(2).strip()
            print(f"Found task {match_count}: owner={owner}, task_length={len(task_text)}")
            tasks.append({
                'task': task_text,
                'owner': owner
            })
    
    if match_count == 0:
        print(f"No tasks matched pattern. First 500 chars of text_content: {text_content[:500]}")
    
    return tasks


def clean_task_text(task_text, owner):
    """
    Remove owner prefixes like "Anthony to...", "David to..." from task text.
    Capitalizes the first letter of the task.
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
    
    cleaned = cleaned.strip()
    
    # Capitalize the first letter
    if cleaned:
        cleaned = cleaned[0].upper() + cleaned[1:] if len(cleaned) > 1 else cleaned.upper()
    
    return cleaned


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
        print(f"Payload structure: {type(payload).__name__}")
        if isinstance(payload, (list, dict)):
            print(f"Payload keys/structure preview: {json.dumps(payload, indent=2)[:1000]}")
        
        # Extract meeting_name and meeting_date from payload
        meeting_name = None
        meeting_date = None
        
        # Handle list format - check all items in the list
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    # Check if this item has meeting_name or meeting_date
                    if 'meeting_name' in item and meeting_name is None:
                        meeting_name = item.get('meeting_name')
                    if 'meeting_date' in item and meeting_date is None:
                        meeting_date = item.get('meeting_date')
                    # Also check nested structures
                    if meeting_name is None or meeting_date is None:
                        # Check if there are nested objects
                        for key, value in item.items():
                            if isinstance(value, dict):
                                if 'meeting_name' in value and meeting_name is None:
                                    meeting_name = value.get('meeting_name')
                                if 'meeting_date' in value and meeting_date is None:
                                    meeting_date = value.get('meeting_date')
        # Handle dict format
        elif isinstance(payload, dict):
            meeting_name = payload.get('meeting_name')
            meeting_date = payload.get('meeting_date')
            # Also check nested structures
            if meeting_name is None or meeting_date is None:
                for key, value in payload.items():
                    if isinstance(value, dict):
                        if 'meeting_name' in value and meeting_name is None:
                            meeting_name = value.get('meeting_name')
                        if 'meeting_date' in value and meeting_date is None:
                            meeting_date = value.get('meeting_date')
        
        if meeting_name:
            print(f"Extracted meeting_name: {meeting_name}")
        else:
            print("WARNING: meeting_name not found in payload")
        if meeting_date:
            print(f"Extracted meeting_date: {meeting_date}")
        else:
            print("WARNING: meeting_date not found in payload")
        
        # Parse tasks from payload
        tasks = parse_tasks_from_payload(payload)
        print(f"Parsed {len(tasks)} task(s) from payload")
        
        if not tasks:
            print("No tasks found in payload")
            print(f"Payload content: {json.dumps(payload, indent=2)[:500]}")  # Print first 500 chars for debugging
            return jsonify({
                "status": "success",
                "message": "Payload received and saved, but no tasks found",
                "id": payload_id,
                "received_at": received_at.isoformat(),
                "tasks_processed": 0
            }), 200
        
        # Process and forward each task to Zapier
        results = []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for idx, task_data in enumerate(tasks, 1):
                        # Clean the task text
                        cleaned_task = clean_task_text(task_data['task'], task_data['owner'])
                        
                        # Prepare data for Zapier
                        zapier_data = {
                            'task': cleaned_task,
                            'owner': task_data['owner']
                        }
                        
                        # Add meeting_name and meeting_date if available
                        if meeting_name:
                            zapier_data['meeting_name'] = meeting_name
                        if meeting_date:
                            zapier_data['meeting_date'] = meeting_date
                        
                        # Post to Zapier
                        success, response_text = post_to_zapier(zapier_data)
                        
                        # Save to database
                        try:
                            cursor.execute("""
                                INSERT INTO sent_tasks (payload_id, task, owner, zapier_response, success, meeting_name, meeting_date)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (payload_id, cleaned_task, task_data['owner'], response_text, success, meeting_name, meeting_date))
                            print(f"Task {idx}/{len(tasks)} saved to database: {cleaned_task[:50]}... (Owner: {task_data['owner']})")
                        except Exception as db_error:
                            print(f"ERROR saving task {idx} to database: {str(db_error)}")
                            print(f"Task data: task={cleaned_task[:100]}, owner={task_data['owner']}, payload_id={payload_id}")
                            # Continue processing other tasks even if one fails
                        
                        results.append({
                            'task': cleaned_task,
                            'owner': task_data['owner'],
                            'success': success,
                            'response': response_text
                        })
                        
                        if success:
                            print(f"Successfully posted task {idx} to Zapier: {cleaned_task[:50]}... (Owner: {task_data['owner']})")
                        else:
                            print(f"Failed to post task {idx} to Zapier: {response_text}")
                    
                    conn.commit()
                    print(f"Committed {len(tasks)} task(s) to database")
        except Exception as db_error:
            print(f"ERROR in database transaction: {str(db_error)}")
            import traceback
            traceback.print_exc()
        
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
                    SELECT id, payload_id, task, owner, sent_at, zapier_response, success, meeting_name, meeting_date
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
                "zapier_response": row["zapier_response"],
                "meeting_name": row.get("meeting_name"),
                "meeting_date": row.get("meeting_date")
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
                    SELECT id, payload_id, task, owner, sent_at, zapier_response, success, meeting_name, meeting_date
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
            "zapier_response": result["zapier_response"],
            "meeting_name": result.get("meeting_name"),
            "meeting_date": result.get("meeting_date")
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
                    SELECT id, task, owner, sent_at, zapier_response, success, meeting_name, meeting_date
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
                "zapier_response": row["zapier_response"],
                "meeting_name": row.get("meeting_name"),
                "meeting_date": row.get("meeting_date")
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
