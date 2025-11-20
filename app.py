import json
import os
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
    """Initialize database table if it doesn't exist"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create table if it doesn't exist
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
                
                conn.commit()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        # Don't fail startup - connection might be temporary
        pass


# Initialize database on startup
init_db()


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
    """Endpoint to receive JSON payload from Zapier"""
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
        
        return jsonify({
            "status": "success",
            "message": "Payload received and saved",
            "id": payload_id,
            "received_at": received_at.isoformat()
        }), 200
        
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
