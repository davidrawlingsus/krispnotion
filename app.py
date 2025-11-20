import json
import os
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# Directory to store JSON files
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "ok", "message": "Webhook receiver is running"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    """Endpoint to receive JSON payload from Zapier"""
    try:
        # Get JSON payload from request
        payload = request.get_json(force=True) if request.is_json else request.form.to_dict()
        
        if not payload:
            return jsonify({"error": "No payload received"}), 400
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # milliseconds precision
        filename = f"payload_{timestamp}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Write payload to JSON file
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        
        print(f"Received payload and saved to {filepath}")
        
        return jsonify({
            "status": "success",
            "message": "Payload received and saved",
            "filename": filename
        }), 200
        
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

