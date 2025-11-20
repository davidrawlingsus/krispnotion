# Krisp to Notion Webhook Receiver

A simple Flask webhook receiver that accepts JSON payloads from Zapier and saves them to JSON files.

## Setup

1. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python app.py
```

The server will start on `http://0.0.0.0:5000` (or the PORT environment variable if set).

**Note:** On macOS, port 5000 may be in use by AirPlay Receiver. To use a different port locally:
```bash
PORT=5001 python app.py
```

## Usage

### Webhook Endpoint

POST requests to `/webhook` will be saved to JSON files in the `data/` directory.

**Example:**
```bash
curl -X POST http://localhost:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{"test": "data", "key": "value"}'
```

### Health Check

GET requests to `/` will return the service status.

## Railway Deployment

For Railway deployment, set the `PORT` environment variable. The app will automatically use it.

## Output

All received payloads are saved to `data/payload_<timestamp>.json` files with timestamps for unique filenames.

