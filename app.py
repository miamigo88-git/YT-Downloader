# Minimal Flask app so the image builds and runs.
# Replace this file later with the full YT-Downloader code.
from flask import Flask, jsonify, request
import os

app = Flask(__name__)

@app.route("/")
def index():
    return """
    <h2>YT-Downloader (starter)</h2>
    <p>This is a placeholder app. Replace app.py with the full app when ready.</p>
    """

@app.route("/health")
def health():
    return jsonify({"status":"ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)