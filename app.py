import os
import threading
import time
import sqlite3
from datetime import datetime
from functools import wraps
from flask import Flask, request, jsonify, render_template, send_from_directory, abort
from flask_socketio import SocketIO, emit, disconnect
from downloader import Downloader
from utils_search import search_videos

APP_DB = "/config/jobs.db"
DOWNLOAD_ROOT = "/data"
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")  # set this in the container for auth; if empty -> auth disabled

os.makedirs("/config", exist_ok=True)
os.makedirs("/data", exist_ok=True)

def init_db():
    conn = sqlite3.connect(APP_DB)
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT,
        language TEXT,
        is_series INTEGER,
        always_series INTEGER,
        min_length INTEGER,
        max_length INTEGER,
        folder_name TEXT,
        status TEXT,
        created_at TEXT,
        updated_at TEXT,
        yt_id TEXT UNIQUE,
        parent_id INTEGER
    )
    ''')
    conn.commit()
    conn.close()

init_db()

app = Flask(__name__, static_folder="static", template_folder="templates")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

downloader = Downloader(APP_DB, DOWNLOAD_ROOT, socketio)
worker_thread = threading.Thread(target=downloader.run, daemon=True)
worker_thread.start()

def check_token_header():
    if not ADMIN_TOKEN:
        return True
    # Accept "Authorization: Bearer <token>" or header "X-API-KEY"
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1].strip()
        return token == ADMIN_TOKEN
    api_key = request.headers.get("X-API-KEY", "")
    return api_key == ADMIN_TOKEN

def require_token(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not check_token_header():
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapped

@app.route("/")
def index():
    # UI is public but will not be able to call protected APIs without ADMIN_TOKEN
    return render_template("index.html", token_required=bool(ADMIN_TOKEN))

@app.route("/api/submit", methods=["POST"])
@require_token
def submit():
    payload = request.json or {}
    query = (payload.get("query") or "").strip()
    language = payload.get("language", "") or ""
    is_series = 1 if payload.get("is_series") else 0
    always_series = 1 if payload.get("always_series") else 0
    min_length = payload.get("min_length") or 0
    max_length = payload.get("max_length") or 0
    folder_name = payload.get("folder_name") or suggest_folder(query)

    if not query:
        return jsonify({"error": "query required"}), 400

    conn = sqlite3.connect(APP_DB)
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    c.execute('''
       INSERT INTO jobs (query, language, is_series, always_series, min_length, max_length, folder_name, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (query, language, is_series, always_series, min_length, max_length, folder_name, 'pending', now, now))
    parent_id = c.lastrowid
    conn.commit()
    conn.close()

    if is_series and always_series:
        t = threading.Thread(target=downloader.monitor_series, args=(parent_id,), daemon=True)
        t.start()

    socketio.emit("job_updated", {"event": "new_job", "job_id": parent_id})
    return jsonify({"job_id": parent_id}), 201

@app.route("/api/jobs", methods=["GET"])
@require_token
def jobs():
    conn = sqlite3.connect(APP_DB)
    c = conn.cursor()
    c.execute('SELECT id, query, language, is_series, always_series, min_length, max_length, folder_name, status, created_at, updated_at, yt_id, parent_id FROM jobs ORDER BY id DESC')
    rows = c.fetchall()
    conn.close()
    keys = ["id","query","language","is_series","always_series","min_length","max_length","folder_name","status","created_at","updated_at","yt_id","parent_id"]
    return jsonify([dict(zip(keys, r)) for r in rows])

@app.route("/api/cancel/<int:job_id>", methods=["POST"])
@require_token
def cancel(job_id):
    conn = sqlite3.connect(APP_DB)
    c = conn.cursor()
    c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?", ("cancelled", datetime.utcnow().isoformat(), job_id))
    conn.commit()
    conn.close()
    socketio.emit("job_updated", {"event":"cancel", "job_id": job_id})
    return jsonify({"ok": True})

@app.route("/api/search", methods=["GET"])
@require_token
def api_search():
    q = request.args.get("q", "")
    lang = request.args.get("lang", "")
    if not q:
        return jsonify([])
    res = search_videos(q, language=lang, limit=10)
    return jsonify(res)

@app.route("/static/<path:p>")
def static_files(p):
    return send_from_directory("static", p)

def suggest_folder(query):
    base = query.strip().replace(" ", "_")[:50]
    return f"{base}"

@socketio.on('connect')
def handle_connect(auth):
    # auth may be provided by Socket.IO client as {token: "..."} or query string param
    if ADMIN_TOKEN:
        token = None
        # auth param
        if isinstance(auth, dict):
            token = auth.get("token")
        # fallback to query param
        if not token:
            token = request.args.get("token")
        # fallback to header
        if not token:
            auth_hdr = request.headers.get("Authorization", "")
            if auth_hdr.startswith("Bearer "):
                token = auth_hdr.split(" ", 1)[1].strip()
        if token != ADMIN_TOKEN:
            # reject connection
            disconnect()
            return
    # allow connection
    emit("connected", {"msg": "welcome"})

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000)
