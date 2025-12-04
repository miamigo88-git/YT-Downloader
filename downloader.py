# Minimal downloader module used by app.py
import os
import sqlite3
import time
from datetime import datetime
from yt_dlp import YoutubeDL
from utils_search import search_videos

class Downloader:
    def __init__(self, db_path, download_root, socketio=None):
        self.db = db_path
        self.root = download_root
        self.socketio = socketio
        self.running = True

    def run(self):
        # simple loop: search pending jobs and queue downloads
        while self.running:
            try:
                conn = sqlite3.connect(self.db)
                c = conn.cursor()

                # process parent queries (pending, no yt_id)
                c.execute(
                    "SELECT id, query, language, is_series, folder_name, min_length, max_length, always_series "
                    "FROM jobs WHERE status='pending' AND yt_id IS NULL ORDER BY id"
                )
                parents = c.fetchall()
                for p in parents:
                    pid, query, language, is_series, folder_name, min_l, max_l, always_series = p
                    candidates = search_videos(query, language, limit=10)
                    filtered = []
                    for v in candidates:
                        dur = v.get("duration") or 0
                        if min_l and dur < int(min_l) * 60:
                            continue
                        if max_l and dur > int(max_l) * 60:
                            continue
                        filtered.append(v)
                    if not filtered:
                        c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                                  ("waiting", datetime.utcnow().isoformat(), pid))
                        conn.commit()
                        continue
                    if is_series:
                        for v in filtered:
                            try:
                                c.execute(
                                    '''
                                    INSERT OR IGNORE INTO jobs (
                                        query, language, is_series, always_series, min_length, max_length,
                                        folder_name, status, created_at, updated_at, yt_id, parent_id
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ''',
                                    (query, language, is_series, always_series, min_l, max_l, folder_name,
                                     'queued', datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                                     v.get("id"), pid)
                                )
                            except Exception:
                                pass
                        c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                                  ("active", datetime.utcnow().isoformat(), pid))
                        conn.commit()
                    else:
                        v = filtered[0]
                        c.execute("UPDATE jobs SET yt_id=?, status=?, updated_at=? WHERE id=?",
                                  (v.get("id"), "queued", datetime.utcnow().isoformat(), pid))
                        conn.commit()
                conn.close()

                # download queued jobs
                conn = sqlite3.connect(self.db)
                c = conn.cursor()
                c.execute("SELECT id, yt_id, folder_name FROM jobs WHERE status='queued' ORDER BY id")
                queued = c.fetchall()
                conn.close()
                for q in queued:
                    self._download_job(q)
            except Exception as e:
                # emit log if socketio available
                try:
                    if self.socketio:
                        self.socketio.emit("log", {"level": "error", "msg": str(e)})
                except Exception:
                    pass
            time.sleep(5)

    def _download_job(self, qrow):
        job_id, yt_id, folder_name = qrow
        dest_dir = os.path.join(self.root, folder_name or "misc")
        os.makedirs(dest_dir, exist_ok=True)

        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?", ("running", datetime.utcnow().isoformat(), job_id))
        conn.commit()
        conn.close()

        outtmpl = os.path.join(dest_dir, '%(playlist_index)s - %(title)s.%(ext)s')
        ydl_opts = {
            'outtmpl': outtmpl,
            'format': 'bestvideo[ext=mp4]+bestaudio/best/best',
            'noplaylist': True,
            'quiet': True,
            'progress_hooks': [self._progress_hook],
            'ignoreerrors': True,
            'retries': 3,
        }
        url = f"https://www.youtube.com/watch?v={yt_id}"
        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except Exception as e:
            conn = sqlite3.connect(self.db)
            c = conn.cursor()
            c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?", ("failed", datetime.utcnow().isoformat(), job_id))
            conn.commit()
            conn.close()
            if self.socketio:
                try:
                    self.socketio.emit("job_updated", {"event": "failed", "job_id": job_id, "error": str(e)})
                except Exception:
                    pass
            return

        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?", ("done", datetime.utcnow().isoformat(), job_id))
        conn.commit()
        conn.close()
        if self.socketio:
            try:
                self.socketio.emit("job_updated", {"event": "done", "job_id": job_id})
            except Exception:
                pass

    def _progress_hook(self, d):
        # Emit progress dict via socketio if available
        if self.socketio:
            try:
                self.socketio.emit("download_progress", d)
            except Exception:
                pass

    def monitor_series(self, parent_id):
        # monitor parent job and enqueue new unique videos when always_series is set
        while True:
            conn = sqlite3.connect(self.db)
            c = conn.cursor()
            c.execute("SELECT query, language, min_length, max_length, folder_name, always_series, status FROM jobs WHERE id=?", (parent_id,))
            row = c.fetchone()
            conn.close()
            if not row:
                return
            query, language, min_l, max_l, folder_name, always_series, status = row
            if status == "cancelled" or not always_series:
                return
            candidates = search_videos(query, language, limit=20)
            conn = sqlite3.connect(self.db)
            c = conn.cursor()
            for v in candidates:
                vid = v.get("id")
                dur = v.get("duration") or 0
                if min_l and dur < int(min_l) * 60:
                    continue
                if max_l and dur > int(max_l) * 60:
                    continue
                try:
                    c.execute(
                        '''
                        INSERT OR IGNORE INTO jobs (
                            query, language, is_series, always_series, min_length, max_length,
                            folder_name, status, created_at, updated_at, yt_id, parent_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (query, language, 1, always_series, min_l, max_l, folder_name,
                         'queued', datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                         vid, parent_id)
                    )
                except Exception:
                    pass
            conn.commit()
            conn.close()
            time.sleep(60 * 5)
