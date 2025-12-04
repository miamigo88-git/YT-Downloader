# Simple search wrapper using yt-dlp to get metadata for a text search
from yt_dlp import YoutubeDL

def search_videos(query, language="", limit=10):
    search_str = f"ytsearch{limit}:{query}"
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'extract_flat': False,
    }
    results = []
    with YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(search_str, download=False)
            entries = info.get('entries') or []
            for e in entries[:limit]:
                results.append({
                    "id": e.get("id"),
                    "title": e.get("title"),
                    "duration": e.get("duration"),
                    "uploader": e.get("uploader"),
                    "webpage_url": e.get("webpage_url"),
                })
        except Exception:
            pass
    return results
