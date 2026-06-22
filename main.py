"""
Work-log server - CLIENT PLANE (runs on the droplet).

Endpoints
---------
Admin-authenticated (Bearer ADMIN_SECRET), called by your local app:
  POST /provision            register/update a client
  POST /ingest               push log markdown + summaries; fires notices
  GET  /admin/{client_id}    pull comments, files, annotations
  POST /work/{client_id}/start   begin a work session
  POST /work/{client_id}/stop    end a work session
  GET  /work/{client_id}/status  current work state + session history

Client-facing (gated by the secret URL token only):
  GET  /log/{token}          the portal page
  GET  /log/{token}/raw      JSON: everything the portal needs
  GET  /log/{token}/stream   SSE live updates + status changes
  POST /log/{token}/comment  client leaves a comment
  POST /log/{token}/upload   client uploads a file
  GET  /log/{token}/file/{id}  download a file
  POST /log/{token}/prefs    client sets notification prefs
  POST /log/{token}/annotate   client annotates log text
  DELETE /log/{token}/annotate/{id}  client deletes an annotation
"""

import asyncio
import html
import json
import logging
import os
import secrets
import sqlite3
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import bleach
import markdown as md
import requests
from fastapi import (FastAPI, Header, HTTPException, Request, UploadFile,
                     File, Form)
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "portal.db"
FILES_DIR = BASE / "uploads"
FILES_DIR.mkdir(exist_ok=True)

ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SUMMARY_MODEL = os.environ.get("SUMMARY_MODEL", "claude-haiku-4-5-20251001")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "notifications@example.com")
NTFY_BASE = os.environ.get("NTFY_BASE", "https://ntfy.sh")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "")
NOTIFY_DRY_RUN = os.environ.get("NOTIFY_DRY_RUN", "") == "1"

MAX_UPLOAD_BYTES = 25 * 1024 * 1024
ALLOWED_EXT = {
    ".pdf", ".txt", ".md", ".csv", ".tsv", ".json", ".log",
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".xlsx", ".xls", ".docx", ".zip", ".parquet", ".ipynb", ".yaml", ".yml",
}
BLOCKED_EXT = {".html", ".htm", ".js", ".exe", ".sh", ".bat", ".com",
               ".scr", ".php", ".phtml"}

ALLOWED_TAGS = ["p", "br", "hr", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol",
                "li", "strong", "em", "del", "blockquote", "code", "pre", "a",
                "table", "thead", "tbody", "tr", "th", "td",
                "img", "figure", "figcaption", "sup", "sub", "abbr", "dd", "dt", "dl"]
ALLOWED_ATTRS = {"a": ["href", "title"],
                 "img": ["src", "alt", "width", "height"],
                 "abbr": ["title"]}


# --------------------------------------------------------------------------- db
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS clients (
                client_id TEXT PRIMARY KEY,
                name      TEXT NOT NULL,
                token     TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS content (
                client_id   TEXT PRIMARY KEY,
                markdown    TEXT NOT NULL DEFAULT '',
                summary     TEXT,
                section_summaries TEXT DEFAULT '',
                updated_at  TEXT,
                notified_at TEXT
            );
            CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                original_name TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                size INTEGER NOT NULL,
                uploaded_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS prefs (
                client_id TEXT PRIMARY KEY,
                email TEXT DEFAULT '',
                ntfy_topic TEXT DEFAULT '',
                channel_email INTEGER DEFAULT 0,
                channel_ntfy INTEGER DEFAULT 0,
                frequency TEXT DEFAULT 'work_30min',
                last_digest TEXT
            );
            CREATE TABLE IF NOT EXISTS work_sessions (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT
            );
            CREATE TABLE IF NOT EXISTS annotations (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                anchor_text TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        for col, default in [("section_summaries", "''"), ("work_note", "''"),
                              ("notif_sent_at", "''"), ("notif_md_len", "0")]:
            try:
                c.execute(f"ALTER TABLE content ADD COLUMN {col} TEXT DEFAULT {default}")
            except Exception:
                pass


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------- rendering
def render_markdown(text: str) -> str:
    h = md.markdown(text or "", extensions=["extra", "sane_lists", "nl2br"])
    return bleach.clean(h, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS, strip=True)


def make_summary(text: str) -> str | None:
    text = (text or "").strip()
    if not text or not ANTHROPIC_API_KEY:
        return None
    prompt = ("Summarize this freelance work log for the client who commissioned "
              "the work. 3-4 sentences, plain and concrete: what's been done, "
              "current state, what's next. No preamble.\n\n" + text[:12000])
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": SUMMARY_MODEL, "max_tokens": 350,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        if r.status_code == 200:
            blocks = r.json().get("content", [])
            return "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    except requests.RequestException:
        pass
    return None


# ----------------------------------------------------------------- pub/sub SSE
subscribers: dict[str, set] = {}


def publish(client_id: str, event_type: str = "update", data: str = "changed"):
    for q in subscribers.get(client_id, set()):
        q.put_nowait((event_type, data))


# ---------------------------------------------------------------- notifications
def _send_email(to_addr: str, subject: str, body: str):
    if NOTIFY_DRY_RUN or not (RESEND_API_KEY and to_addr):
        print(f"[notify:email dry/skip] to={to_addr} subj={subject!r}")
        return
    try:
        r = requests.post("https://api.resend.com/emails",
                          headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                                   "Content-Type": "application/json"},
                          json={"from": EMAIL_FROM, "to": [to_addr],
                                "subject": subject, "text": body},
                          timeout=10)
        if r.status_code == 200:
            print(f"[notify:email sent] to={to_addr} id={r.json().get('id','?')}")
        else:
            print(f"[notify:email error] {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[notify:email error] {e}")


def _send_ntfy(topic: str, title: str, body: str):
    if NOTIFY_DRY_RUN or not topic:
        print(f"[notify:ntfy dry/skip] topic={topic} title={title!r}")
        return
    try:
        requests.post(f"{NTFY_BASE}/{topic}", data=body.encode("utf-8"),
                      headers={"Title": title}, timeout=10)
    except requests.RequestException as e:
        print(f"[notify:ntfy error] {e}")


def _work_seconds_since(client_id: str, since_iso: str) -> float:
    """Sum work session seconds from `since_iso` to now."""
    with db() as c:
        sessions = c.execute(
            "SELECT started_at, ended_at FROM work_sessions WHERE client_id=? AND started_at>=?",
            (client_id, since_iso)).fetchall()
    now = datetime.now(timezone.utc)
    total = 0.0
    for s in sessions:
        start = datetime.fromisoformat(s["started_at"])
        end = datetime.fromisoformat(s["ended_at"]) if s["ended_at"] else now
        total += (end - start).total_seconds()
    return total


def _send_work_email(client_id: str):
    """Build and send a work-time email with recent changes + cached summary."""
    with db() as c:
        cl = c.execute("SELECT * FROM clients WHERE client_id=?", (client_id,)).fetchone()
        co = c.execute("SELECT * FROM content WHERE client_id=?", (client_id,)).fetchone()
        pr = c.execute("SELECT * FROM prefs WHERE client_id=?", (client_id,)).fetchone()
    if not cl or not co or not pr or not pr["email"]:
        return
    link = f"{PUBLIC_BASE}/log/{cl['token']}" if PUBLIC_BASE else "(your work-log link)"
    md_text = co["markdown"] or ""
    prev_len = int(co["notif_md_len"] or 0)
    new_text = md_text[prev_len:].strip() if prev_len < len(md_text) else ""
    summary = co["summary"] or ""

    parts = [f"Work Log Update - {cl['name']}\n"]
    if summary:
        parts.append(f"Summary: {summary}\n")
    if new_text:
        parts.append(f"Recent updates:\n\n{new_text}\n")
    parts.append(f"\nView full log: {link}")

    _send_email(pr["email"], f"Work log update - {cl['name']}", "\n".join(parts))
    with db() as c:
        c.execute("UPDATE content SET notif_sent_at=?, notif_md_len=? WHERE client_id=?",
                  (now_iso(), len(md_text), client_id))
    print(f"[notif] sent work email for {client_id}")


async def notification_loop():
    """Every 60s, check each client's notification preferences and send if due."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            with db() as c:
                rows = c.execute(
                    """SELECT pr.client_id, pr.frequency, pr.email, pr.channel_email,
                              co.notif_sent_at, co.updated_at, co.notif_md_len, co.markdown
                       FROM prefs pr JOIN content co ON co.client_id = pr.client_id
                       WHERE pr.channel_email=1 AND pr.email != ''"""
                ).fetchall()
            for r in rows:
                freq = r["frequency"]
                last_sent = r["notif_sent_at"] or ""
                updated = r["updated_at"] or ""
                md_len = len(r["markdown"] or "")
                prev_len = int(r["notif_md_len"] or 0)
                has_new = md_len > prev_len

                if not has_new:
                    continue

                if freq == "work_30min":
                    since = last_sent or "2000-01-01T00:00:00+00:00"
                    work_secs = _work_seconds_since(r["client_id"], since)
                    if work_secs >= 1800:
                        threading.Thread(target=_send_work_email,
                                         args=(r["client_id"],), daemon=True).start()

                elif freq == "daily_4pm":
                    local_hour = now.hour
                    already_today = (last_sent and
                                     datetime.fromisoformat(last_sent).date() == now.date())
                    if local_hour >= 16 and not already_today:
                        threading.Thread(target=_send_work_email,
                                         args=(r["client_id"],), daemon=True).start()

        except Exception as e:
            print(f"[notif error] {e}")
        await asyncio.sleep(60)


# ------------------------------------------------------------------- app setup
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(notification_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


def require_admin(authorization: str):
    if not ADMIN_SECRET:
        raise HTTPException(500, "Server missing ADMIN_SECRET")
    if authorization != f"Bearer {ADMIN_SECRET}":
        raise HTTPException(401, "Bad admin credentials")


def client_by_token(token: str):
    with db() as c:
        row = c.execute("SELECT * FROM clients WHERE token=?", (token,)).fetchone()
    if not row:
        raise HTTPException(404, "Not found")
    return row


# ----------------------------------------------------------- work session helpers
def _work_state(client_id: str) -> dict:
    with db() as c:
        sessions = [dict(r) for r in c.execute(
            "SELECT started_at, ended_at FROM work_sessions WHERE client_id=? ORDER BY started_at",
            (client_id,))]
        row = c.execute("SELECT work_note FROM content WHERE client_id=?", (client_id,)).fetchone()
    now = datetime.now(timezone.utc)
    total = 0
    working = False
    for s in sessions:
        start = datetime.fromisoformat(s["started_at"])
        if s["ended_at"]:
            end = datetime.fromisoformat(s["ended_at"])
            total += (end - start).total_seconds()
        else:
            total += (now - start).total_seconds()
            working = True
    return {"working": working, "total_seconds": int(total), "sessions": sessions,
            "work_note": (row["work_note"] if row else "") or ""}


# ---------------------------------------------------------------- admin routes
@app.post("/provision")
async def provision(request: Request, authorization: str = Header(default="")):
    require_admin(authorization)
    b = await request.json()
    client_id = b["client_id"]
    name = b.get("name", client_id)
    token = b.get("token") or secrets.token_urlsafe(32)
    with db() as c:
        c.execute(
            """INSERT INTO clients (client_id, name, token, created_at)
               VALUES (?,?,?,?)
               ON CONFLICT(client_id) DO UPDATE SET name=excluded.name""",
            (client_id, name, token, now_iso()))
        c.execute("INSERT OR IGNORE INTO content (client_id) VALUES (?)", (client_id,))
        c.execute(
            """INSERT INTO prefs (client_id, email, ntfy_topic, channel_email,
                                  channel_ntfy, frequency)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(client_id) DO NOTHING""",
            (client_id, b.get("email", ""), b.get("ntfy_topic", ""),
             1 if b.get("email") else 0, 1 if b.get("ntfy_topic") else 0,
             b.get("frequency", "work_30min")))
    return {"ok": True, "client_id": client_id, "token": token}


@app.post("/ingest")
async def ingest(request: Request, authorization: str = Header(default="")):
    require_admin(authorization)
    b = await request.json()
    client_id = b["client_id"]
    markdown_text = b.get("markdown", "")
    with db() as c:
        exists = c.execute("SELECT 1 FROM clients WHERE client_id=?", (client_id,)).fetchone()
        if not exists:
            raise HTTPException(404, f"Unknown client_id: {client_id}")
    summary = b.get("summary") or make_summary(markdown_text)
    section_summaries = b.get("section_summaries", "")
    if isinstance(section_summaries, list):
        section_summaries = json.dumps(section_summaries)
    ts = now_iso()
    with db() as c:
        c.execute(
            """UPDATE content SET markdown=?, summary=?, section_summaries=?, updated_at=?
               WHERE client_id=?""",
            (markdown_text, summary, section_summaries, ts, client_id))
    publish(client_id)
    return {"ok": True, "updated_at": ts, "summary_generated": bool(summary)}


@app.get("/admin/{client_id}")
async def admin_view(client_id: str, authorization: str = Header(default="")):
    require_admin(authorization)
    with db() as c:
        comments = [dict(r) for r in c.execute(
            "SELECT body, created_at FROM comments WHERE client_id=? ORDER BY created_at", (client_id,))]
        files = [dict(r) for r in c.execute(
            "SELECT id, original_name, size, uploaded_by, created_at FROM files WHERE client_id=? ORDER BY created_at", (client_id,))]
        annotations = [dict(r) for r in c.execute(
            "SELECT id, anchor_text, body, created_at FROM annotations WHERE client_id=? ORDER BY created_at", (client_id,))]
    return {"comments": comments, "files": files, "annotations": annotations}


@app.post("/work/{client_id}/start")
async def work_start(client_id: str, authorization: str = Header(default="")):
    require_admin(authorization)
    with db() as c:
        open_session = c.execute(
            "SELECT id FROM work_sessions WHERE client_id=? AND ended_at IS NULL",
            (client_id,)).fetchone()
        if open_session:
            raise HTTPException(409, "Already working")
        sid = uuid.uuid4().hex
        c.execute("INSERT INTO work_sessions (id, client_id, started_at) VALUES (?,?,?)",
                  (sid, client_id, now_iso()))
    publish(client_id, "status", "working")
    return {"ok": True, "session_id": sid}


@app.post("/work/{client_id}/stop")
async def work_stop(client_id: str, authorization: str = Header(default="")):
    require_admin(authorization)
    ts = now_iso()
    with db() as c:
        open_session = c.execute(
            "SELECT id, started_at FROM work_sessions WHERE client_id=? AND ended_at IS NULL",
            (client_id,)).fetchone()
        if not open_session:
            raise HTTPException(404, "No active session")
        c.execute("UPDATE work_sessions SET ended_at=? WHERE id=?", (ts, open_session["id"]))
    publish(client_id, "status", "idle")
    start = datetime.fromisoformat(open_session["started_at"])
    end = datetime.fromisoformat(ts)
    return {"ok": True, "duration_seconds": int((end - start).total_seconds())}


@app.post("/work/{client_id}/note")
async def work_note(client_id: str, request: Request, authorization: str = Header(default="")):
    require_admin(authorization)
    b = await request.json()
    note = (b.get("note") or "").strip()[:200]
    with db() as c:
        c.execute("UPDATE content SET work_note=? WHERE client_id=?", (note, client_id))
    publish(client_id, "status", "working" if note else "idle")
    return {"ok": True}


@app.get("/work/{client_id}/status")
async def work_status(client_id: str, authorization: str = Header(default="")):
    require_admin(authorization)
    return _work_state(client_id)


# --------------------------------------------------------------- client routes
def gather(client_id: str) -> dict:
    with db() as c:
        content = c.execute("SELECT * FROM content WHERE client_id=?", (client_id,)).fetchone()
        comments = [dict(r) for r in c.execute(
            "SELECT body, created_at FROM comments WHERE client_id=? ORDER BY created_at", (client_id,))]
        files = [dict(r) for r in c.execute(
            "SELECT id, original_name, size, uploaded_by, created_at FROM files WHERE client_id=? ORDER BY created_at", (client_id,))]
        annotations = [dict(r) for r in c.execute(
            "SELECT id, anchor_text, body, created_at FROM annotations WHERE client_id=? ORDER BY created_at", (client_id,))]
        prefs = c.execute("SELECT * FROM prefs WHERE client_id=?", (client_id,)).fetchone()

    ws = _work_state(client_id)
    sec_sum_raw = (content["section_summaries"] if content else "") or ""
    try:
        section_summaries = json.loads(sec_sum_raw) if sec_sum_raw else []
    except (json.JSONDecodeError, TypeError):
        section_summaries = []

    return {
        "log_html": render_markdown(content["markdown"]) if content else EMPTY_STATE,
        "summary": (content["summary"] if content else None),
        "section_summaries": section_summaries,
        "updated_at": (content["updated_at"] if content else None),
        "working": ws["working"],
        "work_note": ws["work_note"],
        "total_time_seconds": ws["total_seconds"],
        "sessions": ws["sessions"],
        "comments": [{"body": html.escape(x["body"]), "created_at": x["created_at"]} for x in comments],
        "files": files,
        "annotations": [{"id": a["id"], "anchor_text": a["anchor_text"],
                         "body": html.escape(a["body"]), "created_at": a["created_at"]}
                        for a in annotations],
        "prefs": dict(prefs) if prefs else {},
    }


@app.get("/log/{token}/raw")
async def raw(token: str):
    cl = client_by_token(token)
    return JSONResponse(gather(cl["client_id"]))


@app.post("/log/{token}/comment")
async def add_comment(token: str, request: Request):
    cl = client_by_token(token)
    b = await request.json()
    body = (b.get("body") or "").strip()
    if not body:
        raise HTTPException(400, "Empty comment")
    if len(body) > 4000:
        body = body[:4000]
    with db() as c:
        c.execute("INSERT INTO comments (id, client_id, body, created_at) VALUES (?,?,?,?)",
                  (uuid.uuid4().hex, cl["client_id"], body, now_iso()))
    publish(cl["client_id"])
    return {"ok": True}


@app.post("/log/{token}/upload")
async def upload(token: str, file: UploadFile = File(...)):
    cl = client_by_token(token)
    ext = Path(file.filename or "").suffix.lower()
    if ext in BLOCKED_EXT or ext not in ALLOWED_EXT:
        raise HTTPException(400, f"File type not allowed: {ext or 'unknown'}")
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "File too large (max 25 MB)")
    stored = f"{uuid.uuid4().hex}{ext}"
    (FILES_DIR / stored).write_bytes(data)
    safe_name = Path(file.filename or "file").name
    with db() as c:
        c.execute(
            """INSERT INTO files (id, client_id, original_name, stored_name, size, uploaded_by, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (uuid.uuid4().hex, cl["client_id"], safe_name, stored, len(data), "client", now_iso()))
    publish(cl["client_id"])
    return {"ok": True, "name": safe_name, "size": len(data)}


@app.get("/log/{token}/file/{file_id}")
async def download(token: str, file_id: str):
    cl = client_by_token(token)
    with db() as c:
        row = c.execute("SELECT * FROM files WHERE id=? AND client_id=?",
                        (file_id, cl["client_id"])).fetchone()
    if not row:
        raise HTTPException(404, "Not found")
    path = FILES_DIR / row["stored_name"]
    if not path.exists():
        raise HTTPException(404, "Not found")
    return FileResponse(path, filename=row["original_name"],
                        media_type="application/octet-stream",
                        headers={"Content-Disposition":
                                 f'attachment; filename="{row["original_name"]}"'})


@app.post("/log/{token}/prefs")
async def set_prefs(token: str, request: Request):
    cl = client_by_token(token)
    b = await request.json()
    freq = b.get("frequency", "work_30min")
    if freq not in ("work_30min", "daily_4pm", "off"):
        freq = "work_30min"
    with db() as c:
        c.execute(
            """UPDATE prefs SET email=?, ntfy_topic=?, channel_email=?, channel_ntfy=?, frequency=?
               WHERE client_id=?""",
            (b.get("email", ""), b.get("ntfy_topic", ""),
             1 if b.get("channel_email") else 0, 1 if b.get("channel_ntfy") else 0,
             freq, cl["client_id"]))
    return {"ok": True}


@app.post("/log/{token}/annotate")
async def add_annotation(token: str, request: Request):
    cl = client_by_token(token)
    b = await request.json()
    anchor = (b.get("anchor_text") or "").strip()
    body = (b.get("body") or "").strip()
    if not anchor or not body:
        raise HTTPException(400, "Both anchor_text and body are required")
    if len(anchor) > 500:
        anchor = anchor[:500]
    if len(body) > 2000:
        body = body[:2000]
    aid = uuid.uuid4().hex
    with db() as c:
        c.execute("INSERT INTO annotations (id, client_id, anchor_text, body, created_at) VALUES (?,?,?,?,?)",
                  (aid, cl["client_id"], anchor, body, now_iso()))
    publish(cl["client_id"])
    return {"ok": True, "id": aid}


@app.delete("/log/{token}/annotate/{annot_id}")
async def del_annotation(token: str, annot_id: str):
    cl = client_by_token(token)
    with db() as c:
        row = c.execute("SELECT id FROM annotations WHERE id=? AND client_id=?",
                        (annot_id, cl["client_id"])).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        c.execute("DELETE FROM annotations WHERE id=?", (annot_id,))
    publish(cl["client_id"])
    return {"ok": True}


@app.get("/log/{token}/stream")
async def stream(token: str):
    cl = client_by_token(token)
    cid = cl["client_id"]
    q: asyncio.Queue = asyncio.Queue()
    subscribers.setdefault(cid, set()).add(q)

    async def gen():
        try:
            yield "event: ping\ndata: connected\n\n"
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=25)
                    if isinstance(item, tuple):
                        evt, d = item
                        yield f"event: {evt}\ndata: {d}\n\n"
                    else:
                        yield f"event: update\ndata: {item}\n\n"
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: keepalive\n\n"
        finally:
            subscribers.get(cid, set()).discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/log/{token}", response_class=HTMLResponse)
async def page(token: str):
    cl = client_by_token(token)
    return HTMLResponse(PAGE.replace("{{NAME}}", html.escape(cl["name"]))
                        .replace("{{TOKEN}}", token))


EMPTY_STATE = ('<p class="empty">No entries yet. Work logged during this '
               'engagement will appear here as it happens.</p>')

PAGE = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>{{NAME}} - Work Log</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root{--paper:#f7f7f4;--ink:#1c2b33;--ink-soft:#5a6b73;--rule:#e3e2db;
        --signal:#c8772e;--signal-soft:#f0d9c2;--card:#fffffe;
        --mono:"IBM Plex Mono",ui-monospace,monospace;--sans:"IBM Plex Sans",system-ui,sans-serif;}
  @media (prefers-color-scheme:dark){:root{--paper:#14181b;--ink:#e8eaec;--ink-soft:#93a0a7;
        --rule:#262d31;--card:#181d20;--signal:#e2934a;--signal-soft:#3a2c1c;}}
  *{box-sizing:border-box;}
  body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--sans);
       line-height:1.6;-webkit-font-smoothing:antialiased;}
  .wrap{max-width:720px;margin:0 auto;padding:56px 24px 96px;}
  .eyebrow{font-family:var(--mono);font-size:12px;letter-spacing:.12em;text-transform:uppercase;
           color:var(--ink-soft);margin:0 0 10px;}
  h1{font-size:clamp(26px,5vw,38px);font-weight:600;letter-spacing:-.01em;margin:0 0 18px;line-height:1.15;}
  .status{display:inline-flex;align-items:center;gap:9px;font-family:var(--mono);font-size:12.5px;
          color:var(--ink-soft);padding:6px 12px 6px 10px;border:1px solid var(--rule);
          border-radius:999px;background:var(--card);}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--ink-soft);transition:background .3s;}
  .dot.active{background:var(--signal);animation:pulse 2.4s infinite;}
  @keyframes pulse{0%{box-shadow:0 0 0 0 var(--signal-soft);}70%{box-shadow:0 0 0 7px transparent;}100%{box-shadow:0 0 0 0 transparent;}}
  @media (prefers-reduced-motion:reduce){.dot.active{animation:none;}}
  section{margin-top:34px;}
  .label{font-family:var(--mono);font-size:11px;letter-spacing:.12em;text-transform:uppercase;
         color:var(--ink-soft);margin:0 0 12px;}
  .panel{background:var(--card);border:1px solid var(--rule);border-radius:14px;padding:8px 30px 24px;}

  /* --- time section --- */
  .time-total{font-family:var(--mono);font-size:20px;font-weight:600;margin:16px 0 12px;}
  .time-bar{display:flex;align-items:center;gap:8px;padding:3px 0;}
  .time-bar .day{font-family:var(--mono);font-size:11px;color:var(--ink-soft);min-width:54px;}
  .time-bar .bar{height:6px;background:var(--signal);border-radius:3px;min-width:2px;transition:width .3s;}
  .time-bar .dur{font-family:var(--mono);font-size:11px;color:var(--ink-soft);}

  /* --- summary --- */
  .summary{background:var(--card);border:1px solid var(--rule);border-left:3px solid var(--signal);
           border-radius:10px;padding:18px 22px;font-size:15.5px;}

  /* --- ledger (scrollable) --- */
  .ledger-wrap{position:relative;}
  .ledger{max-height:70vh;overflow-y:auto;padding:8px 30px 24px;background:var(--card);
          border:1px solid var(--rule);border-radius:14px;}
  .ledger-fade{position:absolute;bottom:0;left:0;right:0;height:32px;pointer-events:none;
               background:linear-gradient(transparent,var(--card));border-radius:0 0 14px 14px;}
  .ledger>*:first-child{margin-top:24px;}
  .ledger h1{font-size:21px;}.ledger h2{font-size:18px;}.ledger h3{font-size:15.5px;color:var(--ink-soft);}
  .ledger h2,.ledger h3{padding-top:18px;border-top:1px solid var(--rule);margin:30px 0 8px;font-weight:600;}
  .ledger p,.ledger li{font-size:15.5px;}.ledger ul,.ledger ol{padding-left:22px;}.ledger li{margin:4px 0;}
  .ledger code{font-family:var(--mono);font-size:13px;background:var(--paper);padding:1px 5px;border-radius:4px;}
  .ledger pre{background:var(--paper);border:1px solid var(--rule);border-radius:8px;padding:14px;overflow-x:auto;}
  .ledger pre code{background:none;padding:0;}
  .ledger a{color:var(--signal);}
  .ledger img{max-width:100%;height:auto;border-radius:8px;margin:8px 0;}
  .ledger table{border-collapse:collapse;width:100%;margin:12px 0;}
  .ledger th,.ledger td{border:1px solid var(--rule);padding:6px 10px;font-size:14px;text-align:left;}
  .ledger th{background:var(--paper);font-weight:600;}
  .empty{color:var(--ink-soft);font-style:italic;}

  /* --- section summaries --- */
  .section-summary{font-size:13.5px;color:var(--ink-soft);font-style:italic;
                   padding:6px 14px;margin:4px 0 12px;border-left:2px solid var(--rule);}

  /* --- annotations --- */
  .ledger mark{background:var(--signal-soft);border-radius:2px;cursor:pointer;padding:0 2px;}
  .annot-pop{position:fixed;z-index:100;background:var(--card);border:1px solid var(--rule);
             border-radius:10px;padding:14px;box-shadow:0 4px 20px rgba(0,0,0,.12);width:300px;}
  .annot-pop textarea{min-height:50px;margin-bottom:8px;}
  .annot-tip{position:fixed;z-index:90;background:var(--card);border:1px solid var(--rule);
             border-radius:8px;padding:10px 14px;font-size:13px;white-space:pre-wrap;
             box-shadow:0 4px 16px rgba(0,0,0,.1);max-width:280px;}
  .annot-tip .annot-del{margin-top:8px;font-size:12px;color:#b94040;cursor:pointer;
                        background:none;border:none;padding:0;font-family:var(--sans);}
  .detached-note{padding:8px 0;border-bottom:1px solid var(--rule);font-size:13.5px;}
  .detached-note:last-child{border-bottom:none;}
  .detached-note .dn-anchor{font-style:italic;color:var(--ink-soft);}
  .detached-note .dn-body{margin-top:4px;}

  .cmt{padding:14px 0;border-bottom:1px solid var(--rule);font-size:15px;white-space:pre-wrap;}
  .cmt:last-child{border-bottom:none;}
  .cmt .when{font-family:var(--mono);font-size:11px;color:var(--ink-soft);display:block;margin-top:6px;}
  textarea,input[type=text],input[type=email],select{width:100%;font-family:var(--sans);font-size:14.5px;
    color:var(--ink);background:var(--paper);border:1px solid var(--rule);border-radius:8px;padding:10px 12px;}
  textarea{min-height:74px;resize:vertical;}
  .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:10px;}
  button{font-family:var(--sans);font-size:14px;font-weight:500;color:#fff;background:var(--signal);
    border:none;border-radius:8px;padding:10px 18px;cursor:pointer;}
  button.ghost{background:transparent;color:var(--signal);border:1px solid var(--signal);}
  button:focus-visible,a:focus-visible,textarea:focus-visible,input:focus-visible,select:focus-visible{
    outline:2px solid var(--signal);outline-offset:2px;}
  .file{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid var(--rule);font-size:14.5px;}
  .file:last-child{border-bottom:none;}
  .file .meta{font-family:var(--mono);font-size:11px;color:var(--ink-soft);}
  .file a{color:var(--signal);text-decoration:none;}
  .prefgrid{display:grid;gap:12px;}
  .check{display:flex;align-items:center;gap:8px;font-size:14.5px;}
  .check input{width:auto;}
  footer{margin-top:28px;font-family:var(--mono);font-size:11.5px;color:var(--ink-soft);text-align:center;}
  .flash{animation:flash 1.1s ease;}@keyframes flash{0%{background:var(--signal-soft);}100%{background:var(--card);}}
  @media (prefers-reduced-motion:reduce){.flash{animation:none;}}
</style></head>
<body><div class="wrap">
  <header>
    <p class="eyebrow">Engagement Work Log</p>
    <h1>{{NAME}}</h1>
    <span class="status"><span class="dot" id="dot"></span><span id="status-text">Idle</span></span>
  </header>

  <section id="time-wrap" hidden>
    <p class="label">Time</p>
    <div class="panel">
      <div class="time-total" id="time-total"></div>
      <div id="time-bars"></div>
    </div>
  </section>

  <section id="summary-wrap" hidden>
    <p class="label">Summary</p>
    <div class="summary" id="summary"></div>
  </section>

  <section>
    <p class="label">Activity</p>
    <div class="ledger-wrap">
      <main class="ledger" id="ledger"></main>
      <div class="ledger-fade"></div>
    </div>
    <footer id="updated" data-iso=""></footer>
  </section>

  <section id="detached-wrap" hidden>
    <p class="label">Notes (text no longer in log)</p>
    <div class="panel" id="detached"></div>
  </section>

  <section>
    <p class="label">Files &amp; benchmarks</p>
    <div class="panel">
      <div id="files"></div>
      <div class="row">
        <input type="file" id="fileInput">
        <button id="uploadBtn" class="ghost">Upload</button>
        <span id="uploadMsg" class="meta"></span>
      </div>
    </div>
  </section>

  <section>
    <p class="label">Comments</p>
    <div class="panel">
      <div id="comments"></div>
      <div class="row"><textarea id="cmtBox" placeholder="Leave a comment for the developer…"></textarea></div>
      <div class="row"><button id="cmtBtn">Post comment</button></div>
    </div>
  </section>

  <section>
    <p class="label">Notifications</p>
    <div class="panel">
      <div class="prefgrid">
        <label class="check"><input type="checkbox" id="chEmail"> Email me at:</label>
        <input type="email" id="emailAddr" placeholder="you@company.com">
        <label class="check"><input type="checkbox" id="chNtfy"> Push via ntfy topic:</label>
        <input type="text" id="ntfyTopic" placeholder="my-private-topic">
        <label class="check">How often:&nbsp;
          <select id="freq">
            <option value="work_30min">Every 30 min of work</option>
            <option value="daily_4pm">Daily at 4 PM</option>
            <option value="off">Off</option>
          </select>
        </label>
      </div>
      <div class="row"><button id="prefBtn" class="ghost">Save preferences</button>
        <span id="prefMsg" class="meta"></span></div>
    </div>
  </section>
</div>

<div id="annot-pop" class="annot-pop" hidden></div>
<div id="annot-tip" class="annot-tip" hidden></div>

<script>
const token="{{TOKEN}}";
const $=id=>document.getElementById(id);
function fmtSize(b){if(b<1024)return b+" B";if(b<1048576)return (b/1024).toFixed(0)+" KB";return (b/1048576).toFixed(1)+" MB";}
function fmtTime(iso){return iso?new Date(iso).toLocaleString():"";}
function fmtDur(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h?h+"h "+m+"m":m+"m";}
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}

let _annotations=[];
let _annotSelText="";
let _annotRange=null;

async function load(flash){
  const r=await fetch(`/log/${token}/raw`,{cache:"no-store"});
  const d=await r.json();

  // --- status dot ---
  const dot=$("dot");
  if(d.working){dot.classList.add("active");$("status-text").textContent=d.work_note?"Working - "+d.work_note:"Working";}
  else{dot.classList.remove("active");$("status-text").textContent="Idle";}

  // --- time ---
  if(d.sessions&&d.sessions.length){
    $("time-wrap").hidden=false;
    $("time-total").textContent=fmtDur(d.total_time_seconds);
    const byDay={};
    d.sessions.forEach(s=>{
      const day=s.started_at.slice(0,10);
      const start=new Date(s.started_at);
      const end=s.ended_at?new Date(s.ended_at):new Date();
      byDay[day]=(byDay[day]||0)+(end-start)/1000;
    });
    const days=Object.keys(byDay).sort();
    const maxSec=Math.max(...Object.values(byDay),1);
    $("time-bars").innerHTML=days.map(day=>{
      const pct=Math.max(2,byDay[day]/maxSec*100);
      const label=new Date(day+"T00:00:00").toLocaleDateString(undefined,{month:"short",day:"numeric"});
      return `<div class="time-bar"><span class="day">${label}</span><span class="bar" style="width:${pct}%"></span><span class="dur">${fmtDur(byDay[day])}</span></div>`;
    }).join("");
  } else {$("time-wrap").hidden=true;}

  // --- summary ---
  if(d.summary){$("summary").textContent=d.summary;$("summary-wrap").hidden=false;}
  else{$("summary-wrap").hidden=true;}

  // --- ledger ---
  const led=$("ledger");
  const wasAtBottom=led.scrollHeight-led.scrollTop-led.clientHeight<50;
  led.innerHTML=d.log_html;

  // --- section summaries ---
  if(d.section_summaries&&d.section_summaries.length){
    const h2s=led.querySelectorAll("h2");
    for(const h2 of h2s){
      const txt=h2.textContent.trim();
      const m=d.section_summaries.find(s=>s.date_header===txt);
      if(m){const div=document.createElement("div");div.className="section-summary";div.textContent=m.summary;h2.after(div);}
    }
  }

  // --- annotations ---
  _annotations=d.annotations||[];
  applyAnnotations();

  if(flash){led.classList.remove("flash");void led.offsetWidth;led.classList.add("flash");}
  if(!load._scrolled){led.scrollTop=led.scrollHeight;load._scrolled=true;}
  else if(wasAtBottom){led.scrollTop=led.scrollHeight;}

  $("updated").textContent=d.updated_at?("Last updated "+fmtTime(d.updated_at)):"";

  // --- files ---
  $("files").innerHTML=d.files.length?d.files.map(f=>
    `<div class="file"><a href="/log/${token}/file/${f.id}">${esc(f.original_name)}</a>
     <span class="meta">${fmtSize(f.size)} · ${f.uploaded_by} · ${fmtTime(f.created_at)}</span></div>`).join("")
    :'<p class="empty">No files yet.</p>';

  // --- comments ---
  $("comments").innerHTML=d.comments.length?d.comments.map(c=>
    `<div class="cmt">${c.body}<span class="when">${fmtTime(c.created_at)}</span></div>`).join("")
    :'<p class="empty">No comments yet.</p>';

  // --- prefs ---
  if(!load._prefsLoaded&&d.prefs){
    $("chEmail").checked=!!d.prefs.channel_email;$("emailAddr").value=d.prefs.email||"";
    $("chNtfy").checked=!!d.prefs.channel_ntfy;$("ntfyTopic").value=d.prefs.ntfy_topic||"";
    $("freq").value=d.prefs.frequency||"work_30min";load._prefsLoaded=true;
  }
}

// --- annotations: apply highlights + detached notes ---
function applyAnnotations(){
  const led=$("ledger");
  const detached=[];
  for(const a of _annotations){
    if(!wrapText(led,a.anchor_text,a.id)) detached.push(a);
  }
  if(detached.length){
    $("detached-wrap").hidden=false;
    $("detached").innerHTML=detached.map(a=>
      `<div class="detached-note"><div class="dn-anchor">"${esc(a.anchor_text)}"</div>
       <div class="dn-body">${a.body}</div>
       <button class="annot-del" onclick="delAnnot('${a.id}')">Delete</button></div>`).join("");
  } else {$("detached-wrap").hidden=true;}
}

function wrapText(root,text,id){
  const walker=document.createTreeWalker(root,NodeFilter.SHOW_TEXT);
  while(walker.nextNode()){
    const node=walker.currentNode;
    const idx=node.textContent.indexOf(text);
    if(idx===-1||node.parentElement.closest("mark")) continue;
    const range=document.createRange();
    range.setStart(node,idx);range.setEnd(node,idx+text.length);
    const mark=document.createElement("mark");
    mark.dataset.annotId=id;
    range.surroundContents(mark);
    return true;
  }
  return false;
}

// --- annotation: show tooltip on mark click ---
document.addEventListener("click",e=>{
  const mark=e.target.closest("mark[data-annot-id]");
  const tip=$("annot-tip");
  if(!mark){if(!e.target.closest(".annot-tip"))tip.hidden=true;return;}
  const a=_annotations.find(x=>x.id===mark.dataset.annotId);
  if(!a){tip.hidden=true;return;}
  const rect=mark.getBoundingClientRect();
  tip.innerHTML=`<div>${a.body}</div><button class="annot-del" onclick="delAnnot('${a.id}')">Delete note</button>`;
  tip.hidden=false;
  tip.style.left=Math.min(rect.left,window.innerWidth-290)+"px";
  tip.style.top=(rect.top-tip.offsetHeight-8)+"px";
  if(parseInt(tip.style.top)<0) tip.style.top=(rect.bottom+8)+"px";
});

async function delAnnot(id){
  await fetch(`/log/${token}/annotate/${id}`,{method:"DELETE"});
  $("annot-tip").hidden=true;
  load(false);
}

// --- annotation: text selection -> popover ---
document.addEventListener("mouseup",e=>{
  if(e.target.closest(".annot-pop")) return;
  const sel=window.getSelection();
  if(!sel.rangeCount||sel.isCollapsed){hideAnnotPop();return;}
  const range=sel.getRangeAt(0);
  if(!$("ledger").contains(range.commonAncestorContainer)){hideAnnotPop();return;}
  const text=sel.toString().trim();
  if(!text||text.length>500){hideAnnotPop();return;}
  _annotSelText=text;_annotRange=range.cloneRange();
  const rect=range.getBoundingClientRect();
  const pop=$("annot-pop");
  pop.innerHTML=`<textarea id="annot-body" placeholder="Add a note about this selection…"></textarea>
    <div class="row"><button id="annot-submit">Save note</button>
    <button id="annot-cancel" class="ghost" onclick="hideAnnotPop()">Cancel</button></div>`;
  pop.hidden=false;
  pop.style.left=Math.min(rect.left,window.innerWidth-320)+"px";
  pop.style.top=(rect.bottom+8)+"px";
  pop.querySelector("#annot-submit").onclick=submitAnnot;
  pop.querySelector("#annot-body").focus();
});

function hideAnnotPop(){$("annot-pop").hidden=true;_annotSelText="";_annotRange=null;}

async function submitAnnot(){
  const body=document.querySelector("#annot-body").value.trim();
  if(!body||!_annotSelText)return;
  await fetch(`/log/${token}/annotate`,{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({anchor_text:_annotSelText,body})});
  hideAnnotPop();window.getSelection().removeAllRanges();load(false);
}

// --- actions ---
$("cmtBtn").onclick=async()=>{
  const body=$("cmtBox").value.trim();if(!body)return;
  await fetch(`/log/${token}/comment`,{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({body})});
  $("cmtBox").value="";load(false);
};
$("uploadBtn").onclick=async()=>{
  const f=$("fileInput").files[0];if(!f){$("uploadMsg").textContent="Choose a file first.";return;}
  const fd=new FormData();fd.append("file",f);$("uploadMsg").textContent="Uploading…";
  const r=await fetch(`/log/${token}/upload`,{method:"POST",body:fd});
  if(r.ok){$("uploadMsg").textContent="Uploaded.";$("fileInput").value="";load(false);}
  else{const e=await r.json().catch(()=>({}));$("uploadMsg").textContent=e.detail||"Upload failed.";}
};
$("prefBtn").onclick=async()=>{
  const payload={channel_email:$("chEmail").checked,email:$("emailAddr").value.trim(),
    channel_ntfy:$("chNtfy").checked,ntfy_topic:$("ntfyTopic").value.trim(),frequency:$("freq").value};
  await fetch(`/log/${token}/prefs`,{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(payload)});
  $("prefMsg").textContent="Saved.";setTimeout(()=>$("prefMsg").textContent="",2500);
};

// --- SSE ---
let es;
function connect(){
  es=new EventSource(`/log/${token}/stream`);
  es.addEventListener("update",()=>load(true));
  es.addEventListener("status",()=>load(false));
  es.addEventListener("ping",()=>{});
  es.onerror=()=>{$("status-text").textContent="Reconnecting";es.close();setTimeout(connect,3000);};
}
load(false);connect();
</script>
</body></html>"""
