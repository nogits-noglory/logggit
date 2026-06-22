"""
Work-log - LOCAL ADMIN APP (runs on your machine, bound to localhost).

This is the side that never touches the public droplet. It holds my
job metadata and auth *references*, scaffolds the Obsidian folder for each
job, watches your work logs and pushes them up, and shows you the comments
and files clients leave.

Run:  uvicorn app:app --host 127.0.0.1 --port 8800
Then open http://127.0.0.1:8800

What stays here and never leaves the machine:
  - contact, company, repo link, auth reference, params  (local SQLite only)
What goes to the droplet:
  - client_id, display name, view token, notify defaults  (via /provision)
  - the contents of worklog.md                            (via /ingest)

worklog.md is client-visible by design, so the scaffold deliberately holds
no contact details or auth references - only the dated log itself.
"""

import json
import re
import sqlite3
import secrets
import threading
import time
from pathlib import Path

import requests
import yaml
from fastapi import FastAPI, Form, Request, Response, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse
from contextlib import asynccontextmanager
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

BASE = Path(__file__).resolve().parent
CFG = yaml.safe_load((BASE / "config.yaml").read_text())
DROPLET = CFG["droplet_url"].rstrip("/")
ADMIN_SECRET = CFG["admin_secret"]
VAULT = Path(CFG["vault_clients_dir"]).expanduser()
LOCAL_PASSWORD = CFG.get("local_password", "")
OLLAMA_URL = CFG.get("ollama_url", "").rstrip("/")
OLLAMA_MODEL = CFG.get("ollama_model", "phi3.5")
GROQ_API_KEY = CFG.get("groq_api_key", "")
GROQ_MODEL = CFG.get("groq_model", "llama-3.3-70b-versatile")
DB_PATH = BASE / "admin.db"

SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{1,48}$")


def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS jobs(
                 client_id TEXT PRIMARY KEY,
                 name TEXT, company TEXT, contact TEXT, repo_link TEXT,
                 auth_reference TEXT, params TEXT, token TEXT, created_at TEXT,
                 status TEXT DEFAULT 'active')""")
        # migrate existing rows that predate the status column
        try:
            c.execute("ALTER TABLE jobs ADD COLUMN status TEXT DEFAULT 'active'")
        except Exception:
            pass


def auth_headers():
    return {"Authorization": f"Bearer {ADMIN_SECRET}"}


# ---------------------------------------------------------------- summaries
# Debounced: generate after 5 pushes OR 30 minutes since last summary, not on every save.
_push_counts: dict[str, int] = {}
_last_summary_at: dict[str, float] = {}
SUMMARY_PUSH_THRESHOLD = 3
SUMMARY_TIME_THRESHOLD = 15 * 60   # 15 minutes


def _build_time_ctx(sessions: list | None) -> str:
    if not sessions:
        return ""
    total = 0
    for s in sessions:
        try:
            from datetime import datetime
            start = datetime.fromisoformat(s["started_at"])
            end = datetime.fromisoformat(s["ended_at"]) if s.get("ended_at") else datetime.now(
                start.tzinfo)
            total += (end - start).total_seconds()
        except Exception:
            pass
    if total > 0:
        hours, mins = divmod(int(total), 3600)
        return f"\nTime tracked: {hours}h {mins // 60}m across {len(sessions)} sessions.\n"
    return ""


def _call_groq(messages: list, max_tokens: int = 300, json_mode: bool = False) -> str | None:
    """Call Groq API. Returns response text or None."""
    if not GROQ_API_KEY:
        return None
    body: dict = {"model": GROQ_MODEL, "messages": messages, "max_tokens": max_tokens,
                  "temperature": 0.3}
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                          headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                                   "Content-Type": "application/json"},
                          json=body, timeout=30)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
        print(f"[groq] {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[groq] error: {e}")
    return None


def _generate_summary(text: str, sessions: list | None = None) -> tuple[str | None, list | None]:
    """Returns (overall_summary, section_summaries). Groq first, Ollama fallback."""
    if not (text or "").strip():
        return None, None

    time_ctx = _build_time_ctx(sessions)

    system = ("You summarize freelance work logs for clients. The log title states the "
              "project goal. Entries under ## date headers are grouped by day. Timestamps "
              "mark individual updates. Be concise, plain, and concrete. No filler.")

    # --- overall summary ---
    overall = _call_groq([
        {"role": "system", "content": system},
        {"role": "user", "content":
         "Write a 3-4 sentence summary for the client: what has been accomplished "
         "toward the project goal, where things stand now, and what's next. "
         "No bullet points, no preamble, just the sentences." + time_ctx +
         "\n\n" + text[:12000]}
    ], max_tokens=200)

    if overall:
        print(f"[summary] groq overall: {len(overall)} chars")
    else:
        # Ollama fallback
        if OLLAMA_URL:
            try:
                r = requests.post(f"{OLLAMA_URL}/api/generate",
                                  json={"model": OLLAMA_MODEL, "stream": False,
                                        "prompt": system + "\n\n" + text[:8000]},
                                  timeout=60)
                if r.status_code == 200:
                    overall = r.json().get("response", "").strip() or None
                    print(f"[summary] ollama fallback: {len(overall or '')} chars")
            except Exception as e:
                print(f"[summary] ollama fallback failed: {e}")

    if not overall:
        return None, None

    # --- section summaries ---
    dates = re.findall(r'^## (\d{4}-\d{2}-\d{2})', text, re.MULTILINE)
    sections = None
    if dates:
        sec_result = _call_groq([
            {"role": "system", "content": system},
            {"role": "user", "content":
             "For each date in this work log, write ONE short sentence summarizing "
             "what was accomplished that day. Return JSON only.\n"
             'Format: {"sections": [{"date_header": "YYYY-MM-DD", "summary": "..."}]}\n\n'
             + text[:12000]}
        ], max_tokens=500, json_mode=True)
        if sec_result:
            try:
                parsed = json.loads(sec_result)
                sections = parsed.get("sections", parsed if isinstance(parsed, list) else None)
                print(f"[summary] groq sections: {len(sections or [])} entries")
            except (json.JSONDecodeError, TypeError):
                print("[summary] groq sections: JSON parse failed")

    return overall, sections


def summary_loop():
    """Every 60s, summarise any client whose log has 5+ new pushes or hasn't been
    summarised in 30 minutes. Sends the summary to the droplet bundled with the
    current markdown so the page updates without a separate round-trip."""
    while True:
        time.sleep(60)
        now = time.monotonic()
        try:
            clients = list(_push_counts.keys())
            for cid in clients:
                count = _push_counts.get(cid, 0)
                last = _last_summary_at.get(cid, 0)
                due = count >= SUMMARY_PUSH_THRESHOLD or (count > 0 and now - last >= SUMMARY_TIME_THRESHOLD)
                if not due:
                    continue
                wl = VAULT / cid / "worklog.md"
                if not wl.exists():
                    continue
                text = wl.read_text(encoding="utf-8")
                sessions = None
                try:
                    ws = requests.get(f"{DROPLET}/work/{cid}/status",
                                      headers=auth_headers(), timeout=10).json()
                    sessions = ws.get("sessions")
                except Exception:
                    pass
                overall, sections = _generate_summary(text, sessions)
                if overall is None:
                    continue
                _push_counts[cid] = 0
                _last_summary_at[cid] = now
                payload: dict = {"client_id": cid, "markdown": text, "summary": overall}
                if sections:
                    payload["section_summaries"] = sections
                try:
                    r = requests.post(f"{DROPLET}/ingest", headers=auth_headers(),
                                      json=payload, timeout=15)
                    print(f"[summary] {cid}: pushed ({r.status_code})"
                          + (f" + {len(sections)} section(s)" if sections else ""))
                except requests.RequestException as e:
                    print(f"[summary] {cid}: push failed: {e}")
        except Exception as e:
            print(f"[summary_loop] {e}")


# ---------------------------------------------------------------- the watcher
_last: dict[str, float] = {}


def push_log(client_id: str, path: Path, force: bool = False):
    now = time.monotonic()
    if not force and now - _last.get(client_id, 0) < 1.0:
        return
    _last[client_id] = now
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    if not text.strip():
        return
    try:
        r = requests.post(f"{DROPLET}/ingest", headers=auth_headers(),
                          json={"client_id": client_id, "markdown": text}, timeout=15)
        print(f"[ingest] {client_id}: {r.status_code}")
        _push_counts[client_id] = _push_counts.get(client_id, 0) + 1
    except requests.RequestException as e:
        print(f"[ingest] {client_id}: {e}")


class Handler(FileSystemEventHandler):
    def _maybe(self, p):
        p = Path(p)
        if p.name != "worklog.md":
            return
        try:
            rel = p.resolve().relative_to(VAULT.resolve())
        except ValueError:
            return
        if len(rel.parts) == 2:
            push_log(rel.parts[0], p)

    def on_modified(self, e):
        if not e.is_directory:
            self._maybe(e.src_path)

    def on_created(self, e):
        if not e.is_directory:
            self._maybe(e.src_path)


def start_watcher():
    if not VAULT.exists():
        print(f"[watcher] vault dir missing: {VAULT} (jobs will still scaffold it)")
        VAULT.mkdir(parents=True, exist_ok=True)
    obs = Observer()
    obs.schedule(Handler(), str(VAULT), recursive=True)
    obs.daemon = True
    obs.start()
    print(f"[watcher] watching {VAULT}")
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=comment_sync_loop, daemon=True).start()
    if OLLAMA_URL:
        threading.Thread(target=summary_loop, daemon=True).start()
        print(f"[summary] enabled via {OLLAMA_URL} model={OLLAMA_MODEL}")


# mtime poll: a reliable backstop in case the filesystem watcher misses an event
# (overlay filesystems, network sync, some editors). Respects the same debounce,
# so it won't double-send when the watcher already caught a change.
_mtimes: dict[str, float] = {}


def poll_loop():
    while True:
        try:
            for wl in VAULT.glob("*/worklog.md"):
                cid = wl.parent.name
                try:
                    m = wl.stat().st_mtime
                except OSError:
                    continue
                if _mtimes.get(cid) != m:
                    _mtimes[cid] = m
                    push_log(cid, wl)
        except Exception as e:
            print(f"[poll] {e}")
        time.sleep(2)


def comment_sync_loop():
    """Every 30s, pull client comments and write them to comments.md in each vault folder."""
    while True:
        try:
            with db() as c:
                jobs = [dict(r) for r in c.execute("SELECT client_id, name FROM jobs")]
            for j in jobs:
                cid = j["client_id"]
                folder = VAULT / cid
                if not folder.exists():
                    continue
                try:
                    d = requests.get(f"{DROPLET}/admin/{cid}",
                                     headers=auth_headers(), timeout=10).json()
                except Exception:
                    continue
                comments = d.get("comments", [])
                annotations = d.get("annotations", [])
                dest = folder / "comments.md"
                if not comments:
                    if dest.exists():
                        dest.unlink()
                else:
                    lines = [f"# {j['name']} - Client Comments\n"]
                    for cm in comments:
                        lines.append(f"\n**{cm['created_at']}**\n\n{cm['body']}\n\n---")
                    new_text = "\n".join(lines) + "\n"
                    if not dest.exists() or dest.read_text(encoding="utf-8") != new_text:
                        dest.write_text(new_text, encoding="utf-8")
                        print(f"[comments] synced {len(comments)} comment(s) → {dest}")
                adest = folder / "annotations.md"
                if not annotations:
                    if adest.exists():
                        adest.unlink()
                else:
                    alines = [f"# {j['name']} - Client Annotations\n"]
                    for a in annotations:
                        alines.append(f"\n**{a['created_at']}**\n\n> {a['anchor_text']}\n\n{a['body']}\n\n---")
                    anew = "\n".join(alines) + "\n"
                    if not adest.exists() or adest.read_text(encoding="utf-8") != anew:
                        adest.write_text(anew, encoding="utf-8")
                        print(f"[annotations] synced {len(annotations)} annotation(s) → {adest}")
        except Exception as e:
            print(f"[comment_sync] {e}")
        time.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    threading.Thread(target=start_watcher, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


# ------------------------------------------------------------------ minimal auth
def authed(session: str | None) -> bool:
    if not LOCAL_PASSWORD:
        return True
    return session == LOCAL_PASSWORD


def login_page(msg=""):
    return HTMLResponse(f"""{HEAD}<div class="wrap"><h1>Work Log Admin</h1>
      <form method="post" action="/login"><p class="label">Password</p>
      <input type="password" name="password" autofocus>
      <div class="row"><button>Enter</button> <span class="meta">{msg}</span></div></form>
      </div></body></html>""")


@app.post("/login")
async def login(password: str = Form("")):
    if not LOCAL_PASSWORD or password == LOCAL_PASSWORD:
        r = RedirectResponse("/", status_code=303)
        r.set_cookie("session", password, httponly=True, samesite="lax")
        return r
    return login_page("Wrong password.")


# ----------------------------------------------------------------------- views
STATUS_BADGE = {
    "active":    "",
    "completed": "<span class='badge badge-done'>Completed</span>",
    "aborted":   "<span class='badge badge-abort'>Aborted</span>",
}


@app.get("/", response_class=HTMLResponse)
async def home(session: str | None = Cookie(default=None)):
    if not authed(session):
        return login_page()
    with db() as c:
        jobs = [dict(r) for r in c.execute("SELECT * FROM jobs ORDER BY created_at DESC")]
    rows = "".join(
        f"""<a class="job {'job-inactive' if j.get('status') in ('completed','aborted') else ''}" href="/job/{j['client_id']}">
            <span class="jn">{j['name']} {STATUS_BADGE.get(j.get('status','active'),'')}</span>
            <span class="meta">{j['company'] or ''} · {j['client_id']}</span></a>"""
        for j in jobs) or '<p class="empty">No jobs yet. Create one below.</p>'
    return HTMLResponse(f"""{HEAD}<div class="wrap">
      <header><p class="eyebrow">Local Admin</p><h1>Jobs</h1></header>
      <section><div class="panel">{rows}</div></section>
      <section><p class="label">New job</p><div class="panel">
        <form method="post" action="/job/new" class="grid">
          <div><p class="label">Client ID (slug)</p><input name="client_id" placeholder="acme-rag-build" required></div>
          <div><p class="label">Display name</p><input name="name" placeholder="Acme - RAG Memory System" required></div>
          <div><p class="label">Company</p><input name="company" placeholder="Acme Inc."></div>
          <div><p class="label">Contact</p><input name="contact" placeholder="jane@acme.com"></div>
          <div><p class="label">Repo link</p><input name="repo_link" placeholder="https://github.com/..."></div>
          <div><p class="label">Auth reference (where the secret lives - not the secret)</p>
               <input name="auth_reference" placeholder="1Password &gt; Acme &gt; deploy key"></div>
          <div class="full"><p class="label">Parameters / notes</p><textarea name="params"></textarea></div>
          <div class="full"><p class="label">Client notify defaults (optional)</p>
               <input name="email" placeholder="client email"> </div>
          <div class="full row"><button>Create job</button></div>
        </form></div></section></div></body></html>""")


@app.post("/job/new")
async def job_new(session: str | None = Cookie(default=None),
                  client_id: str = Form(...), name: str = Form(...),
                  company: str = Form(""), contact: str = Form(""),
                  repo_link: str = Form(""), auth_reference: str = Form(""),
                  params: str = Form(""), email: str = Form("")):
    if not authed(session):
        return login_page()
    client_id = client_id.strip().lower()
    if not SLUG.match(client_id):
        return HTMLResponse(f"{HEAD}<div class='wrap'><p class='empty'>Invalid client ID. "
                            f"Use lowercase letters, numbers, hyphens.</p>"
                            f"<a href='/'>Back</a></div></body></html>")

    token = secrets.token_urlsafe(32)

    # 1) store admin-only metadata locally (auth_reference never leaves this machine)
    with db() as c:
        c.execute("""INSERT OR REPLACE INTO jobs
            (client_id,name,company,contact,repo_link,auth_reference,params,token,created_at,status)
            VALUES (?,?,?,?,?,?,?,?,?,'active')""",
            (client_id, name, company, contact, repo_link, auth_reference, params, token,
             time.strftime("%Y-%m-%dT%H:%M:%S")))

    # 2) register the client on the droplet FIRST, so any file event the watcher
    #    sees from the scaffold below lands on an existing client (no 404 race).
    payload = {"client_id": client_id, "name": name, "token": token}
    if email:
        payload["email"] = email
    try:
        requests.post(f"{DROPLET}/provision", headers=auth_headers(), json=payload, timeout=15)
    except requests.RequestException as e:
        print(f"[provision] {client_id}: {e}")

    # 3) scaffold the vault folder. worklog.md is CLIENT-VISIBLE: no contact/auth here.
    folder = VAULT / client_id
    folder.mkdir(parents=True, exist_ok=True)
    wl = folder / "worklog.md"
    if not wl.exists():
        today = time.strftime("%Y-%m-%d")
        wl.write_text(f"# {name} - Work Log\n\n## {today}\n- Engagement started.\n",
                      encoding="utf-8")

    # 4) push the initial log, bypassing debounce so it can't be swallowed by a
    #    near-simultaneous watcher event.
    push_log(client_id, wl, force=True)
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.post("/job/{client_id}/status")
async def job_set_status(client_id: str, status: str = Form(...),
                         session: str | None = Cookie(default=None)):
    if not authed(session):
        return login_page()
    if status not in ("active", "completed", "aborted"):
        status = "active"
    with db() as c:
        c.execute("UPDATE jobs SET status=? WHERE client_id=?", (status, client_id))
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.post("/job/{client_id}/work/start")
async def work_start(client_id: str, session: str | None = Cookie(default=None),
                     note: str = Form("")):
    if not authed(session):
        return login_page()
    try:
        requests.post(f"{DROPLET}/work/{client_id}/start", headers=auth_headers(), timeout=10)
        if note.strip():
            requests.post(f"{DROPLET}/work/{client_id}/note", headers=auth_headers(),
                          json={"note": note.strip()}, timeout=10)
    except requests.RequestException as e:
        print(f"[work] start {client_id}: {e}")
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.post("/job/{client_id}/work/note")
async def work_note(client_id: str, session: str | None = Cookie(default=None),
                    note: str = Form("")):
    if not authed(session):
        return login_page()
    try:
        requests.post(f"{DROPLET}/work/{client_id}/note", headers=auth_headers(),
                      json={"note": note.strip()}, timeout=10)
    except requests.RequestException as e:
        print(f"[work] note {client_id}: {e}")
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.post("/job/{client_id}/work/stop")
async def work_stop(client_id: str, session: str | None = Cookie(default=None)):
    if not authed(session):
        return login_page()
    try:
        requests.post(f"{DROPLET}/work/{client_id}/stop", headers=auth_headers(), timeout=10)
    except requests.RequestException as e:
        print(f"[work] stop {client_id}: {e}")
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.get("/job/{client_id}/edit", response_class=HTMLResponse)
async def job_edit(client_id: str, session: str | None = Cookie(default=None)):
    if not authed(session):
        return login_page()
    with db() as c:
        j = c.execute("SELECT * FROM jobs WHERE client_id=?", (client_id,)).fetchone()
    if not j:
        return HTMLResponse(f"{HEAD}<div class='wrap'><p class='empty'>No such job.</p>"
                            f"<a href='/'>Back</a></div></body></html>")
    return HTMLResponse(f"""{HEAD}<div class="wrap">
      <header><p class="eyebrow">Edit Job</p><h1>{_esc(j['name'])}</h1>
        <p class="meta">ID: {_esc(j['client_id'])} (not editable)</p></header>
      <section><div class="panel">
        <form method="post" action="/job/{client_id}/edit" class="grid">
          <div class="full"><p class="label">Display name</p>
               <input name="name" value="{_esc(j['name'])}" required></div>
          <div><p class="label">Company</p><input name="company" value="{_esc(j['company'] or '')}"></div>
          <div><p class="label">Contact</p><input name="contact" value="{_esc(j['contact'] or '')}"></div>
          <div class="full"><p class="label">Repo link</p>
               <input name="repo_link" value="{_esc(j['repo_link'] or '')}"></div>
          <div class="full"><p class="label">Auth reference</p>
               <input name="auth_reference" value="{_esc(j['auth_reference'] or '')}"></div>
          <div class="full"><p class="label">Parameters / notes</p>
               <textarea name="params">{_esc(j['params'] or '')}</textarea></div>
          <div class="full row"><button>Save changes</button>
               <a href="/job/{client_id}" class="btn-ghost" style="text-decoration:none">Cancel</a></div>
        </form></div></section></div></body></html>""")


@app.post("/job/{client_id}/edit")
async def job_edit_save(client_id: str, session: str | None = Cookie(default=None),
                        name: str = Form(...), company: str = Form(""),
                        contact: str = Form(""), repo_link: str = Form(""),
                        auth_reference: str = Form(""), params: str = Form("")):
    if not authed(session):
        return login_page()
    with db() as c:
        c.execute("""UPDATE jobs SET name=?, company=?, contact=?, repo_link=?,
                     auth_reference=?, params=? WHERE client_id=?""",
                  (name, company, contact, repo_link, auth_reference, params, client_id))
        j = c.execute("SELECT token FROM jobs WHERE client_id=?", (client_id,)).fetchone()
    if j:
        try:
            requests.post(f"{DROPLET}/provision", headers=auth_headers(),
                          json={"client_id": client_id, "name": name, "token": j["token"]},
                          timeout=15)
        except requests.RequestException:
            pass
    return RedirectResponse(f"/job/{client_id}", status_code=303)


@app.get("/job/{client_id}", response_class=HTMLResponse)
async def job_view(client_id: str, session: str | None = Cookie(default=None)):
    if not authed(session):
        return login_page()
    with db() as c:
        j = c.execute("SELECT * FROM jobs WHERE client_id=?", (client_id,)).fetchone()
    if not j:
        return HTMLResponse(f"{HEAD}<div class='wrap'><p class='empty'>No such job.</p>"
                            f"<a href='/'>Back</a></div></body></html>")
    share = f"{DROPLET}/log/{j['token']}"
    status = j["status"] or "active"

    comments, files = [], []
    working, total_secs = False, 0
    try:
        d = requests.get(f"{DROPLET}/admin/{client_id}", headers=auth_headers(), timeout=10).json()
        comments, files = d.get("comments", []), d.get("files", [])
    except requests.RequestException:
        pass
    work_note = ""
    try:
        ws = requests.get(f"{DROPLET}/work/{client_id}/status", headers=auth_headers(), timeout=10).json()
        working = ws.get("working", False)
        total_secs = ws.get("total_seconds", 0)
        work_note = ws.get("work_note", "")
    except Exception:
        pass

    cm = "".join(f"<div class='cmt'>{_esc(c['body'])}<span class='when'>{c['created_at']}</span></div>"
                 for c in comments) or "<p class='empty'>No comments yet.</p>"
    fl = "".join(f"<div class='file'><a href='{share.rsplit('/log/',1)[0]}/log/{j['token']}/file/{f['id']}'>{_esc(f['original_name'])}</a>"
                 f"<span class='meta'>{f['size']} B · {f['uploaded_by']}</span></div>"
                 for f in files) or "<p class='empty'>No files yet.</p>"

    # status controls: show buttons for transitions that make sense
    if status == "active":
        status_html = (f"<span class='badge badge-active'>Active</span>"
                       f"<form method='post' action='/job/{client_id}/status' style='display:inline;margin-left:12px'>"
                       f"<input type='hidden' name='status' value='completed'>"
                       f"<button class='btn-ghost'>Mark completed</button></form>"
                       f"<form method='post' action='/job/{client_id}/status' style='display:inline;margin-left:8px'>"
                       f"<input type='hidden' name='status' value='aborted'>"
                       f"<button class='btn-danger'>Mark aborted</button></form>")
    elif status == "completed":
        status_html = (f"<span class='badge badge-done'>Completed</span>"
                       f"<form method='post' action='/job/{client_id}/status' style='display:inline;margin-left:12px'>"
                       f"<input type='hidden' name='status' value='active'>"
                       f"<button class='btn-ghost'>Reopen</button></form>")
    else:
        status_html = (f"<span class='badge badge-abort'>Aborted</span>"
                       f"<form method='post' action='/job/{client_id}/status' style='display:inline;margin-left:12px'>"
                       f"<input type='hidden' name='status' value='active'>"
                       f"<button class='btn-ghost'>Reopen</button></form>")

    hours, mins = divmod(total_secs, 3600)
    time_display = f"{hours // 1}h {mins // 60}m"
    if working:
        work_btn = (f"<form method='post' action='/job/{client_id}/work/stop' style='display:inline;margin-left:12px'>"
                    f"<button class='btn-danger'>Stop working</button></form>"
                    f"<form method='post' action='/job/{client_id}/work/note' style='display:inline;margin-left:8px'>"
                    f"<input name='note' value='{_esc(work_note)}' placeholder='Status note…' "
                    f"style='width:200px;display:inline;padding:6px 10px;font-size:13px'>"
                    f"<button class='btn-ghost' style='margin-left:4px'>Update</button></form>")
    else:
        work_btn = (f"<form method='post' action='/job/{client_id}/work/start' style='display:inline;margin-left:12px'>"
                    f"<input name='note' placeholder='What are you working on?' "
                    f"style='width:200px;display:inline;padding:6px 10px;font-size:13px;margin-right:4px'>"
                    f"<button>Start working</button></form>")

    return HTMLResponse(f"""{HEAD}<div class="wrap">
      <header><p class="eyebrow">Job</p><h1>{_esc(j['name'])}
        <a href="/job/{client_id}/edit" class="btn-ghost" style="text-decoration:none;font-size:13px;vertical-align:middle;margin-left:12px">Edit</a></h1>
        <p class="meta">{_esc(j['company'] or '')} · {_esc(j['contact'] or '')}</p>
        <div style="margin-top:10px">{status_html} {work_btn}</div>
        <p class="meta" style="margin-top:8px">Total time: {time_display}</p></header>
      <section><p class="label">Client link</p><div class="panel">
        <code class="share">{share}</code>
        <p class="meta">Send this to the client. Only someone with this link can view the log.</p></div></section>
      <section><p class="label">Reference</p><div class="panel kv">
        <div><span class="meta">Repo</span><div>{_esc(j['repo_link'] or '-')}</div></div>
        <div><span class="meta">Auth reference</span><div>{_esc(j['auth_reference'] or '-')}</div></div>
        <div><span class="meta">Params</span><div class="pre">{_esc(j['params'] or '-')}</div></div>
      </div></section>
      <section><p class="label">Client comments</p><div class="panel">{cm}</div></section>
      <section><p class="label">Client files</p><div class="panel">{fl}</div></section>
      <p class="meta">Log lives at <code>{VAULT}/{client_id}/worklog.md</code> - edit it in Obsidian; changes push automatically.</p>
      <a href="/">← all jobs</a></div></body></html>""")


def _esc(s):
    import html
    return html.escape(str(s or ""))


HEAD = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Work Log Admin</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
 :root{--paper:#f7f7f4;--ink:#1c2b33;--ink-soft:#5a6b73;--rule:#e3e2db;--signal:#c8772e;--card:#fffffe;
       --mono:"IBM Plex Mono",monospace;--sans:"IBM Plex Sans",system-ui,sans-serif;}
 @media (prefers-color-scheme:dark){:root{--paper:#14181b;--ink:#e8eaec;--ink-soft:#93a0a7;--rule:#262d31;--card:#181d20;--signal:#e2934a;}}
 *{box-sizing:border-box;} body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--sans);line-height:1.55;}
 .wrap{max-width:760px;margin:0 auto;padding:48px 24px 80px;}
 .eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink-soft);margin:0 0 8px;}
 h1{font-size:30px;font-weight:600;margin:0 0 8px;letter-spacing:-.01em;}
 .label{font-family:var(--mono);font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--ink-soft);margin:0 0 8px;}
 section{margin-top:30px;} .meta{font-family:var(--mono);font-size:11.5px;color:var(--ink-soft);}
 .panel{background:var(--card);border:1px solid var(--rule);border-radius:12px;padding:18px 20px;}
 input,textarea{width:100%;font-family:var(--sans);font-size:14.5px;color:var(--ink);background:var(--paper);
   border:1px solid var(--rule);border-radius:8px;padding:9px 11px;} textarea{min-height:70px;resize:vertical;}
 button{font-family:var(--sans);font-size:14px;font-weight:500;color:#fff;background:var(--signal);border:none;
   border-radius:8px;padding:10px 20px;cursor:pointer;}
 .btn-ghost{background:transparent;color:var(--signal);border:1px solid var(--signal);padding:6px 14px;font-size:13px;}
 .btn-danger{background:transparent;color:#b94040;border:1px solid #b94040;padding:6px 14px;font-size:13px;}
 .row{display:flex;gap:10px;align-items:center;margin-top:12px;}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;} .grid .full{grid-column:1/-1;}
 .job{display:flex;flex-direction:column;padding:12px 0;border-bottom:1px solid var(--rule);text-decoration:none;color:var(--ink);}
 .job:last-child{border-bottom:none;} .job-inactive{opacity:.55;} .jn{font-weight:500;} a{color:var(--signal);}
 .share{display:block;font-family:var(--mono);font-size:13px;word-break:break-all;color:var(--ink);}
 .kv>div{padding:8px 0;border-bottom:1px solid var(--rule);} .kv>div:last-child{border-bottom:none;}
 .pre{white-space:pre-wrap;} .empty{color:var(--ink-soft);font-style:italic;}
 .cmt{padding:10px 0;border-bottom:1px solid var(--rule);white-space:pre-wrap;} .cmt:last-child{border-bottom:none;}
 .when{display:block;margin-top:5px;font-family:var(--mono);font-size:11px;color:var(--ink-soft);}
 .file{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--rule);}
 .file:last-child{border-bottom:none;}
 .badge{font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;
        padding:3px 9px;border-radius:999px;font-weight:500;}
 .badge-active{background:#d4edda;color:#276138;}
 .badge-done{background:#d0e8f5;color:#174f72;}
 .badge-abort{background:#f5dada;color:#7a2020;}
 @media (prefers-color-scheme:dark){
   .badge-active{background:#1a3326;color:#6fcf97;}
   .badge-done{background:#152d40;color:#7ec8e3;}
   .badge-abort{background:#3a1a1a;color:#e07070;}
 }
</style></head><body>"""
