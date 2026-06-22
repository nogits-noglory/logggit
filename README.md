# logggit

A private, per-client work portal. You log work in Obsidian and the client opens a secret link to see a live log, AI summaries, files, comments, annotations, time tracking, and email notifications.

It runs as two separate apps on purpose:

- **Local plane** (your machine, localhost): job creation, the Obsidian vault, and all sensitive metadata like contacts, repo links, and auth references. None of this ever touches a public server.
- **Client plane** (your droplet): only what a client is allowed to see. The rendered log, summaries, their comments and files, their notification preferences, and work session tracking.

```
 LOCAL (your machine)                       DROPLET (client-facing)
 ────────────────────                       ───────────────────────
 New Job form ─┐                            POST /provision   register client + token
 Obsidian vault│ ── push worklog.md ──────► POST /ingest      update log, summary, notify
 admin.db      │ ◄─ pull comments/files ─── GET  /admin/{id}
 (auth refs)   ┘                            GET  /log/{token} the client portal
```

## Security model

The admin plane is localhost-only. Sensitive data like auth references and contacts lives in `admin.db` and never leaves your machine. The auth reference field stores where a secret lives (like a password manager item), not the secret itself.

The work log itself is client-visible by design. The vault scaffold deliberately contains no contact or auth data, only the dated log entries.

Client access is gated by an unguessable token in the URL. Pages are `noindex`. Admin routes require a shared `ADMIN_SECRET`. Per-client isolation is server-enforced: every client route resolves the token to one client and scopes all reads and writes to it. A client cannot fetch another client's data even by guessing an ID.

Client input is contained. Comments are stored raw and rendered escaped. Uploads are extension-allowlisted, size-capped at 25 MB, stored outside any served directory, and returned only as downloads with `Content-Disposition: attachment`. They are never rendered inline.

## Features

- **Live updates** via Server-Sent Events. The client sees changes as you type in Obsidian.
- **AI summaries** via Groq (llama-3.3-70B) with local Ollama fallback. Overall summary at the top, per-date section summaries inline. Debounced so they don't regenerate on every save.
- **Time tracking**. Start/stop work sessions from the admin UI. The client page shows total time, a per-day bar chart, and a live "Working/Idle" indicator with an optional status note.
- **Client annotations**. Clients can highlight text in the log and attach notes. Annotations sync back to Obsidian as `annotations.md`.
- **Comments and files**. Clients leave comments and upload files. Both sync to the vault.
- **Email notifications** via Resend. Clients choose between an email every 30 minutes of work time, a daily email at 4 PM, or off.
- **Rich markdown**. Images, tables, code blocks, and SVGs (via img tags) all render in the client portal.
- **Job management**. Create, edit, and mark jobs as active, completed, or aborted from the admin UI.
- **Scrollable log** with a bottom fade gradient. Auto-scrolls to the latest entry on load.
- **Dark mode** and reduced-motion support out of the box.

## Droplet setup (client plane)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export ADMIN_SECRET="$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))')"
export PUBLIC_BASE="https://worklog.your-domain.example"

# Optional: email notifications via Resend (free tier, HTTP-based, no SMTP needed)
export RESEND_API_KEY="re_..."
export EMAIL_FROM="notifications@your-domain.example"

# Optional: push notifications via ntfy
export NTFY_BASE="https://ntfy.sh"

uvicorn main:app --host 127.0.0.1 --port 8098
```

Front it with nginx and TLS. SSE needs buffering off:

```nginx
location / {
    proxy_pass http://127.0.0.1:8098;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
    client_max_body_size 30m;
}
```

## Local setup (admin plane)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp config.example.yaml config.yaml
# Fill in: droplet_url, admin_secret, vault_clients_dir, and optionally
# local_password, groq_api_key, ollama_url

uvicorn app:app --host 127.0.0.1 --port 8800
```

On Windows, use `start.ps1` to handle port conflicts automatically.

## Day to day

1. **New job**: fill the form in the admin UI. It scaffolds `clients/<id>/worklog.md` in your vault, registers the client on the droplet, and shows you the client link.
2. **Work**: edit `worklog.md` in Obsidian. A file watcher plus a 2-second mtime poll push changes automatically.
3. **Start/stop sessions**: click "Start working" on the job page when you begin, "Stop working" when you're done. The client sees your status live.
4. **Client side**: they open the link and see the live log, a summary, a file area, a comment box, annotations, time tracking, and notification controls.
5. **Their comments, annotations, and files** show up in the admin UI and sync to your Obsidian vault.

## Notes

- Storage is SQLite on each side, created automatically. The droplet's `uploads/` holds client files.
- Summaries regenerate after 3 pushes or 15 minutes of editing, not on every save. Opening the page does not trigger a model call.
- This supplements, never replaces, Upwork's own time tracking on hourly contracts. Offer the link as optional visibility.
