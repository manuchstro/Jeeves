from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3
import requests
from urllib.parse import quote_plus

client = OpenAI()

SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.

User profile:
- UC Berkeley economics student
- Focus: energy markets, especially uranium
- Strong interest in geopolitical catalysts and supply dynamics
- Prefers high-signal, actionable insights over theory
- Strongly dislikes noise, fluff, and irrelevant macro commentary
- Values speed, clarity, and decisiveness
- Comfortable with risk analysis and probabilistic thinking
- Wants alerts that are timely and meaningful, not obvious or delayed

Behavior:
- Be direct, efficient, and precise
- Default to short responses unless depth is clearly needed
- Prioritize usefulness over completeness
- Surface implications, not just facts
- Highlight what actually matters
- When uncertain, always default to saying "I don't know"
- Maintain a sharp, confident tone without arrogance
- Keep responses engaging but not verbose

Interaction rules:
- Default to yes/no first, then explanation when applicable
- Keep responses tight and structured
- Show steps cleanly when solving problems
- Never fabricate sources, numbers, or details
- Reduce cognitive load; avoid unnecessary complexity
- Break problems into first step → confirm → continue when useful
- Do not argue tone; focus on solving the task
- Mirror pace and intensity without amplifying frustration
- Highlight mistakes clearly and early
- Follow user constraints exactly when specified
- Optimize for decision usefulness over explanation length

Objective:
Help the user make better decisions, faster.
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
NYT_API_KEY = os.environ.get("NYT_API_KEY")


# ---------------- DB ----------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conversation_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS watchlist_preferences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL UNIQUE,
        value TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id TEXT,
        category TEXT,
        tier INTEGER,
        headline_hash TEXT,
        sent_to_user INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()


# ---------------- WATCHLIST ----------------

def add_to_watchlist(item):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO watchlist_preferences (key, value) VALUES (?, ?)",
        (item.upper(), "1")
    )
    conn.commit()
    conn.close()


def remove_from_watchlist(item):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM watchlist_preferences WHERE key = ?",
        (item.upper(),)
    )
    conn.commit()
    conn.close()


def get_watchlist():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT key FROM watchlist_preferences ORDER BY key ASC")
    rows = cur.fetchall()
    conn.close()
    return [row["key"] for row in rows]


# ---------------- MEMORY ----------------

def add_message(role, content):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO conversation_messages (role, content) VALUES (?, ?)",
        (role, content)
    )
    conn.commit()
    conn.close()


def get_recent_messages(limit=10):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT role, content FROM conversation_messages ORDER BY id DESC LIMIT ?",
        (limit,)
    )
    rows = cur.fetchall()
    conn.close()
    msgs = [{"role": row["role"], "content": row["content"]} for row in rows]
    msgs.reverse()
    return msgs


# ---------------- FRED ----------------

def debug_fred(series):
    try:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series.upper()}&api_key={FRED_API_KEY}&file_type=json"
        )
        r = requests.get(url, timeout=10)
        return {"status": r.status_code, "text": r.text[:1200]}
    except Exception as e:
        return {"error": str(e)}


def get_fred_latest(series):
    try:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series.upper()}&api_key={FRED_API_KEY}&file_type=json"
        )
        r = requests.get(url, timeout=10)
        data = r.json()
        obs = data.get("observations", [])

        for item in reversed(obs):
            value = item.get("value")
            if value is not None and value != ".":
                return {"series": series.upper(), "date": item.get("date"), "value": value}
    except Exception:
        pass

    return None


# ---------------- NYT ----------------

def get_nyt_articles(query):
    try:
        q = quote_plus(query)
        url = (
            "https://api.nytimes.com/svc/search/v2/articlesearch.json"
            f"?q={q}&sort=newest&api-key={NYT_API_KEY}"
        )
        r = requests.get(url, timeout=10)
        data = r.json()
        docs = data.get("response", {}).get("docs", [])[:3]

        if not docs:
            return None

        lines = []
        for doc in docs:
            headline = doc.get("headline", {}).get("main", "Untitled")
            pub_date = doc.get("pub_date", "")[:10]
            lines.append(f"- {headline} ({pub_date})")

        return "NYT:\n" + "\n".join(lines)
    except Exception:
        return None


# ---------------- ROUTING ----------------

def route_macro_query(text):
    lower = text.lower()

    if "10 year" in lower or "10-year" in lower or "treasury yield" in lower:
        return get_fred_latest("DGS10")
    if "fed funds" in lower or "federal funds" in lower:
        return get_fred_latest("FEDFUNDS")
    if lower == "cpi" or "inflation" in lower or "consumer price index" in lower:
        return get_fred_latest("CPIAUCSL")
    if "unemployment" in lower or "jobless rate" in lower:
        return get_fred_latest("UNRATE")

    return None


def format_macro_result(result):
    if result is None:
        return None
    return f"{result['series']}: {result['value']} ({result['date']})"


init_db()


# ---------------- ROUTES ----------------

@app.route("/", methods=["GET"])
def home():
    return "Jeeves is running"


@app.route("/sms", methods=["POST"])
def sms():
    raw_incoming = request.form.get("Body", "").strip()
    incoming_lower = raw_incoming.lower()
    from_number = request.form.get("From", "").replace("whatsapp:", "")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    # WATCHLIST
    if incoming_lower.startswith("add "):
        item = raw_incoming[4:].strip()
        add_to_watchlist(item)
        resp.message(f"Added {item.upper()}")
        return str(resp)

    if incoming_lower.startswith("remove "):
        item = raw_incoming[7:].strip()
        remove_from_watchlist(item)
        resp.message(f"Removed {item.upper()}")
        return str(resp)

    if "show watchlist" in incoming_lower:
        wl = get_watchlist()
        resp.message("Watchlist is empty" if not wl else "Watchlist: " + ", ".join(wl))
        return str(resp)

    # DEBUG
    if incoming_lower.startswith("debug fred "):
        series = raw_incoming.split(" ")[-1]
        resp.message(str(debug_fred(series))[:1500])
        return str(resp)

    # DIRECT API COMMANDS
    if incoming_lower.startswith("fred "):
        series = raw_incoming.split(" ")[-1]
        result = get_fred_latest(series)
        resp.message(f"{series.upper()}: N/A" if result is None else format_macro_result(result))
        return str(resp)

    if incoming_lower.startswith("news "):
        query = raw_incoming[5:].strip()
        result = get_nyt_articles(query)
        resp.message(result if result else "NYT: N/A")
        return str(resp)

    # LIGHT INTERPRETATION LAYER
    macro_result = route_macro_query(raw_incoming)
    if macro_result is not None:
        resp.message(format_macro_result(macro_result))
        return str(resp)

    if "news on " in incoming_lower:
        query = raw_incoming.lower().split("news on ", 1)[1].strip()
        result = get_nyt_articles(query)
        resp.message(result if result else "NYT: N/A")
        return str(resp)

    # AI
    try:
        add_message("user", raw_incoming)
        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + get_recent_messages(10)
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages
        )
        reply = completion.choices[0].message.content or "I don't know."
        add_message("assistant", reply)
    except Exception:
        reply = "Temporary error."

    resp.message(reply)
    return str(resp)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
