from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3
import requests
import json
import re
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

ROUTER_PROMPT = """
You are a strict intent router for Jeeves.
Return ONLY valid JSON.

Supported intents:
- watchlist_add
- watchlist_remove
- watchlist_show
- macro_series
- nyt_news
- none

Rules:
- Choose macro_series only if the user is asking for a current macro datapoint that maps clearly to a supported FRED series.
- Choose nyt_news only if the user is asking for current news on a topic.
- Choose watchlist_add/remove/show only if clearly requested.
- Choose none if unclear.
- Do not guess symbols or series beyond the supported mappings.

Supported macro mappings:
- DGS10 = 10-year treasury yield / 10 year yield / 10-year rate
- FEDFUNDS = fed funds rate / federal funds rate
- CPIAUCSL = cpi / inflation / consumer price index
- UNRATE = unemployment rate / jobless rate

JSON schema:
{
  "intent": "watchlist_add|watchlist_remove|watchlist_show|macro_series|nyt_news|none",
  "watch_item": "string or empty",
  "series_id": "DGS10|FEDFUNDS|CPIAUCSL|UNRATE or empty",
  "news_query": "string or empty",
  "confidence": 0.0,
  "needs_live_data": true
}
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

def normalize_watch_item(item):
    item = (item or "").strip()
    item = re.sub(r"\s+", " ", item)
    return item.upper()


def add_to_watchlist(item):
    item = normalize_watch_item(item)
    if not item:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO watchlist_preferences (key, value) VALUES (?, ?)",
        (item, "1")
    )
    conn.commit()
    changed = cur.rowcount > 0
    conn.close()
    return changed


def remove_from_watchlist(item):
    item = normalize_watch_item(item)
    if not item:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM watchlist_preferences WHERE key = ?",
        (item,)
    )
    conn.commit()
    changed = cur.rowcount > 0
    conn.close()
    return changed


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


def format_macro_result(result):
    if result is None:
        return None

    labels = {
        "DGS10": "10Y Treasury",
        "FEDFUNDS": "Fed Funds",
        "CPIAUCSL": "CPI",
        "UNRATE": "Unemployment"
    }
    label = labels.get(result["series"], result["series"])
    return f"{label}: {result['value']} ({result['date']})"


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


# ---------------- INTERPRETATION ----------------

def simple_router(text):
    lower = text.lower().strip()

    add_patterns = [
        r"(?:add|watch|track|put)\s+(.+?)\s+(?:to|on)\s+my watchlist$",
        r"(?:add|watch|track)\s+([a-z0-9 .\\-]+)$"
    ]
    for pattern in add_patterns:
        m = re.match(pattern, lower)
        if m:
            return {"intent": "watchlist_add", "watch_item": m.group(1).strip(), "series_id": "", "news_query": "", "confidence": 0.9, "needs_live_data": False}

    remove_patterns = [
        r"(?:remove|delete|drop|unwatch)\s+(.+?)\s+(?:from|off)\s+my watchlist$",
        r"(?:remove|delete|drop|unwatch)\s+([a-z0-9 .\\-]+)$"
    ]
    for pattern in remove_patterns:
        m = re.match(pattern, lower)
        if m:
            return {"intent": "watchlist_remove", "watch_item": m.group(1).strip(), "series_id": "", "news_query": "", "confidence": 0.9, "needs_live_data": False}

    if lower in ["show watchlist", "what is on my watchlist", "what's on my watchlist"]:
        return {"intent": "watchlist_show", "watch_item": "", "series_id": "", "news_query": "", "confidence": 0.98, "needs_live_data": False}

    if "how often is cpi updated" in lower or "when is cpi updated" in lower:
        return {"intent": "none", "watch_item": "", "series_id": "", "news_query": "", "confidence": 0.0, "needs_live_data": False}

    macro_map = {
        "DGS10": ["10 year", "10-year", "10y treasury", "treasury yield", "10 year treasury yield"],
        "FEDFUNDS": ["fed funds", "federal funds rate"],
        "CPIAUCSL": ["cpi", "inflation", "consumer price index"],
        "UNRATE": ["unemployment", "jobless rate", "unemployment rate"]
    }
    for series_id, phrases in macro_map.items():
        if any(p in lower for p in phrases):
            return {"intent": "macro_series", "watch_item": "", "series_id": series_id, "news_query": "", "confidence": 0.8, "needs_live_data": True}

    if lower.startswith("news "):
        return {"intent": "nyt_news", "watch_item": "", "series_id": "", "news_query": text[5:].strip(), "confidence": 0.95, "needs_live_data": True}

    if "news on " in lower:
        return {"intent": "nyt_news", "watch_item": "", "series_id": "", "news_query": text.lower().split("news on ", 1)[1].strip(), "confidence": 0.85, "needs_live_data": True}

    return {"intent": "none", "watch_item": "", "series_id": "", "news_query": "", "confidence": 0.0, "needs_live_data": False}


def model_router(text):
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": ROUTER_PROMPT},
                {"role": "user", "content": text}
            ]
        )
        raw = completion.choices[0].message.content.strip()
        data = json.loads(raw)
        return {
            "intent": data.get("intent", "none"),
            "watch_item": data.get("watch_item", ""),
            "series_id": data.get("series_id", ""),
            "news_query": data.get("news_query", ""),
            "confidence": float(data.get("confidence", 0.0)),
            "needs_live_data": bool(data.get("needs_live_data", False))
        }
    except Exception:
        return {"intent": "none", "watch_item": "", "series_id": "", "news_query": "", "confidence": 0.0, "needs_live_data": False}

def handle_macro_meta_question(text):
    lower = text.lower().strip()

    if "how often is cpi updated" in lower or "when is cpi updated" in lower:
        return "CPI is typically released monthly."

    if "how often is unemployment updated" in lower or "when is unemployment updated" in lower:
        return "The unemployment rate is typically released monthly."

    if "how often is fed funds updated" in lower or "when is the fed funds rate updated" in lower:
        return "Fed Funds updates when the Fed changes the target rate, not on a fixed daily schedule."

    if "how often is the 10 year updated" in lower or "when is the 10 year updated" in lower:
        return "The 10Y Treasury yield updates on market days."

    return None

def classify_request(text):
    rule_result = simple_router(text)
    if rule_result["intent"] != "none":
        return rule_result

    model_result = model_router(text)
    if model_result["confidence"] >= 0.7:
        return model_result

    return rule_result


def handle_routed_request(route):
    intent = route.get("intent", "none")

    if intent == "watchlist_show":
        wl = get_watchlist()
        return "Watchlist is empty" if not wl else "Watchlist: " + ", ".join(wl)

    if intent == "watchlist_add":
        item = route.get("watch_item", "")
        normalized = normalize_watch_item(item)
        if not normalized:
            return "I don't know."
        added = add_to_watchlist(normalized)
        return f"Added {normalized}" if added else f"Already watching {normalized}"

    if intent == "watchlist_remove":
        item = route.get("watch_item", "")
        normalized = normalize_watch_item(item)
        if not normalized:
            return "I don't know."
        removed = remove_from_watchlist(normalized)
        return f"Removed {normalized}" if removed else f"{normalized} was not on the watchlist"

    if intent == "macro_series":
        series_id = route.get("series_id", "")
        if not series_id:
            return "I don't know."
        result = get_fred_latest(series_id)
        return format_macro_result(result) if result else f"{series_id}: N/A"

    if intent == "nyt_news":
        query = route.get("news_query", "").strip()
        if not query:
            return "I don't know."
        result = get_nyt_articles(query)
        return result if result else "NYT: N/A"

    return None


def looks_like_live_data_request(text):
    lower = text.lower()
    live_terms = [
        "yield", "rate", "cpi", "inflation", "unemployment", "latest", "current", "news",
        "today", "treasury", "fed", "headline", "macro"
    ]
    return any(term in lower for term in live_terms)


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

    # DEBUG
    if incoming_lower.startswith("debug fred "):
        series = raw_incoming.split(" ")[-1]
        resp.message(str(debug_fred(series))[:1500])
        return str(resp)

    # DIRECT COMMANDS
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

  macro_meta = handle_macro_meta_question(raw_incoming)
if macro_meta is not None:
    resp.message(macro_meta)
    return str(resp)
  
    # INTERPRETATION LAYER
    route = classify_request(raw_incoming)
    routed_response = handle_routed_request(route)
    if routed_response is not None:
        resp.message(routed_response)
        return str(resp)

    # CAPABILITY GATE FOR LIVE DATA
    if looks_like_live_data_request(raw_incoming):
        resp.message("I don't know.")
        return str(resp)

    # AI CHAT
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
