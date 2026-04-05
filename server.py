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

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
NYT_API_KEY = os.environ.get("NYT_API_KEY")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")

FRED_SERIES = {
    "DGS10": {
        "label": "10Y Treasury",
        "frequency": "daily on market days",
    },
    "CPIAUCSL": {
        "label": "CPI",
        "frequency": "monthly",
    },
    "FEDFUNDS": {
        "label": "Fed Funds",
        "frequency": "when the Fed changes the target rate",
    },
    "UNRATE": {
        "label": "Unemployment rate",
        "frequency": "monthly",
    },
}

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
        value TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()

# ---------------- WATCHLIST ----------------

def normalize_watchlist_item(text):
    cleaned = text.strip().upper()
    cleaned = re.sub(r"^(ADD|REMOVE)\s+", "", cleaned)
    cleaned = re.sub(r"\s+(TO|FROM)\s+MY\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"\s+MY\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"[^A-Z0-9.\- ]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def extract_watchlist_item(text, action):
    cleaned = text.strip()

    patterns = {
        "add": [
            r"^\s*add\s+(.+?)\s+to\s+my\s+watchlist\s*$",
            r"^\s*add\s+(.+?)\s+to\s+watchlist\s*$",
            r"^\s*add\s+(.+?)\s*$",
        ],
        "remove": [
            r"^\s*remove\s+(.+?)\s+from\s+my\s+watchlist\s*$",
            r"^\s*remove\s+(.+?)\s+from\s+watchlist\s*$",
            r"^\s*remove\s+(.+?)\s*$",
        ],
    }

    for pattern in patterns.get(action, []):
        match = re.match(pattern, cleaned, re.IGNORECASE)
        if match:
            item = normalize_watchlist_item(match.group(1))
            if item:
                return item

    return None

def add_to_watchlist(item):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO watchlist_preferences (key,value) VALUES (?,?)", (item,"1"))
    conn.commit()
    conn.close()


def remove_from_watchlist(item):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM watchlist_preferences WHERE key=?", (item,))
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


def get_watchlist():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT key FROM watchlist_preferences ORDER BY key ASC")
    rows = cur.fetchall()
    conn.close()
    return [r["key"] for r in rows]

# ---------------- MEMORY ----------------

def add_message(role, content):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO conversation_messages (role,content) VALUES (?,?)", (role,content))
    conn.commit()
    conn.close()


def get_recent_messages(limit=10):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT role,content FROM conversation_messages ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    msgs = [{"role":r["role"],"content":r["content"]} for r in rows]
    msgs.reverse()
    return msgs

# ---------------- FRED ----------------

def get_fred(series):
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={FRED_API_KEY}&file_type=json"
        data = requests.get(url, timeout=10).json()
        obs = data.get("observations", [])
        for x in reversed(obs):
            if x["value"] != ".":
                return {
                    "series": series,
                    "value": x["value"],
                    "date": x["date"],
                }
    except:
        pass
    return None

# ---------------- NYT ----------------

def get_news(query):
    try:
        q = quote_plus(query)
        url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={q}&sort=newest&api-key={NYT_API_KEY}"
        data = requests.get(url, timeout=10).json()
        docs = data.get("response",{}).get("docs",[])[:3]
        if not docs:
            return None
        return "\n".join(["- "+d["headline"]["main"] for d in docs])
    except:
        return None

# ---------------- INTERPRETATION ----------------

def is_frequency_question(text):
    t = text.lower()
    return "how often" in t or "when is" in t or "when does" in t or "updated" in t or "released" in t


def format_month(date_text):
    parts = date_text.split("-")
    if len(parts) != 3:
        return date_text
    year, month, _ = parts
    months = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }
    return f"{months.get(month, month)} {year}"


def format_fred_reply(series, observation, user_text):
    meta = FRED_SERIES.get(series, {"label": series, "frequency": "unknown"})
    label = meta["label"]

    if observation is None:
        return "I don't know"

    latest = f"Latest reading: {observation['value']} for {format_month(observation['date'])}."

    if is_frequency_question(user_text):
        return f"{label} is usually updated {meta['frequency']}. {latest}"

    return f"{label}: {observation['value']} ({observation['date']})"


def is_watchlist_stats_question(text):
    t = text.lower()

    keywords = [
        "doing",
        "performance",
        "performing",
        "stats",
        "today",
        "moves",
        "movers",
        "up",
        "down",
        "what happened",
        "how is",
        "how's",
    ]

    watchlist_terms = [
        "watchlist",
        "my names",
        "my stocks",
        "my holdings",
        "my portfolio",
    ]

    has_stats_phrase = any(keyword in t for keyword in keywords)
    has_watchlist_reference = any(term in t for term in watchlist_terms)

    return has_stats_phrase and has_watchlist_reference

# ---------------- MASSIVE ----------------

def get_massive_watchlist_snapshot(tickers):
    if not MASSIVE_API_KEY or not tickers:
        return None

    try:
        url = "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {
            "tickers": ",".join(tickers),
            "apiKey": MASSIVE_API_KEY,
        }
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        return data.get("tickers", [])
    except:
        return None


def format_price(value):
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def format_change(value):
    if value is None:
        return "N/A"
    return f"{value:+.2f}"


def format_pct(value):
    if value is None:
        return "N/A"
    return f"{value:+.2f}%"


def extract_snapshot_price(snapshot):
    day = snapshot.get("day") or {}
    last_trade = snapshot.get("lastTrade") or {}
    min_bar = snapshot.get("min") or {}
    return day.get("c") or last_trade.get("p") or min_bar.get("c")


def format_watchlist_stats_reply(watchlist, snapshots):
    if not watchlist:
        return "Watchlist is empty."

    if snapshots is None:
        return "Watchlist is loaded, but market data is unavailable."

    by_ticker = {item.get("ticker"): item for item in snapshots if item.get("ticker")}
    parts = []

    for ticker in watchlist:
        item = by_ticker.get(ticker, {})
        price = extract_snapshot_price(item)
        change = item.get("todaysChange")
        pct = item.get("todaysChangePerc")
        parts.append(f"{ticker} {format_price(price)} ({format_pct(pct)}, {format_change(change)})")

    return "Watchlist today: " + "; ".join(parts)

# ---------------- ROUTER ----------------

def route(text):
    t = text.lower()

    if t.startswith("add "):
        return ("add", extract_watchlist_item(text, "add"))

    if t.startswith("remove "):
        return ("remove", extract_watchlist_item(text, "remove"))

    if is_watchlist_stats_question(text):
        return ("watchlist_stats", None)

    if "watchlist" in t:
        return ("show", None)

    if "10 year" in t or "treasury" in t:
        return ("fred","DGS10")

    if "fed funds" in t or "federal funds" in t:
        return ("fred","FEDFUNDS")

    if "unemployment" in t or "jobless" in t:
        return ("fred","UNRATE")

    if "inflation" in t or "cpi" in t:
        return ("fred","CPIAUCSL")

    if "news" in t:
        return ("news", text.replace("news",""))

    return ("none", None)

init_db()

@app.route("/sms", methods=["POST"])
def sms():
    msg = request.form.get("Body","")
    from_number = request.form.get("From",""
        ).replace("whatsapp:","")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    intent, value = route(msg)

    if intent == "add":
        if not value:
            resp.message("I don't know what to add.")
            return str(resp)
        add_to_watchlist(value)
        resp.message(f"Added {value}.")
        return str(resp)

    if intent == "remove":
        if not value:
            resp.message("I don't know what to remove.")
            return str(resp)
        removed = remove_from_watchlist(value)
        resp.message(f"Removed {value}." if removed else f"{value} is not on your watchlist.")
        return str(resp)

    if intent == "show":
        wl = get_watchlist()
        resp.message(f"Watchlist: {', '.join(wl)}" if wl else "Watchlist is empty.")
        return str(resp)

    if intent == "watchlist_stats":
        wl = get_watchlist()
        snapshots = get_massive_watchlist_snapshot(wl)
        resp.message(format_watchlist_stats_reply(wl, snapshots))
        return str(resp)

    if intent == "fred":
        out = get_fred(value)
        resp.message(format_fred_reply(value, out, msg))
        return str(resp)

    if intent == "news":
        out = get_news(value)
        resp.message(out if out else "N/A")
        return str(resp)

    try:
        add_message("user", msg)
        messages = [{"role":"system","content":SYSTEM_PROMPT}] + get_recent_messages()
        completion = client.chat.completions.create(model="gpt-4o-mini",messages=messages)
        reply = completion.choices[0].message.content
        add_message("assistant", reply)
    except:
        reply = "Temporary error."

    resp.message(reply)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port)
