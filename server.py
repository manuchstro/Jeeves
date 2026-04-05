from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3
import requests

client = OpenAI()

SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.
Be concise, sharp, and decision-focused.
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")


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
    cur.execute("SELECT key FROM watchlist_preferences")
    rows = cur.fetchall()
    conn.close()
    return [row["key"] for row in rows]


# ---------------- STOCK DATA ----------------

def get_stock_price(ticker):
    api_key = os.environ.get("FMP_API_KEY")
    url = f"https://financialmodelingprep.com/api/v3/quote/{ticker.upper()}?apikey={api_key}"

    try:
        r = requests.get(url)
        data = r.json()

        if not data or "price" not in data[0]:
            return None

        return data[0]["price"]

    except:
        return None


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
    cur.execute("""
        SELECT role, content
        FROM conversation_messages
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()

    messages = [{"role": row["role"], "content": row["content"]} for row in rows]
    messages.reverse()
    return messages


# Initialize DB
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

    # -------- WATCHLIST COMMANDS --------

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
        if not wl:
            resp.message("Watchlist is empty")
        else:
            resp.message("Watchlist: " + ", ".join(wl))
        return str(resp)

    # -------- STOCK PRICE COMMAND --------

    if incoming_lower.startswith("price "):
        ticker = raw_incoming[6:].strip()
        price = get_stock_price(ticker)

        if price is None:
            resp.message(f"{ticker.upper()}: N/A")
        else:
            resp.message(f"{ticker.upper()}: ${price}")

        return str(resp)

    # -------- NORMAL AI --------

    try:
        add_message("user", raw_incoming)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ] + get_recent_messages(10)

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


# ---------------- RUN ----------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
