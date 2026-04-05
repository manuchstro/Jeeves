from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3

client = OpenAI()

SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.
Be concise, sharp, and decision-focused.
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")


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


init_db()


@app.route("/", methods=["GET"])
def home():
    return "Jeeves is running"


@app.route("/sms", methods=["POST"])
def sms():
    raw_incoming = request.form.get("Body", "").strip()
    from_number = request.form.get("From", "").replace("whatsapp:", "")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
