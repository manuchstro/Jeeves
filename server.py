from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3
import requests
import json
import re
import hashlib
import math
import base64
import random
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urlparse
from jeeves_config import (
    AI_ALERT_SHORTLIST_MAX,
    CURRENTS_MIN_INTERVAL_MINUTES,
    FEEDBACK_RESPONSES,
    FRED_SERIES,
    KNOWN_MARKET_NAME_MAP,
    MEMORY_VECTOR_CATEGORY_WEIGHTS,
    MEMORY_INSTRUCTIONS,
    NEWS_QUERIES,
    POLL_SERIES,
    PROTECTED_MEMORY_CATEGORIES,
    SOURCE_GUIDANCE,
    SYSTEM_PROMPT,
    STORY_STOPWORDS,
)

client = OpenAI()

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
NYT_API_KEY = os.environ.get("NYT_API_KEY")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")
CURRENTS_API_KEY = os.environ.get("CURRENTS_API_KEY")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM")
RAILWAY_GIT_COMMIT_SHA = os.environ.get("RAILWAY_GIT_COMMIT_SHA") or os.environ.get("SOURCE_COMMIT")
RAILWAY_DEPLOYMENT_ID = os.environ.get("RAILWAY_DEPLOYMENT_ID")
LOCAL_TZ = ZoneInfo("America/Los_Angeles")
ALERT_DECISION_MODEL = os.environ.get("ALERT_DECISION_MODEL", "gpt-4o")

# ---------------- DB ----------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_url_domain(url):
    try:
        netloc = urlparse(url or "").netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except:
        return ""


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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL,
        tier INTEGER NOT NULL,
        headline TEXT NOT NULL,
        event_hash TEXT NOT NULL,
        sent_to_user INTEGER NOT NULL DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS event_contexts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_hash TEXT NOT NULL UNIQUE,
        alert_id TEXT,
        category TEXT NOT NULL,
        tier INTEGER,
        source TEXT,
        headline TEXT NOT NULL,
        snippet TEXT,
        section TEXT,
        published_at TEXT,
        fingerprint TEXT,
        semantic_text TEXT,
        body_text TEXT,
        web_url TEXT,
        score INTEGER,
        selection_reasons_json TEXT,
        raw_payload_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS brief_event_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        brief_run_id TEXT NOT NULL,
        reference_code TEXT NOT NULL,
        alert_id TEXT,
        event_hash TEXT NOT NULL,
        headline TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS event_hashes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_hash TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL,
        headline TEXT NOT NULL,
        last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS event_embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_hash TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL,
        headline TEXT NOT NULL,
        semantic_text TEXT NOT NULL,
        embedding_json TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        value TEXT NOT NULL,
        source_text TEXT,
        confidence REAL NOT NULL DEFAULT 0.5,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(scope, category, memory_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gratitude_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_text TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        embedding_json TEXT NOT NULL,
        semantic_text TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(scope, category, memory_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        value TEXT NOT NULL,
        confidence REAL NOT NULL DEFAULT 0.5,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS interaction_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        intent TEXT NOT NULL,
        message_text TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        feedback_type TEXT NOT NULL,
        source_text TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_holdings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL UNIQUE,
        conviction_rank INTEGER,
        note TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    for statement in [
        "ALTER TABLE portfolio_holdings ADD COLUMN source_type TEXT DEFAULT 'manual'",
        "ALTER TABLE portfolio_holdings ADD COLUMN trusted INTEGER DEFAULT 0",
        "ALTER TABLE portfolio_holdings ADD COLUMN effective_date TEXT",
    ]:
        try:
            cur.execute(statement)
        except sqlite3.OperationalError:
            pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_type TEXT NOT NULL,
        trusted INTEGER NOT NULL DEFAULT 0,
        effective_date TEXT,
        holdings_json TEXT NOT NULL,
        summary_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS source_poll_state (
        source_name TEXT PRIMARY KEY,
        last_polled_at DATETIME,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbound_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_type TEXT NOT NULL,
        body TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS security_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        source_number TEXT,
        message_text TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deploy_announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deploy_key TEXT NOT NULL UNIQUE,
        commit_sha TEXT,
        deployment_id TEXT,
        announced_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gmail_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        token_json TEXT NOT NULL,
        scopes_json TEXT NOT NULL,
        connected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()

    for statement in [
        "ALTER TABLE event_contexts ADD COLUMN source_label TEXT",
        "ALTER TABLE event_contexts ADD COLUMN source_refs_json TEXT",
    ]:
        try:
            cur.execute(statement)
        except sqlite3.OperationalError:
            pass

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


def normalize_symbol(token):
    cleaned = re.sub(r"[^A-Za-z]", "", token or "").upper()
    if 1 <= len(cleaned) <= 5:
        return cleaned
    return None


def extract_symbols_from_text(text):
    raw_tokens = re.findall(r"\b[A-Za-z]{1,5}\b", text or "")
    blocked = {
        "I", "MY", "AM", "IN", "AND", "THE", "WITH", "HEAVY", "MOSTLY",
        "CASH", "RISK", "STOCK", "PRICE", "WHAT", "WHATS", "HOW", "HAS",
        "HAVE", "TODAY", "PORTFOLIO", "HOLDINGS", "OWN",
    }
    symbols = []
    for token in raw_tokens:
        symbol = normalize_symbol(token)
        if not symbol or symbol in blocked:
            continue
        if symbol not in symbols:
            symbols.append(symbol)
    return symbols


def upsert_portfolio_symbols(symbols, source_text=None, source_type="manual", trusted=False, effective_date=None):
    if not symbols:
        return

    conn = get_conn()
    cur = conn.cursor()
    for index, symbol in enumerate(symbols, start=1):
        cur.execute(
            """
            INSERT INTO portfolio_holdings (symbol, conviction_rank, note, source_type, trusted, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                conviction_rank = COALESCE(portfolio_holdings.conviction_rank, excluded.conviction_rank),
                note = CASE
                    WHEN excluded.trusted = 1 THEN COALESCE(excluded.note, portfolio_holdings.note)
                    ELSE COALESCE(portfolio_holdings.note, excluded.note)
                END,
                source_type = CASE
                    WHEN excluded.trusted = 1 THEN excluded.source_type
                    ELSE portfolio_holdings.source_type
                END,
                trusted = CASE
                    WHEN excluded.trusted = 1 THEN excluded.trusted
                    ELSE portfolio_holdings.trusted
                END,
                effective_date = COALESCE(excluded.effective_date, portfolio_holdings.effective_date),
                updated_at = CURRENT_TIMESTAMP
            """,
            (symbol, index, source_text, source_type, 1 if trusted else 0, effective_date),
        )
    conn.commit()
    conn.close()


def record_portfolio_snapshot(symbols, source_type="manual", trusted=False, effective_date=None, summary=None):
    if not symbols:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO portfolio_snapshots (source_type, trusted, effective_date, holdings_json, summary_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            source_type,
            1 if trusted else 0,
            effective_date,
            json.dumps(symbols),
            json.dumps(summary or {}),
        ),
    )
    conn.commit()
    conn.close()


def replace_trusted_portfolio_snapshot(symbols, effective_date=None, summary=None, source_type="gmail"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM portfolio_holdings WHERE trusted = 1")
    conn.commit()
    conn.close()

    upsert_portfolio_symbols(
        symbols,
        source_text=json.dumps(summary or {}),
        source_type=source_type,
        trusted=True,
        effective_date=effective_date,
    )
    record_portfolio_snapshot(
        symbols,
        source_type=source_type,
        trusted=True,
        effective_date=effective_date,
        summary=summary,
    )


def get_portfolio_holdings(limit=8, trusted_only=False):
    conn = get_conn()
    cur = conn.cursor()
    if trusted_only:
        cur.execute(
            """
            SELECT symbol, conviction_rank, note, updated_at, source_type, trusted, effective_date
            FROM portfolio_holdings
            WHERE trusted = 1
            ORDER BY
                CASE WHEN conviction_rank IS NULL THEN 999 ELSE conviction_rank END ASC,
                symbol ASC
            LIMIT ?
            """,
            (limit,),
        )
    else:
        cur.execute(
            """
            SELECT symbol, conviction_rank, note, updated_at, source_type, trusted, effective_date
            FROM portfolio_holdings
            ORDER BY
                CASE WHEN conviction_rank IS NULL THEN 999 ELSE conviction_rank END ASC,
                symbol ASC
            LIMIT ?
            """,
            (limit,),
        )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_trusted_portfolio_symbols(limit=10):
    return [item["symbol"] for item in get_portfolio_holdings(limit=limit, trusted_only=True)]


def upsert_gmail_account(email, token_payload, scopes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO gmail_accounts (email, token_json, scopes_json)
        VALUES (?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            token_json = excluded.token_json,
            scopes_json = excluded.scopes_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            email.strip().lower(),
            json.dumps(token_payload),
            json.dumps(scopes or []),
        ),
    )
    conn.commit()
    conn.close()


def get_gmail_accounts():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT email, scopes_json, connected_at, updated_at
        FROM gmail_accounts
        ORDER BY updated_at DESC
        """
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    for row in rows:
        try:
            row["scopes"] = json.loads(row.get("scopes_json") or "[]")
        except:
            row["scopes"] = []
        row.pop("scopes_json", None)
    return rows


def get_latest_trusted_portfolio_snapshot():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_type, trusted, effective_date, holdings_json, summary_json, created_at
        FROM portfolio_snapshots
        WHERE trusted = 1
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

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

# ---------------- MEMORY ----------------

def upsert_memory(scope, category, memory_key, value, source_text=None, confidence=0.6):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memory_items (scope, category, memory_key, value, source_text, confidence)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope, category, memory_key) DO UPDATE SET
            value = excluded.value,
            source_text = excluded.source_text,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (scope, category, memory_key, value, source_text, confidence),
    )
    conn.commit()
    conn.close()


def add_gratitude_entry(entry_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO gratitude_entries (entry_text) VALUES (?)",
        (entry_text.strip(),),
    )
    conn.commit()
    conn.close()


def add_memory_observation(category, memory_key, value, confidence=0.6):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memory_observations (category, memory_key, value, confidence)
        VALUES (?, ?, ?, ?)
        """,
        (category, memory_key, value, confidence),
    )
    conn.commit()
    conn.close()


def log_interaction_event(intent, message_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO interaction_events (intent, message_text)
        VALUES (?, ?)
        """,
        (intent, message_text),
    )
    conn.commit()
    conn.close()


def log_alert_feedback(feedback_type, source_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_feedback (feedback_type, source_text)
        VALUES (?, ?)
        """,
        (feedback_type, source_text),
    )
    conn.commit()
    conn.close()


def log_outbound_message(message_type, body):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO outbound_messages (message_type, body)
        VALUES (?, ?)
        """,
        (message_type, body),
    )
    conn.commit()
    conn.close()


def get_current_deploy_key():
    commit_sha = (RAILWAY_GIT_COMMIT_SHA or "").strip()
    deployment_id = (RAILWAY_DEPLOYMENT_ID or "").strip()

    if deployment_id and commit_sha:
        return f"{deployment_id}:{commit_sha}"
    if deployment_id:
        return deployment_id
    if commit_sha:
        return commit_sha
    return None


def mark_deploy_announced(deploy_key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO deploy_announcements (deploy_key, commit_sha, deployment_id)
        VALUES (?, ?, ?)
        ON CONFLICT(deploy_key) DO NOTHING
        """,
        (deploy_key, RAILWAY_GIT_COMMIT_SHA, RAILWAY_DEPLOYMENT_ID),
    )
    conn.commit()
    inserted = cur.rowcount > 0
    conn.close()
    return inserted


def log_security_event(event_type, source_number=None, message_text=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO security_events (event_type, source_number, message_text)
        VALUES (?, ?, ?)
        """,
        (event_type, source_number, message_text),
    )
    conn.commit()
    conn.close()


def build_suspicious_message_warning(source_number):
    timestamp = datetime.now(LOCAL_TZ).strftime("%-I:%M %p PT on %B %-d")
    if source_number:
        return f"Warning. Suspicious message activity at {timestamp} from {source_number}. Check Twilio logs."
    return f"Warning. Suspicious message activity at {timestamp}. Check Twilio logs."


def get_recent_alert_feedback(limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT feedback_type, source_text, created_at
        FROM alert_feedback
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_feedback_profile(limit=20):
    recent = get_recent_alert_feedback(limit=limit)
    sensitivity = 0
    urgency = 0
    affirmations = 0
    noise = 0

    for item in recent:
        feedback_type = item["feedback_type"]
        if feedback_type == "more like this":
            sensitivity += 2
        elif feedback_type == "too much noise":
            sensitivity -= 2
            noise += 1
        elif feedback_type == "late":
            urgency += 2
        elif feedback_type == "good alert":
            affirmations += 1

    sensitivity = max(-4, min(4, sensitivity))
    urgency = max(0, min(4, urgency))
    tier2_cap = max(2, min(6, 4 + math.floor(sensitivity / 2)))
    brief_tier_limit = 2 if noise >= 2 else 3
    require_higher_score = sensitivity < 0
    promote_quick_alerts = urgency >= 2

    return {
        "sensitivity": sensitivity,
        "urgency": urgency,
        "affirmations": affirmations,
        "noise": noise,
        "tier2_cap": tier2_cap,
        "brief_tier_limit": brief_tier_limit,
        "require_higher_score": require_higher_score,
        "promote_quick_alerts": promote_quick_alerts,
    }


def get_last_non_feedback_intent(limit=6):
    events = get_recent_interaction_events(limit=limit)
    for event in events:
        if event["intent"] != "alert_feedback":
            return event["intent"]
    return None


def feedback_context_allowed():
    recent_intent = get_last_non_feedback_intent(limit=6)
    if recent_intent in {"daily_brief", "alert_delivery"}:
        return True

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_type
        FROM outbound_messages
        WHERE datetime(created_at) >= datetime('now', '-3 hours')
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return row is not None and row["message_type"] in {"daily_brief", "alert"}


def article_request_context_allowed():
    recent_intent = get_last_non_feedback_intent(limit=8)
    return recent_intent in {"daily_brief", "event_expand", "alert_delivery"}


def get_memory_items(scope, limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, value, confidence, updated_at
        FROM memory_items
        WHERE scope = ?
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (scope, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_recent_gratitude(limit=30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT entry_text, created_at
        FROM gratitude_entries
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_recent_observations(limit=200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT category, memory_key, value, confidence, created_at
        FROM memory_observations
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_recent_interaction_events(limit=120):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT intent, message_text, created_at
        FROM interaction_events
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def build_memory_semantic_text(item):
    return " | ".join([
        item.get("scope", ""),
        item.get("category", ""),
        item.get("memory_key", ""),
        item.get("value", ""),
    ])


def record_memory_embedding(scope, category, memory_key, value):
    semantic_text = build_memory_semantic_text({
        "scope": scope,
        "category": category,
        "memory_key": memory_key,
        "value": value,
    })
    embedding = get_embedding(semantic_text)
    if not embedding:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memory_embeddings (scope, category, memory_key, embedding_json, semantic_text)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(scope, category, memory_key) DO UPDATE SET
            embedding_json = excluded.embedding_json,
            semantic_text = excluded.semantic_text,
            updated_at = CURRENT_TIMESTAMP
        """,
        (scope, category, memory_key, json.dumps(embedding), semantic_text),
    )
    conn.commit()
    conn.close()


def get_memory_embedding_rows(limit=100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, embedding_json, semantic_text, updated_at
        FROM memory_embeddings
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_relevant_memories(text, limit=8):
    query_embedding = get_embedding(text)
    if not query_embedding:
        working = get_memory_items("working", limit=4)
        long_term = get_memory_items("long_term", limit=6)
        return {
            "working": working,
            "long_term": long_term,
        }

    scored = []
    for row in get_memory_embedding_rows():
        try:
            existing_embedding = json.loads(row["embedding_json"])
        except:
            continue
        similarity = cosine_similarity(query_embedding, existing_embedding)
        scored.append({
            "scope": row["scope"],
            "category": row["category"],
            "memory_key": row["memory_key"],
            "similarity": similarity,
        })

    scored.sort(key=lambda item: item["similarity"], reverse=True)
    picked_keys = {(item["scope"], item["category"], item["memory_key"]) for item in scored[:limit]}

    working = []
    long_term = []
    for item in get_memory_items("working", limit=20):
        key = (item["scope"], item["category"], item["memory_key"])
        if key in picked_keys:
            working.append(item)
    for item in get_memory_items("long_term", limit=30):
        key = (item["scope"], item["category"], item["memory_key"])
        if key in picked_keys:
            long_term.append(item)

    if not working and not long_term:
        working = get_memory_items("working", limit=4)
        long_term = get_memory_items("long_term", limit=6)

    return {
        "working": working[:4],
        "long_term": long_term[:6],
    }


def format_memory_context(user_text):
    relevant = get_relevant_memories(user_text, limit=10)
    working = relevant["working"]
    long_term = relevant["long_term"]

    lines = [MEMORY_INSTRUCTIONS.strip()]

    if working:
        lines.append("Working memory:")
        for item in working:
            lines.append(f"- {item['category']}: {item['value']}")

    if long_term:
        lines.append("Long-term memory:")
        for item in long_term:
            lines.append(f"- {item['category']}: {item['value']}")

    return "\n".join(lines)


def build_alert_memory_context(candidates):
    combined_text = "\n".join(
        filter(
            None,
            [
                " ".join([
                    candidate.get("headline", ""),
                    candidate.get("snippet", ""),
                    candidate.get("section", ""),
                ])
                for candidate in candidates
            ],
        )
    )

    relevant = get_relevant_memories(combined_text or "alert relevance", limit=12)
    trusted_portfolio = get_trusted_portfolio_symbols(limit=20)
    watchlist = get_watchlist()[:20]

    return {
        "working": relevant["working"],
        "long_term": relevant["long_term"],
        "watchlist": watchlist,
        "trusted_portfolio": trusted_portfolio,
    }


def parse_sqlite_timestamp(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except:
            continue
    try:
        return datetime.fromisoformat(value)
    except:
        return None


def build_memory_interest_vector(limit=80):
    weighted_rows = []
    aggregate = None
    total_weight = 0.0
    now = datetime.now()

    for row in get_memory_embedding_rows(limit=limit):
        category = row.get("category") or ""
        base_weight = MEMORY_VECTOR_CATEGORY_WEIGHTS.get(category, 0.0)
        if base_weight <= 0:
            continue

        try:
            embedding = json.loads(row["embedding_json"])
        except:
            continue
        if not embedding:
            continue

        updated_at = parse_sqlite_timestamp(row.get("updated_at"))
        age_days = max(0.0, (now - updated_at).total_seconds() / 86400.0) if updated_at else 0.0
        recency_weight = max(0.45, 1.0 - min(age_days, 45.0) / 90.0)
        weight = base_weight * recency_weight

        if aggregate is None:
            aggregate = [0.0] * len(embedding)

        for idx, value in enumerate(embedding):
            aggregate[idx] += value * weight
        total_weight += weight
        weighted_rows.append({
            "category": category,
            "memory_key": row.get("memory_key"),
            "semantic_text": row.get("semantic_text", ""),
            "weight": round(weight, 4),
        })

    if not aggregate or total_weight <= 0:
        return {
            "vector": None,
            "evidence": [],
        }

    vector = [value / total_weight for value in aggregate]
    weighted_rows.sort(key=lambda item: item["weight"], reverse=True)
    return {
        "vector": vector,
        "evidence": weighted_rows[:8],
    }


def get_candidate_interest_similarity(candidate, memory_vector):
    if not memory_vector:
        return 0.0

    cached = candidate.get("_interest_similarity")
    if cached is not None:
        return cached

    semantic_text = build_semantic_text(candidate)
    embedding = get_embedding(semantic_text)
    similarity = cosine_similarity(memory_vector, embedding) if embedding else 0.0
    candidate["_interest_similarity"] = similarity
    return similarity


def build_usage_pattern_summary(events):
    counts = {}
    for event in events:
        intent = event["intent"]
        counts[intent] = counts.get(intent, 0) + 1

    patterns = []
    if counts.get("fred", 0) >= 3:
        patterns.append("frequently asks for macro data")
    if counts.get("watchlist_stats", 0) >= 2:
        patterns.append("checks market/watchlist performance often")
    if counts.get("news", 0) >= 2:
        patterns.append("uses Jeeves for news scanning")
    if counts.get("none", 0) >= 4:
        patterns.append("uses Jeeves for broader open-ended conversation")

    return "; ".join(patterns)


def is_protected_memory_item(item):
    return item.get("category") in PROTECTED_MEMORY_CATEGORIES


def apply_memory_decay():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, category, confidence, julianday('now') - julianday(updated_at) AS age_days
        FROM memory_items
        WHERE scope = 'long_term'
        """
    )
    rows = [dict(row) for row in cur.fetchall()]

    for row in rows:
        if row["category"] in PROTECTED_MEMORY_CATEGORIES:
            continue
        age_days = row["age_days"] or 0
        if age_days < 7:
            continue
        random_factor = 1 + random.uniform(-0.01, 0.01)
        decay_step = 0.015 * random_factor
        new_confidence = max(0.25, float(row["confidence"]) - decay_step)
        if new_confidence < float(row["confidence"]):
            cur.execute(
                """
                UPDATE memory_items
                SET confidence = ?, updated_at = updated_at
                WHERE id = ?
                """,
                (round(new_confidence, 4), row["id"]),
            )

    conn.commit()
    conn.close()


def upsert_trend_memory(memory_key, value, confidence=0.78):
    upsert_memory(
        "long_term",
        "behavior_trends",
        memory_key,
        value,
        source_text="trend_consolidation",
        confidence=confidence,
    )
    record_memory_embedding(
        "long_term",
        "behavior_trends",
        memory_key,
        value,
    )


def consolidate_memory_trends():
    observations = get_recent_observations(limit=250)
    if observations:
        grouped = {}
        for obs in observations:
            key = (obs["category"], obs["memory_key"], obs["value"].strip().lower())
            grouped[key] = grouped.get(key, 0) + 1

        best_by_category = {}
        for (category, memory_key, normalized_value), count in grouped.items():
            existing = best_by_category.get(category)
            if not existing or count > existing["count"]:
                best_by_category[category] = {
                    "memory_key": memory_key,
                    "value": normalized_value,
                    "count": count,
                }

        for category, item in best_by_category.items():
            if item["count"] >= 2 and category in {
                "preferences",
                "priorities",
                "learning_style",
                "frictions",
                "taste",
                "portfolio_profile",
                "risk_profile",
            }:
                upsert_memory(
                    "long_term",
                    category,
                    f"trend_{item['memory_key']}",
                    item["value"],
                    source_text="trend_consolidation",
                    confidence=min(0.95, 0.6 + (item["count"] * 0.08)),
                )
                record_memory_embedding(
                    "long_term",
                    category,
                    f"trend_{item['memory_key']}",
                    item["value"],
                )

            if item["count"] >= 3 and category == "preferences":
                upsert_memory(
                    "long_term",
                    "deep_preferences",
                    f"core_{item['memory_key']}",
                    item["value"],
                    source_text="trend_consolidation",
                    confidence=min(0.97, 0.7 + (item["count"] * 0.06)),
                )
                record_memory_embedding(
                    "long_term",
                    "deep_preferences",
                    f"core_{item['memory_key']}",
                    item["value"],
                )

            if item["count"] >= 3 and category == "frictions":
                upsert_memory(
                    "long_term",
                    "long_term_frictions",
                    f"core_{item['memory_key']}",
                    item["value"],
                    source_text="trend_consolidation",
                    confidence=min(0.97, 0.7 + (item["count"] * 0.06)),
                )
                record_memory_embedding(
                    "long_term",
                    "long_term_frictions",
                    f"core_{item['memory_key']}",
                    item["value"],
                )

    interaction_events = get_recent_interaction_events(limit=60)
    usage_summary = build_usage_pattern_summary(interaction_events)
    if usage_summary:
        upsert_memory(
            "working",
            "usage_patterns",
            "recent_usage",
            usage_summary,
            source_text="interaction_trends",
            confidence=0.75,
        )
        record_memory_embedding(
            "working",
            "usage_patterns",
            "recent_usage",
            usage_summary,
        )

    texts = " ".join(event["message_text"].lower() for event in interaction_events)
    if texts:
        recurrent_focus = []
        if any(term in texts for term in ["uranium", "nuclear", "ccj", "urnm", "uuuu"]):
            recurrent_focus.append("uranium and nuclear")
        if any(term in texts for term in ["cpi", "inflation", "fed", "treasury", "unemployment"]):
            recurrent_focus.append("macro and rates")
        if any(term in texts for term in ["watchlist", "stock price", "portfolio", "performed today"]):
            recurrent_focus.append("market performance")
        if recurrent_focus:
            upsert_trend_memory("recurrent_focus", "; ".join(recurrent_focus), confidence=0.82)

    feedback = get_recent_alert_feedback(limit=30)
    if feedback:
        counts = {}
        for item in feedback:
            counts[item["feedback_type"]] = counts.get(item["feedback_type"], 0) + 1
        if counts.get("too much noise", 0) >= 2:
            upsert_trend_memory("alert_tolerance", "prefers stricter, lower-noise alerts", confidence=0.86)
        if counts.get("more like this", 0) >= 1 or counts.get("good alert", 0) >= 2:
            upsert_trend_memory("alert_preferences", "rewards highly relevant alerts with direct implication", confidence=0.84)
        if counts.get("late", 0) >= 1:
            upsert_trend_memory("timing_preference", "cares strongly about alert speed", confidence=0.85)

    apply_memory_decay()


def extract_memory_updates(text):
    t = text.strip()
    lower = t.lower()
    updates = []
    gratitude_entry = None

    if lower.startswith("grateful for "):
        gratitude_entry = t[len("grateful for "):].strip()
    elif "i was grateful for " in lower:
        gratitude_entry = t[lower.index("i was grateful for ") + len("i was grateful for "):].strip()

    feeling_match = re.search(r"\b(?:i feel|i'm feeling|i am feeling)\s+(.+)", t, re.IGNORECASE)
    if feeling_match:
        updates.append({
            "scope": "working",
            "category": "emotional_state",
            "memory_key": "current_state",
            "value": feeling_match.group(1).strip(),
            "confidence": 0.7,
        })

    preference_patterns = [
        (r"\bi prefer\s+(.+)", "preferences", "preference"),
        (r"\bi dislike\s+(.+)", "preferences", "dislike"),
        (r"\bi hate\s+(.+)", "preferences", "dislike"),
        (r"\bi care about\s+(.+)", "priorities", "care_about"),
        (r"\bfocus on\s+(.+)", "priorities", "focus"),
        (r"\bi want to learn about\s+(.+)", "learning_style", "learning_targets"),
        (r"\bi like to learn by\s+(.+)", "learning_style", "learning_method"),
        (r"\bi learn best by\s+(.+)", "learning_style", "learning_method"),
        (r"\bi get frustrated by\s+(.+)", "frictions", "frustration"),
        (r"\bi am frustrated by\s+(.+)", "frictions", "frustration"),
        (r"\bi like\s+(.+)", "taste", "likes"),
        (r"\bi love\s+(.+)", "taste", "likes"),
        (r"\bi enjoy\s+(.+)", "taste", "enjoys"),
        (r"\bi am someone who\s+(.+)", "core_traits", "identity_statement"),
        (r"\ba core trait of mine is\s+(.+)", "core_traits", "identity_statement"),
        (r"\bi am proud of\s+(.+)", "major_successes", "success_memory"),
        (r"\bmy biggest success was\s+(.+)", "major_successes", "success_memory"),
        (r"\bmy biggest failure was\s+(.+)", "major_failures", "failure_memory"),
        (r"\bi regret\s+(.+)", "major_failures", "regret_pattern"),
        (r"\ba defining moment for me was\s+(.+)", "defining_moments", "defining_memory"),
        (r"\bi want\s+(.+)", "goals", "stated_goal"),
        (r"\bmy portfolio is\s+(.+)", "portfolio_profile", "portfolio_shape"),
        (r"\bmy holdings are\s+(.+)", "portfolio_profile", "holdings_shape"),
    ]

    for pattern, category, memory_key in preference_patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            updates.append({
                "scope": "long_term",
                "category": category,
                "memory_key": memory_key,
                "value": match.group(1).strip(),
                "confidence": 0.75,
            })

    risk_patterns = [
        r"\bi am risk averse\b",
        r"\bi am conservative\b",
        r"\bi take a lot of risk\b",
        r"\bi am willing to take risk\b",
        r"\bi am in cash\b",
    ]

    for pattern in risk_patterns:
        match = re.search(pattern, lower)
        if match:
            updates.append({
                "scope": "long_term",
                "category": "risk_profile",
                "memory_key": "risk_style",
                "value": match.group(0),
                "confidence": 0.8,
            })
            break

    portfolio_patterns = [
        (r"\bi own\s+(.+)", "portfolio_profile", "holdings"),
        (r"\bi am heavy in\s+(.+)", "portfolio_profile", "concentration"),
        (r"\bi am mostly in\s+(.+)", "portfolio_profile", "concentration"),
        (r"\bi am moving into cash\b", "portfolio_profile", "cash_shift"),
    ]

    for pattern, category, memory_key in portfolio_patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            value = match.group(1).strip() if match.groups() else match.group(0).strip()
            updates.append({
                "scope": "long_term",
                "category": category,
                "memory_key": memory_key,
                "value": value,
                "confidence": 0.8,
            })

    return updates, gratitude_entry


def process_memory_updates(text):
    updates, gratitude_entry = extract_memory_updates(text)
    portfolio_symbols = []

    for update in updates:
        add_memory_observation(
            update["category"],
            update["memory_key"],
            update["value"],
            confidence=update["confidence"],
        )
        upsert_memory(
            update["scope"],
            update["category"],
            update["memory_key"],
            update["value"],
            source_text=text,
            confidence=update["confidence"],
        )
        record_memory_embedding(
            update["scope"],
            update["category"],
            update["memory_key"],
            update["value"],
        )
        if update["category"] == "portfolio_profile":
            portfolio_symbols.extend(extract_symbols_from_text(update["value"]))

    if gratitude_entry:
        add_gratitude_entry(gratitude_entry)
        add_memory_observation(
            "gratitude",
            "latest_gratitude",
            gratitude_entry,
            confidence=0.9,
        )
        upsert_memory(
            "working",
            "gratitude",
            "latest_gratitude",
            gratitude_entry,
            source_text=text,
            confidence=0.9,
        )
        upsert_memory(
            "long_term",
            "gratitude",
            "gratitude_reflection",
            gratitude_entry,
            source_text=text,
            confidence=0.8,
        )
        record_memory_embedding(
            "working",
            "gratitude",
            "latest_gratitude",
            gratitude_entry,
        )
        record_memory_embedding(
            "long_term",
            "gratitude",
            "gratitude_reflection",
            gratitude_entry,
        )

    if portfolio_symbols:
        upsert_portfolio_symbols(portfolio_symbols, source_text=text)

    consolidate_memory_trends()


def run_nightly_memory_consolidation():
    consolidate_memory_trends()
    interaction_events = get_recent_interaction_events(limit=120)
    observations = get_recent_observations(limit=120)

    if interaction_events:
        topic_summary = build_usage_pattern_summary(interaction_events)
        if topic_summary:
            upsert_memory(
                "long_term",
                "behavior_trends",
                "nightly_usage_summary",
                topic_summary,
                source_text="nightly_consolidation",
                confidence=0.8,
            )
            record_memory_embedding(
                "long_term",
                "behavior_trends",
                "nightly_usage_summary",
                topic_summary,
            )

    if observations:
        categories = {}
        for obs in observations:
            categories[obs["category"]] = categories.get(obs["category"], 0) + 1
        if categories:
            strongest = sorted(categories.items(), key=lambda item: item[1], reverse=True)[:3]
            summary = "; ".join(f"{name}:{count}" for name, count in strongest)
            upsert_memory(
                "working",
                "nightly_summary",
                "recent_personal_signal",
                summary,
                source_text="nightly_consolidation",
                confidence=0.72,
            )
            record_memory_embedding(
                "working",
                "nightly_summary",
                "recent_personal_signal",
                summary,
            )

    apply_memory_decay()


def get_memory_debug_summary():
    return {
        "working_memory": get_memory_items("working", limit=20),
        "long_term_memory": get_memory_items("long_term", limit=20),
        "portfolio_holdings": get_portfolio_holdings(limit=12),
        "trusted_portfolio_snapshot": get_latest_trusted_portfolio_snapshot(),
        "recent_gratitude": get_recent_gratitude(limit=30),
        "recent_observations": get_recent_observations(limit=12),
        "recent_interactions": get_recent_interaction_events(limit=30),
        "recent_alert_feedback": get_recent_alert_feedback(limit=12),
        "semantic_memory_count": len(get_memory_embedding_rows(limit=500)),
    }

# ---------------- ALERTS ----------------

def build_event_hash(category, headline):
    normalized = f"{category.strip().upper()}|{headline.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def normalize_event_text(text):
    cleaned = (text or "").lower()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def build_event_fingerprint(candidate):
    headline = normalize_event_text(candidate.get("headline", ""))
    snippet = normalize_event_text(candidate.get("snippet", ""))
    section = normalize_event_text(candidate.get("section", ""))
    category = candidate.get("category", "").upper()

    if snippet:
        base = f"{category}|{headline}|{snippet}|{section}"
    else:
        base = f"{category}|{headline}|{section}"

    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def build_semantic_text(candidate):
    parts = [
        candidate.get("headline", ""),
        candidate.get("snippet", ""),
        candidate.get("section", ""),
    ]
    return " | ".join([part.strip() for part in parts if part and part.strip()])


def count_tier_alerts_today(category, tier):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM alert_log
        WHERE category = ?
          AND tier = ?
          AND date(created_at) = date('now')
        """,
        (category, tier),
    )
    row = cur.fetchone()
    conn.close()
    return row["count"] if row else 0


def prune_old_event_memory(hours=36):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM event_hashes
        WHERE datetime(last_seen_at) < datetime('now', ?)
        """,
        (f"-{hours} hours",),
    )
    cur.execute(
        """
        DELETE FROM event_embeddings
        WHERE datetime(created_at) < datetime('now', ?)
        """,
        (f"-{hours} hours",),
    )
    conn.commit()
    conn.close()


def has_seen_event_hash(event_hash):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM event_hashes
        WHERE event_hash = ?
          AND datetime(last_seen_at) >= datetime('now', '-36 hours')
        """,
        (event_hash,),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_recent_event_embeddings(category, limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_hash, headline, semantic_text, embedding_json, created_at
        FROM event_embeddings
        WHERE category = ?
          AND datetime(created_at) >= datetime('now', '-36 hours')
        ORDER BY id DESC
        LIMIT ?
        """,
        (category, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def record_event_hash(event_hash, category, headline):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_hashes (event_hash, category, headline)
        VALUES (?, ?, ?)
        ON CONFLICT(event_hash) DO UPDATE SET
            last_seen_at = CURRENT_TIMESTAMP
        """,
        (event_hash, category, headline),
    )
    conn.commit()
    conn.close()


def record_event_embedding(event_hash, category, headline, semantic_text, embedding):
    if not semantic_text or not embedding:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_embeddings (event_hash, category, headline, semantic_text, embedding_json)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(event_hash) DO NOTHING
        """,
        (event_hash, category, headline, semantic_text, json.dumps(embedding)),
    )
    conn.commit()
    conn.close()


def serialize_json(value, fallback=None):
    try:
        return json.dumps(value)
    except:
        return json.dumps(fallback if fallback is not None else {})


def get_candidate_source_label(candidate):
    source = candidate.get("source") or ""
    if source == "NYT":
        return "NYT"
    if source == "FRED":
        return "FRED"
    if source == "CURRENTS":
        domain = extract_url_domain(candidate.get("web_url", ""))
        return f"{domain} via Currents" if domain else "Currents"
    return source or "Unknown"


def get_candidate_source_refs(candidate):
    refs = candidate.get("source_refs") or []
    if refs:
        return list(dict.fromkeys(refs))
    return [get_candidate_source_label(candidate)]


def build_candidate_body_text(candidate):
    if not candidate:
        return ""

    parts = []
    for value in [
        candidate.get("body_text"),
        candidate.get("lead_paragraph"),
        candidate.get("abstract"),
        candidate.get("snippet"),
    ]:
        cleaned = (value or "").strip()
        if cleaned and cleaned not in parts:
            parts.append(cleaned)

    if candidate.get("source") == "FRED" and not parts:
        parts.append(candidate.get("headline", ""))

    return "\n\n".join(parts).strip()


def upsert_event_context(alert_id, event_hash, category, tier, candidate):
    if not candidate:
        return

    body_text = build_candidate_body_text(candidate)
    selection_reasons = candidate.get("selection_reasons", [])
    raw_payload = candidate.get("raw_payload", candidate)
    source_label = get_candidate_source_label(candidate)
    source_refs = get_candidate_source_refs(candidate)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_contexts (
            event_hash, alert_id, category, tier, source, headline, snippet, section,
            published_at, fingerprint, semantic_text, body_text, web_url, score,
            source_label, source_refs_json, selection_reasons_json, raw_payload_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(event_hash) DO UPDATE SET
            alert_id = excluded.alert_id,
            category = excluded.category,
            tier = excluded.tier,
            source = excluded.source,
            headline = excluded.headline,
            snippet = excluded.snippet,
            section = excluded.section,
            published_at = excluded.published_at,
            fingerprint = excluded.fingerprint,
            semantic_text = excluded.semantic_text,
            body_text = excluded.body_text,
            web_url = excluded.web_url,
            score = excluded.score,
            source_label = excluded.source_label,
            source_refs_json = excluded.source_refs_json,
            selection_reasons_json = excluded.selection_reasons_json,
            raw_payload_json = excluded.raw_payload_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            event_hash,
            alert_id,
            category.upper(),
            tier,
            candidate.get("source"),
            candidate.get("headline"),
            candidate.get("snippet", ""),
            candidate.get("section", ""),
            candidate.get("published_at"),
            build_event_fingerprint(candidate),
            build_semantic_text(candidate),
            body_text,
            candidate.get("web_url", ""),
            candidate.get("score"),
            source_label,
            serialize_json(source_refs, fallback=[]),
            serialize_json(selection_reasons, fallback=[]),
            serialize_json(raw_payload, fallback={}),
        ),
    )
    conn.commit()
    conn.close()


def get_event_context(event_hash=None, alert_id=None):
    if not event_hash and not alert_id:
        return None

    conn = get_conn()
    cur = conn.cursor()
    if event_hash:
        cur.execute(
            """
            SELECT *
            FROM event_contexts
            WHERE event_hash = ?
            LIMIT 1
            """,
            (event_hash,),
        )
    else:
        cur.execute(
            """
            SELECT *
            FROM event_contexts
            WHERE alert_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (alert_id,),
        )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def build_brief_display_codes(alerts):
    counts = {}
    order = {}
    for item in alerts:
        base = f"{item['category']}{item['tier']}"
        counts[base] = counts.get(base, 0) + 1

    for item in alerts:
        base = f"{item['category']}{item['tier']}"
        order[base] = order.get(base, 0) + 1
        item["display_code"] = base if counts[base] == 1 else f"{base}.{order[base]}"

    return alerts


def store_brief_event_map(alerts):
    if not alerts:
        return

    brief_run_id = datetime.now(LOCAL_TZ).strftime("%Y%m%d%H%M%S")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM brief_event_map
        WHERE id NOT IN (
            SELECT id
            FROM brief_event_map
            ORDER BY id DESC
            LIMIT 200
        )
        """
    )

    for item in alerts:
        reference_codes = [item["display_code"]]
        if item.get("alert_id"):
            reference_codes.append(item["alert_id"])
        base_code = f"{item['category']}{item['tier']}"
        if item["display_code"] != base_code:
            reference_codes.append(base_code)

        for reference_code in reference_codes:
            cur.execute(
                """
                INSERT INTO brief_event_map (brief_run_id, reference_code, alert_id, event_hash, headline)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    brief_run_id,
                    reference_code.upper(),
                    item.get("alert_id"),
                    item["event_hash"],
                    item["headline"],
                ),
            )

    conn.commit()
    conn.close()


def resolve_brief_reference(reference_code):
    ref = (reference_code or "").strip().upper()
    if not ref:
        return {"status": "missing"}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT brief_run_id
        FROM brief_event_map
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"status": "missing"}

    brief_run_id = row["brief_run_id"]
    cur.execute(
        """
        SELECT reference_code, alert_id, event_hash, headline
        FROM brief_event_map
        WHERE brief_run_id = ?
          AND reference_code = ?
        ORDER BY id DESC
        """,
        (brief_run_id, ref),
    )
    matches = [dict(item) for item in cur.fetchall()]
    conn.close()

    if not matches:
        return {"status": "missing"}
    if len(matches) > 1:
        return {"status": "ambiguous", "matches": matches}
    return {"status": "ok", "match": matches[0]}


def get_source_poll_state(source_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_name, last_polled_at, note
        FROM source_poll_state
        WHERE source_name = ?
        LIMIT 1
        """,
        (source_name,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def source_poll_due(source_name, min_interval_minutes):
    row = get_source_poll_state(source_name)
    if not row or not row.get("last_polled_at"):
        return True
    try:
        last = datetime.fromisoformat(row["last_polled_at"])
        now = datetime.now()
        return (now - last).total_seconds() >= (min_interval_minutes * 60)
    except:
        return True


def mark_source_polled(source_name, note=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO source_poll_state (source_name, last_polled_at, note)
        VALUES (?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_polled_at = CURRENT_TIMESTAMP,
            note = excluded.note
        """,
        (source_name, note or ""),
    )
    conn.commit()
    conn.close()


def get_embedding(text):
    if not text:
        return None

    try:
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text,
        )
        return response.data[0].embedding
    except:
        return None


def cosine_similarity(vec_a, vec_b):
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0

    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def find_semantic_duplicate(category, candidate, threshold=0.90):
    if candidate.get("source") == "FRED":
        return None

    semantic_text = build_semantic_text(candidate)
    candidate_embedding = get_embedding(semantic_text)
    if not candidate_embedding:
        return None

    best_match = None
    best_score = 0.0

    for row in get_recent_event_embeddings(category):
        try:
            existing_embedding = json.loads(row["embedding_json"])
        except:
            continue

        similarity = cosine_similarity(candidate_embedding, existing_embedding)
        if similarity > best_score:
            best_score = similarity
            best_match = row

    if best_match and best_score >= threshold:
        return {
            "match": best_match,
            "similarity": round(best_score, 4),
            "embedding": candidate_embedding,
            "semantic_text": semantic_text,
        }

    return {
        "match": None,
        "similarity": round(best_score, 4),
        "embedding": candidate_embedding,
        "semantic_text": semantic_text,
    }


def build_alert_id(category, tier):
    existing_count = count_tier_alerts_today(category, tier)
    return f"{category.upper()}{tier}-{existing_count + 1}"


def can_send_alert(category, tier, event_hash, candidate=None):
    feedback_profile = get_feedback_profile(limit=20)
    prune_old_event_memory(hours=36)

    if has_seen_event_hash(event_hash):
        return False, "duplicate_event", None

    semantic_result = None
    if candidate is not None:
        semantic_result = find_semantic_duplicate(category, candidate)
        if semantic_result and semantic_result.get("match") is not None:
            return False, "semantic_duplicate", semantic_result

    if tier == 1:
        return True, "tier_1", semantic_result

    if tier == 2 and count_tier_alerts_today(category, tier) >= feedback_profile["tier2_cap"]:
        return False, "tier_2_cap_reached", semantic_result

    return True, "allowed", semantic_result


def log_alert(category, tier, headline, sent_to_user=1, candidate=None):
    event_hash = build_event_hash(category, headline)
    allowed, reason, semantic_result = can_send_alert(category, tier, event_hash, candidate=candidate)

    if not allowed:
        if candidate is not None:
            upsert_event_context(None, event_hash, category, tier, candidate)
        return {
            "ok": False,
            "reason": reason,
            "event_hash": event_hash,
            "semantic_similarity": semantic_result["similarity"] if semantic_result else None,
        }

    alert_id = build_alert_id(category, tier)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_log (alert_id, category, tier, headline, event_hash, sent_to_user)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (alert_id, category.upper(), tier, headline, event_hash, sent_to_user),
    )
    conn.commit()
    conn.close()

    record_event_hash(event_hash, category.upper(), headline)
    if candidate is not None:
        upsert_event_context(alert_id, event_hash, category, tier, candidate)
    if candidate is not None and semantic_result:
        record_event_embedding(
            event_hash,
            category.upper(),
            headline,
            semantic_result.get("semantic_text", ""),
            semantic_result.get("embedding"),
        )

    return {
        "ok": True,
        "alert_id": alert_id,
        "event_hash": event_hash,
        "reason": reason,
        "semantic_similarity": semantic_result["similarity"] if semantic_result else None,
    }


def get_alert_debug_summary():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS count FROM alert_log")
    alert_count = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) AS count FROM event_hashes")
    hash_count = cur.fetchone()["count"]

    cur.execute(
        """
        SELECT alert_id, category, tier, headline, created_at
        FROM alert_log
        ORDER BY id DESC
        LIMIT 10
        """
    )
    recent_alerts = [dict(row) for row in cur.fetchall()]
    conn.close()

    return {
        "alert_log_count": alert_count,
        "event_hash_count": hash_count,
        "recent_alerts": recent_alerts,
    }


def get_recent_alerts_for_brief(limit=12, include_debug=False):
    conn = get_conn()
    cur = conn.cursor()

    if include_debug:
        cur.execute(
            """
            SELECT
                a.alert_id,
                a.category,
                a.tier,
                a.headline,
                a.event_hash,
                a.sent_to_user,
                a.created_at,
                e.source,
                e.snippet,
                e.section,
                e.published_at,
                e.body_text,
                e.web_url,
                e.source_label,
                e.source_refs_json,
                e.selection_reasons_json
            FROM alert_log a
            LEFT JOIN event_contexts e
              ON e.event_hash = a.event_hash
            WHERE DATE(a.created_at) = DATE('now')
            ORDER BY a.tier ASC, a.id DESC
            LIMIT ?
            """,
            (limit,),
        )
    else:
        cur.execute(
            """
            SELECT
                a.alert_id,
                a.category,
                a.tier,
                a.headline,
                a.event_hash,
                a.sent_to_user,
                a.created_at,
                e.source,
                e.snippet,
                e.section,
                e.published_at,
                e.body_text,
                e.web_url,
                e.source_label,
                e.source_refs_json,
                e.selection_reasons_json
            FROM alert_log a
            LEFT JOIN event_contexts e
              ON e.event_hash = a.event_hash
            WHERE DATE(a.created_at) = DATE('now')
              AND a.sent_to_user = 1
            ORDER BY a.tier ASC, a.id DESC
            LIMIT ?
            """,
            (limit,),
        )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def is_trading_day_now():
    return datetime.now(LOCAL_TZ).weekday() < 5


def build_market_section(title, symbols, snapshots, max_items=4):
    if not symbols or not snapshots:
        return None

    by_ticker = {item.get("ticker"): item for item in snapshots if item.get("ticker")}
    parts = []
    for symbol in symbols[:max_items]:
        item = by_ticker.get(symbol)
        if not item:
            continue
        price = extract_snapshot_price(item)
        change = extract_snapshot_change(item)
        pct = extract_snapshot_pct(item)
        if price is None and change is None and pct is None:
            continue
        if change in (0, 0.0, None) and pct in (0, 0.0, None):
            continue
        parts.append(f"{symbol} {format_price(price)} ({format_pct(pct)}, {format_change(change)})")

    if not parts:
        return None
    return f"{title}: " + "; ".join(parts)


def get_portfolio_market_section(max_items=4):
    trusted_symbols = get_trusted_portfolio_symbols(limit=max_items + 2)
    if not trusted_symbols:
        return None
    snapshots = get_twelvedata_watchlist_snapshot(trusted_symbols[:max_items])
    return build_market_section("Portfolio", trusted_symbols[:max_items], snapshots, max_items=max_items)


def get_watchlist_market_section(max_items=4):
    watchlist = get_watchlist()
    if not watchlist:
        return None
    snapshots = get_twelvedata_watchlist_snapshot(watchlist[:max_items])
    return build_market_section("Watchlist", watchlist[:max_items], snapshots, max_items=max_items)


def format_brief_source_suffix(item):
    refs = []
    try:
        refs = json.loads(item.get("source_refs_json") or "[]")
    except:
        refs = []
    if not refs and item.get("source_label"):
        refs = [item["source_label"]]
    if not refs:
        return ""
    return f" [{', '.join(refs[:2])}]"


def format_source_refs_list(refs, limit=3):
    refs = [ref for ref in (refs or []) if ref]
    if not refs:
        return ""
    return ", ".join(refs[:limit])


def format_alert_message(candidate, alert_result):
    code = alert_result.get("alert_id") or f"{candidate['category']}{candidate.get('assigned_tier', candidate.get('tier', 2))}"
    source_text = format_source_refs_list(candidate.get("source_refs"), limit=3)
    suffix = f" [{source_text}]" if source_text else ""
    return f"{code}: {candidate['headline']}{suffix}"


def compose_daily_brief(include_debug=False):
    feedback_profile = get_feedback_profile(limit=20)
    alerts = get_recent_alerts_for_brief(limit=12, include_debug=include_debug)
    brief_tier_limit = feedback_profile["brief_tier_limit"]
    filtered_alerts = [item for item in alerts if item["tier"] <= brief_tier_limit]
    filtered_alerts = filtered_alerts[:5]
    filtered_alerts = build_brief_display_codes(filtered_alerts)

    lines = []
    if filtered_alerts:
        lines.append("Daily brief:")
        for item in filtered_alerts:
            lines.append(f"{item['display_code']}: {item['headline']}{format_brief_source_suffix(item)}")
        store_brief_event_map(filtered_alerts)

    if is_trading_day_now():
        portfolio_section = get_portfolio_market_section(max_items=4)
        if portfolio_section:
            lines.append(portfolio_section)

        watchlist_section = get_watchlist_market_section(max_items=4)
        if watchlist_section:
            lines.append(watchlist_section)

    if not lines:
        return "Nothing to report."

    return "\n".join(lines)


def backfill_event_context_for_reference(match):
    if not match:
        return None

    watchlist = get_watchlist()
    target_hash = match.get("event_hash")
    target_headline = (match.get("headline") or "").strip().lower()

    for candidate in build_poll_candidates():
        event_hash = build_event_hash(candidate["category"], candidate["headline"])
        if event_hash != target_hash and candidate["headline"].strip().lower() != target_headline:
            continue

        scoring = score_candidate(candidate, watchlist)
        enriched_candidate = {
            **candidate,
            "score": scoring["score"],
            "selection_reasons": scoring["reasons"],
            "assigned_tier": scoring["tier"],
        }
        upsert_event_context(
            match.get("alert_id"),
            event_hash,
            candidate["category"],
            scoring["tier"],
            enriched_candidate,
        )
        return get_event_context(event_hash=event_hash)

    return None


def expand_brief_event(reference_code):
    resolved = resolve_brief_reference(reference_code)
    if resolved["status"] == "missing":
        return "I don't know which brief item that is."
    if resolved["status"] == "ambiguous":
        headlines = [item["headline"] for item in resolved["matches"][:2]]
        return "That code is ambiguous. Be more specific."

    match = resolved["match"]
    context = get_event_context(event_hash=match["event_hash"])
    if not context:
        context = backfill_event_context_for_reference(match)
    if not context:
        return "I don't have enough stored context for that item."

    reasons = []
    try:
        reasons = json.loads(context.get("selection_reasons_json") or "[]")
    except:
        reasons = []
    source_refs = []
    try:
        source_refs = json.loads(context.get("source_refs_json") or "[]")
    except:
        source_refs = []

    prompt = f"""
You are Jeeves. Expand briefly on this alert item for Manu.

Reference code: {reference_code}
Headline: {context.get("headline", "")}
Snippet: {context.get("snippet", "")}
Section: {context.get("section", "")}
Source: {context.get("source", "")}
Source references: {", ".join(source_refs) if source_refs else context.get("source_label", "")}
Published at: {context.get("published_at", "")}
Selection reasons: {", ".join(reasons)}
Stored article/body text:
{context.get("body_text", "")}

Write a short response with exactly these parts:
1. What happened
2. Why it matters
3. Why Jeeves selected it

Keep it concise and specific. Do not mention missing data unless necessary.
"""

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are Jeeves. Be concise, specific, and useful."},
                {"role": "user", "content": prompt},
            ],
        )
        return completion.choices[0].message.content.strip()
    except:
        lines = [
            f"{reference_code}: {context.get('headline', '')}",
        ]
        if context.get("snippet"):
            lines.append(context["snippet"])
        if source_refs:
            lines.append("Sources: " + ", ".join(source_refs[:3]))
        if reasons:
            lines.append("Why selected: " + ", ".join(reasons[:3]))
        if context.get("web_url"):
            lines.append(context["web_url"])
        return "\n".join(lines)


def ensure_whatsapp_prefix(number):
    if not number:
        return None
    return number if number.startswith("whatsapp:") else f"whatsapp:{number}"


def send_whatsapp_message(body):
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM, MY_NUMBER]):
        return {"ok": False, "reason": "missing_twilio_config"}

    try:
        auth = base64.b64encode(f"{TWILIO_ACCOUNT_SID}:{TWILIO_AUTH_TOKEN}".encode("utf-8")).decode("utf-8")
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
            headers={
                "Authorization": f"Basic {auth}",
            },
            data={
                "From": ensure_whatsapp_prefix(TWILIO_WHATSAPP_FROM),
                "To": ensure_whatsapp_prefix(MY_NUMBER),
                "Body": body,
            },
            timeout=20,
        )
        if response.status_code >= 300:
            return {"ok": False, "reason": "twilio_error", "status_code": response.status_code, "response": response.text[:300]}
        data = response.json()
        return {"ok": True, "sid": data.get("sid")}
    except Exception as exc:
        return {"ok": False, "reason": "exception", "error": str(exc)}


def announce_current_deploy_once():
    deploy_key = get_current_deploy_key()
    if not deploy_key:
        return {"ok": False, "reason": "missing_deploy_key"}

    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM, MY_NUMBER]):
        return {"ok": False, "reason": "missing_twilio_config"}

    inserted = mark_deploy_announced(deploy_key)
    if not inserted:
        return {"ok": False, "reason": "already_announced"}

    short_sha = (RAILWAY_GIT_COMMIT_SHA or "unknown")[:7]
    message = f"Jeeves updated: deployed commit {short_sha}."
    result = send_whatsapp_message(message)
    if result.get("ok"):
        log_outbound_message("deploy_update", message)
        return {"ok": True, "message": message}
    return result

# ---------------- POLLING ----------------

def score_candidate(candidate, watchlist, memory_vector_bundle=None):
    headline = candidate["headline"].lower()
    combined_text = " ".join([
        candidate.get("headline", ""),
        candidate.get("snippet", ""),
        candidate.get("section", ""),
    ]).lower()
    score = 0
    reasons = []
    feedback_profile = get_feedback_profile(limit=20)
    trusted_portfolio_symbols = get_trusted_portfolio_symbols(limit=10)
    has_trusted_portfolio_match = False
    memory_vector_bundle = memory_vector_bundle or build_memory_interest_vector()
    interest_similarity = get_candidate_interest_similarity(candidate, memory_vector_bundle.get("vector"))

    if interest_similarity >= 0.42:
        score += 5
        reasons.append(f"memory_vector:{interest_similarity:.2f}")
    elif interest_similarity >= 0.34:
        score += 3
        reasons.append(f"memory_vector:{interest_similarity:.2f}")
    elif interest_similarity >= 0.27:
        score += 1
        reasons.append(f"memory_vector:{interest_similarity:.2f}")

    for item in watchlist:
        if item.lower() in combined_text:
            score += 5
            reasons.append(f"watchlist:{item}")

    for item in trusted_portfolio_symbols:
        if item.lower() in combined_text:
            score += 6
            reasons.append(f"portfolio:{item}")
            has_trusted_portfolio_match = True

    if candidate["source"] == "FRED":
        score += 2
        reasons.append("source:fred")

        if "cpi" in headline or "10y treasury" in headline or "fed funds" in headline:
            score += 1
            reasons.append("macro:core")

    if candidate["source"] == "NYT":
        score += 1
        reasons.append("source:nyt")

    if candidate["source"] == "CURRENTS":
        score += 1
        reasons.append("source:currents")

    if candidate["category"] == "P" and has_trusted_portfolio_match:
        score += 2
        reasons.append("category:portfolio")
    elif candidate["category"] == "E":
        score += 1
        reasons.append("category:macro")
    elif candidate["category"] == "L":
        score += 1
        reasons.append("category:local")

    if feedback_profile["sensitivity"] != 0:
        score += feedback_profile["sensitivity"]
        reasons.append(f"feedback:sensitivity:{feedback_profile['sensitivity']:+d}")

    if feedback_profile["urgency"] > 0 and candidate["source"] == "NYT":
        score += min(2, feedback_profile["urgency"])
        reasons.append("feedback:urgency")

    tier_1_threshold = 8 if feedback_profile["require_higher_score"] else 6
    tier_2_threshold = 4 if feedback_profile["promote_quick_alerts"] else 3

    if score >= tier_1_threshold:
        tier = 1
    elif score >= tier_2_threshold:
        tier = 2
    else:
        tier = 3

    if candidate["category"] == "P" and not has_trusted_portfolio_match:
        tier = max(tier, 3)
        reasons.append("portfolio:awaiting_trusted_data")

    return {
        "score": score,
        "tier": tier,
        "reasons": reasons,
    }


def build_story_signature(candidate):
    text = " ".join([
        candidate.get("headline", ""),
        candidate.get("snippet", ""),
    ]).lower()
    tokens = re.findall(r"[a-z0-9]+", text)
    kept = [token for token in tokens if token not in STORY_STOPWORDS and len(token) > 2]
    kept = kept[:12]
    return " ".join(sorted(set(kept)))


def story_overlap(candidate_a, candidate_b):
    tokens_a = set(build_story_signature(candidate_a).split())
    tokens_b = set(build_story_signature(candidate_b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b))


def dedupe_candidates(candidates):
    deduped = []
    seen_fingerprints = set()

    for candidate in candidates:
        candidate = {
            **candidate,
            "source_refs": get_candidate_source_refs(candidate),
        }
        fingerprint = build_event_fingerprint(candidate)
        if fingerprint in seen_fingerprints:
            continue

        duplicate = False
        for existing in deduped:
            if candidate.get("category") != existing.get("category"):
                continue
            if candidate.get("source") == "FRED" and existing.get("source") == "FRED":
                continue
            if story_overlap(candidate, existing) >= 0.6:
                merged_refs = list(dict.fromkeys((existing.get("source_refs") or []) + (candidate.get("source_refs") or [])))
                existing["source_refs"] = merged_refs
                if len(candidate.get("body_text", "")) > len(existing.get("body_text", "")):
                    existing["body_text"] = candidate.get("body_text", "")
                if len(candidate.get("snippet", "")) > len(existing.get("snippet", "")):
                    existing["snippet"] = candidate.get("snippet", "")
                if not existing.get("web_url") and candidate.get("web_url"):
                    existing["web_url"] = candidate.get("web_url")
                duplicate = True
                break

        if duplicate:
            continue

        seen_fingerprints.add(fingerprint)
        deduped.append(candidate)

    return deduped


def classify_news_category(query, headline, snippet, section, watchlist=None):
    text = " ".join([
        query or "",
        headline or "",
        snippet or "",
        section or "",
    ]).lower()
    if any(term in text for term in ["bay area", "san francisco", "berkeley", "baja california", "bcs", "earthquake"]):
        return "L"
    if any(term in text for term in ["fed", "inflation", "cpi", "rates", "rate cut", "jobs", "employment", "treasury"]):
        return "E"
    if any(term in text for term in ["uranium", "nuclear", "cameco", "ccj", "kazatomprom", "kazakhstan"]):
        return "G"
    if any(term in text for term in ["iran", "russia", "sanction", "war", "shipping", "strait", "conflict"]):
        return "G"
    return "G"


def get_currents_candidates(query, category_hint=None, watchlist=None):
    if not CURRENTS_API_KEY:
        return []

    try:
        response = requests.get(
            "https://api.currentsapi.services/v1/search",
            params={
                "keywords": query,
                "language": "en",
                "page_size": 3,
                "apiKey": CURRENTS_API_KEY,
            },
            timeout=10,
        )
        if response.status_code != 200:
            return []

        data = response.json()
        articles = data.get("news", [])[:3]
        candidates = []

        for article in articles:
            headline = article.get("title") or ""
            snippet = article.get("description") or ""
            body_text = article.get("description") or ""
            section = ", ".join(article.get("category") or []) if isinstance(article.get("category"), list) else (article.get("category") or "")
            web_url = article.get("url") or ""
            published_at = (article.get("published") or "")[:19]
            if not headline:
                continue

            category = category_hint or classify_news_category(query, headline, snippet, section, watchlist=watchlist)
            source_label = get_candidate_source_label({
                "source": "CURRENTS",
                "web_url": web_url,
            })
            candidates.append({
                "category": category,
                "tier": 2,
                "headline": headline,
                "snippet": snippet,
                "body_text": body_text,
                "section": section,
                "source": "CURRENTS",
                "source_label": source_label,
                "source_refs": [source_label],
                "published_at": published_at,
                "web_url": web_url,
                "raw_payload": article,
            })

        return candidates
    except:
        return []


def get_nyt_headline_candidates(query, category_hint=None, watchlist=None):
    try:
        q = quote_plus(query)
        url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={q}&sort=newest&api-key={NYT_API_KEY}"
        data = requests.get(url, timeout=10).json()
        docs = data.get("response", {}).get("docs", [])[:2]
        candidates = []

        for doc in docs:
            headline = (doc.get("headline") or {}).get("main")
            pub_date = (doc.get("pub_date") or "")[:19]
            snippet = doc.get("snippet") or doc.get("abstract") or ""
            abstract = doc.get("abstract") or ""
            lead_paragraph = doc.get("lead_paragraph") or ""
            section = doc.get("section_name") or ""
            web_url = doc.get("web_url") or ""
            if headline:
                category = category_hint or classify_news_category(query, headline, snippet, section, watchlist=watchlist)
                candidates.append({
                    "category": category,
                    "tier": 2,
                    "headline": headline,
                    "snippet": snippet,
                    "abstract": abstract,
                    "lead_paragraph": lead_paragraph,
                    "body_text": build_candidate_body_text({
                        "lead_paragraph": lead_paragraph,
                        "abstract": abstract,
                        "snippet": snippet,
                    }),
                    "section": section,
                    "source": "NYT",
                    "source_label": "NYT",
                    "source_refs": ["NYT"],
                    "published_at": pub_date,
                    "web_url": web_url,
                    "raw_payload": doc,
                })

        return candidates
    except:
        return []


def get_fred_candidate(category, tier, series):
    observation = get_fred(series)
    if observation is None:
        return None

    label = FRED_SERIES.get(series, {}).get("label", series)
    headline = f"{label}: {observation['value']} ({observation['date']})"
    body_text = f"{label} latest reading is {observation['value']} as of {observation['date']}."
    return {
        "category": category,
        "tier": tier,
        "headline": headline,
        "snippet": "",
        "body_text": body_text,
        "section": "macro",
        "source": "FRED",
        "published_at": observation["date"],
        "raw_payload": observation,
    }


def build_poll_candidates():
    candidates = []
    watchlist = get_watchlist()
    source_debug = {
        "nyt_queries": len(NEWS_QUERIES),
        "currents_due": source_poll_due("CURRENTS", CURRENTS_MIN_INTERVAL_MINUTES),
        "currents_query": None,
        "currents_added": 0,
    }

    for category, tier, series in POLL_SERIES:
        candidate = get_fred_candidate(category, tier, series)
        if candidate:
            candidates.append(candidate)

    for query, category_hint in NEWS_QUERIES:
        candidates.extend(get_nyt_headline_candidates(query, category_hint=category_hint, watchlist=watchlist))

    if source_debug["currents_due"]:
        query_index = int(datetime.now(LOCAL_TZ).timestamp() // (CURRENTS_MIN_INTERVAL_MINUTES * 60)) % len(NEWS_QUERIES)
        query, category_hint = NEWS_QUERIES[query_index]
        source_debug["currents_query"] = query
        current_candidates = get_currents_candidates(query, category_hint=category_hint, watchlist=watchlist)
        source_debug["currents_added"] = len(current_candidates)
        candidates.extend(current_candidates)
        mark_source_polled("CURRENTS", note=query)

    return dedupe_candidates(candidates), source_debug


def prepare_alert_shortlist(candidates, watchlist, limit=AI_ALERT_SHORTLIST_MAX):
    memory_vector_bundle = build_memory_interest_vector()
    scored = []
    for candidate in candidates:
        scoring = score_candidate(candidate, watchlist, memory_vector_bundle=memory_vector_bundle)
        scored.append({
            **candidate,
            "score": scoring["score"],
            "selection_reasons": scoring["reasons"],
            "assigned_tier": scoring["tier"],
        })

    scored.sort(
        key=lambda item: (
            item.get("assigned_tier", 3),
            -item.get("score", 0),
            item.get("published_at", ""),
        )
    )
    return scored[:limit], scored


def build_alert_decision_prompt(candidates, memory_context):
    candidate_lines = []
    for idx, candidate in enumerate(candidates, start=1):
        source_guidance = SOURCE_GUIDANCE.get(candidate.get("source"), "")
        candidate_lines.append(
            "\n".join([
                f"Candidate {idx}",
                f"id: C{idx}",
                f"headline: {candidate.get('headline', '')}",
                f"snippet: {candidate.get('snippet', '')}",
                f"section: {candidate.get('section', '')}",
                f"source: {candidate.get('source', '')}",
                f"source_refs: {', '.join(candidate.get('source_refs', []))}",
                f"published_at: {candidate.get('published_at', '')}",
                f"initial_category: {candidate.get('category', '')}",
                f"initial_tier: {candidate.get('assigned_tier', '')}",
                f"initial_score: {candidate.get('score', '')}",
                f"selection_reasons: {', '.join(candidate.get('selection_reasons', []))}",
                f"source_guidance: {source_guidance}",
            ])
        )

    working_lines = [f"- {item['category']}: {item['value']}" for item in memory_context.get("working", [])]
    long_term_lines = [f"- {item['category']}: {item['value']}" for item in memory_context.get("long_term", [])]

    return f"""
You are Jeeves deciding whether Manu should actually receive alerts.

Rules:
- Use meaning and relevance, not static keyword logic.
- Portfolio relevance must be extremely strict. Do not treat a story as portfolio-critical unless it truly matches trusted portfolio state.
- Use watchlist, memory, current patterns, and source quality.
- It is acceptable to send zero alerts.
- Prefer fewer, better alerts.
- Return valid JSON only.

Trusted portfolio:
{", ".join(memory_context.get("trusted_portfolio", [])) or "none"}

Watchlist:
{", ".join(memory_context.get("watchlist", [])) or "none"}

Working memory:
{chr(10).join(working_lines) or "- none"}

Long-term memory:
{chr(10).join(long_term_lines) or "- none"}

Candidates:
{chr(10).join(candidate_lines)}

Return this exact shape:
{{
  "decisions": [
    {{
      "candidate_id": "C1",
      "send": true,
      "category": "G",
      "tier": 2,
      "why": "short reason"
    }}
  ]
}}
"""


def ai_decide_alert_candidates(candidates):
    if not candidates:
        return {}

    memory_context = build_alert_memory_context(candidates)
    prompt = build_alert_decision_prompt(candidates, memory_context)

    try:
        completion = client.chat.completions.create(
            model=ALERT_DECISION_MODEL,
            messages=[
                {"role": "system", "content": "You are Jeeves. Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
    except:
        return {}

    decision_map = {}
    for item in payload.get("decisions", []):
        candidate_id = item.get("candidate_id")
        if not candidate_id:
            continue
        decision_map[candidate_id] = {
            "send": bool(item.get("send")),
            "category": (item.get("category") or "").upper()[:1] or None,
            "tier": int(item.get("tier") or 3),
            "why": (item.get("why") or "").strip(),
        }
    return decision_map


def run_poll_cycle(log_to_alerts=True, send_messages=False):
    candidates, source_debug = build_poll_candidates()
    watchlist = get_watchlist()
    results = []
    shortlist, scored_candidates = prepare_alert_shortlist(candidates, watchlist, limit=AI_ALERT_SHORTLIST_MAX)
    ai_decisions = ai_decide_alert_candidates(shortlist)

    shortlist_lookup = {}
    for idx, candidate in enumerate(shortlist, start=1):
        shortlist_lookup[build_event_hash(candidate["category"], candidate["headline"])] = {
            **candidate,
            "candidate_id": f"C{idx}",
            "ai_decision": ai_decisions.get(f"C{idx}", {}),
        }

    for candidate in scored_candidates:
        event_hash = build_event_hash(candidate["category"], candidate["headline"])
        shortlist_item = shortlist_lookup.get(event_hash)
        ai_decision = (shortlist_item or {}).get("ai_decision", {})
        effective_category = ai_decision.get("category") or candidate["category"]
        effective_tier = ai_decision.get("tier") if shortlist_item else candidate["assigned_tier"]
        if effective_tier is None:
            effective_tier = candidate["assigned_tier"]

        result = {
            "category": effective_category,
            "tier": effective_tier,
            "headline": candidate["headline"],
            "snippet": candidate.get("snippet", ""),
            "section": candidate.get("section", ""),
            "source": candidate["source"],
            "source_refs": candidate.get("source_refs", [get_candidate_source_label(candidate)]),
            "published_at": candidate["published_at"],
            "score": candidate["score"],
            "score_reasons": candidate["selection_reasons"],
            "fingerprint": build_event_fingerprint(candidate),
            "shortlisted": shortlist_item is not None,
            "ai_candidate_id": shortlist_item.get("candidate_id") if shortlist_item else None,
            "ai_send": ai_decision.get("send") if shortlist_item else None,
            "ai_why": ai_decision.get("why") if shortlist_item else None,
        }

        if log_to_alerts:
            if shortlist_item and not ai_decision.get("send"):
                result["alert_result"] = {
                    "ok": False,
                    "reason": "ai_filtered_out",
                }
                results.append(result)
                continue

            effective_candidate = {
                **candidate,
                "category": effective_category,
                "assigned_tier": effective_tier,
                "selection_reasons": candidate["selection_reasons"] + ([f"ai:{ai_decision.get('why')}"] if ai_decision.get("why") else []),
            }
            alert_result = log_alert(
                effective_category,
                effective_tier,
                candidate["headline"],
                sent_to_user=1 if send_messages else 0,
                candidate=effective_candidate,
            )
            result["alert_result"] = alert_result
            if send_messages and alert_result.get("ok"):
                alert_message = format_alert_message(effective_candidate, alert_result)
                send_result = send_whatsapp_message(alert_message)
                result["send_result"] = send_result
                if send_result.get("ok"):
                    log_outbound_message("alert", alert_message)
        else:
            if shortlist_item and not ai_decision.get("send"):
                result["alert_result"] = {
                    "ok": False,
                    "reason": "ai_filtered_out",
                }
                results.append(result)
                continue

            allowed, reason, semantic_result = can_send_alert(
                effective_category,
                effective_tier,
                event_hash,
                candidate=candidate,
            )
            result["alert_result"] = {
                "ok": allowed,
                "reason": reason,
                "event_hash": event_hash,
                "semantic_similarity": semantic_result["similarity"] if semantic_result else None,
            }

        results.append(result)

    return {
        "candidate_count": len(candidates),
        "shortlist_count": len(shortlist),
        "source_debug": source_debug,
        "results": results,
    }

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

    watchlist_terms = [
        "watchlist",
        "my names",
        "names on my list",
        "stocks on my list",
    ]

    performance_patterns = [
        r"\bhow (?:is|did|was)\b.+\bwatchlist\b.+\b(?:doing|performing|do|perform|move|moved)\b",
        r"\bhow (?:is|did|was)\b.+\b(?:my names|names on my list|stocks on my list)\b.+\b(?:doing|performing|do|perform|move|moved)\b",
        r"\bwatchlist\b.+\b(?:performance|stats|moves|movers|today|up|down)\b",
        r"\b(?:performance|stats|moves|movers|today|up|down)\b.+\bwatchlist\b",
        r"\bwhat happened\b.+\bwatchlist\b",
    ]

    has_stats_phrase = any(re.search(pattern, t, re.IGNORECASE) for pattern in performance_patterns)
    has_watchlist_reference = any(term in t for term in watchlist_terms)

    return has_stats_phrase and has_watchlist_reference


def is_feedback_message(text):
    return text.strip().lower() in FEEDBACK_RESPONSES


def is_daily_brief_question(text):
    t = text.lower().strip()
    return t in {
        "daily brief",
        "brief me",
        "what is today's brief",
        "what's today's brief",
        "today's brief",
    }


def interpret_event_reference(text):
    t = text.strip()

    direct_match = re.match(r"^\s*([A-Za-z]\d(?:\.\d+|-\d+)?)\s*$", t, re.IGNORECASE)
    if direct_match:
        return direct_match.group(1).upper()

    followup_patterns = [
        r"^\s*(?:expand(?:\s+on)?|follow up on|follow-up on|tell me more about|more on|what about|talk about)\s+([A-Za-z]\d(?:\.\d+|-\d+)?)\s*$",
        r"^\s*([A-Za-z]\d(?:\.\d+|-\d+)?)\s+(?:please|details|context|more)\s*$",
    ]
    for pattern in followup_patterns:
        match = re.match(pattern, t, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    return None


def is_full_article_request(text):
    t = text.lower().strip()
    patterns = [
        r"\bentire article\b",
        r"\bfull article\b",
        r"\bwhole article\b",
        r"\bverbatim\b",
        r"\bpaste\b.+\barticle\b",
    ]
    return any(re.search(pattern, t, re.IGNORECASE) for pattern in patterns)


def format_full_article_unavailable_reply():
    return (
        "I do not store the full article body from this source. "
        "I can expand using the context I have, summarize it further, or give you the source link."
    )


def is_portfolio_show_question(text):
    t = text.lower().strip()
    phrases = {
        "what's in my portfolio",
        "whats in my portfolio",
        "show my portfolio",
        "what is in my portfolio",
        "show portfolio",
    }
    return t in phrases


def extract_portfolio_symbols(text):
    patterns = [
        r"^\s*add\s+(.+?)\s+to\s+my\s+portfolio\s*$",
        r"^\s*my portfolio is\s+(.+?)\s*$",
        r"^\s*i own\s+(.+?)\s*$",
        r"^\s*my holdings are\s+(.+?)\s*$",
    ]
    for pattern in patterns:
        match = re.match(pattern, text.strip(), re.IGNORECASE)
        if match:
            symbols = extract_symbols_from_text(match.group(1))
            if symbols:
                return symbols
    return None


def normalize_ticker_candidate(token):
    cleaned = re.sub(r"[^A-Za-z]", "", token or "").upper()
    if not cleaned or len(cleaned) > 5:
        return None

    stopwords = {
        "WHAT", "WHATS", "IS", "THE", "MY", "PRICE", "STOCK", "QUOTE",
        "OF", "AND", "FOR", "HOW", "DOING", "TODAY", "AT", "TRADING",
        "SHARE", "SHARES", "WAS", "ARE", "HAS", "PERFORMED", "WITH",
        "SHOW", "TELL", "ME", "ABOUT",
    }
    if cleaned in stopwords:
        return None
    return cleaned


def get_known_market_symbols():
    symbols = set(get_watchlist())
    symbols.update(get_trusted_portfolio_symbols(limit=30))
    symbols.update(item["symbol"] for item in get_portfolio_holdings(limit=30))
    symbols.update(KNOWN_MARKET_NAME_MAP.values())
    return {symbol for symbol in symbols if symbol}


def has_market_intent(text, tickers=None):
    t = (text or "").strip().lower()
    if not t:
        return False

    finance_question_patterns = [
        r"\b(?:what is|what's)\b.+\b(?:price|quote|stock price|share price|performance)\b",
        r"\bhow (?:has|is|did)\b.+\b(?:performed|performing|doing|trading)\b",
        r"\b(?:show me|tell me)\b.+\b(?:price|quote|performance)\b",
        r"\b(?:price|quote|stock price|share price|performance)\s+of\b",
    ]
    explicit_finance_reference = any(
        re.search(pattern, t, re.IGNORECASE) for pattern in finance_question_patterns
    )
    has_market_noun = bool(
        re.search(r"\b(?:stock|ticker|shares?|price|quote|performance|performed|trading)\b", t, re.IGNORECASE)
    )

    if re.search(r"\bquoted you\b|\bquote you\b|\bthis chat\b|\bfuture chats?\b", t):
        return False

    if tickers and explicit_finance_reference:
        return True

    if tickers and has_market_noun:
        return True

    if tickers and has_market_noun and re.search(r"\b(?:what|how|show|tell)\b", t):
        return True

    return False


def extract_market_tickers(text):
    known_symbols = get_known_market_symbols()
    patterns = [
        r"\b([A-Za-z]{1,5})\b(?=\s+(?:stock|shares?|ticker)\b)",
        r"(?:price|quote|performance|performed|trading at|traded at)\s+(?:of\s+)?(?:the\s+)?([A-Za-z]{1,5})\b",
        r"(?:what is|what's|how is|how has|how did|show me|tell me)\s+(?:the\s+)?([A-Za-z]{1,5})\b(?=(?:\s+stock|\s+ticker|\s+shares|\s+price|\s+quote|\s+performance|\s+performed|\b))",
        r"(?:what is|what's|how is|how has|how did)\s+(?:the\s+)?(?:price|quote|performance|stock price|share price)\s+(?:of\s+)?([A-Za-z]{1,5})\b",
        r"\b([A-Za-z]{1,5})\b\s+(?:price|quote|performance|stock price|share price)\b",
    ]

    tickers = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            ticker = normalize_ticker_candidate(match.group(1))
            if ticker and ticker not in tickers:
                tickers.append(ticker)

    lower_text = (text or "").lower()
    for name, mapped_symbol in KNOWN_MARKET_NAME_MAP.items():
        if re.search(rf"\b{re.escape(name)}\b", lower_text) and mapped_symbol not in tickers:
            tickers.append(mapped_symbol)

    for symbol in known_symbols:
        if re.search(rf"\b{re.escape(symbol)}\b", text, re.IGNORECASE) and symbol not in tickers:
            tickers.append(symbol)

    return tickers


def interpret_market_data_question(text):
    t = text.lower()
    if "watchlist" in t:
        return None

    tickers = extract_market_tickers(text)
    if not tickers or not has_market_intent(text, tickers=tickers):
        return None

    return {
        "intent": "ticker_quote",
        "tickers": tickers,
    }

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


def get_twelvedata_watchlist_snapshot(tickers):
    if not TWELVEDATA_API_KEY or not tickers:
        return None

    snapshots = []

    try:
        for ticker in tickers:
            url = "https://api.twelvedata.com/quote"
            params = {
                "symbol": ticker,
                "apikey": TWELVEDATA_API_KEY,
            }
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                continue

            data = response.json()
            if data.get("code") or not data.get("symbol"):
                continue

            try:
                price = float(data.get("close")) if data.get("close") not in (None, "") else None
            except:
                price = None

            try:
                change = float(data.get("change")) if data.get("change") not in (None, "") else None
            except:
                change = None

            try:
                pct = float(str(data.get("percent_change", "")).replace("%", "")) if data.get("percent_change") not in (None, "") else None
            except:
                pct = None

            snapshots.append({
                "ticker": data.get("symbol"),
                "price": price,
                "change": change,
                "pct": pct,
                "source": "TWELVEDATA",
            })

        return snapshots if snapshots else None
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
    if snapshot.get("source") == "TWELVEDATA":
        return snapshot.get("price")

    day = snapshot.get("day") or {}
    last_trade = snapshot.get("lastTrade") or {}
    min_bar = snapshot.get("min") or {}
    return day.get("c") or last_trade.get("p") or min_bar.get("c")


def extract_snapshot_change(snapshot):
    if snapshot.get("source") == "TWELVEDATA":
        return snapshot.get("change")
    return snapshot.get("todaysChange")


def extract_snapshot_pct(snapshot):
    if snapshot.get("source") == "TWELVEDATA":
        return snapshot.get("pct")
    return snapshot.get("todaysChangePerc")


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
        change = extract_snapshot_change(item)
        pct = extract_snapshot_pct(item)
        parts.append(f"{ticker} {format_price(price)} ({format_pct(pct)}, {format_change(change)})")

    return "Watchlist today: " + "; ".join(parts)


def format_ticker_quote_reply(tickers, snapshots):
    if not tickers:
        return "I don't know which ticker you mean."

    if snapshots is None:
        return "Market data is unavailable."

    by_ticker = {item.get("ticker"): item for item in snapshots if item.get("ticker")}
    parts = []
    missing = []

    for ticker in tickers:
        item = by_ticker.get(ticker)
        if not item:
            missing.append(ticker)
            continue
        price = extract_snapshot_price(item)
        change = extract_snapshot_change(item)
        pct = extract_snapshot_pct(item)
        parts.append(f"{ticker} {format_price(price)} ({format_pct(pct)}, {format_change(change)})")

    if parts and not missing:
        return "; ".join(parts)

    if parts and missing:
        return "; ".join(parts) + f". No quote found for: {', '.join(missing)}."

    return f"No quote found for: {', '.join(missing)}."

# ---------------- ROUTER ----------------

def route(text):
    t = text.lower()
    market_question = interpret_market_data_question(text)
    expand_reference = interpret_event_reference(text)

    if is_feedback_message(text):
        return ("alert_feedback", t.strip())

    if is_daily_brief_question(text):
        return ("daily_brief", None)

    if is_full_article_request(text):
        return ("full_article_request", None)

    if expand_reference:
        return ("event_expand", expand_reference)

    portfolio_symbols = extract_portfolio_symbols(text)
    if portfolio_symbols:
        return ("portfolio_update", portfolio_symbols)

    if is_portfolio_show_question(text):
        return ("portfolio_show", None)

    if t.startswith("add "):
        return ("add", extract_watchlist_item(text, "add"))

    if t.startswith("remove "):
        return ("remove", extract_watchlist_item(text, "remove"))

    if is_watchlist_stats_question(text):
        return ("watchlist_stats", None)

    if market_question:
        return (market_question["intent"], market_question["tickers"])

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
announce_current_deploy_once()

@app.route("/", methods=["GET"])
def home():
    return "Jeeves is running"


@app.route("/debug/alerts", methods=["GET"])
def debug_alerts():
    return app.response_class(
        response=json.dumps(get_alert_debug_summary(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/alerts/test", methods=["POST"])
def debug_alert_test():
    category = (request.form.get("category") or "P").upper()
    tier = int(request.form.get("tier") or 2)
    headline = request.form.get("headline") or "Test alert"
    result = log_alert(category, tier, headline, sent_to_user=0)
    return app.response_class(
        response=json.dumps(result, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/poll/preview", methods=["GET"])
def debug_poll_preview():
    return app.response_class(
        response=json.dumps(run_poll_cycle(log_to_alerts=False), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/poll/run", methods=["POST"])
def debug_poll_run():
    return app.response_class(
        response=json.dumps(run_poll_cycle(log_to_alerts=True), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/memory", methods=["GET"])
def debug_memory():
    return app.response_class(
        response=json.dumps(get_memory_debug_summary(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/gmail", methods=["GET"])
def debug_gmail():
    accounts = get_gmail_accounts()
    return app.response_class(
        response=json.dumps({
            "connected": bool(accounts),
            "account_count": len(accounts),
            "accounts": accounts,
        }, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/daily-brief", methods=["GET"])
def debug_daily_brief():
    return app.response_class(
        response=json.dumps({"brief": compose_daily_brief(include_debug=True)}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/memory/consolidate", methods=["POST"])
def debug_memory_consolidate():
    run_nightly_memory_consolidation()
    return app.response_class(
        response=json.dumps(get_memory_debug_summary(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/poll", methods=["GET", "POST"])
def task_poll():
    return app.response_class(
        response=json.dumps(run_poll_cycle(log_to_alerts=True, send_messages=True), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/daily-brief", methods=["GET", "POST"])
def task_daily_brief():
    brief = compose_daily_brief(include_debug=True)
    result = send_whatsapp_message(brief)
    if result.get("ok"):
        log_outbound_message("daily_brief", brief)
    return app.response_class(
        response=json.dumps({"message": brief, "send_result": result}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/gratitude", methods=["GET", "POST"])
def task_gratitude():
    run_nightly_memory_consolidation()
    prompt = "What is one thing you were grateful for today?"
    result = send_whatsapp_message(prompt)
    if result.get("ok"):
        log_outbound_message("gratitude", prompt)
    return app.response_class(
        response=json.dumps({"message": prompt, "send_result": result}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/memory-consolidation", methods=["GET", "POST"])
def task_memory_consolidation():
    run_nightly_memory_consolidation()
    return app.response_class(
        response=json.dumps({"ok": True, "memory": get_memory_debug_summary()}, indent=2),
        status=200,
        mimetype="application/json",
    )

@app.route("/sms", methods=["POST"])
def sms():
    msg = request.form.get("Body","")
    from_number = request.form.get("From","").replace("whatsapp:","")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        log_security_event("unauthorized_message", source_number=from_number, message_text=msg)
        warning = build_suspicious_message_warning(from_number)
        result = send_whatsapp_message(warning)
        if result.get("ok"):
            log_outbound_message("security_warning", warning)
        return ""

    process_memory_updates(msg)
    intent, value = route(msg)
    log_interaction_event(intent, msg)

    if intent == "alert_feedback":
        if not feedback_context_allowed():
            resp.message("Feedback only applies to alerts or daily briefs.")
            return str(resp)
        log_alert_feedback(value, msg)
        upsert_memory(
            "working",
            "alert_feedback",
            "latest_feedback",
            value,
            source_text=msg,
            confidence=0.9,
        )
        resp.message(FEEDBACK_RESPONSES[value])
        return str(resp)

    if intent == "add":
        if not value:
            resp.message("I don't know what to add.")
            return str(resp)
        add_to_watchlist(value)
        resp.message(f"Added {value}.")
        return str(resp)

    if intent == "portfolio_update":
        upsert_portfolio_symbols(value, source_text=msg)
        record_portfolio_snapshot(value, source_type="manual", trusted=False, summary={"source_text": msg})
        resp.message(f"Portfolio noted: {', '.join(value)}.")
        return str(resp)

    if intent == "portfolio_show":
        holdings = [item["symbol"] for item in get_portfolio_holdings(limit=12)]
        resp.message(f"Portfolio: {', '.join(holdings)}" if holdings else "Portfolio is empty.")
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
        if snapshots is None:
            snapshots = get_twelvedata_watchlist_snapshot(wl)
        resp.message(format_watchlist_stats_reply(wl, snapshots))
        return str(resp)

    if intent == "ticker_quote":
        snapshots = get_twelvedata_watchlist_snapshot(value)
        resp.message(format_ticker_quote_reply(value, snapshots))
        return str(resp)

    if intent == "daily_brief":
        resp.message(compose_daily_brief(include_debug=True))
        return str(resp)

    if intent == "full_article_request":
        if article_request_context_allowed():
            resp.message(format_full_article_unavailable_reply())
        else:
            resp.message("I don't know which article you mean.")
        return str(resp)

    if intent == "event_expand":
        resp.message(expand_brief_event(value))
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
        messages = [
            {"role":"system","content":SYSTEM_PROMPT},
            {"role":"system","content":format_memory_context(msg)},
        ] + get_recent_messages()
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
