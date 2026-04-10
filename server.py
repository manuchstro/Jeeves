from openai import OpenAI
from flask import Flask, request, has_request_context
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator
import os
import html
import sqlite3
import requests
import json
import re
import hashlib
import math
import base64
import random
import time
from email.utils import parsedate_to_datetime
from difflib import SequenceMatcher
from collections import Counter
from datetime import datetime
from datetime import timedelta
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urlparse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from jeeves_config import (
    AI_ALERT_SHORTLIST_MAX,
    BASELINE_NEWS_QUERIES,
    COMMAND_KEY_REPLY,
    CURRENTS_MIN_INTERVAL_MINUTES,
    FEEDBACK_RESPONSES,
    FRED_SERIES,
    KNOWN_MARKET_NAME_MAP,
    MEMORY_VECTOR_CATEGORY_WEIGHTS,
    MEMORY_INSTRUCTIONS,
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
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL") or "").strip()
INTERNAL_API_KEY = (os.environ.get("INTERNAL_API_KEY") or "").strip()
ENFORCE_TWILIO_SIGNATURE = (os.environ.get("ENFORCE_TWILIO_SIGNATURE", "1").strip() != "0")
RAILWAY_GIT_COMMIT_SHA = os.environ.get("RAILWAY_GIT_COMMIT_SHA") or os.environ.get("SOURCE_COMMIT")
RAILWAY_DEPLOYMENT_ID = os.environ.get("RAILWAY_DEPLOYMENT_ID")
LOCAL_TZ = ZoneInfo("America/Los_Angeles")
ALERT_DECISION_MODEL = os.environ.get("ALERT_DECISION_MODEL", "gpt-4o")
ALERT_PUSH_TIER_MAX = max(1, min(3, int(os.environ.get("ALERT_PUSH_TIER_MAX", "2"))))
CURR_BURST_QUERIES_PER_CYCLE = max(1, min(4, int(os.environ.get("CURR_BURST_QUERIES_PER_CYCLE", "3"))))
PROVENANCE_SKIP_DEDUPE_SECONDS = max(30, int(os.environ.get("PROVENANCE_SKIP_DEDUPE_SECONDS", "180")))
GMAIL_ACCOUNT_EMAIL = os.environ.get("GMAIL_ACCOUNT_EMAIL", "").strip().lower()
GMAIL_TOKEN_JSON = os.environ.get("GMAIL_TOKEN_JSON")
DAILY_BRIEF_HOUR = 20
DAILY_BRIEF_MINUTE = 0
GRATITUDE_HOUR = 22
GRATITUDE_MINUTE = 15
JOURNAL_RESPONSE_WINDOW_HOURS = 12
OPEN_METEO_LAT = os.environ.get("OPEN_METEO_LAT", "").strip()
OPEN_METEO_LON = os.environ.get("OPEN_METEO_LON", "").strip()
MEMORY_CONFIDENCE_MAX = 0.99
MEMORY_CORRELATION_THRESHOLD = 0.8
MEMORY_DELETE_THRESHOLD = 0.10
MEMORY_DELETE_MIN_AGE_DAYS = 80
KNOWN_ETF_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "VEA", "VWO",
    "TLT", "IEF", "SHY", "HYG", "LQD", "XLE", "XLF", "XLK", "XLY",
    "XLI", "XLP", "XLU", "XLV", "XLB", "XLC", "SMH", "SOXX", "ARKK",
    "KWEB", "EEM", "GLD", "SLV", "USO", "URA", "URNM", "NLR",
}
QUERY_CATEGORY_BUDGET_WITH_LOCAL = {
    "E": 2,
    "L": 2,
    "G": 3,
    "P": 3,
}
QUERY_CATEGORY_BUDGET_NO_LOCAL = {
    "E": 2,
    "L": 0,
    "G": 3,
    "P": 5,
}
QUERY_NEAR_DUPLICATE_JACCARD = 0.7
LOW_QUALITY_CURRENTS_DOMAINS = {
    "nypost.com",
    "mirror.co.uk",
    "sott.net",
}
IBKR_TRUSTED_PORTFOLIO_FILENAME_RE = re.compile(r"^Jeeves_#1\..+\.html$", re.IGNORECASE)

# ---------------- DB ----------------

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    except:
        pass
    return conn


def execute_write_with_retry(sql, params=(), attempts=6, base_sleep=0.08):
    last_exc = None
    for attempt in range(attempts):
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            conn.commit()
            rowcount = cur.rowcount
            conn.close()
            return {"ok": True, "rowcount": rowcount}
        except sqlite3.OperationalError as exc:
            conn.rollback()
            conn.close()
            if "locked" not in str(exc).lower():
                raise
            last_exc = exc
            time.sleep(base_sleep * (attempt + 1))
        except Exception:
            conn.rollback()
            conn.close()
            raise
    if last_exc:
        raise last_exc
    return {"ok": False, "rowcount": 0}


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
        source_label TEXT,
        source_refs_json TEXT,
        selection_reasons_json TEXT,
        source_evaluation_json TEXT,
        dedupe_lineage_json TEXT,
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
        subtype TEXT,
        memory_key TEXT NOT NULL,
        value TEXT NOT NULL,
        source_text TEXT,
        source_trust TEXT NOT NULL DEFAULT 'inferred',
        confidence REAL NOT NULL DEFAULT 0.5,
        stability TEXT NOT NULL DEFAULT 'situational',
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
        subtype TEXT,
        memory_key TEXT NOT NULL,
        value TEXT NOT NULL,
        source_trust TEXT NOT NULL DEFAULT 'inferred',
        confidence REAL NOT NULL DEFAULT 0.5,
        stability TEXT NOT NULL DEFAULT 'situational',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_provenance_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        action TEXT NOT NULL,
        source_text TEXT,
        source_trust TEXT NOT NULL DEFAULT 'inferred',
        confidence REAL,
        stability TEXT,
        old_value TEXT,
        new_value TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_decay_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        from_confidence REAL NOT NULL,
        to_confidence REAL NOT NULL,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_decay_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        ran_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_correlation_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        related_memory_key TEXT NOT NULL,
        correlation REAL NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(scope, category, memory_key, related_memory_key)
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
        "ALTER TABLE portfolio_holdings ADD COLUMN shares REAL",
        "ALTER TABLE portfolio_holdings ADD COLUMN market_value REAL",
        "ALTER TABLE portfolio_holdings ADD COLUMN pct_net_liq REAL",
        "ALTER TABLE portfolio_holdings ADD COLUMN is_etf INTEGER DEFAULT 0",
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_task_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_key TEXT NOT NULL,
        local_date TEXT NOT NULL,
        executed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(task_key, local_date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nightly_consolidation_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        payload_json TEXT,
        depth_label TEXT NOT NULL DEFAULT 'light',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_daily_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        temperature_c REAL,
        precipitation_mm REAL,
        cloud_cover_pct REAL,
        humidity_pct REAL,
        weather_code INTEGER,
        weather_label TEXT,
        source TEXT NOT NULL DEFAULT 'open-meteo',
        confidence REAL NOT NULL DEFAULT 0.7,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS calendar_daily_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        busy_score REAL NOT NULL DEFAULT 0.0,
        event_count INTEGER NOT NULL DEFAULT 0,
        deep_work_blocks INTEGER NOT NULL DEFAULT 0,
        stress_windows INTEGER NOT NULL DEFAULT 0,
        summary_text TEXT,
        source TEXT NOT NULL DEFAULT 'calendar',
        confidence REAL NOT NULL DEFAULT 0.6,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sleep_daily_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        sleep_hours REAL,
        sleep_quality REAL,
        steps INTEGER,
        resting_hr REAL,
        fatigue_score REAL,
        summary_text TEXT,
        source TEXT NOT NULL DEFAULT 'health',
        confidence REAL NOT NULL DEFAULT 0.6,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS journal_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_text TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        meaningful_count INTEGER NOT NULL DEFAULT 0,
        intents_json TEXT NOT NULL DEFAULT '{}',
        snippets_json TEXT NOT NULL DEFAULT '[]',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_hash TEXT,
        stage TEXT NOT NULL,
        outcome TEXT NOT NULL,
        reason TEXT,
        details_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS event_lineage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_hash TEXT NOT NULL,
        related_event_hash TEXT,
        relation_type TEXT NOT NULL,
        details_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()

    for statement in [
        "ALTER TABLE event_contexts ADD COLUMN source_label TEXT",
        "ALTER TABLE event_contexts ADD COLUMN source_refs_json TEXT",
        "ALTER TABLE event_contexts ADD COLUMN source_evaluation_json TEXT",
        "ALTER TABLE event_contexts ADD COLUMN dedupe_lineage_json TEXT",
        "ALTER TABLE memory_items ADD COLUMN subtype TEXT",
        "ALTER TABLE memory_items ADD COLUMN stability TEXT DEFAULT 'situational'",
        "ALTER TABLE memory_items ADD COLUMN source_trust TEXT DEFAULT 'inferred'",
        "ALTER TABLE memory_observations ADD COLUMN subtype TEXT",
        "ALTER TABLE memory_observations ADD COLUMN stability TEXT DEFAULT 'situational'",
        "ALTER TABLE memory_observations ADD COLUMN source_trust TEXT DEFAULT 'inferred'",
    ]:
        try:
            cur.execute(statement)
        except sqlite3.OperationalError:
            pass

    try:
        cur.execute(
            """
            INSERT INTO journal_entries (entry_text, created_at)
            SELECT entry_text, created_at
            FROM gratitude_entries
            WHERE NOT EXISTS (
                SELECT 1
                FROM journal_entries j
                WHERE j.entry_text = gratitude_entries.entry_text
                  AND j.created_at = gratitude_entries.created_at
            )
            """
        )
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()
    bootstrap_gmail_account_from_env()

# ---------------- WATCHLIST ----------------

def normalize_watchlist_item(text):
    cleaned = text.strip().upper()
    cleaned = re.sub(r"^(ADD|REMOVE)\s+", "", cleaned)
    cleaned = re.sub(r"^(PLEASE|CAN YOU|COULD YOU|WOULD YOU)\s+", "", cleaned)
    cleaned = re.sub(r"\s+(TO|FROM)\s+MY\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"\s+(TO|FROM)\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"\s+MY\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"\s+WATCHLIST$", "", cleaned)
    cleaned = re.sub(r"[^A-Z0-9.\- ]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def split_watchlist_candidates(raw_text):
    text = raw_text or ""
    text = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    text = text.replace("/", ",")
    parts = [part.strip() for part in text.split(",")]
    items = []
    for part in parts:
        item = normalize_watchlist_item(part)
        if not item:
            continue
        symbol = re.sub(r"[^A-Z0-9.\-]", "", item)
        if 1 <= len(symbol) <= 8:
            items.append(symbol)
    # Preserve order while deduplicating.
    return list(dict.fromkeys(items))


def cleanup_watchlist_clause(text):
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^\s*(?:please\s+)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*(?:can|could|would)\s+you\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*(?:add|remove|put|take|drop|include|track|untrack)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*(?:start|stop)\s+tracking\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:on|onto|to|off|from)\s+(?:my\s+)?watchlist\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:on|off)\s+the\s+list\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:for\s+)?my\s+watchlist\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\btracking\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .?!,;:")
    return cleaned


def infer_symbols_from_watchlist_message(text):
    cleaned = cleanup_watchlist_clause(text)
    return split_watchlist_candidates(cleaned)


def merge_symbol_lists(*symbol_lists):
    merged = []
    for symbol_list in symbol_lists:
        for symbol in symbol_list or []:
            if symbol and symbol not in merged:
                merged.append(symbol)
    return merged


def extract_watchlist_items(text, action):
    cleaned = text.strip()

    patterns = {
        "add": [
            r"^\s*(?:can|could|would)\s+you\s+add\s+(.+?)\s+to\s+my\s+watchlist\s*$",
            r"^\s*(?:can|could|would)\s+you\s+add\s+(.+?)\s+to\s+watchlist\s*$",
            r"^\s*(?:can|could|would)\s+you\s+add\s+(.+?)\s*$",
            r"^\s*(?:please\s+)?add\s+(.+?)\s+to\s+my\s+watchlist\s*$",
            r"^\s*(?:please\s+)?add\s+(.+?)\s+to\s+watchlist\s*$",
            r"^\s*add\s+(.+?)\s+to\s+my\s+watchlist\s*$",
            r"^\s*add\s+(.+?)\s+to\s+watchlist\s*$",
            r"^\s*(?:please\s+)?add\s+(.+?)\s*$",
        ],
        "remove": [
            r"^\s*(?:can|could|would)\s+you\s+remove\s+(.+?)\s+from\s+my\s+watchlist\s*$",
            r"^\s*(?:can|could|would)\s+you\s+remove\s+(.+?)\s+from\s+watchlist\s*$",
            r"^\s*(?:can|could|would)\s+you\s+remove\s+(.+?)\s*$",
            r"^\s*(?:please\s+)?remove\s+(.+?)\s+from\s+my\s+watchlist\s*$",
            r"^\s*(?:please\s+)?remove\s+(.+?)\s+from\s+watchlist\s*$",
            r"^\s*remove\s+(.+?)\s+from\s+my\s+watchlist\s*$",
            r"^\s*remove\s+(.+?)\s+from\s+watchlist\s*$",
            r"^\s*(?:please\s+)?remove\s+(.+?)\s*$",
        ],
    }

    for pattern in patterns.get(action, []):
        match = re.match(pattern, cleaned, re.IGNORECASE)
        if match:
            items = split_watchlist_candidates(match.group(1))
            if items:
                return items

    return []


def extract_watchlist_item(text, action):
    items = extract_watchlist_items(text, action)
    return items[0] if items else None


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


def is_watchlist_action_message(text, action):
    patterns = {
        "add": [
            r"^\s*(?:please\s+)?add\b",
            r"^\s*(?:can|could|would)\s+you\s+add\b",
        ],
        "remove": [
            r"^\s*(?:please\s+)?remove\b",
            r"^\s*(?:can|could|would)\s+you\s+remove\b",
        ],
    }
    t = text.strip().lower()
    return any(re.match(pattern, t, re.IGNORECASE) for pattern in patterns.get(action, []))


def infer_watchlist_action_fallback(text):
    t = (text or "").strip().lower()

    add_patterns = [
        r"\bput\s+(.+?)\s+(?:on|onto)\s+(?:my\s+)?watchlist\b",
        r"\badd\s+(.+?)\s+(?:to\s+)?(?:my\s+)?watchlist\b",
        r"\bstart\s+tracking\s+(.+)$",
        r"\btrack\s+(.+?)(?:\s+(?:for\s+me|too))?$",
        r"\binclude\s+(.+?)\s+(?:on|in)\s+(?:my\s+)?watchlist\b",
    ]
    remove_patterns = [
        r"\btake\s+(.+?)\s+off\s+(?:my\s+)?watchlist\b",
        r"\bremove\s+(.+?)\s+(?:from\s+)?(?:my\s+)?watchlist\b",
        r"\bstop\s+tracking\s+(.+)$",
        r"\bdrop\s+(.+?)\s+(?:from\s+)?(?:my\s+)?watchlist\b",
        r"\buntrack\s+(.+)$",
    ]

    for pattern in add_patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            items = merge_symbol_lists(
                split_watchlist_candidates(cleanup_watchlist_clause(match.group(1))),
                infer_symbols_from_watchlist_message(text),
            )
            if items:
                return {"intent": "add", "symbols": items}

    for pattern in remove_patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            items = merge_symbol_lists(
                split_watchlist_candidates(cleanup_watchlist_clause(match.group(1))),
                infer_symbols_from_watchlist_message(text),
            )
            if items:
                return {"intent": "remove", "symbols": items}

    show_patterns = [
        r"\bwhat(?:'s| is)\s+on\s+(?:my\s+)?watchlist\b",
        r"\bshow\s+me\s+(?:my\s+)?watchlist\b",
        r"\bwhich\s+(?:stocks|names|tickers)\s+am\s+i\s+tracking\b",
        r"\bwhat\s+am\s+i\s+tracking\b",
        r"\blist\s+(?:my\s+)?watchlist\b",
    ]
    if any(re.search(pattern, t, re.IGNORECASE) for pattern in show_patterns):
        return {"intent": "show", "symbols": []}

    stats_patterns = [
        r"\bhow(?:'s| is| did)\s+(?:my\s+)?watchlist\b.+\b(?:doing|performing|do|perform|move|moved)\b",
        r"\bhow(?:'s| is| did)\s+(?:the\s+)?list\b.+\b(?:doing|performing|do|perform|move|moved)\b",
        r"\bhow\s+are\s+the\s+(?:names|stocks|tickers)\s+i(?:'m| am)\s+tracking\b",
        r"\bwhat\s+happened\s+to\s+(?:my\s+)?watchlist\b",
        r"\bwatchlist\b.+\b(?:today|performance|moves|movers|up|down)\b",
    ]
    if any(re.search(pattern, t, re.IGNORECASE) for pattern in stats_patterns):
        return {"intent": "watchlist_stats", "symbols": []}

    return {"intent": "none", "symbols": []}


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


def parse_portfolio_numeric(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").replace("%", "")
    try:
        return float(text)
    except:
        return None


def normalize_portfolio_positions(symbols):
    positions = []
    for index, item in enumerate(symbols or [], start=1):
        if isinstance(item, dict):
            raw_symbol = item.get("symbol")
            symbol = re.sub(r"[^A-Z0-9.\-]", "", str(raw_symbol or "").upper())
            if not symbol:
                continue
            pct_net_liq = parse_portfolio_numeric(item.get("pct_net_liq"))
            market_value = parse_portfolio_numeric(item.get("market_value"))
            shares = parse_portfolio_numeric(item.get("shares"))
            is_etf = item.get("is_etf")
            if isinstance(is_etf, str):
                is_etf = is_etf.strip().lower() in {"1", "true", "yes", "y"}
            is_etf = bool(is_etf) if is_etf is not None else (symbol in KNOWN_ETF_SYMBOLS)
            positions.append({
                "symbol": symbol,
                "incoming_index": index,
                "pct_net_liq": pct_net_liq,
                "market_value": market_value,
                "shares": shares,
                "is_etf": 1 if is_etf else 0,
            })
        else:
            symbol = re.sub(r"[^A-Z0-9.\-]", "", str(item or "").upper())
            if not symbol:
                continue
            positions.append({
                "symbol": symbol,
                "incoming_index": index,
                "pct_net_liq": None,
                "market_value": None,
                "shares": None,
                "is_etf": 1 if symbol in KNOWN_ETF_SYMBOLS else 0,
            })

    deduped = {}
    for position in positions:
        deduped[position["symbol"]] = position
    positions = list(deduped.values())

    positions.sort(
        key=lambda row: (
            -(row.get("pct_net_liq") if row.get("pct_net_liq") is not None else -1e12),
            -(row.get("market_value") if row.get("market_value") is not None else -1e12),
            row.get("incoming_index", 9999),
        )
    )
    for rank, row in enumerate(positions, start=1):
        row["conviction_rank"] = rank
    return positions


def _upsert_portfolio_symbols_with_cursor(cur, positions, source_text=None, source_type="manual", trusted=False, effective_date=None):
    for position in positions:
        symbol = position["symbol"]
        cur.execute(
            """
            INSERT INTO portfolio_holdings (
                symbol, conviction_rank, note, source_type, trusted, effective_date,
                shares, market_value, pct_net_liq, is_etf
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                conviction_rank = COALESCE(excluded.conviction_rank, portfolio_holdings.conviction_rank),
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
                shares = COALESCE(excluded.shares, portfolio_holdings.shares),
                market_value = COALESCE(excluded.market_value, portfolio_holdings.market_value),
                pct_net_liq = COALESCE(excluded.pct_net_liq, portfolio_holdings.pct_net_liq),
                is_etf = COALESCE(excluded.is_etf, portfolio_holdings.is_etf),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                symbol,
                position.get("conviction_rank"),
                source_text,
                source_type,
                1 if trusted else 0,
                effective_date,
                position.get("shares"),
                position.get("market_value"),
                position.get("pct_net_liq"),
                position.get("is_etf"),
            ),
        )


def upsert_portfolio_symbols(symbols, source_text=None, source_type="manual", trusted=False, effective_date=None):
    if not symbols:
        return

    positions = normalize_portfolio_positions(symbols)
    if not positions:
        return

    conn = get_conn()
    cur = conn.cursor()
    _upsert_portfolio_symbols_with_cursor(
        cur,
        positions,
        source_text=source_text,
        source_type=source_type,
        trusted=trusted,
        effective_date=effective_date,
    )
    conn.commit()
    conn.close()


def _record_portfolio_snapshot_with_cursor(cur, symbols, source_type="manual", trusted=False, effective_date=None, summary=None):
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


def record_portfolio_snapshot(symbols, source_type="manual", trusted=False, effective_date=None, summary=None):
    if not symbols:
        return
    conn = get_conn()
    cur = conn.cursor()
    _record_portfolio_snapshot_with_cursor(
        cur,
        symbols,
        source_type=source_type,
        trusted=trusted,
        effective_date=effective_date,
        summary=summary,
    )
    conn.commit()
    conn.close()


def validate_trusted_portfolio_payload(symbols, summary=None):
    positions = normalize_portfolio_positions(symbols or [])
    if not positions:
        return {"ok": False, "reason": "empty_positions"}

    symbols_seen = set()
    for item in positions:
        symbol = (item.get("symbol") or "").upper().strip()
        if not symbol:
            return {"ok": False, "reason": "invalid_symbol"}
        if symbol in symbols_seen:
            return {"ok": False, "reason": "duplicate_symbol", "symbol": symbol}
        symbols_seen.add(symbol)
        mv = item.get("market_value")
        pct = item.get("pct_net_liq")
        if mv is not None and mv < 0:
            return {"ok": False, "reason": "negative_market_value", "symbol": symbol}
        if pct is not None and pct < 0:
            return {"ok": False, "reason": "negative_pct_net_liq", "symbol": symbol}

    summary = summary or {}
    net_liq = parse_portfolio_numeric(summary.get("net_liq_total"))
    sum_market_value = sum((item.get("market_value") or 0.0) for item in positions)
    if net_liq is not None and net_liq > 0:
        # Holdings exclude cash/dividend accruals, so allow headroom.
        if sum_market_value > (net_liq * 1.35):
            return {
                "ok": False,
                "reason": "market_value_exceeds_net_liq_guardrail",
                "sum_market_value": round(sum_market_value, 2),
                "net_liq_total": round(net_liq, 2),
            }

    return {
        "ok": True,
        "positions": positions,
        "position_count": len(positions),
        "sum_market_value": round(sum_market_value, 2),
        "net_liq_total": None if net_liq is None else round(net_liq, 2),
    }


def replace_trusted_portfolio_snapshot(symbols, effective_date=None, summary=None, source_type="gmail"):
    validation = validate_trusted_portfolio_payload(symbols, summary=summary)
    if not validation.get("ok"):
        raise ValueError(f"trusted_portfolio_validation_failed:{validation.get('reason')}")

    positions = validation.get("positions") or []
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM portfolio_holdings WHERE trusted = 1")
        _upsert_portfolio_symbols_with_cursor(
            cur,
            positions,
            source_text=json.dumps(summary or {}),
            source_type=source_type,
            trusted=True,
            effective_date=effective_date,
        )
        _record_portfolio_snapshot_with_cursor(
            cur,
            positions,
            source_type=source_type,
            trusted=True,
            effective_date=effective_date,
            summary=summary,
        )
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_portfolio_holdings(limit=8, trusted_only=False):
    conn = get_conn()
    cur = conn.cursor()
    if trusted_only:
        cur.execute(
            """
            SELECT
                symbol, conviction_rank, note, updated_at, source_type, trusted, effective_date,
                shares, market_value, pct_net_liq, is_etf
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
            SELECT
                symbol, conviction_rank, note, updated_at, source_type, trusted, effective_date,
                shares, market_value, pct_net_liq, is_etf
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


def get_gmail_account_token(email=None):
    conn = get_conn()
    cur = conn.cursor()
    if email:
        cur.execute(
            """
            SELECT email, token_json, scopes_json, updated_at
            FROM gmail_accounts
            WHERE email = ?
            LIMIT 1
            """,
            (email.strip().lower(),),
        )
    else:
        cur.execute(
            """
            SELECT email, token_json, scopes_json, updated_at
            FROM gmail_accounts
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_gmail_service():
    row = get_gmail_account_token()
    if not row:
        return None, None
    try:
        token_payload = json.loads(row["token_json"])
        creds = Credentials.from_authorized_user_info(token_payload)
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return service, row["email"]
    except:
        return None, row.get("email")


def extract_gmail_headers(message):
    headers = {}
    for item in ((message.get("payload") or {}).get("headers") or []):
        name = (item.get("name") or "").lower()
        value = item.get("value") or ""
        if name:
            headers[name] = value
    return headers


def decode_gmail_body_part(part):
    if not part:
        return ""
    body = part.get("body") or {}
    data = body.get("data")
    if not data:
        return ""
    try:
        padded = data + "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8", errors="ignore")
    except:
        return ""


def extract_gmail_body(message):
    payload = message.get("payload") or {}
    direct = decode_gmail_body_part(payload)
    if direct.strip():
        return direct.strip()
    for part in payload.get("parts") or []:
        text = decode_gmail_body_part(part)
        if text.strip():
            return text.strip()
        for nested in part.get("parts") or []:
            nested_text = decode_gmail_body_part(nested)
            if nested_text.strip():
                return nested_text.strip()
    snippet = message.get("snippet") or ""
    return snippet.strip()


def iter_gmail_parts(payload):
    if not payload:
        return
    yield payload
    for part in payload.get("parts") or []:
        yield from iter_gmail_parts(part)


def extract_gmail_attachments(message):
    attachments = []
    payload = message.get("payload") or {}
    for part in iter_gmail_parts(payload):
        filename = (part.get("filename") or "").strip()
        body = part.get("body") or {}
        attachment_id = body.get("attachmentId")
        inline_data = body.get("data")
        if not filename:
            continue
        attachments.append({
            "filename": filename,
            "mime_type": (part.get("mimeType") or "").lower(),
            "attachment_id": attachment_id,
            "inline_data": inline_data,
            "size": body.get("size"),
        })
    return attachments


def decode_gmail_base64(data):
    if not data:
        return None
    try:
        padded = data + "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(padded.encode("utf-8"))
    except:
        return None


def fetch_gmail_attachment_bytes(service, message_id, attachment):
    inline_data = attachment.get("inline_data")
    if inline_data:
        return decode_gmail_base64(inline_data)

    attachment_id = attachment.get("attachment_id")
    if not attachment_id:
        return None
    try:
        payload = service.users().messages().attachments().get(
            userId="me",
            messageId=message_id,
            id=attachment_id,
        ).execute()
        return decode_gmail_base64(payload.get("data"))
    except:
        return None


def parse_email_datetime(value):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt and dt.tzinfo:
            return dt.astimezone(LOCAL_TZ)
        return dt
    except:
        return None


def parse_human_date_to_iso(value):
    text = " ".join(str(value or "").replace("\xa0", " ").split()).strip(", ")
    if not text:
        return None
    formats = [
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except:
            continue
    return None


class StatementSectionTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.div_stack = []
        self.sections = {}
        self.current_row = None
        self.current_cell = None
        self.current_row_section = None

    def _active_section(self):
        for section_id in reversed(self.div_stack):
            if not section_id:
                continue
            if section_id == "tblAccountSummaryBody":
                return section_id
            if section_id.startswith("tblNAV_") and section_id.endswith("Body"):
                return section_id
            if section_id.startswith("tblPosAndMTM_") and section_id.endswith("Body"):
                return section_id
        return None

    def handle_starttag(self, tag, attrs):
        attr = dict(attrs or [])
        if tag == "div":
            self.div_stack.append(attr.get("id", ""))
            return
        if tag == "tr":
            section_id = self._active_section()
            if section_id:
                self.current_row_section = section_id
                self.current_row = []
            return
        if tag in {"td", "th"} and self.current_row is not None:
            self.current_cell = []

    def handle_data(self, data):
        if self.current_cell is not None:
            self.current_cell.append(data)

    def handle_entityref(self, name):
        if self.current_cell is not None:
            self.current_cell.append(f"&{name};")

    def handle_charref(self, name):
        if self.current_cell is not None:
            self.current_cell.append(f"&#{name};")

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and self.current_cell is not None and self.current_row is not None:
            text = html.unescape("".join(self.current_cell))
            text = " ".join(text.replace("\xa0", " ").split()).strip()
            self.current_row.append(text)
            self.current_cell = None
            return
        if tag == "tr" and self.current_row is not None and self.current_row_section:
            if self.current_row:
                self.sections.setdefault(self.current_row_section, []).append(self.current_row)
            self.current_row = None
            self.current_row_section = None
            return
        if tag == "div":
            if self.div_stack:
                self.div_stack.pop()


def is_probable_position_row(cells):
    if not cells or len(cells) < 9:
        return False
    symbol = (cells[0] or "").strip().upper()
    if not symbol:
        return False
    skip_prefixes = (
        "TOTAL",
        "CARRIED BY",
    )
    skip_exact = {
        "STOCKS",
        "FOREX",
        "CRYPTO",
        "USD",
        "CAD",
        "EUR",
        "GBP",
        "JPY",
        "SYMBOL",
    }
    if symbol in skip_exact:
        return False
    if any(symbol.startswith(prefix) for prefix in skip_prefixes):
        return False
    if len(symbol) > 24:
        return False
    if re.search(r"[^A-Z0-9.\-]", symbol):
        return False
    return True


def classify_is_etf(symbol, description):
    symbol_u = (symbol or "").upper()
    desc_u = (description or "").upper()
    if symbol_u in KNOWN_ETF_SYMBOLS:
        return True
    etf_markers = [" ETF", "ISHARES", "VANECK", "SPDR", "INDEX FUND", "ETN"]
    return any(marker in desc_u for marker in etf_markers)


def parse_ibkr_activity_statement_html(html_text):
    parser = StatementSectionTableParser()
    parser.feed(html_text or "")
    sections = parser.sections

    statement_date = None
    title_match = re.search(r"<title>\s*MTM Summary\s+([^<]+?)\s*-\s*Interactive Brokers\s*</title>", html_text or "", re.IGNORECASE)
    if title_match:
        statement_date = parse_human_date_to_iso(title_match.group(1))
    if not statement_date:
        summary_date_match = re.search(r"Account Summary\s*<br>\s*<span>([^<]+)</span>", html_text or "", re.IGNORECASE)
        if summary_date_match:
            statement_date = parse_human_date_to_iso(summary_date_match.group(1))

    account_navs = {}
    total_nav = None
    for row in sections.get("tblAccountSummaryBody", []):
        account_label = row[0]
        current_nav = parse_portfolio_numeric(row[4] if len(row) > 4 else None)
        if current_nav is None and "TOTAL" in (account_label or "").upper() and len(row) >= 3:
            current_nav = parse_portfolio_numeric(row[2])
        if current_nav is None:
            continue
        if account_label.upper().startswith("U"):
            account_navs[account_label] = current_nav
        elif "TOTAL" in account_label.upper():
            total_nav = current_nav

    primary_account = None
    for account_label, nav_value in sorted(account_navs.items(), key=lambda kv: kv[1], reverse=True):
        if "ZERO HASH" not in account_label.upper():
            primary_account = account_label
            break
    if not primary_account and account_navs:
        primary_account = max(account_navs.items(), key=lambda kv: kv[1])[0]

    nav_sections = {}
    for section_id, rows in sections.items():
        if section_id.startswith("tblNAV_") and section_id.endswith("Body"):
            account_label = html.unescape(section_id[len("tblNAV_"):-len("Body")]).strip()
            nav_sections[account_label] = rows

    primary_nav_rows = []
    if primary_account and primary_account in nav_sections:
        primary_nav_rows = nav_sections[primary_account]
    elif nav_sections:
        primary_nav_rows = max(nav_sections.values(), key=lambda rows: len(rows or []))

    nav_breakdown = {
        "cash_total": 0.0,
        "stock_total": 0.0,
        "crypto_total": 0.0,
        "account_combined_assets": {},
    }
    for account_label, rows in nav_sections.items():
        for row in rows:
            if len(row) < 5:
                continue
            label = (row[0] or "").strip().upper()
            current_total = parse_portfolio_numeric(row[4] if len(row) > 4 else None)
            if current_total is None:
                continue
            if "TOTAL (COMBINED ASSETS)" in label:
                nav_breakdown["account_combined_assets"][account_label] = current_total

    for row in primary_nav_rows:
        if len(row) < 5:
            continue
        label = (row[0] or "").strip().upper()
        current_total = parse_portfolio_numeric(row[4] if len(row) > 4 else None)
        if current_total is None:
            continue
        if label == "CASH":
            nav_breakdown["cash_total"] += current_total
        elif label == "STOCK":
            nav_breakdown["stock_total"] += current_total
        elif label == "CRYPTO":
            nav_breakdown["crypto_total"] += current_total

    if total_nav is None:
        if nav_breakdown["account_combined_assets"]:
            total_nav = sum(nav_breakdown["account_combined_assets"].values())
        elif account_navs:
            total_nav = sum(account_navs.values())

    pos_sections = {}
    for section_id, rows in sections.items():
        if section_id.startswith("tblPosAndMTM_") and section_id.endswith("Body"):
            account_label = html.unescape(section_id[len("tblPosAndMTM_"):-len("Body")]).strip()
            pos_sections[account_label] = rows

    primary_pos_rows = []
    if primary_account and primary_account in pos_sections:
        primary_pos_rows = pos_sections[primary_account]
    elif pos_sections:
        primary_pos_rows = max(
            pos_sections.values(),
            key=lambda rows: sum(1 for row in rows if is_probable_position_row(row)),
        )

    aggregated = {}
    for row in primary_pos_rows:
        if not is_probable_position_row(row):
            continue
        symbol = (row[0] or "").strip().upper()
        description = row[1] if len(row) > 1 else ""
        current_qty = parse_portfolio_numeric(row[3] if len(row) > 3 else None)
        current_market_value = parse_portfolio_numeric(row[7] if len(row) > 7 else None)
        if current_qty is None and current_market_value is None:
            continue

        key = symbol
        if key not in aggregated:
            aggregated[key] = {
                "symbol": symbol,
                "shares": 0.0,
                "market_value": 0.0,
                "description": description,
                "is_etf": classify_is_etf(symbol, description),
            }
        item = aggregated[key]
        if description and not item.get("description"):
            item["description"] = description
        if current_qty is not None:
            item["shares"] += current_qty
        if current_market_value is not None:
            item["market_value"] += current_market_value

    holdings = []
    for item in aggregated.values():
        market_value = item.get("market_value")
        pct_net_liq = None
        if total_nav and total_nav > 0 and market_value is not None:
            pct_net_liq = (market_value / total_nav) * 100.0
        holdings.append({
            "symbol": item["symbol"],
            "shares": item["shares"],
            "market_value": market_value,
            "pct_net_liq": pct_net_liq,
            "is_etf": bool(item.get("is_etf")),
        })

    holdings.sort(key=lambda x: (-(x.get("market_value") or 0.0), x.get("symbol") or ""))

    summary = {
        "statement_date": statement_date,
        "net_liq_total": total_nav,
        "cash_total": nav_breakdown["cash_total"],
        "stock_total": nav_breakdown["stock_total"],
        "crypto_total": nav_breakdown["crypto_total"],
        "account_navs": account_navs,
        "account_combined_assets": nav_breakdown["account_combined_assets"],
        "primary_account": primary_account,
        "position_count": len(holdings),
    }
    return holdings, summary


def find_latest_trusted_ibkr_statement(days_window=14, max_results=10):
    service, account_email = get_gmail_service()
    if not service:
        return {"ok": False, "reason": "gmail_not_connected"}

    query = (
        "from:interactivebrokers OR from:interactivebrokers.com "
        "subject:(\"Customized Activity Statement\") has:attachment "
        f"newer_than:{max(1, min(30, int(days_window)))}d"
    )
    try:
        response = service.users().messages().list(
            userId="me",
            maxResults=max_results,
            q=query,
        ).execute()
        messages = response.get("messages") or []
        if not messages:
            return {"ok": False, "reason": "no_messages", "query": query, "account_email": account_email}

        for item in messages:
            message_id = item.get("id")
            if not message_id:
                continue
            message = service.users().messages().get(
                userId="me",
                id=message_id,
                format="full",
            ).execute()
            headers = extract_gmail_headers(message)
            attachments = extract_gmail_attachments(message)
            trusted = [a for a in attachments if IBKR_TRUSTED_PORTFOLIO_FILENAME_RE.match(a.get("filename") or "")]
            if not trusted:
                continue
            trusted.sort(key=lambda a: (a.get("filename") or "").lower(), reverse=True)
            chosen = trusted[0]
            raw_bytes = fetch_gmail_attachment_bytes(service, message_id, chosen)
            if not raw_bytes:
                continue
            text = raw_bytes.decode("utf-8", errors="ignore")
            if "Positions and Mark-to-Market Profit and Loss" not in text:
                continue
            return {
                "ok": True,
                "account_email": account_email,
                "query": query,
                "message_id": message_id,
                "subject": headers.get("subject", ""),
                "from": headers.get("from", ""),
                "date": headers.get("date", ""),
                "date_local": (parse_email_datetime(headers.get("date")) or datetime.now(LOCAL_TZ)).isoformat(),
                "filename": chosen.get("filename"),
                "html_text": text,
                "attachment_count": len(attachments),
            }
        return {"ok": False, "reason": "no_trusted_attachment_found", "query": query, "account_email": account_email}
    except:
        return {"ok": False, "reason": "gmail_error"}


def sync_trusted_portfolio_from_gmail(days_window=14):
    found = find_latest_trusted_ibkr_statement(days_window=days_window, max_results=10)
    if not found.get("ok"):
        return found

    holdings, summary = parse_ibkr_activity_statement_html(found.get("html_text") or "")
    if not holdings:
        return {
            "ok": False,
            "reason": "parse_no_holdings",
            "filename": found.get("filename"),
            "subject": found.get("subject"),
            "date": found.get("date"),
        }

    effective_date = summary.get("statement_date")
    try:
        replace_trusted_portfolio_snapshot(
            holdings,
            effective_date=effective_date,
            summary=summary,
            source_type="gmail_trusted_html",
        )
    except Exception as exc:
        return {
            "ok": False,
            "reason": "trusted_snapshot_replace_failed",
            "error": str(exc),
            "filename": found.get("filename"),
            "statement_date": effective_date,
        }

    return {
        "ok": True,
        "filename": found.get("filename"),
        "subject": found.get("subject"),
        "message_id": found.get("message_id"),
        "statement_date": effective_date,
        "parsed_positions": len(holdings),
        "net_liq_total": summary.get("net_liq_total"),
        "cash_total": summary.get("cash_total"),
        "stock_total": summary.get("stock_total"),
        "crypto_total": summary.get("crypto_total"),
    }


def get_latest_email_message(query=None):
    service, account_email = get_gmail_service()
    if not service:
        return None
    try:
        response = service.users().messages().list(
            userId="me",
            maxResults=1,
            q=query or "",
        ).execute()
        messages = response.get("messages") or []
        if not messages:
            return {
                "account_email": account_email,
                "query": query or "",
                "found": False,
            }
        message_id = messages[0]["id"]
        message = service.users().messages().get(
            userId="me",
            id=message_id,
            format="full",
        ).execute()
        headers = extract_gmail_headers(message)
        return {
            "account_email": account_email,
            "query": query or "",
            "found": True,
            "id": message_id,
            "subject": headers.get("subject", ""),
            "from": headers.get("from", ""),
            "date": headers.get("date", ""),
            "snippet": message.get("snippet", "") or "",
            "body_text": extract_gmail_body(message),
        }
    except:
        return None


def get_recent_email_messages(query=None, max_results=12):
    service, account_email = get_gmail_service()
    if not service:
        return None
    try:
        response = service.users().messages().list(
            userId="me",
            maxResults=max_results,
            q=query or "",
        ).execute()
        messages = response.get("messages") or []
        results = []
        for item in messages:
            message = service.users().messages().get(
                userId="me",
                id=item["id"],
                format="metadata",
                metadataHeaders=["From", "Subject", "Date"],
            ).execute()
            headers = extract_gmail_headers(message)
            results.append({
                "account_email": account_email,
                "id": item["id"],
                "subject": headers.get("subject", ""),
                "from": headers.get("from", ""),
                "date": headers.get("date", ""),
                "snippet": message.get("snippet", "") or "",
                "body_text": (message.get("snippet") or "")[:1200],
            })
        return results
    except:
        return None


EMAIL_SENDER_ALIASES = {
    "ibkr": "from:interactivebrokers OR from:interactivebrokers.com",
    "interactive brokers": "from:interactivebrokers OR from:interactivebrokers.com",
    "interactivebrokers": "from:interactivebrokers OR from:interactivebrokers.com",
}


def extract_email_days_window(text):
    t = (text or "").lower()
    if "today" in t:
        return 1
    if "yesterday" in t:
        return 2
    if "this week" in t or "past week" in t or "last week" in t:
        return 7
    if "this month" in t or "past month" in t or "last month" in t:
        return 30

    match = re.search(r"\b(?:past|last|recent)\s+(\d{1,2})\s+days?\b", t)
    if match:
        return max(1, min(30, int(match.group(1))))

    match = re.search(r"\b(\d{1,2})\s+days?\s+ago\b", t)
    if match:
        return max(1, min(30, int(match.group(1)) + 1))

    return 7


def extract_email_sender_hint(text):
    t = (text or "").strip().lower()
    for alias, query in EMAIL_SENDER_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", t):
            return query

    match = re.search(r"\bfrom\s+([a-z0-9._%+\-@ ]+?)(?:\s+(?:today|yesterday|this week|past week|last week|this month|past month|last month|please|again|lately|recently)|[?.!,]|$)", t, re.IGNORECASE)
    if match:
        sender = match.group(1).strip()
        sender = re.sub(r"\s+", " ", sender)
        if sender in {"the", "past week", "last week", "this week", "past month", "last month", "this month"}:
            return ""
        if sender.startswith(("the past ", "the last ", "the this ")):
            return ""
        if "@" in sender:
            return f"from:{sender}"
        if sender:
            return sender

    return ""


def fallback_email_request(text):
    t = (text or "").strip().lower()
    sender_hint = extract_email_sender_hint(text)
    days_window = extract_email_days_window(text)

    email_signals = [
        "email", "emails", "inbox", "mail", "message", "messages",
        "sent today", "send today", "emailed", "latest", "new email",
        "new emails", "recent email", "recent emails",
    ]
    has_email_context = any(signal in t for signal in email_signals) or bool(sender_hint)
    if not has_email_context:
        return {"intent": "none", "query_hint": "", "days_window": 7}

    if re.search(r"\b(?:what|which|any).+\b(?:important|most important|matters most|urgent|action needed)\b", t) or \
       re.search(r"\bcheck\b.+\b(?:emails|inbox|mail)\b", t) or \
       re.search(r"\b(?:important|urgent|action needed)\b.+\b(?:emails|inbox|mail)\b", t):
        return {
            "intent": "important_recent_email",
            "query_hint": sender_hint,
            "days_window": days_window,
        }

    if re.search(r"\bdid\b.+\bsend\b.+\b(?:today|yet|recently)\b", t):
        query_hint = sender_hint
        if days_window <= 2:
            query_hint = ((query_hint + " ") if query_hint else "") + "newer_than:2d"
        return {
            "intent": "latest_ibkr_email" if "interactive brokers" in t or "ibkr" in t or "interactivebrokers" in t else "latest_email",
            "query_hint": query_hint.strip(),
            "days_window": days_window,
        }

    if re.search(r"\b(?:summarize|summary|sum up|brief me on)\b", t):
        return {
            "intent": "email_summary",
            "query_hint": sender_hint,
            "days_window": days_window,
        }

    if re.search(r"\b(?:latest|newest|most recent|any new|new)\b", t) or \
       re.search(r"\bwhat(?:'s| is)\s+(?:my\s+)?latest\b", t):
        return {
            "intent": "latest_ibkr_email" if "interactive brokers" in t or "ibkr" in t or "interactivebrokers" in t else "latest_email",
            "query_hint": sender_hint,
            "days_window": days_window,
        }

    if re.search(r"\b(?:what came in|what came through|what did i get)\b", t):
        return {
            "intent": "important_recent_email",
            "query_hint": sender_hint,
            "days_window": days_window,
        }

    if sender_hint:
        return {
            "intent": "email_summary" if any(term in t for term in ["summarize", "summary", "brief"]) else "latest_email",
            "query_hint": sender_hint,
            "days_window": days_window,
        }

    if "email" in t or "inbox" in t or "mail" in t:
        return {
            "intent": "important_recent_email" if any(term in t for term in ["important", "urgent", "action"]) else "latest_email",
            "query_hint": "",
            "days_window": days_window,
        }

    return {"intent": "none", "query_hint": "", "days_window": 7}


def interpret_email_request(text):
    prompt = f"""
Interpret this user message for inbox/email intent.

Rules:
- Use meaning, not keyword matching.
- Return JSON only.
- If this is not really about email/inbox, return inbox_intent = "none".

Message:
\"\"\"{text}\"\"\"

Return:
{{
  "inbox_intent": "none|latest_email|latest_ibkr_email|email_summary|important_recent_email",
  "query_hint": "optional Gmail search hint",
  "days_window": 7
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        intent = (payload.get("inbox_intent") or "none").strip().lower()
        if intent not in {"none", "latest_email", "latest_ibkr_email", "email_summary", "important_recent_email"}:
            intent = "none"
        result = {
            "intent": intent,
            "query_hint": (payload.get("query_hint") or "").strip(),
            "days_window": int(payload.get("days_window") or 7),
        }
        if result["intent"] == "none":
            fallback = fallback_email_request(text)
            if fallback["intent"] != "none":
                return fallback
        return result
    except:
        return fallback_email_request(text)


def format_latest_email_reply(email_data, summary_mode=False):
    if email_data is None:
        return "I don't know. Gmail access may not be available."
    if not email_data.get("found"):
        return "No matching email found."

    subject = email_data.get("subject") or "(no subject)"
    sender = email_data.get("from") or "unknown sender"
    date = email_data.get("date") or "unknown time"
    snippet = (email_data.get("body_text") or email_data.get("snippet") or "").strip()
    snippet = re.sub(r"\s+", " ", snippet)

    if summary_mode:
        prompt = f"""
Summarize this email briefly and directly.

Subject: {subject}
From: {sender}
Date: {date}
Body:
\"\"\"{snippet[:4000]}\"\"\"
"""
        try:
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Be concise and factual."},
                    {"role": "user", "content": prompt},
                ],
            )
            summary = (completion.choices[0].message.content or "").strip()
            if summary:
                return f"Latest email: {subject} from {sender}. {summary}"
        except:
            pass

    short_body = snippet[:240] + ("..." if len(snippet) > 240 else "")
    return f"Latest email: {subject} from {sender} at {date}. {short_body}"


def rank_important_emails(email_messages, user_request):
    if not email_messages:
        return None
    prompt = f"""
Rank these recent emails by importance for Manu.

Rules:
- Use sender importance, urgency, actionability, personal relevance, and novelty.
- Distinguish newsletters from genuinely important emails.
- Prefer what most matters to Manu, not generic corporate importance.
- Return JSON only.

User request:
\"\"\"{user_request}\"\"\"

Emails:
{json.dumps(email_messages[:12], ensure_ascii=True)}

Return:
{{
  "top_ids": ["id1", "id2", "id3"],
  "summary": "short direct answer",
  "why": ["short reason 1", "short reason 2"]
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        if not isinstance(payload, dict):
            return None
        return payload
    except:
        return None


def format_important_recent_email_reply(email_messages, ranked):
    if not email_messages:
        return "I don't know. Gmail access may not be available."
    if not ranked:
        return "I couldn't rank the inbox reliably."

    by_id = {item["id"]: item for item in email_messages}
    top = [by_id[email_id] for email_id in ranked.get("top_ids", []) if email_id in by_id][:3]
    summary = (ranked.get("summary") or "").strip()
    reasons = ranked.get("why") or []

    lines = []
    if summary:
        lines.append(summary)
    if top:
        lines.append("Top emails:")
        for item in top:
            sender = item.get("from") or "unknown sender"
            subject = item.get("subject") or "(no subject)"
            lines.append(f"- {subject} from {sender}")
    if reasons:
        lines.append("Why:")
        for reason in reasons[:3]:
            lines.append(f"- {reason}")
    return "\n".join(lines) if lines else "I couldn't find anything clearly important."


def fallback_watchlist_request(text):
    t = (text or "").strip().lower()

    richer_fallback = infer_watchlist_action_fallback(text)
    if richer_fallback["intent"] != "none":
        return richer_fallback

    if is_watchlist_action_message(text, "add"):
        items = merge_symbol_lists(
            extract_watchlist_items(text, "add"),
            infer_symbols_from_watchlist_message(text),
        )
        return {"intent": "add", "symbols": items}

    if is_watchlist_action_message(text, "remove"):
        items = merge_symbol_lists(
            extract_watchlist_items(text, "remove"),
            infer_symbols_from_watchlist_message(text),
        )
        return {"intent": "remove", "symbols": items}

    if is_watchlist_stats_question(text):
        return {"intent": "watchlist_stats", "symbols": []}

    if "watchlist" in t:
        return {"intent": "show", "symbols": []}

    return {"intent": "none", "symbols": []}


def interpret_watchlist_request(text):
    prompt = f"""
Interpret this user message for watchlist intent.

Rules:
- Use meaning, not brittle keyword matching.
- The action must be one of: none, add, remove, show, watchlist_stats.
- Only return add/remove when the user is clearly talking about a watchlist or tracking list.
- For add/remove, extract ticker-like symbols when possible.
- Return JSON only.

Message:
\"\"\"{text}\"\"\"

Return:
{{
  "watchlist_intent": "none|add|remove|show|watchlist_stats",
  "symbols": ["AAPL", "MSFT"]
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        intent = (payload.get("watchlist_intent") or "none").strip().lower()
        if intent not in {"none", "add", "remove", "show", "watchlist_stats"}:
            intent = "none"

        raw_symbols = payload.get("symbols") or []
        symbols = []
        if isinstance(raw_symbols, list):
            for symbol in raw_symbols:
                normalized = normalize_watchlist_item(str(symbol))
                normalized = re.sub(r"[^A-Z0-9.\-]", "", normalized)
                if normalized and normalized not in symbols:
                    symbols.append(normalized)

        if intent in {"add", "remove"} and not symbols:
            return fallback_watchlist_request(text)

        return {"intent": intent, "symbols": symbols}
    except:
        return fallback_watchlist_request(text)


def bootstrap_gmail_account_from_env():
    if not GMAIL_ACCOUNT_EMAIL or not GMAIL_TOKEN_JSON:
        return {"ok": False, "reason": "missing_env"}

    try:
        token_payload = json.loads(GMAIL_TOKEN_JSON)
    except:
        return {"ok": False, "reason": "invalid_token_json"}

    scopes = token_payload.get("scopes") or ["https://www.googleapis.com/auth/gmail.readonly"]
    upsert_gmail_account(GMAIL_ACCOUNT_EMAIL, token_payload, scopes)
    return {"ok": True, "email": GMAIL_ACCOUNT_EMAIL}


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


def build_portfolio_truth_payload():
    snapshot = get_latest_trusted_portfolio_snapshot() or {}
    holdings = get_portfolio_holdings(limit=200, trusted_only=True)
    summary = {}
    try:
        summary = json.loads(snapshot.get("summary_json") or "{}")
    except:
        summary = {}
    return {
        "as_of_local": get_local_now().isoformat(),
        "trusted_snapshot": snapshot,
        "trusted_holdings_count": len(holdings),
        "trusted_holdings": holdings,
        "summary": summary,
    }


def build_portfolio_integrity_report():
    payload = build_portfolio_truth_payload()
    snapshot = payload.get("trusted_snapshot") or {}
    holdings = payload.get("trusted_holdings") or []
    summary = payload.get("summary") or {}
    issues = []

    if not snapshot:
        issues.append("missing_trusted_snapshot")
    if not holdings:
        issues.append("missing_trusted_holdings")

    symbols = [str(item.get("symbol") or "").upper().strip() for item in holdings]
    if any(not s for s in symbols):
        issues.append("blank_symbol_in_holdings")
    if len(set(symbols)) != len(symbols):
        issues.append("duplicate_symbols_in_holdings")

    ranks = [int(item.get("conviction_rank") or 0) for item in holdings if item.get("conviction_rank") is not None]
    if ranks:
        expected = list(range(1, len(ranks) + 1))
        if sorted(ranks) != expected:
            issues.append("conviction_rank_gap_or_duplicate")

    net_liq = parse_portfolio_numeric(summary.get("net_liq_total"))
    total_mv = sum((parse_portfolio_numeric(item.get("market_value")) or 0.0) for item in holdings)
    if net_liq is not None and net_liq > 0 and total_mv > (net_liq * 1.35):
        issues.append("sum_market_value_exceeds_guardrail")

    return {
        "ok": len(issues) == 0,
        "checked_at": get_local_now().isoformat(),
        "issues": issues,
        "trusted_holdings_count": len(holdings),
        "sum_market_value": round(total_mv, 2),
        "net_liq_total": net_liq,
        "effective_date": snapshot.get("effective_date"),
        "source_type": snapshot.get("source_type"),
    }

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

def classify_memory_metadata(scope, category, memory_key):
    category_l = (category or "").lower()
    key_l = (memory_key or "").lower()

    subtype = "general"
    stability = "situational"

    if category_l in {"core_identity", "core_traits", "defining_moments", "major_successes", "major_failures", "deep_preferences", "long_term_frictions"}:
        subtype = "deep_self"
        stability = "durable"
    elif category_l in {"relationship_preferences"}:
        subtype = "relationship"
        stability = "adaptive"
    elif category_l in {"memory_threads"}:
        subtype = "thread"
        stability = "evolving"
    elif category_l in {"behavior_trends"}:
        subtype = "trend"
        stability = "adaptive"
    elif category_l in {"journal"}:
        subtype = "journal"
        stability = "situational"
    elif category_l in {"gratitude"}:
        subtype = "journal"
        stability = "situational"
    elif category_l in {"risk_profile"}:
        subtype = "portfolio"
        stability = "adaptive"
    elif category_l in {"state", "emotional_state", "nightly_summary"}:
        subtype = "state"
        stability = "situational"
    elif scope == "long_term":
        subtype = "long_term"
        stability = "adaptive"

    if "contradiction_" in key_l:
        subtype = "contradiction"
        stability = "evolving"

    return subtype, stability


def classify_source_trust(scope, category, source_text=None, source_trust=None):
    if source_trust in {"trusted", "semi_trusted", "inferred", "untrusted"}:
        return source_trust
    source = (source_text or "").lower()
    category_l = (category or "").lower()

    if "ibkr" in source or "trusted_portfolio" in source:
        return "trusted"
    if "nightly_deep_consolidation" in source or "trend_consolidation" in source:
        return "semi_trusted"
    if category_l in {"journal", "emotional_state", "state"} and source:
        return "semi_trusted"
    if scope == "long_term" and source:
        return "semi_trusted"
    return "inferred"


def get_effective_protected_memory_categories():
    # Expanded protected scope for memory upgrade behavior.
    return set(PROTECTED_MEMORY_CATEGORIES) | {"journal", "risk_profile"}


def normalize_memory_value_for_correlation(value):
    text = (value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text[:500]


def memory_correlation_score(value_a, value_b):
    a = normalize_memory_value_for_correlation(value_a)
    b = normalize_memory_value_for_correlation(value_b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def correlation_bonus(correlation):
    if correlation < MEMORY_CORRELATION_THRESHOLD:
        return 0.0
    span = min(1.0, max(0.0, (correlation - MEMORY_CORRELATION_THRESHOLD) / 0.2))
    return round(0.02 + (0.03 * span), 4)


def apply_memory_correlation_reinforcement(scope, category, memory_key, value):
    if scope != "long_term":
        return
    if category in get_effective_protected_memory_categories():
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, memory_key, value, confidence
        FROM memory_items
        WHERE scope = ? AND category = ? AND memory_key != ?
        """,
        (scope, category, memory_key),
    )
    peers = [dict(row) for row in cur.fetchall()]
    if not peers:
        conn.close()
        return

    cur.execute(
        """
        SELECT id, confidence, value
        FROM memory_items
        WHERE scope = ? AND category = ? AND memory_key = ?
        LIMIT 1
        """,
        (scope, category, memory_key),
    )
    current = cur.fetchone()
    if not current:
        conn.close()
        return

    current_id = int(current["id"])
    current_conf = float(current["confidence"] or 0.0)
    current_value = current["value"] or value
    current_best_bonus = 0.0

    for peer in peers:
        corr = memory_correlation_score(current_value, peer.get("value", ""))
        if corr < MEMORY_CORRELATION_THRESHOLD:
            continue
        bonus = correlation_bonus(corr)
        if bonus <= 0:
            continue

        peer_conf = float(peer.get("confidence") or 0.0)
        boosted_peer = min(MEMORY_CONFIDENCE_MAX, peer_conf + bonus)
        if boosted_peer > peer_conf:
            cur.execute(
                """
                UPDATE memory_items
                SET confidence = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (round(boosted_peer, 4), int(peer["id"])),
            )
        current_best_bonus = max(current_best_bonus, bonus)

        cur.execute(
            """
            INSERT INTO memory_correlation_links (scope, category, memory_key, related_memory_key, correlation, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(scope, category, memory_key, related_memory_key) DO UPDATE SET
                correlation = excluded.correlation,
                updated_at = CURRENT_TIMESTAMP
            """,
            (scope, category, memory_key, peer["memory_key"], round(corr, 4)),
        )
        cur.execute(
            """
            INSERT INTO memory_correlation_links (scope, category, memory_key, related_memory_key, correlation, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(scope, category, memory_key, related_memory_key) DO UPDATE SET
                correlation = excluded.correlation,
                updated_at = CURRENT_TIMESTAMP
            """,
            (scope, category, peer["memory_key"], memory_key, round(corr, 4)),
        )

    if current_best_bonus > 0:
        boosted_current = min(MEMORY_CONFIDENCE_MAX, current_conf + current_best_bonus)
        if boosted_current > current_conf:
            cur.execute(
                """
                UPDATE memory_items
                SET confidence = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (round(boosted_current, 4), current_id),
            )

    conn.commit()
    conn.close()


def add_memory_provenance_event(scope, category, memory_key, action, source_text=None, source_trust="inferred", confidence=None, stability=None, old_value=None, new_value=None):
    conn = get_conn()
    cur = conn.cursor()
    if action == "refresh_skipped_no_new_signal":
        # Collapse rapid repeat no-signal events for the same memory row.
        cur.execute(
            """
            SELECT id
            FROM memory_provenance_events
            WHERE scope = ?
              AND category = ?
              AND memory_key = ?
              AND action = ?
              AND COALESCE(old_value, '') = COALESCE(?, '')
              AND COALESCE(new_value, '') = COALESCE(?, '')
              AND created_at >= datetime('now', ?)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                scope,
                category,
                memory_key,
                action,
                old_value,
                new_value,
                f"-{int(PROVENANCE_SKIP_DEDUPE_SECONDS)} seconds",
            ),
        )
        if cur.fetchone():
            conn.close()
            return
    cur.execute(
        """
        INSERT INTO memory_provenance_events (
            scope, category, memory_key, action, source_text, source_trust,
            confidence, stability, old_value, new_value
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scope,
            category,
            memory_key,
            action,
            source_text or "",
            source_trust or "inferred",
            confidence,
            stability,
            old_value,
            new_value,
        ),
    )
    conn.commit()
    conn.close()


def upsert_memory(scope, category, memory_key, value, source_text=None, confidence=0.6, subtype=None, stability=None, source_trust=None):
    inferred_subtype, inferred_stability = classify_memory_metadata(scope, category, memory_key)
    subtype = subtype or inferred_subtype
    stability = stability or inferred_stability
    source_trust = classify_source_trust(scope, category, source_text=source_text, source_trust=source_trust)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT value, stability, confidence, source_trust, updated_at
        FROM memory_items
        WHERE scope = ? AND category = ? AND memory_key = ?
        LIMIT 1
        """,
        (scope, category, memory_key),
    )
    existing = cur.fetchone()
    proposed_confidence = max(0.0, min(MEMORY_CONFIDENCE_MAX, float(confidence)))
    old_value = existing["value"] if existing else None
    old_stability = existing["stability"] if existing else None
    old_confidence = existing["confidence"] if existing else None
    old_updated_at = existing["updated_at"] if existing else None
    old_source_trust = existing["source_trust"] if existing else None
    has_new_signal = has_real_new_signal_since(old_updated_at) if existing else True

    if existing:
        existing_value = existing["value"] or ""
        existing_confidence = float(existing["confidence"] or 0.0)
        corr = memory_correlation_score(existing_value, value)
        if corr >= MEMORY_CORRELATION_THRESHOLD:
            proposed_confidence = min(
                MEMORY_CONFIDENCE_MAX,
                max(proposed_confidence, existing_confidence) + correlation_bonus(corr),
            )
        # Confidence can only move upward when there is real new signal since last update.
        if proposed_confidence > existing_confidence and not has_new_signal:
            proposed_confidence = existing_confidence

    # If value is unchanged and there is no real new signal, skip the write entirely.
    if existing and old_value == value and not has_new_signal:
        conn.close()
        add_memory_provenance_event(
            scope=scope,
            category=category,
            memory_key=memory_key,
            action="refresh_skipped_no_new_signal",
            source_text=source_text,
            source_trust=source_trust,
            confidence=old_confidence,
            stability=old_stability or stability,
            old_value=old_value,
            new_value=value,
        )
        return

    refresh_candidate = bool(
        existing
        and old_value == value
        and old_confidence == proposed_confidence
        and old_stability == stability
        and old_source_trust == source_trust
    )
    if refresh_candidate and not has_real_new_signal_since(old_updated_at):
        conn.close()
        add_memory_provenance_event(
            scope=scope,
            category=category,
            memory_key=memory_key,
            action="refresh_skipped_no_new_signal",
            source_text=source_text,
            source_trust=source_trust,
            confidence=proposed_confidence,
            stability=stability,
            old_value=old_value,
            new_value=value,
        )
        return

    cur.execute(
        """
        INSERT INTO memory_items (scope, category, subtype, memory_key, value, source_text, source_trust, confidence, stability)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope, category, memory_key) DO UPDATE SET
            subtype = excluded.subtype,
            value = excluded.value,
            source_text = excluded.source_text,
            source_trust = excluded.source_trust,
            confidence = excluded.confidence,
            stability = excluded.stability,
            updated_at = CURRENT_TIMESTAMP
        """,
        (scope, category, subtype, memory_key, value, source_text, source_trust, proposed_confidence, stability),
    )
    conn.commit()
    conn.close()

    action = "insert" if existing is None else "update"
    if existing and old_stability != stability:
        action = "stability_transition"
    if existing and old_value == value and old_confidence == proposed_confidence and old_stability == stability and old_source_trust == source_trust:
        action = "refresh"
    add_memory_provenance_event(
        scope=scope,
        category=category,
        memory_key=memory_key,
        action=action,
        source_text=source_text,
        source_trust=source_trust,
        confidence=proposed_confidence,
        stability=stability,
        old_value=old_value,
        new_value=value,
    )
    if has_new_signal:
        apply_memory_correlation_reinforcement(scope, category, memory_key, value)


def add_journal_entry(entry_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO journal_entries (entry_text) VALUES (?)",
        (entry_text.strip(),),
    )
    conn.commit()
    conn.close()


def add_memory_observation(category, memory_key, value, confidence=0.6, subtype=None, stability=None, source_trust=None):
    inferred_subtype, inferred_stability = classify_memory_metadata("working", category, memory_key)
    subtype = subtype or inferred_subtype
    stability = stability or inferred_stability
    source_trust = classify_source_trust("working", category, source_text=None, source_trust=source_trust)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memory_observations (category, subtype, memory_key, value, source_trust, confidence, stability)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (category, subtype, memory_key, value, source_trust, confidence, stability),
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


def log_alert_outcome(stage, outcome, reason=None, event_hash=None, details=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_outcomes (event_hash, stage, outcome, reason, details_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (event_hash, stage, outcome, reason or "", serialize_json(details, fallback={})),
    )
    conn.commit()
    conn.close()


def log_event_lineage(event_hash, related_event_hash, relation_type, details=None):
    if not event_hash:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_lineage (event_hash, related_event_hash, relation_type, details_json)
        VALUES (?, ?, ?, ?)
        """,
        (event_hash, related_event_hash, relation_type, serialize_json(details, fallback={})),
    )
    conn.commit()
    conn.close()


def is_meaningful_daily_interaction(intent, message_text, memory_result=None):
    if memory_result and memory_result.get("journal_entry"):
        return True
    if intent in {
        "add",
        "remove",
        "watchlist_stats",
        "ticker_quote",
        "daily_brief",
        "alert_feedback",
        "event_expand",
        "fred",
        "news",
        "email_request",
        "portfolio_show",
        "full_article_request",
    }:
        return True
    stripped = (message_text or "").strip()
    if intent == "none" and len(stripped) >= 80 and "?" not in stripped:
        return True
    return False


def upsert_daily_log(intent, message_text, memory_result=None, local_date=None):
    if not is_meaningful_daily_interaction(intent, message_text, memory_result=memory_result):
        return

    date_key = local_date or get_local_date_string()
    snippet = re.sub(r"\s+", " ", (message_text or "").strip())[:220]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT meaningful_count, intents_json, snippets_json
        FROM daily_logs
        WHERE local_date = ?
        LIMIT 1
        """,
        (date_key,),
    )
    row = cur.fetchone()

    if row:
        try:
            intents = json.loads(row["intents_json"] or "{}")
        except:
            intents = {}
        try:
            snippets = json.loads(row["snippets_json"] or "[]")
        except:
            snippets = []
        intents[intent] = intents.get(intent, 0) + 1
        if snippet and snippet not in snippets and len(snippets) < 8:
            snippets.append(snippet)
        cur.execute(
            """
            UPDATE daily_logs
            SET meaningful_count = ?,
                intents_json = ?,
                snippets_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE local_date = ?
            """,
            (int(row["meaningful_count"]) + 1, json.dumps(intents), json.dumps(snippets), date_key),
        )
    else:
        intents = {intent: 1}
        snippets = [snippet] if snippet else []
        cur.execute(
            """
            INSERT INTO daily_logs (local_date, meaningful_count, intents_json, snippets_json)
            VALUES (?, ?, ?, ?)
            """,
            (date_key, 1, json.dumps(intents), json.dumps(snippets)),
        )

    conn.commit()
    conn.close()


def has_real_new_signal_since(timestamp):
    if not timestamp:
        return True

    conn = get_conn()
    cur = conn.cursor()
    checks = [
        ("SELECT COUNT(*) AS count FROM interaction_events WHERE datetime(created_at) > datetime(?)", (timestamp,)),
        ("SELECT COUNT(*) AS count FROM memory_observations WHERE datetime(created_at) > datetime(?)", (timestamp,)),
        ("SELECT COUNT(*) AS count FROM journal_entries WHERE datetime(created_at) > datetime(?)", (timestamp,)),
    ]
    has_signal = False
    for sql, params in checks:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row and int(row["count"] or 0) > 0:
            has_signal = True
            break
    conn.close()
    return has_signal


def get_recent_daily_logs(limit=14):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, meaningful_count, intents_json, snippets_json, updated_at
        FROM daily_logs
        ORDER BY local_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    for row in rows:
        try:
            row["intents"] = json.loads(row.get("intents_json") or "{}")
        except:
            row["intents"] = {}
        try:
            row["snippets"] = json.loads(row.get("snippets_json") or "[]")
        except:
            row["snippets"] = []
        row.pop("intents_json", None)
        row.pop("snippets_json", None)
    return rows


def log_outbound_message(message_type, body):
    execute_write_with_retry(
        """
        INSERT INTO outbound_messages (message_type, body)
        VALUES (?, ?)
        """,
        (message_type, body),
    )


def add_nightly_consolidation_log(summary_text, payload=None, depth_label="light", local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO nightly_consolidation_logs (local_date, summary_text, payload_json, depth_label)
        VALUES (?, ?, ?, ?)
        """,
        (
            local_date or get_local_date_string(),
            summary_text.strip(),
            json.dumps(payload or {}),
            depth_label,
        ),
    )
    conn.commit()
    conn.close()


def get_recent_nightly_consolidation_logs(limit=10):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, summary_text, payload_json, depth_label, created_at
        FROM nightly_consolidation_logs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    for row in rows:
        try:
            row["payload"] = json.loads(row.get("payload_json") or "{}")
        except:
            row["payload"] = {}
        row.pop("payload_json", None)
    return rows


def get_local_now():
    return datetime.now(LOCAL_TZ)


def get_local_date_string(dt=None):
    return (dt or get_local_now()).strftime("%Y-%m-%d")


def weather_code_to_label(code):
    mapping = {
        0: "clear",
        1: "mostly_clear",
        2: "partly_cloudy",
        3: "overcast",
        45: "fog",
        48: "rime_fog",
        51: "light_drizzle",
        53: "drizzle",
        55: "dense_drizzle",
        61: "light_rain",
        63: "rain",
        65: "heavy_rain",
        71: "light_snow",
        73: "snow",
        75: "heavy_snow",
        80: "light_showers",
        81: "showers",
        82: "heavy_showers",
        95: "thunderstorm",
    }
    return mapping.get(int(code), "unknown")


def upsert_weather_daily_context(local_date, payload):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO weather_daily_context (
            local_date, temperature_c, precipitation_mm, cloud_cover_pct,
            humidity_pct, weather_code, weather_label, source, confidence, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(local_date) DO UPDATE SET
            temperature_c = excluded.temperature_c,
            precipitation_mm = excluded.precipitation_mm,
            cloud_cover_pct = excluded.cloud_cover_pct,
            humidity_pct = excluded.humidity_pct,
            weather_code = excluded.weather_code,
            weather_label = excluded.weather_label,
            source = excluded.source,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            local_date,
            payload.get("temperature_c"),
            payload.get("precipitation_mm"),
            payload.get("cloud_cover_pct"),
            payload.get("humidity_pct"),
            payload.get("weather_code"),
            payload.get("weather_label"),
            payload.get("source", "open-meteo"),
            float(payload.get("confidence") or 0.7),
        ),
    )
    conn.commit()
    conn.close()


def get_weather_daily_context(local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM weather_daily_context
        WHERE local_date = ?
        LIMIT 1
        """,
        (local_date or get_local_date_string(),),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def fetch_open_meteo_daily_context(local_date=None):
    if not OPEN_METEO_LAT or not OPEN_METEO_LON:
        return None
    try:
        params = {
            "latitude": OPEN_METEO_LAT,
            "longitude": OPEN_METEO_LON,
            "hourly": "temperature_2m,precipitation,cloud_cover,relative_humidity_2m,weather_code",
            "timezone": "America/Los_Angeles",
            "forecast_days": 1,
        }
        response = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=8)
        if response.status_code != 200:
            return None
        data = response.json()
        hourly = data.get("hourly") or {}
        temperatures = hourly.get("temperature_2m") or []
        precip = hourly.get("precipitation") or []
        clouds = hourly.get("cloud_cover") or []
        humidity = hourly.get("relative_humidity_2m") or []
        codes = hourly.get("weather_code") or []
        if not temperatures:
            return None

        avg_temp = sum(temperatures) / len(temperatures)
        total_precip = sum(precip) if precip else 0.0
        avg_cloud = (sum(clouds) / len(clouds)) if clouds else None
        avg_humidity = (sum(humidity) / len(humidity)) if humidity else None
        dominant_code = max(set(codes), key=codes.count) if codes else 0

        payload = {
            "temperature_c": round(avg_temp, 2),
            "precipitation_mm": round(total_precip, 2),
            "cloud_cover_pct": round(avg_cloud, 2) if avg_cloud is not None else None,
            "humidity_pct": round(avg_humidity, 2) if avg_humidity is not None else None,
            "weather_code": int(dominant_code),
            "weather_label": weather_code_to_label(dominant_code),
            "source": "open-meteo",
            "confidence": 0.78,
        }
        upsert_weather_daily_context(local_date or get_local_date_string(), payload)
        return payload
    except:
        return None


def upsert_calendar_daily_context(local_date, payload):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO calendar_daily_context (
            local_date, busy_score, event_count, deep_work_blocks,
            stress_windows, summary_text, source, confidence, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(local_date) DO UPDATE SET
            busy_score = excluded.busy_score,
            event_count = excluded.event_count,
            deep_work_blocks = excluded.deep_work_blocks,
            stress_windows = excluded.stress_windows,
            summary_text = excluded.summary_text,
            source = excluded.source,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            local_date,
            float(payload.get("busy_score") or 0.0),
            int(payload.get("event_count") or 0),
            int(payload.get("deep_work_blocks") or 0),
            int(payload.get("stress_windows") or 0),
            (payload.get("summary_text") or "").strip(),
            payload.get("source", "calendar"),
            float(payload.get("confidence") or 0.6),
        ),
    )
    conn.commit()
    conn.close()


def get_calendar_daily_context(local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM calendar_daily_context
        WHERE local_date = ?
        LIMIT 1
        """,
        (local_date or get_local_date_string(),),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_sleep_daily_context(local_date, payload):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sleep_daily_context (
            local_date, sleep_hours, sleep_quality, steps, resting_hr,
            fatigue_score, summary_text, source, confidence, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(local_date) DO UPDATE SET
            sleep_hours = excluded.sleep_hours,
            sleep_quality = excluded.sleep_quality,
            steps = excluded.steps,
            resting_hr = excluded.resting_hr,
            fatigue_score = excluded.fatigue_score,
            summary_text = excluded.summary_text,
            source = excluded.source,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            local_date,
            payload.get("sleep_hours"),
            payload.get("sleep_quality"),
            payload.get("steps"),
            payload.get("resting_hr"),
            payload.get("fatigue_score"),
            (payload.get("summary_text") or "").strip(),
            payload.get("source", "health"),
            float(payload.get("confidence") or 0.6),
        ),
    )
    conn.commit()
    conn.close()


def get_sleep_daily_context(local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM sleep_daily_context
        WHERE local_date = ?
        LIMIT 1
        """,
        (local_date or get_local_date_string(),),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def mark_scheduled_task_run(task_key, local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scheduled_task_runs (task_key, local_date)
        VALUES (?, ?)
        ON CONFLICT(task_key, local_date) DO NOTHING
        """,
        (task_key, local_date or get_local_date_string()),
    )
    conn.commit()
    inserted = cur.rowcount > 0
    conn.close()
    return inserted


def has_scheduled_task_run(task_key, local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM scheduled_task_runs
        WHERE task_key = ?
          AND local_date = ?
        LIMIT 1
        """,
        (task_key, local_date or get_local_date_string()),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def scheduled_task_due(task_key, target_hour, target_minute, now_local=None):
    now_local = now_local or get_local_now()
    local_date = get_local_date_string(now_local)
    if has_scheduled_task_run(task_key, local_date=local_date):
        return False
    current_minutes = now_local.hour * 60 + now_local.minute
    target_minutes = target_hour * 60 + target_minute
    return current_minutes >= target_minutes


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


def build_unauthorized_message_warning(source_number, message_text):
    timestamp = datetime.now(LOCAL_TZ).strftime("%-I:%M %p PT on %B %-d")
    sender = source_number or "unknown"
    verbatim = message_text if message_text is not None else ""
    if verbatim == "":
        verbatim = "[empty message]"
    return (
        f"Warning. Unauthorized inbound message at {timestamp}.\n"
        f"From: {sender}\n"
        f"Message (verbatim):\n{verbatim}"
    )


def require_internal_api_key():
    if not INTERNAL_API_KEY:
        return None
    provided = (
        request.headers.get("X-Internal-Key")
        or request.args.get("key")
        or request.form.get("key")
    )
    if provided == INTERNAL_API_KEY:
        return None
    return app.response_class(
        response=json.dumps({"ok": False, "reason": "unauthorized"}, indent=2),
        status=401,
        mimetype="application/json",
    )


def get_twilio_validation_url():
    if PUBLIC_BASE_URL:
        base = PUBLIC_BASE_URL.rstrip("/")
        return f"{base}{request.path}"
    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}{request.path}"


def verify_twilio_request():
    if not ENFORCE_TWILIO_SIGNATURE:
        return True
    if not TWILIO_AUTH_TOKEN:
        return False
    signature = request.headers.get("X-Twilio-Signature", "")
    if not signature:
        return False
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    url = get_twilio_validation_url()
    return validator.validate(url, request.form.to_dict(), signature)


def get_public_base_url():
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    if has_request_context():
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        host = request.headers.get("X-Forwarded-Host", request.host)
        return f"{proto}://{host}".rstrip("/")
    return ""


def append_internal_key(url):
    if not INTERNAL_API_KEY:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}key={quote_plus(INTERNAL_API_KEY)}"


def build_command_key_reply():
    base = get_public_base_url()
    privacy_link = f"{base}/privacy" if base else "/privacy"
    terms_link = f"{base}/terms" if base else "/terms"
    memory_raw = append_internal_key(f"{base}/debug/memory" if base else "/debug/memory")
    memory_compact = append_internal_key(f"{base}/debug/memory?compact=1" if base else "/debug/memory?compact=1")
    memory_view = append_internal_key(f"{base}/debug/memory/view" if base else "/debug/memory/view")
    return "\n".join([
        COMMAND_KEY_REPLY,
        "",
        f"privacy: {privacy_link}",
        f"terms: {terms_link}",
        f"memory debug raw: {memory_raw}",
        f"memory debug compact: {memory_compact}",
        f"memory debug view: {memory_view}",
    ])


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


def get_latest_outbound_message(message_type=None, hours=24):
    conn = get_conn()
    cur = conn.cursor()
    if message_type:
        cur.execute(
            """
            SELECT id, message_type, body, created_at
            FROM outbound_messages
            WHERE message_type = ?
              AND datetime(created_at) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (message_type, f"-{hours} hours"),
        )
    else:
        cur.execute(
            """
            SELECT id, message_type, body, created_at
            FROM outbound_messages
            WHERE datetime(created_at) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (f"-{hours} hours",),
        )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def count_interactions_since(timestamp):
    if not timestamp:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM interaction_events
        WHERE datetime(created_at) > datetime(?)
        """,
        (timestamp,),
    )
    row = cur.fetchone()
    conn.close()
    return int(row["count"] or 0) if row else 0


def in_gratitude_journal_context():
    latest = get_latest_outbound_message("gratitude", hours=JOURNAL_RESPONSE_WINDOW_HOURS)
    if not latest:
        return False
    # Do not require gratitude to be the latest outbound message.
    # Alerts/daily briefs may be sent after gratitude prompt and must not break
    # journal-context capture for the first inbound user response.
    return count_interactions_since(latest.get("created_at")) == 0


def recent_outbound_message_type_within(message_type, hours=6):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM outbound_messages
        WHERE message_type = ?
          AND datetime(created_at) >= datetime('now', ?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (message_type, f"-{hours} hours"),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def should_send_no_reply(text):
    stripped = (text or "").strip()
    if not stripped:
        return True

    if in_gratitude_journal_context():
        return True

    # Explicit silent marker: any message ending with "-" gets processed
    # normally but does not trigger an outbound reply.
    if stripped.endswith("-"):
        return True

    if len(stripped) > 160 and "?" not in stripped:
        return True

    return False


def get_memory_items(scope, limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            scope,
            category,
            subtype,
            memory_key,
            value,
            confidence,
            stability,
            updated_at,
            julianday('now') - julianday(updated_at) AS recency_days
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


def get_recent_journal(limit=30):
    conn = get_conn()
    cur = conn.cursor()
    rows = []
    try:
        cur.execute(
            """
            SELECT entry_text, created_at
            FROM journal_entries
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        cur.execute(
            """
            SELECT entry_text, created_at
            FROM gratitude_entries
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_recent_gratitude(limit=30):
    # Backward-compatible alias during migration from gratitude -> journal naming.
    return get_recent_journal(limit=limit)


def get_recent_observations(limit=200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            category,
            subtype,
            memory_key,
            value,
            confidence,
            stability,
            created_at
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


def get_recent_memory_provenance(limit=80):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, action, source_text, source_trust, confidence, stability, old_value, new_value, created_at
        FROM memory_provenance_events
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_memory_stability_timeline(limit=80):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, action, stability, confidence, created_at
        FROM memory_provenance_events
        WHERE action IN ('insert', 'update', 'stability_transition')
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_contradiction_view(limit=30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, value, confidence, stability, updated_at
        FROM memory_items
        WHERE category = 'behavior_trends'
          AND memory_key LIKE 'contradiction_%'
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    parsed = []
    for row in rows:
        try:
            payload = json.loads(row.get("value") or "{}")
        except:
            payload = {}
        parsed.append({
            "memory_key": row.get("memory_key"),
            "usually_true": payload.get("usually_true", ""),
            "recently_true": payload.get("recently_true", ""),
            "confidence": row.get("confidence"),
            "stability": row.get("stability"),
            "updated_at": row.get("updated_at"),
        })
    return parsed


def get_thread_map(limit=20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, value, confidence, stability, updated_at
        FROM memory_items
        WHERE category = 'memory_threads'
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    thread_rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    interactions = get_recent_interaction_events(limit=80)
    interaction_texts = [((item.get("message_text") or "").lower()) for item in interactions]
    mapped = []
    for item in thread_rows:
        value = (item.get("value") or "").lower()
        keywords = [token for token in re.findall(r"[a-z]{4,}", value)[:4]]
        mention_count = 0
        if keywords:
            for text in interaction_texts:
                if any(keyword in text for keyword in keywords):
                    mention_count += 1
        status = "cooling"
        if mention_count >= 3:
            status = "intensifying"
        elif mention_count >= 1:
            status = "active"
        mapped.append({
            "memory_key": item.get("memory_key"),
            "value": item.get("value"),
            "confidence": item.get("confidence"),
            "stability": item.get("stability"),
            "status": status,
            "mention_count_recent": mention_count,
            "updated_at": item.get("updated_at"),
        })
    return mapped


def get_recent_decay_audit(limit=80):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT scope, category, memory_key, from_confidence, to_confidence, reason, created_at
        FROM memory_decay_audit
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_reinforce_decay_audit(limit=12):
    logs = get_recent_nightly_consolidation_logs(limit=limit)
    audit = []
    for row in logs:
        payload = row.get("payload") or {}
        audit.append({
            "local_date": row.get("local_date"),
            "depth_label": row.get("depth_label"),
            "reinforce_decisions": payload.get("reinforce_decisions", []),
            "decay_decisions": payload.get("decay_decisions", []),
            "uncertainty_flags": payload.get("uncertainty_flags", []),
            "protected_updates": payload.get("protected_updates", []),
            "summary_text": row.get("summary_text"),
            "created_at": row.get("created_at"),
        })
    return audit


def build_no_filler_validation(days=7):
    end_date = get_local_now().date()
    start_date = end_date - timedelta(days=max(1, days) - 1)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, meaningful_count
        FROM daily_logs
        WHERE local_date BETWEEN ? AND ?
        """,
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )
    log_rows = {row["local_date"]: int(row["meaningful_count"]) for row in cur.fetchall()}

    cur.execute(
        """
        SELECT date(created_at, 'localtime') AS local_date, COUNT(*) AS count
        FROM memory_observations
        WHERE date(created_at, 'localtime') BETWEEN ? AND ?
          AND category IN ('journal', 'emotional_state', 'state', 'frictions', 'behavior_trends')
        GROUP BY date(created_at, 'localtime')
        """,
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )
    personal_rows = {row["local_date"]: int(row["count"]) for row in cur.fetchall()}
    conn.close()

    timeline = []
    for offset in range(days):
        day = start_date + timedelta(days=offset)
        key = day.strftime("%Y-%m-%d")
        meaningful_count = log_rows.get(key, 0)
        personal_count = personal_rows.get(key, 0)
        no_filler_ok = not (meaningful_count == 0 and personal_count > 0)
        timeline.append({
            "local_date": key,
            "meaningful_count": meaningful_count,
            "personal_memory_count": personal_count,
            "no_filler_ok": no_filler_ok,
        })
    return timeline


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
        SELECT
            e.scope,
            e.category,
            e.memory_key,
            e.embedding_json,
            e.semantic_text,
            e.updated_at,
            COALESCE(m.confidence, 0.5) AS memory_confidence
        FROM memory_embeddings e
        LEFT JOIN memory_items m
          ON m.scope = e.scope
         AND m.category = e.category
         AND m.memory_key = e.memory_key
        ORDER BY e.updated_at DESC
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
        confidence_value = max(0.0, min(0.99, float(row.get("memory_confidence") or 0.5)))
        # Confidence now directly controls influence: low-confidence memories still
        # contribute but are muted; high-confidence memories pull harder.
        confidence_weight = 0.35 + (0.95 * confidence_value)
        weight = base_weight * recency_weight * confidence_weight

        if aggregate is None:
            aggregate = [0.0] * len(embedding)

        for idx, value in enumerate(embedding):
            aggregate[idx] += value * weight
        total_weight += weight
        weighted_rows.append({
            "category": category,
            "memory_key": row.get("memory_key"),
            "confidence": round(confidence_value, 4),
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


def build_recent_memory_material(interaction_limit=50, observation_limit=60, journal_limit=8):
    interactions = get_recent_interaction_events(limit=interaction_limit)
    observations = get_recent_observations(limit=observation_limit)
    journal_entries = get_recent_journal(limit=journal_limit)
    return {
        "interactions": interactions,
        "observations": observations,
        "journal": journal_entries,
    }


def should_run_deep_memory_consolidation(material):
    interactions = material.get("interactions", [])
    observations = material.get("observations", [])
    journal_entries = material.get("journal", [])
    if len(interactions) >= 8:
        return True
    if len(observations) >= 10:
        return True
    if journal_entries:
        return True
    return False


def fallback_deep_memory_consolidation(material):
    interactions = material.get("interactions", [])
    journal_entries = material.get("journal", [])
    summary = "Light consolidation only; limited recent material."
    if journal_entries:
        summary = "Recent journal signal present; reflective signal recorded."
    elif interactions:
        summary = "Recent interactions reviewed; no strong durable shift identified."
    return {
        "summary": summary,
        "more_true": [],
        "less_true": [],
        "emerging": [],
        "reinforce_decisions": [],
        "decay_decisions": [],
        "uncertainty_flags": [],
        "relationship_memory": [],
        "thread_memory": [],
        "deep_self_memory": [],
        "contradictions": [],
        "protect": [],
        "protected_updates": [],
        "depth_label": "light",
    }


def sanitize_deep_memory_payload(payload):
    if not isinstance(payload, dict):
        return None

    allowed_deep_categories = {
        "core_identity",
        "core_traits",
        "defining_moments",
        "major_successes",
        "major_failures",
        "deep_preferences",
        "long_term_frictions",
    }

    def clean_text_list(items, max_items=8, max_len=260):
        cleaned = []
        for item in (items or []):
            value = re.sub(r"\s+", " ", str(item or "")).strip()
            if not value:
                continue
            cleaned.append(value[:max_len])
            if len(cleaned) >= max_items:
                break
        return cleaned

    def clean_obj_list(items, fields, max_items=8):
        cleaned = []
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            obj = {}
            for field in fields:
                obj[field] = re.sub(r"\s+", " ", str(item.get(field, "") or "")).strip()
            if "confidence" in item:
                try:
                    obj["confidence"] = float(item.get("confidence") or 0.0)
                except:
                    obj["confidence"] = 0.0
            cleaned.append(obj)
            if len(cleaned) >= max_items:
                break
        return cleaned

    summary = re.sub(r"\s+", " ", str(payload.get("summary") or "")).strip()[:320] or "Nightly consolidation completed."
    depth_label = (str(payload.get("depth_label") or "light").strip().lower())
    if depth_label not in {"light", "deep"}:
        depth_label = "light"

    cleaned = {
        "summary": summary,
        "more_true": clean_text_list(payload.get("more_true"), max_items=5),
        "less_true": clean_text_list(payload.get("less_true"), max_items=5),
        "emerging": clean_text_list(payload.get("emerging"), max_items=6),
        "reinforce_decisions": clean_text_list(payload.get("reinforce_decisions"), max_items=6),
        "decay_decisions": clean_text_list(payload.get("decay_decisions"), max_items=6),
        "uncertainty_flags": clean_text_list(payload.get("uncertainty_flags"), max_items=6),
        "relationship_memory": clean_obj_list(payload.get("relationship_memory"), ["memory_key", "value"], max_items=6),
        "thread_memory": clean_obj_list(payload.get("thread_memory"), ["memory_key", "value"], max_items=8),
        "deep_self_memory": clean_obj_list(payload.get("deep_self_memory"), ["category", "memory_key", "value"], max_items=8),
        "contradictions": clean_obj_list(payload.get("contradictions"), ["memory_key", "usually_true", "recently_true"], max_items=6),
        "protect": clean_text_list(payload.get("protect"), max_items=8),
        "protected_updates": clean_text_list(payload.get("protected_updates"), max_items=8),
        "depth_label": depth_label,
    }

    valid_deep = []
    for item in cleaned["deep_self_memory"]:
        category = (item.get("category") or "").strip()
        if category not in allowed_deep_categories:
            continue
        if not item.get("memory_key") or not item.get("value"):
            continue
        valid_deep.append(item)
    cleaned["deep_self_memory"] = valid_deep

    cleaned["relationship_memory"] = [
        item for item in cleaned["relationship_memory"]
        if item.get("memory_key") and item.get("value")
    ]
    cleaned["thread_memory"] = [
        item for item in cleaned["thread_memory"]
        if item.get("memory_key") and item.get("value")
    ]
    cleaned["contradictions"] = [
        item for item in cleaned["contradictions"]
        if item.get("memory_key") and (item.get("usually_true") or item.get("recently_true"))
    ]

    return cleaned


def run_ai_deep_memory_consolidation(material):
    interactions = material.get("interactions", [])[:40]
    observations = material.get("observations", [])[:50]
    journal_entries = material.get("journal", [])[:8]

    prompt = f"""
You are consolidating long-term memory for Manu.

Rules:
- Be deep, but do not hallucinate.
- Only claim something if the material supports it.
- Prefer nuance over flattening him into one trait.
- Track contradictions instead of overwriting too fast.
- Identify relationship preferences, ongoing threads, and deep-self signals.
- Be specific and human, not generic.
- Return JSON only.

Recent interactions:
{json.dumps(interactions, ensure_ascii=True)}

Recent observations:
{json.dumps(observations, ensure_ascii=True)}

Recent journal entries:
{json.dumps(journal_entries, ensure_ascii=True)}

Return:
{{
  "summary": "1-3 sentence nightly summary",
  "more_true": ["what seems more true now"],
  "less_true": ["what seems less true now"],
  "emerging": ["possible emerging patterns"],
  "reinforce_decisions": ["what should be reinforced and why"],
  "decay_decisions": ["what should decay and why"],
  "uncertainty_flags": ["what remains uncertain and should not be overfit"],
  "relationship_memory": [
    {{"memory_key": "response_preference", "value": "short specific description", "confidence": 0.0}}
  ],
  "thread_memory": [
    {{"memory_key": "open_loop", "value": "short unresolved topic or ongoing arc", "confidence": 0.0}}
  ],
  "deep_self_memory": [
    {{"category": "core_identity|core_traits|defining_moments|major_successes|major_failures|deep_preferences|long_term_frictions", "memory_key": "short_key", "value": "durable self insight", "confidence": 0.0}}
  ],
  "contradictions": [
    {{"memory_key": "short_key", "usually_true": "old pattern", "recently_true": "recent shift", "confidence": 0.0}}
  ],
  "protect": ["memory key or category that should be treated as protected"],
  "protected_updates": ["what protected memory was reaffirmed or adjusted"],
  "depth_label": "light|deep"
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        sanitized = sanitize_deep_memory_payload(payload)
        if not sanitized:
            return fallback_deep_memory_consolidation(material)
        return sanitized
    except:
        return fallback_deep_memory_consolidation(material)


def store_deep_memory_consolidation(payload):
    payload = sanitize_deep_memory_payload(payload) or sanitize_deep_memory_payload(fallback_deep_memory_consolidation({}))
    summary = (payload.get("summary") or "Nightly consolidation completed.").strip()
    depth_label = (payload.get("depth_label") or "light").strip().lower()

    for idx, value in enumerate(payload.get("more_true", [])[:5], start=1):
        upsert_memory("working", "nightly_summary", f"more_true_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.7)
        record_memory_embedding("working", "nightly_summary", f"more_true_{idx}", value)

    for idx, value in enumerate(payload.get("less_true", [])[:3], start=1):
        upsert_memory("working", "nightly_summary", f"less_true_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.65)
        record_memory_embedding("working", "nightly_summary", f"less_true_{idx}", value)

    for idx, value in enumerate(payload.get("emerging", [])[:5], start=1):
        upsert_memory("long_term", "behavior_trends", f"emerging_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.68)
        record_memory_embedding("long_term", "behavior_trends", f"emerging_{idx}", value)

    for idx, value in enumerate(payload.get("reinforce_decisions", [])[:5], start=1):
        upsert_memory("working", "nightly_summary", f"reinforce_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.7)
        record_memory_embedding("working", "nightly_summary", f"reinforce_{idx}", value)

    for idx, value in enumerate(payload.get("decay_decisions", [])[:5], start=1):
        upsert_memory("working", "nightly_summary", f"decay_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.68)
        record_memory_embedding("working", "nightly_summary", f"decay_{idx}", value)

    for idx, value in enumerate(payload.get("uncertainty_flags", [])[:5], start=1):
        upsert_memory("working", "nightly_summary", f"uncertain_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.62)
        record_memory_embedding("working", "nightly_summary", f"uncertain_{idx}", value)

    for item in payload.get("relationship_memory", [])[:6]:
        key = (item.get("memory_key") or "relationship_preference").strip()[:64]
        value = (item.get("value") or "").strip()
        if not value:
            continue
        confidence = max(0.45, min(0.95, float(item.get("confidence") or 0.68)))
        upsert_memory("long_term", "relationship_preferences", key, value, source_text="nightly_deep_consolidation", confidence=confidence)
        record_memory_embedding("long_term", "relationship_preferences", key, value)

    for item in payload.get("thread_memory", [])[:8]:
        key = (item.get("memory_key") or "open_loop").strip()[:64]
        value = (item.get("value") or "").strip()
        if not value:
            continue
        confidence = max(0.45, min(0.95, float(item.get("confidence") or 0.66)))
        upsert_memory("working", "memory_threads", key, value, source_text="nightly_deep_consolidation", confidence=confidence)
        record_memory_embedding("working", "memory_threads", key, value)

    for item in payload.get("deep_self_memory", [])[:8]:
        category = (item.get("category") or "core_identity").strip()
        key = (item.get("memory_key") or "deep_self").strip()[:64]
        value = (item.get("value") or "").strip()
        if not value:
            continue
        confidence = max(0.5, min(0.98, float(item.get("confidence") or 0.72)))
        upsert_memory("long_term", category, key, value, source_text="nightly_deep_consolidation", confidence=confidence)
        record_memory_embedding("long_term", category, key, value)

    for item in payload.get("contradictions", [])[:6]:
        key = (item.get("memory_key") or "identity_shift").strip()[:64]
        usually_true = (item.get("usually_true") or "").strip()
        recently_true = (item.get("recently_true") or "").strip()
        if not usually_true and not recently_true:
            continue
        confidence = max(0.45, min(0.95, float(item.get("confidence") or 0.65)))
        contradiction_value = json.dumps({
            "usually_true": usually_true,
            "recently_true": recently_true,
        })
        upsert_memory("long_term", "behavior_trends", f"contradiction_{key}", contradiction_value, source_text="nightly_deep_consolidation", confidence=confidence)
        record_memory_embedding("long_term", "behavior_trends", f"contradiction_{key}", f"usually: {usually_true}; recently: {recently_true}")

    for idx, value in enumerate(payload.get("protected_updates", [])[:6], start=1):
        upsert_memory("long_term", "core_identity", f"protected_update_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.82)
        record_memory_embedding("long_term", "core_identity", f"protected_update_{idx}", value)

    for idx, value in enumerate(payload.get("protect", [])[:8], start=1):
        upsert_memory("working", "nightly_summary", f"protect_{idx}", value, source_text="nightly_deep_consolidation", confidence=0.72)

    add_nightly_consolidation_log(summary, payload=payload, depth_label=depth_label)
    return {
        "summary": summary,
        "depth_label": depth_label,
    }


def is_protected_memory_item(item):
    return item.get("category") in get_effective_protected_memory_categories()


def apply_memory_decay():
    conn = get_conn()
    cur = conn.cursor()
    local_date = get_local_date_string()
    cur.execute(
        """
        INSERT INTO memory_decay_runs (local_date)
        VALUES (?)
        ON CONFLICT(local_date) DO NOTHING
        """,
        (local_date,),
    )
    if cur.rowcount == 0:
        conn.commit()
        conn.close()
        return

    protected = get_effective_protected_memory_categories()
    cur.execute(
        """
        SELECT memory_key, MAX(correlation) AS corr
        FROM memory_correlation_links
        WHERE scope = 'long_term'
          AND datetime(updated_at) >= datetime('now', '-45 days')
        GROUP BY memory_key
        """
    )
    correlation_map = {}
    for row in cur.fetchall():
        try:
            correlation_map[row["memory_key"]] = float(row["corr"] or 0.0)
        except:
            correlation_map[row["memory_key"]] = 0.0

    cur.execute(
        """
        SELECT id, category, memory_key, confidence, julianday('now') - julianday(updated_at) AS age_days
        FROM memory_items
        WHERE scope = 'long_term'
        """
    )
    rows = [dict(row) for row in cur.fetchall()]

    for row in rows:
        if row["category"] in protected:
            continue
        age_days = row["age_days"] or 0
        if age_days < 7:
            continue
        current_conf = float(row["confidence"] or 0.0)
        if current_conf <= MEMORY_DELETE_THRESHOLD and age_days >= MEMORY_DELETE_MIN_AGE_DAYS:
            cur.execute(
                """
                DELETE FROM memory_items
                WHERE id = ?
                """,
                (row["id"],),
            )
            cur.execute(
                """
                INSERT INTO memory_decay_audit (scope, category, memory_key, from_confidence, to_confidence, reason)
                VALUES ('long_term', ?, ?, ?, ?, ?)
                """,
                (row["category"], row.get("memory_key", ""), current_conf, 0.0, "low_confidence_delete"),
            )
            cur.execute(
                """
                INSERT INTO memory_provenance_events (
                    scope, category, memory_key, action, source_text, source_trust,
                    confidence, stability, old_value, new_value
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "long_term",
                    row["category"],
                    row.get("memory_key", ""),
                    "delete_low_confidence",
                    "memory_decay",
                    "inferred",
                    0.0,
                    "decayed_out",
                    None,
                    None,
                ),
            )
            continue

        random_factor = 1 + random.uniform(-0.01, 0.01)
        decay_step = 0.015 * random_factor
        corr = float(correlation_map.get(row.get("memory_key", ""), 0.0))
        if corr >= MEMORY_CORRELATION_THRESHOLD:
            slowdown_multiplier = max(0.5, 1 - (0.05 * ((corr - MEMORY_CORRELATION_THRESHOLD) / 0.1)))
            decay_step *= slowdown_multiplier
        new_confidence = max(0.0, current_conf - decay_step)
        if new_confidence < current_conf:
            from_conf = current_conf
            to_conf = round(new_confidence, 4)
            cur.execute(
                """
                UPDATE memory_items
                SET confidence = ?, updated_at = updated_at
                WHERE id = ?
                """,
                (to_conf, row["id"]),
            )
            cur.execute(
                """
                INSERT INTO memory_decay_audit (scope, category, memory_key, from_confidence, to_confidence, reason)
                VALUES ('long_term', ?, ?, ?, ?, ?)
                """,
                (row["category"], row.get("memory_key", ""), from_conf, to_conf, "age_decay"),
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

    return updates, gratitude_entry


def fallback_journal_analysis(text):
    return {
        "summary": text.strip()[:220],
        "emotional_tone": "reflective",
        "energy_level": "medium",
        "outlook": "steady",
        "stress_level": "normal",
        "friction_signal": "low",
        "notable_shift": "",
        "durable_signal": "",
        "confidence": 0.35,
    }


def analyze_journal_entry(text):
    prompt = f"""
Analyze this private gratitude/journal response for memory only.

Rules:
- Be emotionally perceptive but not sycophantic.
- Do not flatter.
- Do not moralize.
- Only identify useful human signals.
- This is for memory and self-understanding, not for replying.
- Return JSON only.

Text:
\"\"\"{text}\"\"\"

Return:
{{
  "summary": "short neutral summary",
  "emotional_tone": "positive|mixed|strained|calm|reflective",
  "energy_level": "low|medium|high",
  "outlook": "guarded|steady|upbeat",
  "stress_level": "low|normal|elevated",
  "friction_signal": "low|present",
  "notable_shift": "optional short note",
  "durable_signal": "optional short note",
  "confidence": 0.0
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        if not isinstance(payload, dict):
            return fallback_journal_analysis(text)
        payload["confidence"] = float(payload.get("confidence") or 0.55)
        return payload
    except:
        return fallback_journal_analysis(text)


def store_journal_analysis(text):
    analysis = analyze_journal_entry(text)

    summary = (analysis.get("summary") or text.strip())[:240]
    tone = (analysis.get("emotional_tone") or "reflective").strip()
    energy = (analysis.get("energy_level") or "medium").strip()
    outlook = (analysis.get("outlook") or "steady").strip()
    stress = (analysis.get("stress_level") or "normal").strip()
    friction = (analysis.get("friction_signal") or "low").strip()
    notable_shift = (analysis.get("notable_shift") or "").strip()
    durable_signal = (analysis.get("durable_signal") or "").strip()
    confidence = max(0.4, min(0.95, float(analysis.get("confidence") or 0.55)))

    add_memory_observation("journal", "entry_summary", summary, confidence=confidence)
    add_memory_observation("journal", "emotional_tone", tone, confidence=confidence)
    add_memory_observation("journal", "energy_level", energy, confidence=confidence)
    add_memory_observation("journal", "outlook", outlook, confidence=confidence)
    add_memory_observation("journal", "stress_level", stress, confidence=confidence)

    upsert_memory("working", "emotional_state", "journal_tone", tone, source_text=text, confidence=confidence)
    upsert_memory("working", "state", "journal_energy", energy, source_text=text, confidence=confidence)
    upsert_memory("working", "state", "journal_outlook", outlook, source_text=text, confidence=confidence)
    upsert_memory("working", "state", "journal_stress", stress, source_text=text, confidence=confidence)

    record_memory_embedding("working", "emotional_state", "journal_tone", f"{summary}. tone: {tone}",)
    record_memory_embedding("working", "state", "journal_energy", f"{summary}. energy: {energy}",)
    record_memory_embedding("working", "state", "journal_outlook", f"{summary}. outlook: {outlook}",)

    if friction == "present":
        upsert_memory("working", "frictions", "journal_friction", summary, source_text=text, confidence=max(0.55, confidence))

    if notable_shift:
        upsert_memory("long_term", "behavior_trends", "journal_shift", notable_shift, source_text=text, confidence=max(0.55, confidence))
        record_memory_embedding("long_term", "behavior_trends", "journal_shift", notable_shift)

    if durable_signal:
        upsert_memory("long_term", "behavior_trends", "journal_durable_signal", durable_signal, source_text=text, confidence=max(0.55, confidence))
        record_memory_embedding("long_term", "behavior_trends", "journal_durable_signal", durable_signal)

    return {
        "summary": summary,
        "tone": tone,
        "energy": energy,
        "outlook": outlook,
        "stress": stress,
        "friction": friction,
        "notable_shift": notable_shift,
        "durable_signal": durable_signal,
        "confidence": confidence,
    }


def get_weather_context_snapshot():
    local_date = get_local_date_string()
    stored = get_weather_daily_context(local_date=local_date)
    if not stored:
        fetched = fetch_open_meteo_daily_context(local_date=local_date)
        if fetched:
            stored = get_weather_daily_context(local_date=local_date)
    if not stored:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    label = (stored.get("weather_label") or "unknown").replace("_", " ")
    return {
        "available": True,
        "status": "connected",
        "summary": f"{label}, {stored.get('temperature_c')}C, precip {stored.get('precipitation_mm')}mm",
        "features": {
            "temperature_c": stored.get("temperature_c"),
            "precipitation_mm": stored.get("precipitation_mm"),
            "cloud_cover_pct": stored.get("cloud_cover_pct"),
            "humidity_pct": stored.get("humidity_pct"),
            "weather_label": stored.get("weather_label"),
            "confidence": stored.get("confidence"),
        },
    }


def get_sleep_context_snapshot():
    stored = get_sleep_daily_context(local_date=get_local_date_string())
    if not stored:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    return {
        "available": True,
        "status": "connected",
        "summary": (stored.get("summary_text") or f"sleep {stored.get('sleep_hours')}h, fatigue {stored.get('fatigue_score')}"),
        "features": {
            "sleep_hours": stored.get("sleep_hours"),
            "sleep_quality": stored.get("sleep_quality"),
            "steps": stored.get("steps"),
            "resting_hr": stored.get("resting_hr"),
            "fatigue_score": stored.get("fatigue_score"),
            "confidence": stored.get("confidence"),
        },
    }


def get_calendar_context_snapshot():
    stored = get_calendar_daily_context(local_date=get_local_date_string())
    if not stored:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    return {
        "available": True,
        "status": "connected",
        "summary": (stored.get("summary_text") or f"{stored.get('event_count')} events, busy score {stored.get('busy_score')}"),
        "features": {
            "busy_score": stored.get("busy_score"),
            "event_count": stored.get("event_count"),
            "deep_work_blocks": stored.get("deep_work_blocks"),
            "stress_windows": stored.get("stress_windows"),
            "confidence": stored.get("confidence"),
        },
    }


def build_journal_context_snapshot():
    return {
        "weather": get_weather_context_snapshot(),
        "sleep": get_sleep_context_snapshot(),
        "calendar": get_calendar_context_snapshot(),
    }


def clamp01(value):
    return max(0.0, min(1.0, float(value)))


def compute_market_stress_signal():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM alert_outcomes
        WHERE outcome = 'allowed'
          AND datetime(created_at) >= datetime('now', '-24 hours')
        """
    )
    row = cur.fetchone()
    allowed = row["count"] if row else 0
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM alert_outcomes
        WHERE outcome = 'blocked'
          AND datetime(created_at) >= datetime('now', '-24 hours')
        """
    )
    row = cur.fetchone()
    blocked = row["count"] if row else 0
    conn.close()
    volume = allowed + blocked
    if volume <= 2:
        return 0.2
    if volume <= 6:
        return 0.45
    if volume <= 12:
        return 0.7
    return 0.85


def get_memory_state_confidence_profile():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT category, memory_key, value, confidence
        FROM memory_items
        WHERE scope = 'working'
          AND (
              (category = 'state' AND memory_key IN ('journal_stress', 'journal_energy', 'journal_outlook'))
              OR (category = 'emotional_state' AND memory_key = 'journal_tone')
          )
        """
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    profile = {
        "stress": {"value": None, "confidence": 0.0},
        "energy": {"value": None, "confidence": 0.0},
        "outlook": {"value": None, "confidence": 0.0},
        "tone": {"value": None, "confidence": 0.0},
    }
    key_map = {
        ("state", "journal_stress"): "stress",
        ("state", "journal_energy"): "energy",
        ("state", "journal_outlook"): "outlook",
        ("emotional_state", "journal_tone"): "tone",
    }
    for row in rows:
        slot = key_map.get((row.get("category"), row.get("memory_key")))
        if not slot:
            continue
        conf = max(0.0, min(0.99, float(row.get("confidence") or 0.0)))
        if conf >= profile[slot]["confidence"]:
            profile[slot] = {
                "value": (row.get("value") or "").strip().lower(),
                "confidence": conf,
            }
    return profile


def build_tone_vector(journal_analysis, context_snapshot):
    weather = (context_snapshot or {}).get("weather", {})
    sleep = (context_snapshot or {}).get("sleep", {})
    calendar = (context_snapshot or {}).get("calendar", {})

    weather_features = weather.get("features") or {}
    sleep_features = sleep.get("features") or {}
    calendar_features = calendar.get("features") or {}

    stress_level = (journal_analysis or {}).get("stress", "normal").lower()
    friction = (journal_analysis or {}).get("friction", "low").lower()
    energy = (journal_analysis or {}).get("energy", "medium").lower()
    outlook = (journal_analysis or {}).get("outlook", "steady").lower()

    stress_map = {"low": 0.2, "normal": 0.45, "elevated": 0.78}
    energy_map = {"low": 0.25, "medium": 0.5, "high": 0.8}
    outlook_map = {"guarded": 0.75, "steady": 0.45, "upbeat": 0.2}

    stress_signal = stress_map.get(stress_level, 0.45)
    friction_signal = 0.75 if friction == "present" else 0.25
    energy_signal = energy_map.get(energy, 0.5)
    outlook_signal = outlook_map.get(outlook, 0.45)
    memory_profile = get_memory_state_confidence_profile()
    memory_stress = stress_map.get(memory_profile["stress"]["value"], 0.45)
    memory_energy = energy_map.get(memory_profile["energy"]["value"], 0.5)
    memory_outlook = outlook_map.get(memory_profile["outlook"]["value"], 0.45)
    stress_signal = ((1.0 - memory_profile["stress"]["confidence"]) * stress_signal) + (memory_profile["stress"]["confidence"] * memory_stress)
    energy_signal = ((1.0 - memory_profile["energy"]["confidence"]) * energy_signal) + (memory_profile["energy"]["confidence"] * memory_energy)
    outlook_signal = ((1.0 - memory_profile["outlook"]["confidence"]) * outlook_signal) + (memory_profile["outlook"]["confidence"] * memory_outlook)

    busy_score = clamp01((calendar_features.get("busy_score") or 0.0))
    fatigue_score = clamp01((sleep_features.get("fatigue_score") or 0.5))
    market_stress = compute_market_stress_signal()

    weather_label = (weather_features.get("weather_label") or "").lower()
    weather_drag = 0.0
    if any(word in weather_label for word in ["rain", "storm", "overcast", "fog"]):
        weather_drag = 0.2
    if weather_label in {"clear", "mostly_clear"}:
        weather_drag = -0.1

    brevity = clamp01(0.22 + (0.35 * busy_score) + (0.22 * fatigue_score) + (0.18 * stress_signal))
    directness = clamp01(0.3 + (0.28 * busy_score) + (0.2 * stress_signal) + (0.12 * friction_signal))
    seriousness = clamp01(0.22 + (0.2 * market_stress) + (0.2 * stress_signal) + (0.2 * outlook_signal) + max(0.0, weather_drag))
    warmth = clamp01(0.58 + (0.12 * energy_signal) - (0.18 * stress_signal) - (0.16 * fatigue_score) - (0.14 * busy_score) - weather_drag)

    style = "balanced"
    if brevity >= 0.65 and directness >= 0.6:
        style = "concise_direct"
    elif warmth >= 0.62 and seriousness <= 0.45:
        style = "cheery_light"
    elif seriousness >= 0.62:
        style = "restrained_serious"

    return {
        "brevity": round(brevity, 3),
        "directness": round(directness, 3),
        "warmth": round(warmth, 3),
        "seriousness": round(seriousness, 3),
        "style": style,
        "signals": {
            "busy_score": round(busy_score, 3),
            "fatigue_score": round(fatigue_score, 3),
            "market_stress": round(market_stress, 3),
            "stress_signal": round(stress_signal, 3),
            "friction_signal": round(friction_signal, 3),
            "weather_drag": round(weather_drag, 3),
        },
    }


def fallback_journal_response_decision(journal_analysis):
    return {
        "mode": "silent",
        "reply": "",
        "why": "default_silent",
    }


def decide_journal_response(text, journal_analysis, context_snapshot):
    tone_vector = build_tone_vector(journal_analysis, context_snapshot)
    prompt = f"""
Decide whether Jeeves should reply to this journal/gratitude response.

Rules:
- Most of the time choose silent.
- Choose brief_reply only when a short, human response would feel earned.
- Choose deeper_reply only when something feels genuinely pressing or important.
- Never be sycophantic.
- Never flatter for the sake of it.
- Be human, restrained, perceptive.
- Weather, sleep, and calendar context matter when available.
- Use the tone vector as a control matrix.
- If tone style is concise_direct, prefer shorter responses.
- If tone style is restrained_serious, avoid cheerfulness.
- If tone style is cheery_light, warmth can increase slightly but do not be fluffy.
- Return JSON only.

Journal text:
\"\"\"{text}\"\"\"

Journal analysis:
{json.dumps(journal_analysis, ensure_ascii=True)}

Context:
{json.dumps(context_snapshot, ensure_ascii=True)}

Tone vector:
{json.dumps(tone_vector, ensure_ascii=True)}

Return:
{{
  "mode": "silent|brief_reply|deeper_reply",
  "reply": "empty if silent",
  "why": "short reason"
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        mode = (payload.get("mode") or "silent").strip().lower()
        if mode not in {"silent", "brief_reply", "deeper_reply"}:
            return fallback_journal_response_decision(journal_analysis)
        reply = (payload.get("reply") or "").strip()
        if mode == "silent":
            reply = ""
        return {
            "mode": mode,
            "reply": reply,
            "why": (payload.get("why") or "").strip(),
            "tone_vector": tone_vector,
        }
    except:
        out = fallback_journal_response_decision(journal_analysis)
        out["tone_vector"] = tone_vector
        return out


def process_memory_updates(text):
    updates, gratitude_entry = extract_memory_updates(text)
    journal_context = in_gratitude_journal_context()
    journal_analysis = None

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

    journal_entry = gratitude_entry or (text.strip() if journal_context and text.strip() else None)

    if journal_entry:
        add_journal_entry(journal_entry)
        add_memory_observation(
            "journal",
            "latest_journal",
            journal_entry,
            confidence=0.9,
        )
        upsert_memory(
            "working",
            "journal",
            "latest_journal",
            journal_entry,
            source_text=text,
            confidence=0.9,
        )
        upsert_memory(
            "long_term",
            "journal",
            "journal_reflection",
            journal_entry,
            source_text=text,
            confidence=0.8,
        )
        record_memory_embedding(
            "working",
            "journal",
            "latest_journal",
            journal_entry,
        )
        record_memory_embedding(
            "long_term",
            "journal",
            "journal_reflection",
            journal_entry,
        )
        journal_analysis = store_journal_analysis(journal_entry)

    consolidate_memory_trends()
    return {
        "journal_context": journal_context,
        "journal_entry": journal_entry,
        "journal_analysis": journal_analysis,
    }


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

    material = build_recent_memory_material(interaction_limit=60, observation_limit=80, gratitude_limit=10)
    payload = run_ai_deep_memory_consolidation(material)
    store_deep_memory_consolidation(payload)

    apply_memory_decay()


def get_memory_debug_summary():
    context_snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, context_snapshot)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT stage, outcome, reason, event_hash, details_json, created_at
        FROM alert_outcomes
        ORDER BY id DESC
        LIMIT 20
        """
    )
    recent_alert_outcomes = [dict(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT event_hash, related_event_hash, relation_type, details_json, created_at
        FROM event_lineage
        ORDER BY id DESC
        LIMIT 20
        """
    )
    recent_event_lineage = [dict(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT source_trust, COUNT(*) AS count
        FROM memory_items
        GROUP BY source_trust
        """
    )
    memory_source_trust = [dict(row) for row in cur.fetchall()]
    conn.close()
    for row in recent_alert_outcomes:
        try:
            row["details"] = json.loads(row.get("details_json") or "{}")
        except:
            row["details"] = {}
        row.pop("details_json", None)
    for row in recent_event_lineage:
        try:
            row["details"] = json.loads(row.get("details_json") or "{}")
        except:
            row["details"] = {}
        row.pop("details_json", None)

    return {
        "working_memory": get_memory_items("working", limit=20),
        "long_term_memory": get_memory_items("long_term", limit=20),
        "portfolio_holdings": get_portfolio_holdings(limit=12),
        "trusted_portfolio_snapshot": get_latest_trusted_portfolio_snapshot(),
        "recent_journal": get_recent_journal(limit=30),
        "recent_observations": get_recent_observations(limit=12),
        "recent_interactions": get_recent_interaction_events(limit=30),
        "recent_alert_feedback": get_recent_alert_feedback(limit=12),
        "recent_daily_logs": get_recent_daily_logs(limit=14),
        "no_filler_validation": build_no_filler_validation(days=7),
        "memory_source_trust": memory_source_trust,
        "recent_memory_provenance": get_recent_memory_provenance(limit=60),
        "stability_timeline": get_memory_stability_timeline(limit=60),
        "contradiction_view": get_contradiction_view(limit=20),
        "thread_map": get_thread_map(limit=20),
        "reinforce_decay_audit": get_reinforce_decay_audit(limit=10),
        "decay_audit": get_recent_decay_audit(limit=40),
        "recent_alert_outcomes": recent_alert_outcomes,
        "recent_event_lineage": recent_event_lineage,
        "recent_nightly_logs": get_recent_nightly_consolidation_logs(limit=6),
        "context_snapshot": context_snapshot,
        "tone_vector": tone_vector,
        "semantic_memory_count": len(get_memory_embedding_rows(limit=500)),
    }


def build_memory_debug_compact(summary):
    def slim_memory_item(item):
        return {
            "category": item.get("category"),
            "memory_key": item.get("memory_key"),
            "value": item.get("value"),
            "confidence": item.get("confidence"),
            "updated_at": item.get("updated_at"),
        }

    def slim_provenance(item):
        return {
            "scope": item.get("scope"),
            "category": item.get("category"),
            "memory_key": item.get("memory_key"),
            "action": item.get("action"),
            "created_at": item.get("created_at"),
        }

    working = summary.get("working_memory") or []
    long_term = summary.get("long_term_memory") or []
    recent_journal = summary.get("recent_journal") or []
    recent_prov = summary.get("recent_memory_provenance") or []
    recent_decay = summary.get("decay_audit") or []
    nightly_logs = summary.get("recent_nightly_logs") or []

    journal_groups = build_grouped_journal_rows(recent_journal)
    provenance_rollup = build_provenance_rollup(recent_prov)
    diagnostics = detect_memory_debug_issues(summary, journal_groups, provenance_rollup)

    return {
        "counts": {
            "working_memory": len(working),
            "long_term_memory": len(long_term),
            "recent_journal": len(recent_journal),
            "recent_provenance": len(recent_prov),
            "decay_audit": len(recent_decay),
            "semantic_memory_count": summary.get("semantic_memory_count"),
        },
        "top_working_memory": [slim_memory_item(item) for item in working[:8]],
        "top_long_term_memory": [slim_memory_item(item) for item in long_term[:8]],
        "recent_journal": recent_journal[:8],
        "recent_journal_grouped": journal_groups[:10],
        "recent_provenance": [slim_provenance(item) for item in recent_prov[:16]],
        "recent_provenance_rollup": provenance_rollup[:12],
        "recent_decay_audit": recent_decay[:16],
        "recent_nightly_logs": [
            {
                "local_date": item.get("local_date"),
                "depth_label": item.get("depth_label"),
                "created_at": item.get("created_at"),
                "summary_text": item.get("summary_text"),
            }
            for item in nightly_logs[:6]
        ],
        "portfolio": {
            "portfolio_holdings": summary.get("portfolio_holdings"),
            "trusted_portfolio_snapshot": summary.get("trusted_portfolio_snapshot"),
        },
        "tone_vector": summary.get("tone_vector"),
        "diagnostics": diagnostics,
    }


def normalize_debug_text(value):
    text = re.sub(r"\s+", " ", (value or "").strip().lower())
    text = re.sub(r"[^a-z0-9 ?!.,'-]", "", text)
    return text


def build_grouped_journal_rows(rows):
    grouped = {}
    for row in rows or []:
        text = (row.get("entry_text") or "").strip()
        created_at = row.get("created_at") or ""
        normalized = normalize_debug_text(text)
        if not normalized:
            continue
        key = normalized
        item = grouped.get(key)
        if not item:
            grouped[key] = {
                "entry_text": text,
                "count": 1,
                "latest_at": created_at,
                "oldest_at": created_at,
                "is_question": "?" in text,
            }
        else:
            item["count"] += 1
            if created_at > (item.get("latest_at") or ""):
                item["latest_at"] = created_at
            if (not item.get("oldest_at")) or created_at < item["oldest_at"]:
                item["oldest_at"] = created_at
    out = list(grouped.values())
    out.sort(key=lambda item: (-int(item.get("count") or 0), item.get("latest_at") or ""), reverse=False)
    out.sort(key=lambda item: int(item.get("count") or 0), reverse=True)
    return out


def build_provenance_rollup(rows):
    grouped = {}
    for row in rows or []:
        key = (
            row.get("scope") or "",
            row.get("category") or "",
            row.get("memory_key") or "",
            row.get("action") or "",
        )
        item = grouped.get(key)
        created_at = row.get("created_at") or ""
        if not item:
            grouped[key] = {
                "scope": key[0],
                "category": key[1],
                "memory_key": key[2],
                "action": key[3],
                "count": 1,
                "latest_at": created_at,
            }
        else:
            item["count"] += 1
            if created_at > (item.get("latest_at") or ""):
                item["latest_at"] = created_at
    out = list(grouped.values())
    out.sort(key=lambda item: (int(item.get("count") or 0), item.get("latest_at") or ""), reverse=True)
    return out


def detect_memory_debug_issues(summary, journal_groups=None, provenance_rollup=None):
    issues = []
    journal_groups = journal_groups or build_grouped_journal_rows(summary.get("recent_journal") or [])
    provenance_rollup = provenance_rollup or build_provenance_rollup(summary.get("recent_memory_provenance") or [])

    duplicated_journal = [item for item in journal_groups if int(item.get("count") or 0) >= 2]
    if duplicated_journal:
        issues.append({
            "type": "duplicate_journal_entries",
            "severity": "info",
            "detail": f"{len(duplicated_journal)} repeated journal text groups detected.",
        })

    gratitude_rows = []
    for item in (summary.get("working_memory") or []) + (summary.get("long_term_memory") or []):
        if (item.get("category") or "") == "gratitude":
            gratitude_rows.append(item)
    likely_miscategorized = []
    for row in gratitude_rows:
        value = (row.get("value") or "").strip().lower()
        if "?" in value or value in {"hello", "hello?"} or value.startswith(("what ", "show ", "tell ", "check ")):
            likely_miscategorized.append(row)
    if likely_miscategorized:
        issues.append({
            "type": "likely_gratitude_miscategorization",
            "severity": "warning",
            "detail": f"{len(likely_miscategorized)} gratitude memory rows look like commands/questions.",
        })

    repetitive_refresh = [
        item for item in provenance_rollup
        if item.get("action") == "refresh" and int(item.get("count") or 0) >= 3
    ]
    if repetitive_refresh:
        issues.append({
            "type": "high_refresh_repetition",
            "severity": "info",
            "detail": f"{len(repetitive_refresh)} memory keys show repeated refreshes in recent provenance.",
        })

    return issues


def memory_label(category, memory_key):
    labels = {
        ("nightly_summary", "recent_personal_signal"): "Recent Personal Signal Mix",
        ("usage_patterns", "recent_usage"): "How You Have Been Using Jeeves",
        ("memory_threads", "open_loop"): "Open Loop Topic",
        ("nightly_summary", "more_true_1"): "Model-Inferred 'More True' Shift",
        ("nightly_summary", "less_true_1"): "Model-Inferred 'Less True' Shift",
        ("emotional_state", "journal_tone"): "Journal Tone",
        ("state", "journal_energy"): "Journal Energy",
        ("state", "journal_outlook"): "Journal Outlook",
        ("state", "journal_stress"): "Journal Stress",
        ("gratitude", "latest_gratitude"): "Most Recent Journal/Gratitude Entry",
        ("frictions", "journal_friction"): "Recent Friction Signal",
        ("behavior_trends", "nightly_usage_summary"): "Long-Term Usage Pattern",
        ("behavior_trends", "recurrent_focus"): "Recurring Focus Theme",
    }
    return labels.get((category or "", memory_key or ""), f"{(category or 'memory').replace('_', ' ').title()} / {(memory_key or 'entry').replace('_', ' ')}")

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


def get_source_trust_for_source(source, source_label=""):
    src = (source or "").upper()
    label = (source_label or "").lower()
    if src == "FRED":
        return "trusted"
    if src == "NYT":
        return "semi_trusted"
    if src == "CURRENTS":
        if any(domain in label for domain in ["reuters", "apnews", "ft.com", "wsj.com", "bloomberg"]):
            return "semi_trusted"
        return "inferred"
    return "inferred"


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
    source_trust = get_source_trust_for_source(candidate.get("source"), source_label=source_label)
    source_evaluation = candidate.get("source_evaluation") or {
        "source": candidate.get("source"),
        "source_refs": source_refs,
        "source_trust": source_trust,
        "selection_reasons": selection_reasons,
        "score": candidate.get("score"),
        "ai_why": candidate.get("ai_why"),
    }
    dedupe_lineage = candidate.get("dedupe_lineage") or {}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_contexts (
            event_hash, alert_id, category, tier, source, headline, snippet, section,
            published_at, fingerprint, semantic_text, body_text, web_url, score,
            source_label, source_refs_json, selection_reasons_json, source_evaluation_json, dedupe_lineage_json, raw_payload_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
            source_evaluation_json = excluded.source_evaluation_json,
            dedupe_lineage_json = excluded.dedupe_lineage_json,
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
            serialize_json(source_evaluation, fallback={}),
            serialize_json(dedupe_lineage, fallback={}),
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
    execute_write_with_retry(
        """
        INSERT INTO source_poll_state (source_name, last_polled_at, note)
        VALUES (?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_polled_at = CURRENT_TIMESTAMP,
            note = excluded.note
        """,
        (source_name, note or ""),
    )


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
            lineage = None
            if reason == "semantic_duplicate" and semantic_result and semantic_result.get("match"):
                related_hash = semantic_result["match"].get("event_hash")
                lineage = {
                    "relation_type": "semantic_duplicate",
                    "related_event_hash": related_hash,
                    "similarity": semantic_result.get("similarity"),
                }
                if related_hash:
                    log_event_lineage(event_hash, related_hash, "semantic_duplicate", details=lineage)
            candidate = {
                **candidate,
                "dedupe_lineage": lineage,
                "source_evaluation": {
                    "selection_reasons": candidate.get("selection_reasons", []),
                    "source": candidate.get("source"),
                    "source_refs": candidate.get("source_refs", []),
                    "semantic_similarity": semantic_result.get("similarity") if semantic_result else None,
                },
            }
            upsert_event_context(None, event_hash, category, tier, candidate)
        log_alert_outcome(
            stage="decision",
            outcome="blocked",
            reason=reason,
            event_hash=event_hash,
            details={
                "category": category,
                "tier": tier,
                "headline": headline,
                "semantic_similarity": semantic_result.get("similarity") if semantic_result else None,
            },
        )
        return {
            "ok": False,
            "reason": reason,
            "event_hash": event_hash,
            "semantic_similarity": semantic_result["similarity"] if semantic_result else None,
        }

    alert_id = None
    inserted = False
    for attempt in range(8):
        candidate_alert_id = build_alert_id(category, tier)
        try:
            result = execute_write_with_retry(
                """
                INSERT OR IGNORE INTO alert_log (alert_id, category, tier, headline, event_hash, sent_to_user)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (candidate_alert_id, category.upper(), tier, headline, event_hash, sent_to_user),
            )
            if result.get("rowcount", 0) > 0:
                alert_id = candidate_alert_id
                inserted = True
                break
        except sqlite3.IntegrityError:
            pass
        time.sleep(0.03 * (attempt + 1))

    if not inserted or not alert_id:
        alert_id = f"{category.upper()}{tier}-{int(time.time())}-{random.randint(100, 999)}"
        fallback_result = execute_write_with_retry(
            """
            INSERT OR IGNORE INTO alert_log (alert_id, category, tier, headline, event_hash, sent_to_user)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (alert_id, category.upper(), tier, headline, event_hash, sent_to_user),
        )
        if fallback_result.get("rowcount", 0) <= 0:
            raise RuntimeError("failed_to_insert_alert_log")

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

    log_alert_outcome(
        stage="decision",
        outcome="allowed",
        reason=reason,
        event_hash=event_hash,
        details={
            "alert_id": alert_id,
            "category": category,
            "tier": tier,
            "headline": headline,
            "sent_to_user": sent_to_user,
        },
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


def count_sent_tier_alerts_today(tier):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM alert_log
        WHERE DATE(created_at) = DATE('now')
          AND tier = ?
          AND sent_to_user = 1
        """,
        (tier,),
    )
    row = cur.fetchone()
    conn.close()
    return int(row["count"]) if row else 0


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
            WHERE datetime(a.created_at) >= datetime('now', '-24 hours')
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
            WHERE datetime(a.created_at) >= datetime('now', '-24 hours')
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


def build_daily_brief_insight_lines(selected_alerts, portfolio_section=None, watchlist_section=None):
    if not selected_alerts:
        return []

    alerts_text = []
    for item in selected_alerts[:10]:
        alerts_text.append(
            f"{item.get('display_code', '')}: {item.get('headline', '')} [{item.get('source_label') or item.get('source') or ''}]"
        )

    prompt = f"""
You are Jeeves. Create a concise daily brief meta-summary.

Rules:
- Return JSON only with keys: day_analysis, portfolio_effect, insight.
- day_analysis: exactly one sentence.
- portfolio_effect: exactly one sentence, predictive where possible.
- insight: exactly one short sentence, nuanced and non-obvious.
- No fluff, no disclaimers, no numbered lists.

Headlines:
{chr(10).join(alerts_text)}

Portfolio line:
{portfolio_section or "none"}

Watchlist line:
{watchlist_section or "none"}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        day_analysis = " ".join(str(payload.get("day_analysis", "")).split()).strip()
        portfolio_effect = " ".join(str(payload.get("portfolio_effect", "")).split()).strip()
        insight = " ".join(str(payload.get("insight", "")).split()).strip()
        lines = []
        if day_analysis:
            lines.append(f"- Day analysis: {day_analysis}")
        if portfolio_effect:
            lines.append(f"- Portfolio effect: {portfolio_effect}")
        if insight:
            lines.append(f"- Insight: {insight}")
        return lines
    except:
        top = selected_alerts[0]
        fallback = [
            f"- Day analysis: The day was led by {top.get('category', '')}-coded developments, with the strongest signal in \"{top.get('headline', '')}\".",
            "- Portfolio effect: Near-term portfolio sensitivity is highest to macro-rate and energy headline volatility, so position-level dispersion may stay elevated.",
            "- Insight: Cross-source clustering around the same theme is currently a stronger signal than any single headline.",
        ]
        return fallback


def compose_daily_brief(include_debug=False):
    alerts = get_recent_alerts_for_brief(limit=80, include_debug=True)
    section_order = ["P", "E", "G", "L"]
    by_section = {code: [] for code in section_order}
    for item in alerts:
        code = (item.get("category") or "").upper()
        if code in by_section:
            by_section[code].append(item)

    selected_alerts = []
    for code in section_order:
        section_items = by_section[code]
        tier_1 = [item for item in section_items if int(item.get("tier") or 3) == 1]
        tier_2 = [item for item in section_items if int(item.get("tier") or 3) == 2]
        if tier_1 or tier_2:
            selected_alerts.extend(tier_1 + tier_2)
        else:
            tier_3 = [item for item in section_items if int(item.get("tier") or 3) == 3]
            if tier_3:
                selected_alerts.append(tier_3[0])

    selected_alerts = build_brief_display_codes(selected_alerts)

    lines = []
    if selected_alerts:
        lines.append("Daily brief:")
        for code in section_order:
            section_selected = [item for item in selected_alerts if item.get("category") == code]
            if not section_selected:
                continue
            for item in section_selected:
                lines.append(f"{item['display_code']}: {item['headline']}{format_brief_source_suffix(item)}")
        store_brief_event_map(selected_alerts)

    portfolio_section = None
    watchlist_section = None
    if is_trading_day_now():
        portfolio_section = get_portfolio_market_section(max_items=4)
        if portfolio_section:
            lines.append(portfolio_section)

        watchlist_section = get_watchlist_market_section(max_items=4)
        if watchlist_section:
            lines.append(watchlist_section)

    insight_lines = build_daily_brief_insight_lines(
        selected_alerts,
        portfolio_section=portfolio_section,
        watchlist_section=watchlist_section,
    )
    if insight_lines:
        lines.append("")
        lines.extend(insight_lines)

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
3. Why I selected it
4. Certainty score: <0-100> and one short reason

Keep it concise and specific. Use first person ("I"), not third person ("Jeeves").
If a core part cannot be answered from the stored context, explicitly say "I don't know" for that part.
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
        lines.append("Certainty score: 55/100 (fallback mode with partial stored context).")
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

def tokenize_news_text(text):
    tokens = re.findall(r"[a-z0-9]{4,}", (text or "").lower())
    return [tok for tok in tokens if tok not in STORY_STOPWORDS]


def build_recent_news_baseline_context(hours=72, limit=250):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT headline, created_at
        FROM alert_log
        WHERE datetime(created_at) >= datetime('now', ?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (f"-{hours} hours", limit),
    )
    rows = cur.fetchall()
    conn.close()

    counter = Counter()
    docs = 0
    for row in rows:
        headline = row["headline"] if isinstance(row, sqlite3.Row) else row[0]
        tokens = set(tokenize_news_text(headline))
        if not tokens:
            continue
        docs += 1
        for token in tokens:
            counter[token] += 1

    return {
        "token_counts": counter,
        "docs": docs,
    }


def calculate_candidate_novelty(candidate, baseline_context=None):
    baseline_context = baseline_context or {"token_counts": Counter(), "docs": 0}
    token_counts = baseline_context.get("token_counts") or Counter()
    text = " ".join([
        candidate.get("headline", ""),
        candidate.get("snippet", ""),
        candidate.get("section", ""),
    ])
    tokens = set(tokenize_news_text(text))
    if not tokens:
        return {
            "novelty_score": 0.0,
            "unseen_share": 0.0,
            "avg_rarity": 0.0,
        }

    unseen = 0
    rarity_values = []
    for token in tokens:
        freq = token_counts.get(token, 0)
        if freq == 0:
            unseen += 1
        rarity_values.append(1.0 / (1.0 + float(freq)))

    unseen_share = unseen / max(1, len(tokens))
    avg_rarity = sum(rarity_values) / max(1, len(rarity_values))
    novelty_score = (unseen_share * 3.0) + (avg_rarity * 2.0)
    return {
        "novelty_score": round(novelty_score, 3),
        "unseen_share": round(unseen_share, 3),
        "avg_rarity": round(avg_rarity, 3),
    }


def score_candidate(candidate, watchlist, memory_vector_bundle=None, baseline_context=None):
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
    novelty = calculate_candidate_novelty(candidate, baseline_context=baseline_context)
    novelty_score = float(novelty.get("novelty_score", 0.0))
    unseen_share = float(novelty.get("unseen_share", 0.0))

    if interest_similarity >= 0.42:
        score += 5
        reasons.append(f"memory_vector:{interest_similarity:.2f}")
    elif interest_similarity >= 0.34:
        score += 3
        reasons.append(f"memory_vector:{interest_similarity:.2f}")
    elif interest_similarity >= 0.27:
        score += 1
        reasons.append(f"memory_vector:{interest_similarity:.2f}")

    # Portfolio/watchlist matching should only influence portfolio-classified items.
    if candidate["category"] == "P":
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

    # Slightly loosen novelty influence so more genuinely new stories can
    # clear push thresholds without changing the rest of the scoring system.
    if novelty_score >= 3.0:
        score += 4
        reasons.append(f"novelty:{novelty_score:.2f}")
    elif novelty_score >= 2.1:
        score += 3
        reasons.append(f"novelty:{novelty_score:.2f}")
    elif novelty_score >= 1.3:
        score += 2
        reasons.append(f"novelty:{novelty_score:.2f}")

    if candidate["category"] == "G" and unseen_share >= 0.6:
        score += 1
        reasons.append(f"baseline_shift:{unseen_share:.2f}")

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
        if trusted_portfolio_symbols:
            reasons.append("portfolio:no_direct_match")
        else:
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
    # Local should be explicit to Bay Area/California context only.
    if any(term in text for term in [
        "bay area", "san francisco", "berkeley", "oakland", "san jose",
        "sacramento", "california", "earthquake", "wildfire",
    ]):
        return "L"
    # Finance/macro/business should route to E (not default to G).
    if any(term in text for term in [
        "fed", "inflation", "cpi", "rates", "rate cut", "jobs", "employment", "treasury",
        "stock", "stocks", "shares", "earnings", "guidance", "nasdaq", "nyse",
        "dow", "s&p", "market", "gdp", "imf", "bond", "bonds", "yield", "yields",
        "finance", "business",
    ]):
        return "E"
    # Detect common ticker syntax in headlines/snippets (e.g. NASDAQ:CRWD, NYSE:XYZ).
    if re.search(r"\b(?:nasdaq|nyse|arca)\s*:\s*[a-z]{1,6}\b", text):
        return "E"
    if any(term in text for term in [
        "iran", "russia", "china", "taiwan", "ukraine", "israel", "gaza",
        "sanction", "war", "shipping", "strait", "conflict", "ceasefire",
        "nato", "missile", "drone", "military", "embassy", "diplomatic", "geopolitics"
    ]):
        return "G"
    return "G"


def _query_terms(text):
    return set(re.findall(r"[a-z0-9]{3,}", (text or "").lower()))


def is_near_duplicate_query(query_a, query_b, threshold=QUERY_NEAR_DUPLICATE_JACCARD):
    terms_a = _query_terms(query_a)
    terms_b = _query_terms(query_b)
    if not terms_a or not terms_b:
        return False
    overlap = len(terms_a & terms_b)
    denom = max(len(terms_a), len(terms_b))
    if denom <= 0:
        return False
    return (overlap / denom) >= float(threshold)


def normalize_candidate_category(candidate, watchlist=None):
    original = ((candidate.get("category") or "G").upper()[:1]) or "G"
    source = candidate.get("source") or ""
    if source == "FRED":
        return "E", "source_fred"
    if original == "P":
        return "P", None
    normalized = classify_news_category(
        "",
        candidate.get("headline") or "",
        candidate.get("snippet") or "",
        candidate.get("section") or "",
        watchlist=watchlist,
    )
    if normalized not in {"P", "E", "G", "L"}:
        normalized = "G"
    if normalized != original:
        return normalized, "content_classifier"
    return original, None


def _normalize_domain(web_url):
    domain = (urlparse(web_url or "").netloc or "").lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def currents_quality_reject_reason(headline, snippet, section, web_url):
    domain = _normalize_domain(web_url)
    if domain in LOW_QUALITY_CURRENTS_DOMAINS:
        return f"blocked_domain:{domain}"
    title = (headline or "").strip().lower()
    text = " ".join([headline or "", snippet or "", section or ""]).lower()
    if not title or len(title) < 20:
        return "weak_headline"
    # Very light pattern filter for tabloid-style ragebait language.
    if any(token in text for token in ["deranged", "rage against", "whoppers", "nut jobs"]):
        return "tabloid_pattern"
    return None


def get_currents_candidates(query, category_hint=None, watchlist=None):
    if not CURRENTS_API_KEY:
        return [], {"filtered": 0, "filter_reasons": {}, "considered": 0}

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
            return [], {"filtered": 0, "filter_reasons": {}, "considered": 0}

        data = response.json()
        articles = data.get("news", [])[:3]
        candidates = []
        filtered = 0
        filter_reasons = Counter()
        considered = 0

        for article in articles:
            considered += 1
            headline = article.get("title") or ""
            snippet = article.get("description") or ""
            body_text = article.get("description") or ""
            section = ", ".join(article.get("category") or []) if isinstance(article.get("category"), list) else (article.get("category") or "")
            web_url = article.get("url") or ""
            published_at = (article.get("published") or "")[:19]
            if not headline:
                continue
            reject_reason = currents_quality_reject_reason(headline, snippet, section, web_url)
            if reject_reason:
                filtered += 1
                filter_reasons[reject_reason] += 1
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

        return candidates, {
            "filtered": filtered,
            "filter_reasons": dict(filter_reasons),
            "considered": considered,
        }
    except:
        return [], {"filtered": 0, "filter_reasons": {}, "considered": 0}


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


def is_fred_observation_recent(observation_date, max_age_days=5):
    date_text = (observation_date or "")[:10]
    if not date_text:
        return False
    try:
        observation_dt = datetime.strptime(date_text, "%Y-%m-%d")
        age_days = (get_local_now().date() - observation_dt.date()).days
        return age_days <= max_age_days
    except:
        return False


def infer_category_hint_from_text(text):
    t = (text or "").lower()
    if any(term in t for term in [
        "berkeley", "bay area", "san francisco", "oakland", "san jose",
        "sacramento", "california", "local", "city", "county", "state",
        "wildfire", "transit", "housing", "school", "police", "mayor", "earthquake"
    ]):
        return "L"
    if any(term in t for term in ["fed", "inflation", "cpi", "rate", "rates", "jobs", "employment", "treasury"]):
        return "E"
    if any(term in t for term in [
        "iran", "russia", "china", "taiwan", "ukraine", "israel", "gaza",
        "sanction", "war", "shipping", "strait", "conflict", "ceasefire",
        "nato", "missile", "drone", "military", "embassy", "diplomatic", "geopolitics"
    ]):
        return "G"
    return "G"


def normalize_candidate_query_text(text, max_terms=7):
    raw = (text or "").strip().lower()
    if not raw:
        return None

    compact = re.sub(r"\s+", " ", raw)
    compact = compact.replace("\n", " ").replace("\r", " ")
    compact = re.sub(r"[\"'`]", "", compact)

    # If text already looks like a concise query phrase, keep it.
    if len(compact) <= 80 and all(ch not in compact for ch in [";", "{", "}", "[", "]"]):
        word_count = len(re.findall(r"[a-z0-9]{2,}", compact))
        if 2 <= word_count <= 10:
            return compact

    # Otherwise distill freeform text into a short query phrase.
    blocked = {
        "manu", "jeeves", "appears", "seems", "using", "uses", "used", "often",
        "through", "conversation", "communications", "indicated", "entries",
        "normal", "levels", "context", "current", "interests", "recurring",
        "focus", "active", "concerns", "today", "yesterday",
    }
    tokens = re.findall(r"[a-z0-9]{3,}", compact)
    cleaned = []
    for token in tokens:
        if token in STORY_STOPWORDS or token in blocked:
            continue
        if token.isdigit():
            continue
        if token not in cleaned:
            cleaned.append(token)
        if len(cleaned) >= max_terms:
            break

    if len(cleaned) < 2:
        return None
    return " ".join(cleaned)


def is_news_query_signal(query_text, watchlist=None, trusted_portfolio=None):
    t = (query_text or "").lower()
    if not t:
        return False
    watchlist_l = [(item or "").lower() for item in (watchlist or []) if item]
    trusted_l = [(item or "").lower() for item in (trusted_portfolio or []) if item]

    # Reject meta-behavior phrases that are not actual news queries.
    meta_noise_markers = [
        "checks ",
        "uses jeeves",
        "open ended",
        "broader",
        "conversation",
        "recurring focus",
        "active concerns",
    ]
    if any(marker in t for marker in meta_noise_markers):
        return False
    if "watchlist" in t and not any(sym in t for sym in watchlist_l + trusted_l):
        return False

    signal_terms = [
        "fed", "inflation", "cpi", "rate", "rates", "jobs", "employment", "treasury",
        "uranium", "nuclear", "energy", "oil", "gas", "iran", "russia", "sanction",
        "shipping", "strait", "conflict", "war", "ceasefire", "stock",
        "earnings", "guidance", "company", "earthquake", "bay area", "california",
    ]
    if any(term in t for term in signal_terms):
        return True

    for symbol in (watchlist or []) + (trusted_portfolio or []):
        if symbol and symbol.lower() in t:
            return True
    return False


def build_dynamic_news_queries(limit=10, include_local=False):
    query_items = []
    seen = set()
    watchlist = get_watchlist()
    trusted_portfolio = get_trusted_portfolio_symbols(limit=10)
    trusted_top_holdings = get_trusted_portfolio_symbols(limit=10)
    query_debug = {
        "input_total": 0,
        "exact_deduped": 0,
        "near_deduped_non_p": 0,
        "category_limited": 0,
        "p_query_count": 0,
        "p_query_reason": None,
        "category_counts": {},
    }

    def add_query(query, category_hint=None):
        query_debug["input_total"] += 1
        if (category_hint or "").upper() == "P":
            query = re.sub(r"\s+", " ", str(query or "").strip().lower())
            query = query.replace("\n", " ").replace("\r", " ")
            query = re.sub(r"[\"'`]", "", query)
        else:
            query = normalize_candidate_query_text(query)
        if not query or len(query) < 8:
            return
        if not is_news_query_signal(query, watchlist=watchlist, trusted_portfolio=trusted_portfolio):
            return
        resolved_hint = (category_hint or infer_category_hint_from_text(query))
        if not include_local and resolved_hint == "L":
            return
        if query in seen:
            query_debug["exact_deduped"] += 1
            return
        seen.add(query)
        query_items.append((query, resolved_hint))

    for query, category_hint in BASELINE_NEWS_QUERIES:
        add_query(query, category_hint)

    # P-query symbols: use up to top 10 trusted portfolio symbols (ETF-inclusive).
    # If trusted portfolio is not available yet, leave P-symbol expansion empty.
    p_query_symbols = trusted_top_holdings
    if p_query_symbols:
        add_query(" ".join(p_query_symbols) + " stock market performance", "P")
        add_query(" ".join(p_query_symbols) + " company news", "P")
        add_query(" ".join(p_query_symbols) + " earnings guidance risk", "P")
    else:
        query_debug["p_query_reason"] = "no_trusted_symbols"

    relevant = get_relevant_memories("current interests recurring focus active concerns", limit=12)
    for item in relevant.get("working", []) + relevant.get("long_term", []):
        value = (item.get("value") or "").strip()
        if not value:
            continue
        category = item.get("category") or ""
        if category in {"behavior_trends", "priorities", "deep_preferences", "preferences", "goals"}:
            add_query(value, infer_category_hint_from_text(value))

    for event in get_recent_interaction_events(limit=20):
        message_text = (event.get("message_text") or "").strip()
        if len(message_text) < 8:
            continue
        if event.get("intent") in {"news", "watchlist_stats", "ticker_quote", "fred", "daily_brief"}:
            add_query(message_text, infer_category_hint_from_text(message_text))

    selected = []
    category_counts = Counter()
    budget_map = QUERY_CATEGORY_BUDGET_WITH_LOCAL if include_local else QUERY_CATEGORY_BUDGET_NO_LOCAL
    for query, category_hint in query_items:
        category = ((category_hint or "G").upper()[:1]) or "G"
        if category not in {"P", "E", "G", "L"}:
            category = infer_category_hint_from_text(query)
        if not include_local and category == "L":
            query_debug["category_limited"] += 1
            continue
        budget = budget_map.get(category, limit)
        if category_counts[category] >= budget:
            query_debug["category_limited"] += 1
            continue
        if category != "P":
            if any(
                existing_category != "P" and is_near_duplicate_query(query, existing_query)
                for existing_query, existing_category in selected
            ):
                query_debug["near_deduped_non_p"] += 1
                continue
        selected.append((query, category))
        category_counts[category] += 1
        if len(selected) >= limit:
            break

    query_debug["category_counts"] = dict(category_counts)
    query_debug["p_query_count"] = int(category_counts.get("P", 0))
    query_debug["include_local"] = bool(include_local)
    return selected, query_debug


def build_poll_candidates(force_currents=False, include_local=False):
    candidates = []
    watchlist = get_watchlist()
    news_queries, query_debug = build_dynamic_news_queries(limit=10, include_local=include_local)
    currents_due = bool(force_currents) or source_poll_due("CURRENTS", CURRENTS_MIN_INTERVAL_MINUTES)
    source_debug = {
        "nyt_queries": len(news_queries),
        "generated_queries": news_queries,
        "query_debug": query_debug,
        "p_queries_expected": True,
        "p_queries_present": bool(query_debug.get("p_query_count")),
        "p_queries_reason": query_debug.get("p_query_reason"),
        "include_local_queries": bool(include_local),
        "currents_due": currents_due,
        "currents_forced": bool(force_currents),
        "currents_query": None,
        "currents_queries": [],
        "currents_burst_size": CURR_BURST_QUERIES_PER_CYCLE,
        "currents_added_by_query": [],
        "currents_filtered_by_query": [],
        "currents_added": 0,
        "currents_filtered": 0,
        "currents_filter_reasons": {},
    }

    for category, tier, series in POLL_SERIES:
        candidate = get_fred_candidate(category, tier, series)
        if candidate and candidate.get("source") == "FRED":
            if not is_fred_observation_recent(candidate.get("published_at"), max_age_days=5):
                continue
        if candidate:
            candidates.append(candidate)

    for query, category_hint in news_queries:
        candidates.extend(get_nyt_headline_candidates(query, category_hint=category_hint, watchlist=watchlist))

    if source_debug["currents_due"] and news_queries:
        step_bucket = int(datetime.now(LOCAL_TZ).timestamp() // (CURRENTS_MIN_INTERVAL_MINUTES * 60))
        start_index = step_bucket % len(news_queries)
        burst_size = len(news_queries) if force_currents else min(CURR_BURST_QUERIES_PER_CYCLE, len(news_queries))
        selected = []
        for offset in range(burst_size):
            selected.append(news_queries[(start_index + offset) % len(news_queries)])

        total_added = 0
        total_filtered = 0
        total_filter_reasons = Counter()
        for query, category_hint in selected:
            current_candidates, quality_meta = get_currents_candidates(query, category_hint=category_hint, watchlist=watchlist)
            added_count = len(current_candidates)
            source_debug["currents_queries"].append(query)
            source_debug["currents_added_by_query"].append({"query": query, "added": added_count})
            source_debug["currents_filtered_by_query"].append({
                "query": query,
                "filtered": int((quality_meta or {}).get("filtered", 0)),
                "considered": int((quality_meta or {}).get("considered", 0)),
                "reasons": (quality_meta or {}).get("filter_reasons", {}),
            })
            total_added += added_count
            total_filtered += int((quality_meta or {}).get("filtered", 0))
            total_filter_reasons.update((quality_meta or {}).get("filter_reasons", {}))
            candidates.extend(current_candidates)

        source_debug["currents_query"] = selected[0][0] if selected else None
        source_debug["currents_added"] = total_added
        source_debug["currents_filtered"] = total_filtered
        source_debug["currents_filter_reasons"] = dict(total_filter_reasons)
        mark_source_polled("CURRENTS", note=" | ".join(source_debug["currents_queries"]))

    normalized_candidates = []
    category_reassigned_count = 0
    category_reassigned_examples = []
    for candidate in candidates:
        normalized_category, reason = normalize_candidate_category(candidate, watchlist=watchlist)
        if normalized_category != candidate.get("category"):
            category_reassigned_count += 1
            if len(category_reassigned_examples) < 6:
                category_reassigned_examples.append({
                    "headline": candidate.get("headline"),
                    "from": candidate.get("category"),
                    "to": normalized_category,
                    "source": candidate.get("source"),
                    "reason": reason,
                })
        normalized_candidates.append({**candidate, "category": normalized_category})

    source_debug["category_reassigned_count"] = category_reassigned_count
    source_debug["category_reassigned_examples"] = category_reassigned_examples
    return dedupe_candidates(normalized_candidates), source_debug


def prepare_alert_shortlist(candidates, watchlist, limit=AI_ALERT_SHORTLIST_MAX):
    memory_vector_bundle = build_memory_interest_vector()
    baseline_context = build_recent_news_baseline_context(hours=72, limit=250)
    scored = []
    for candidate in candidates:
        scoring = score_candidate(
            candidate,
            watchlist,
            memory_vector_bundle=memory_vector_bundle,
            baseline_context=baseline_context,
        )
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
        raw_category = (item.get("category") or "").upper()[:1] or None
        if raw_category not in {"P", "E", "G", "L"}:
            raw_category = None
        decision_map[candidate_id] = {
            "send": bool(item.get("send")),
            "category": raw_category,
            "tier": int(item.get("tier") or 3),
            "why": (item.get("why") or "").strip(),
        }
    return decision_map


def run_poll_cycle(log_to_alerts=True, send_messages=False, force_currents=False, include_local=False):
    candidates, source_debug = build_poll_candidates(force_currents=force_currents, include_local=include_local)
    watchlist = get_watchlist()
    feedback_profile = get_feedback_profile(limit=20)
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

    tier2_push_cap = max(0, int(feedback_profile.get("tier2_cap", 4)))
    tier2_pushed_today = count_sent_tier_alerts_today(2)

    for candidate in scored_candidates:
        event_hash = build_event_hash(candidate["category"], candidate["headline"])
        shortlist_item = shortlist_lookup.get(event_hash)
        ai_decision = (shortlist_item or {}).get("ai_decision", {})
        effective_category = ai_decision.get("category") or candidate["category"]
        effective_tier = ai_decision.get("tier") if shortlist_item else candidate["assigned_tier"]
        if effective_tier is None:
            effective_tier = candidate["assigned_tier"]
        if candidate.get("source") == "FRED":
            effective_category = "E"

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
            if shortlist_item and ai_decision.get("send") is False:
                blocked_event_hash = build_event_hash(effective_category, candidate["headline"])
                log_alert_outcome(
                    stage="ai_gate",
                    outcome="blocked",
                    reason="ai_filtered_out",
                    event_hash=blocked_event_hash,
                    details={
                        "candidate_id": shortlist_item.get("candidate_id"),
                        "headline": candidate["headline"],
                        "score_reasons": candidate["selection_reasons"],
                        "ai_why": ai_decision.get("why"),
                    },
                )
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
                "ai_why": ai_decision.get("why"),
                "selection_reasons": candidate["selection_reasons"] + ([f"ai:{ai_decision.get('why')}"] if ai_decision.get("why") else []),
                "source_evaluation": {
                    "source": candidate.get("source"),
                    "source_refs": candidate.get("source_refs", [get_candidate_source_label(candidate)]),
                    "selection_reasons": candidate.get("selection_reasons", []),
                    "score": candidate.get("score"),
                    "shortlisted": shortlist_item is not None,
                    "ai_candidate_id": shortlist_item.get("candidate_id") if shortlist_item else None,
                    "ai_send": ai_decision.get("send") if shortlist_item else None,
                    "ai_why": ai_decision.get("why") if shortlist_item else None,
                },
            }
            should_push = False
            if send_messages:
                tier_value = int(effective_tier or 3)
                if tier_value == 1 and ALERT_PUSH_TIER_MAX >= 1:
                    should_push = True
                elif tier_value == 2:
                    # Tier 2 is never pushed as a live alert. It can still be logged
                    # for downstream use (e.g., daily brief ranking/selection).
                    should_push = False
                if should_push and not is_recent_fred_candidate(effective_candidate, max_age_days=5):
                    should_push = False

            alert_result = log_alert(
                effective_category,
                effective_tier,
                candidate["headline"],
                sent_to_user=1 if should_push else 0,
                candidate=effective_candidate,
            )
            result["alert_result"] = alert_result
            if should_push and alert_result.get("ok"):
                alert_message = format_alert_message(effective_candidate, alert_result)
                send_result = send_whatsapp_message(alert_message)
                result["send_result"] = send_result
                log_alert_outcome(
                    stage="delivery",
                    outcome="sent" if send_result.get("ok") else "failed",
                    reason=send_result.get("error"),
                    event_hash=alert_result.get("event_hash"),
                    details={"alert_id": alert_result.get("alert_id"), "headline": candidate["headline"]},
                )
                if send_result.get("ok"):
                    log_outbound_message("alert", alert_message)
                    if int(effective_tier or 3) == 2:
                        tier2_pushed_today += 1
            elif send_messages and alert_result.get("ok"):
                log_alert_outcome(
                    stage="delivery",
                    outcome="skipped",
                    reason="below_push_threshold_or_tier2_daily_cap",
                    event_hash=alert_result.get("event_hash"),
                    details={
                        "alert_id": alert_result.get("alert_id"),
                        "headline": candidate["headline"],
                        "effective_tier": effective_tier,
                        "push_tier_max": ALERT_PUSH_TIER_MAX,
                        "tier2_pushed_today": tier2_pushed_today,
                        "tier2_push_cap": tier2_push_cap,
                    },
                )
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


def build_readable_poll_summary(poll_payload, limit=20):
    source_debug = poll_payload.get("source_debug") or {}
    results = poll_payload.get("results") or []
    lines = [
        "Poll summary:",
        f"- candidates: {poll_payload.get('candidate_count', 0)}",
        f"- shortlist: {poll_payload.get('shortlist_count', 0)}",
        f"- currents_due: {source_debug.get('currents_due')}",
        f"- currents_forced: {source_debug.get('currents_forced')}",
        f"- currents_added: {source_debug.get('currents_added')}",
    ]

    currents_queries = source_debug.get("currents_queries") or []
    if currents_queries:
        lines.append("- currents_queries: " + "; ".join(currents_queries[:3]))

    lines.append("Top results:")
    for item in results[:limit]:
        headline = (item.get("headline") or "").strip()
        category = item.get("category")
        tier = item.get("tier")
        source = item.get("source")
        ai_send = item.get("ai_send")
        alert_result = item.get("alert_result") or {}
        reason = alert_result.get("reason") or ("sent" if alert_result.get("ok") else "unknown")
        lines.append(
            f"- [{category}{tier}] {headline} | source={source} | ai_send={ai_send} | outcome={reason}"
        )
    return "\n".join(lines)


def is_recent_fred_candidate(candidate, max_age_days=5):
    if (candidate or {}).get("source") != "FRED":
        return True
    published = (candidate or {}).get("published_at") or ""
    if not published:
        return False
    try:
        day_part = published[:10]
        published_dt = datetime.strptime(day_part, "%Y-%m-%d")
        age_days = (get_local_now().date() - published_dt.date()).days
        return age_days <= max_age_days
    except:
        return False

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


def is_command_key_request(text):
    return text.strip().lower() == "key"


def interpret_event_reference(text):
    t = text.strip()

    direct_match = re.match(r"^\s*([A-Za-z]\d(?:\.\d+|-\d+)?)\s*$", t, re.IGNORECASE)
    if direct_match:
        return direct_match.group(1).upper()

    followup_patterns = [
        r"^\s*(?:please\s+)?(?:expand(?:\s+on)?|follow up on|follow-up on|tell me more about|more on|what about|talk about)\s+([A-Za-z]\d(?:\.\d+|-\d+)?)\s*(?:please)?\s*[.!?]*\s*$",
        r"^\s*([A-Za-z]\d(?:\.\d+|-\d+)?)\s+(?:please|details|context|more)\s*[.!?]*\s*$",
    ]
    for pattern in followup_patterns:
        match = re.match(pattern, t, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    loose_match = re.search(r"\b([A-Za-z]\d(?:\.\d+|-\d+)?)\b", t, re.IGNORECASE)
    if loose_match:
        return loose_match.group(1).upper()

    return None


def interpret_event_references(text):
    matches = re.findall(r"\b([A-Za-z]\d(?:\.\d+|-\d+)?)\b", text or "", re.IGNORECASE)
    refs = []
    for item in matches:
        ref = (item or "").upper().strip()
        if ref and ref not in refs:
            refs.append(ref)
    return refs


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
    expand_references = interpret_event_references(text)
    expand_reference = interpret_event_reference(text)
    email_request = {"intent": "none", "query_hint": "", "days_window": 7}
    watchlist_request = {"intent": "none", "symbols": []}

    email_gate_patterns = [
        r"\bemail(s)?\b",
        r"\binbox\b",
        r"\bmail\b",
        r"\bmessage(s)?\b",
        r"\bdid\b.+\bsend\b",
        r"\bfrom\s+[a-z0-9._%+\-@ ]+",
        r"\bibkr\b",
        r"\binteractive brokers\b",
    ]
    watchlist_gate_patterns = [
        r"\bwatchlist\b",
        r"\btracking\b",
        r"\btrack\b",
        r"\buntrack\b",
        r"^\s*(?:please\s+)?(?:add|remove)\b",
        r"^\s*(?:can|could|would)\s+you\s+(?:add|remove)\b",
        r"\b(?:add|remove|put|take|drop|include|show|list)\b.+\b(?:stocks|names|tickers|list)\b",
        r"\bnames i(?:'m| am) tracking\b",
        r"\bstocks i(?:'m| am) tracking\b",
    ]

    likely_email = any(re.search(pattern, t, re.IGNORECASE) for pattern in email_gate_patterns)
    likely_watchlist = any(re.search(pattern, t, re.IGNORECASE) for pattern in watchlist_gate_patterns)

    if likely_email:
        email_request = interpret_email_request(text)
    if likely_watchlist:
        watchlist_request = interpret_watchlist_request(text)

    if is_command_key_request(text):
        return ("command_key", None)

    if is_feedback_message(text):
        return ("alert_feedback", t.strip())

    if is_daily_brief_question(text):
        return ("daily_brief", None)

    if is_full_article_request(text):
        return ("full_article_request", None)

    if len(expand_references) >= 2:
        return ("event_expand_multi", expand_references)

    if expand_reference:
        return ("event_expand", expand_reference)

    if is_portfolio_show_question(text):
        return ("portfolio_show", None)

    if watchlist_request["intent"] == "add":
        return ("add", watchlist_request["symbols"])

    if watchlist_request["intent"] == "remove":
        return ("remove", watchlist_request["symbols"])

    if watchlist_request["intent"] == "watchlist_stats":
        return ("watchlist_stats", None)

    if email_request["intent"] != "none":
        return ("email_request", email_request)

    if market_question:
        return (market_question["intent"], market_question["tickers"])

    if watchlist_request["intent"] == "show":
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


def should_attempt_multi_task(text):
    t = (text or "").strip().lower()
    if len(t) < 45:
        return False
    separators = [
        ";",
        "\n",
        " and then ",
        " then ",
        " also ",
        " plus ",
    ]
    return any(separator in t for separator in separators)


def fallback_split_tasks(text):
    raw = text or ""
    parts = re.split(r"\s*(?:;|\n+|\band then\b|\bthen\b|\balso\b|\bplus\b)\s*", raw, flags=re.IGNORECASE)
    tasks = [part.strip(" .") for part in parts if part and part.strip(" .")]
    return tasks[:6]


def split_multi_tasks(text):
    prompt = f"""
Split this message into separate actionable tasks in order.

Rules:
- Return JSON only.
- Preserve order.
- Keep each task concise and self-contained.
- If there is only one real task, return a single-item list.
- Maximum 6 tasks.

Message:
\"\"\"{text}\"\"\"

Return:
{{
  "tasks": ["task 1", "task 2"]
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        tasks = payload.get("tasks") or []
        if not isinstance(tasks, list):
            return fallback_split_tasks(text)
        cleaned = []
        for task in tasks[:6]:
            task_text = re.sub(r"\s+", " ", str(task or "")).strip(" .")
            if task_text:
                cleaned.append(task_text)
        return cleaned or fallback_split_tasks(text)
    except:
        return fallback_split_tasks(text)


def run_generic_reply(user_text):
    try:
        context_snapshot = build_journal_context_snapshot()
        tone_vector = build_tone_vector({}, context_snapshot)
        tone_instruction = (
            f"Tone vector: {json.dumps(tone_vector, ensure_ascii=True)}. "
            "Follow this style matrix with restraint. "
            "Be concise when brevity/directness is high, more warm when warmth is high, "
            "and serious when seriousness is high. Never be sycophantic."
        )
        add_message("user", user_text)
        messages = [
            {"role":"system","content":SYSTEM_PROMPT},
            {"role":"system","content":format_memory_context(user_text)},
            {"role":"system","content":tone_instruction},
        ] + get_recent_messages()
        completion = client.chat.completions.create(model="gpt-4o-mini",messages=messages)
        reply = completion.choices[0].message.content
        add_message("assistant", reply)
        return reply
    except:
        return "Temporary error."


def send_multi_expand_messages(reference_codes):
    sent = 0
    for ref in reference_codes or []:
        detail = expand_brief_event(ref)
        message = f"{ref}\n{detail}"
        send_result = send_whatsapp_message(message)
        if send_result.get("ok"):
            sent += 1
            log_outbound_message("event_expand", message)
    return sent


def build_reply_for_intent(intent, value, msg, memory_result=None):
    if intent == "alert_feedback":
        if not feedback_context_allowed():
            return "Feedback only applies to alerts or daily briefs."
        log_alert_feedback(value, msg)
        upsert_memory(
            "working",
            "alert_feedback",
            "latest_feedback",
            value,
            source_text=msg,
            confidence=0.9,
        )
        return FEEDBACK_RESPONSES[value]

    if intent == "add":
        if not value:
            return "I don't know what to add."
        added = []
        for item in value:
            add_to_watchlist(item)
            added.append(item)
        return f"Added {', '.join(added)}."

    if intent == "portfolio_show":
        holdings = [item["symbol"] for item in get_portfolio_holdings(limit=12)]
        return f"Portfolio: {', '.join(holdings)}" if holdings else "Portfolio is empty."

    if intent == "remove":
        if not value:
            return "I don't know what to remove."
        removed_items = []
        missing_items = []
        for item in value:
            if remove_from_watchlist(item):
                removed_items.append(item)
            else:
                missing_items.append(item)
        if removed_items and missing_items:
            return f"Removed {', '.join(removed_items)}. Not on your watchlist: {', '.join(missing_items)}."
        if removed_items:
            return f"Removed {', '.join(removed_items)}."
        return f"Not on your watchlist: {', '.join(missing_items)}."

    if intent == "show":
        wl = get_watchlist()
        return f"Watchlist: {', '.join(wl)}" if wl else "Watchlist is empty."

    if intent == "watchlist_stats":
        wl = get_watchlist()
        snapshots = get_massive_watchlist_snapshot(wl)
        if snapshots is None:
            snapshots = get_twelvedata_watchlist_snapshot(wl)
        return format_watchlist_stats_reply(wl, snapshots)

    if intent == "ticker_quote":
        snapshots = get_twelvedata_watchlist_snapshot(value)
        return format_ticker_quote_reply(value, snapshots)

    if intent == "daily_brief":
        return compose_daily_brief(include_debug=False)

    if intent == "command_key":
        return build_command_key_reply()

    if intent == "full_article_request":
        if article_request_context_allowed():
            return format_full_article_unavailable_reply()
        return "I don't know which article you mean."

    if intent == "event_expand":
        return expand_brief_event(value)

    if intent == "event_expand_multi":
        sent = send_multi_expand_messages(value or [])
        return None if sent > 0 else "I don't have enough stored context for those items."

    if intent == "fred":
        out = get_fred(value)
        return format_fred_reply(value, out, msg)

    if intent == "news":
        out = get_news(value)
        return out if out else "N/A"

    if intent == "email_request":
        query_hint = value.get("query_hint") or ""
        if value.get("intent") == "latest_ibkr_email":
            query = query_hint or "from:interactivebrokers OR from:interactivebrokers.com OR subject:(Activity Statement)"
            email_data = get_latest_email_message(query=query)
            return format_latest_email_reply(email_data, summary_mode=True)
        if value.get("intent") == "important_recent_email":
            days_window = max(1, min(30, int(value.get("days_window") or 7)))
            query = (query_hint + " " if query_hint else "") + f"newer_than:{days_window}d"
            email_messages = get_recent_email_messages(query=query, max_results=12)
            ranked = rank_important_emails(email_messages or [], msg)
            return format_important_recent_email_reply(email_messages, ranked)
        if value.get("intent") == "email_summary":
            email_data = get_latest_email_message(query=query_hint or "")
            return format_latest_email_reply(email_data, summary_mode=True)
        email_data = get_latest_email_message(query=query_hint or "")
        return format_latest_email_reply(email_data, summary_mode=False)

    if memory_result and memory_result.get("journal_context") and intent == "none":
        context_snapshot = build_journal_context_snapshot()
        decision = decide_journal_response(
            memory_result.get("journal_entry") or msg,
            memory_result.get("journal_analysis") or {},
            context_snapshot,
        )
        add_message("user", msg)
        if decision.get("mode") == "silent":
            return None
        reply = decision.get("reply", "").strip()
        if reply:
            add_message("assistant", reply)
        return reply or None

    if should_send_no_reply(msg):
        return None

    return run_generic_reply(msg)

init_db()
announce_current_deploy_once()

@app.route("/", methods=["GET"])
def home():
    return "Jeeves is running"


@app.route("/privacy", methods=["GET"])
def privacy_policy():
    # Keep statements aligned with actual runtime behavior in this codebase.
    html = """
    <html>
      <head>
        <title>Jeeves Privacy Policy</title>
        <meta charset="utf-8" />
      </head>
      <body style="font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; max-width: 900px; margin: 40px auto; line-height: 1.5;">
        <h1>Jeeves Privacy Policy</h1>
        <p>Last updated: 2026-04-08</p>

        <h2>Scope</h2>
        <p>This policy applies to the Jeeves assistant service at this domain.</p>

        <h2>Data Collected</h2>
        <p>Jeeves stores message and app data in its application database, including conversation messages, alert history, memory items, outbound message logs, and related debugging/audit records used by the service.</p>

        <h2>How Data Is Used</h2>
        <p>Data is used to run assistant features such as replies, alerts, daily brief generation, watchlist/portfolio behavior, and memory/context functions.</p>

        <h2>Third-Party Services</h2>
        <p>Jeeves uses third-party APIs to operate features. Based on current code, this includes Twilio (messaging), OpenAI (language/decision processing), and optional connected data providers such as Gmail, FRED, NYT, Currents, Massive, TwelveData, and Open-Meteo when configured.</p>

        <h2>Sharing</h2>
        <p>Jeeves does not sell personal data. Data necessary to process requests may be sent to the third-party services listed above.</p>

        <h2>Access Controls</h2>
        <p>Inbound message handling is restricted in code to a single configured sender number. Messages from other numbers are treated as unauthorized.</p>

        <h2>Data Handling Boundaries</h2>
        <p>This deployment does not expose a public bulk data export endpoint. Data is stored in the app database and only used by the server logic for assistant operation, with outbound transfers limited to configured providers required to run features.</p>

        <h2>Retention</h2>
        <p>Data is retained in the application database until removed by the operator. This app does not currently enforce a universal automatic deletion timeline for all stored records.</p>

        <h2>Security</h2>
        <p>Hardcoded security controls include sender-number allowlisting for inbound messages, Twilio request-signature verification (when enabled), and internal-key protection for task/debug endpoints (when configured).</p>
      </body>
    </html>
    """
    return app.response_class(response=html, status=200, mimetype="text/html")


@app.route("/terms", methods=["GET"])
def terms_of_service():
    html = """
    <html>
      <head>
        <title>Jeeves Terms of Service</title>
        <meta charset="utf-8" />
      </head>
      <body style="font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; max-width: 900px; margin: 40px auto; line-height: 1.5;">
        <h1>Jeeves Terms of Service</h1>
        <p>Last updated: 2026-04-08</p>

        <h2>Service Description</h2>
        <p>Jeeves is a personal assistant service that processes incoming messages and can send responses, alerts, and summaries based on configured data sources and schedules.</p>

        <h2>Authorized Use</h2>
        <p>This deployment is configured for personal use by the owner. Inbound handling is restricted in code to a single configured phone number.</p>

        <h2>No Professional Advice</h2>
        <p>Outputs are informational only and are not legal, tax, investment, medical, or other professional advice.</p>

        <h2>Availability</h2>
        <p>Service may be unavailable, delayed, or degraded due to platform outages, upstream API issues, network failures, or configuration errors.</p>

        <h2>No Guarantee</h2>
        <p>The service is provided "as is" without a guarantee of uninterrupted operation, complete accuracy, or fitness for a particular purpose.</p>

        <h2>External Services</h2>
        <p>Use of Jeeves depends on third-party providers (for example, Twilio and OpenAI). Their availability and policies may affect service behavior.</p>

        <h2>User Controls</h2>
        <p>The operator may modify configuration, data sources, thresholds, and schedules at any time.</p>
      </body>
    </html>
    """
    return app.response_class(response=html, status=200, mimetype="text/html")


@app.route("/debug/alerts", methods=["GET"])
def debug_alerts():
    denied = require_internal_api_key()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps(get_alert_debug_summary(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/alerts/test", methods=["POST"])
def debug_alert_test():
    denied = require_internal_api_key()
    if denied:
        return denied
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
    denied = require_internal_api_key()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps(run_poll_cycle(log_to_alerts=False), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/poll/run", methods=["POST"])
def debug_poll_run():
    denied = require_internal_api_key()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps(run_poll_cycle(log_to_alerts=True), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/memory", methods=["GET"])
def debug_memory():
    denied = require_internal_api_key()
    if denied:
        return denied
    compact = (request.args.get("compact") or "").strip() == "1"
    summary = get_memory_debug_summary()
    payload = build_memory_debug_compact(summary) if compact else summary
    return app.response_class(
        response=json.dumps(payload, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/memory/view", methods=["GET"])
def debug_memory_view():
    denied = require_internal_api_key()
    if denied:
        return denied
    summary = get_memory_debug_summary()
    compact = build_memory_debug_compact(summary)
    count_block = compact.get("counts") or {}
    top_working = (summary.get("working_memory") or [])[:8]
    top_long_term = (summary.get("long_term_memory") or [])[:8]
    journal_grouped = compact.get("recent_journal_grouped") or []
    prov_rollup = compact.get("recent_provenance_rollup") or []
    diagnostics = compact.get("diagnostics") or []
    base = get_public_base_url()
    raw_link = append_internal_key(f"{base}/debug/memory" if base else "/debug/memory")
    compact_link = append_internal_key(f"{base}/debug/memory?compact=1" if base else "/debug/memory?compact=1")
    diagnostics_html = "".join([
        f"<li><strong>{html.escape(str(item.get('type')))}</strong> ({html.escape(str(item.get('severity')))}): {html.escape(str(item.get('detail')))}</li>"
        for item in diagnostics
    ]) or "<li>No obvious structural issues detected in this snapshot.</li>"
    journal_html = "".join([
        (
            f"<li><span class='badge'>x{int(item.get('count') or 0)}</span> "
            f"<span class='muted'>{html.escape(str(item.get('latest_at')))}</span> - "
            f"{html.escape(str(item.get('entry_text'))[:240])}</li>"
        )
        for item in journal_grouped
    ]) or "<li>No journal rows.</li>"
    prov_html = "".join([
        (
            f"<li><span class='badge'>x{int(item.get('count') or 0)}</span> "
            f"<code>{html.escape(str(item.get('scope')))}::{html.escape(str(item.get('category')))}::{html.escape(str(item.get('memory_key')))}</code> "
            f"action={html.escape(str(item.get('action')))} "
            f"<span class='muted'>{html.escape(str(item.get('latest_at')))}</span></li>"
        )
        for item in prov_rollup
    ]) or "<li>No provenance rows.</li>"
    top_working_html = "".join([
        (
            f"<div class='memory-item'>"
            f"<div class='memory-title'>{html.escape(memory_label(item.get('category'), item.get('memory_key')))}</div>"
            f"<div><strong>What this memory says:</strong> {html.escape(str(item.get('value') or ''))}</div>"
            f"<div class='muted'>confidence={html.escape(str(item.get('confidence')))} | updated={html.escape(str(item.get('updated_at')))}</div>"
            f"<div class='muted'>internal id: <code>{html.escape(str(item.get('category')))}::{html.escape(str(item.get('memory_key')))}</code></div>"
            f"</div>"
        )
        for item in top_working
    ]) or "<div class='memory-item'>No rows.</div>"
    top_long_term_html = "".join([
        (
            f"<div class='memory-item'>"
            f"<div class='memory-title'>{html.escape(memory_label(item.get('category'), item.get('memory_key')))}</div>"
            f"<div><strong>What this memory says:</strong> {html.escape(str(item.get('value') or ''))}</div>"
            f"<div class='muted'>confidence={html.escape(str(item.get('confidence')))} | updated={html.escape(str(item.get('updated_at')))}</div>"
            f"<div class='muted'>internal id: <code>{html.escape(str(item.get('category')))}::{html.escape(str(item.get('memory_key')))}</code></div>"
            f"</div>"
        )
        for item in top_long_term
    ]) or "<div class='memory-item'>No rows.</div>"
    html_page = f"""
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Jeeves Memory Debug View</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 16px; line-height: 1.45; color: #1f2937; background: #f9fafb; }}
          h1, h2 {{ margin: 0 0 8px 0; }}
          .muted {{ color: #6b7280; font-size: 14px; }}
          .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px; margin: 12px 0; background: #ffffff; }}
          .row {{ display: flex; gap: 12px; flex-wrap: wrap; }}
          .pill {{ background: #f3f4f6; border-radius: 999px; padding: 6px 10px; font-size: 13px; }}
          .badge {{ background: #e5e7eb; border-radius: 999px; padding: 2px 8px; font-size: 12px; margin-right: 6px; }}
          ul {{ margin: 8px 0 0 18px; }}
          a {{ color: #2563eb; text-decoration: none; }}
          a:hover {{ text-decoration: underline; }}
          code {{ background: #f3f4f6; padding: 2px 4px; border-radius: 4px; }}
          .memory-item {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; margin: 8px 0; background: #fcfcfd; }}
          .memory-title {{ font-weight: 700; margin-bottom: 4px; }}
        </style>
      </head>
      <body>
        <h1>Jeeves Memory Debug View</h1>
        <div class="muted">Presentation-only summary. Raw data is unchanged.</div>
        <div class="card">
          <div><a href="{html.escape(raw_link)}">Raw JSON</a> | <a href="{html.escape(compact_link)}">Compact JSON</a></div>
        </div>
        <div class="card">
          <h2>Potential Issues</h2>
          <ul>
            {diagnostics_html}
          </ul>
        </div>
        <div class="card">
          <h2>Counts</h2>
          <div class="row">
            <span class="pill">working: {count_block.get("working_memory", 0)}</span>
            <span class="pill">long-term: {count_block.get("long_term_memory", 0)}</span>
            <span class="pill">journal rows: {count_block.get("recent_journal", 0)}</span>
            <span class="pill">provenance rows: {count_block.get("recent_provenance", 0)}</span>
            <span class="pill">decay rows: {count_block.get("decay_audit", 0)}</span>
            <span class="pill">semantic embeddings: {count_block.get("semantic_memory_count", 0)}</span>
          </div>
        </div>
        <div class="card">
          <h2>Top Working Memory (Plain English)</h2>
          <div class="muted">These are active short-term memory items Jeeves is currently carrying.</div>
          <div>
            {top_working_html}
          </div>
        </div>
        <div class="card">
          <h2>Top Long-Term Memory (Plain English)</h2>
          <div class="muted">These are slower-changing patterns and durable summaries.</div>
          <div>
            {top_long_term_html}
          </div>
        </div>
        <div class="card">
          <h2>Recent Journal (Grouped)</h2>
          <ul>
            {journal_html}
          </ul>
        </div>
        <div class="card">
          <h2>Recent Provenance (Rollup)</h2>
          <ul>
            {prov_html}
          </ul>
        </div>
      </body>
    </html>
    """
    return app.response_class(response=html_page, status=200, mimetype="text/html")


@app.route("/debug/gmail", methods=["GET"])
def debug_gmail():
    denied = require_internal_api_key()
    if denied:
        return denied
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


@app.route("/debug/portfolio/truth", methods=["GET"])
def debug_portfolio_truth():
    denied = require_internal_api_key()
    if denied:
        return denied
    payload = build_portfolio_truth_payload()
    return app.response_class(
        response=json.dumps(payload, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/portfolio/truth/view", methods=["GET"])
def debug_portfolio_truth_view():
    denied = require_internal_api_key()
    if denied:
        return denied
    payload = build_portfolio_truth_payload()
    summary = payload.get("summary") or {}
    holdings = payload.get("trusted_holdings") or []

    rows = []
    for item in holdings:
        symbol = html.escape(str(item.get("symbol") or ""))
        mv = item.get("market_value")
        pct = item.get("pct_net_liq")
        shares = item.get("shares")
        is_etf = "yes" if item.get("is_etf") else "no"
        rows.append(
            f"<tr><td>{symbol}</td>"
            f"<td style='text-align:right'>{'' if shares is None else f'{shares:,.6f}'.rstrip('0').rstrip('.')}</td>"
            f"<td style='text-align:right'>{'' if mv is None else f'{mv:,.2f}'}</td>"
            f"<td style='text-align:right'>{'' if pct is None else f'{pct:.2f}%'}</td>"
            f"<td>{is_etf}</td></tr>"
        )

    html_page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Portfolio Truth</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #111; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin-bottom:20px; }}
    .card {{ border:1px solid #e4e4e7; border-radius:10px; padding:12px; background:#fafafa; }}
    .k {{ color:#666; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
    .v {{ font-size:20px; font-weight:600; margin-top:4px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ border-bottom:1px solid #ececec; padding:8px; font-size:13px; }}
    th {{ text-align:left; background:#f5f5f5; position: sticky; top: 0; }}
  </style>
</head>
<body>
  <h2>Jeeves Portfolio Truth</h2>
  <div class="grid">
    <div class="card"><div class="k">Statement Date</div><div class="v">{html.escape(str(summary.get("statement_date") or ""))}</div></div>
    <div class="card"><div class="k">Net Liq Total</div><div class="v">{"" if summary.get("net_liq_total") is None else f"{summary.get('net_liq_total'):,.2f}"}</div></div>
    <div class="card"><div class="k">Cash Total</div><div class="v">{"" if summary.get("cash_total") is None else f"{summary.get('cash_total'):,.2f}"}</div></div>
    <div class="card"><div class="k">Stock Total</div><div class="v">{"" if summary.get("stock_total") is None else f"{summary.get('stock_total'):,.2f}"}</div></div>
    <div class="card"><div class="k">Crypto Total</div><div class="v">{"" if summary.get("crypto_total") is None else f"{summary.get('crypto_total'):,.2f}"}</div></div>
    <div class="card"><div class="k">Holdings</div><div class="v">{len(holdings)}</div></div>
  </div>
  <table>
    <thead>
      <tr><th>Symbol</th><th style="text-align:right">Shares</th><th style="text-align:right">Market Value</th><th style="text-align:right">% Net Liq</th><th>ETF</th></tr>
    </thead>
    <tbody>
      {"".join(rows) or "<tr><td colspan='5'>No trusted holdings loaded yet.</td></tr>"}
    </tbody>
  </table>
</body>
</html>"""
    return app.response_class(response=html_page, status=200, mimetype="text/html")


@app.route("/debug/portfolio/integrity", methods=["GET"])
def debug_portfolio_integrity():
    denied = require_internal_api_key()
    if denied:
        return denied
    report = build_portfolio_integrity_report()
    return app.response_class(
        response=json.dumps(report, indent=2),
        status=200 if report.get("ok") else 400,
        mimetype="application/json",
    )


@app.route("/debug/daily-brief", methods=["GET"])
def debug_daily_brief():
    denied = require_internal_api_key()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps({"brief": compose_daily_brief(include_debug=True)}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/portfolio/sync", methods=["POST"])
def debug_portfolio_sync():
    denied = require_internal_api_key()
    if denied:
        return denied
    days_window = int((request.args.get("days") or request.form.get("days") or "14").strip() or "14")
    result = sync_trusted_portfolio_from_gmail(days_window=days_window)
    return app.response_class(
        response=json.dumps(result, indent=2),
        status=200 if result.get("ok") else 400,
        mimetype="application/json",
    )


@app.route("/debug/memory/consolidate", methods=["POST"])
def debug_memory_consolidate():
    denied = require_internal_api_key()
    if denied:
        return denied
    run_nightly_memory_consolidation()
    return app.response_class(
        response=json.dumps(get_memory_debug_summary(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/context", methods=["GET"])
def debug_context():
    denied = require_internal_api_key()
    if denied:
        return denied
    snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, snapshot)
    return app.response_class(
        response=json.dumps(
            {
                "local_date": get_local_date_string(),
                "context": snapshot,
                "tone_vector": tone_vector,
            },
            indent=2,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/context/calendar", methods=["POST"])
def debug_context_calendar_upsert():
    denied = require_internal_api_key()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    local_date = (payload.get("local_date") or get_local_date_string()).strip()
    upsert_calendar_daily_context(local_date, payload)
    return app.response_class(
        response=json.dumps({"ok": True, "local_date": local_date, "row": get_calendar_daily_context(local_date)}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/context/sleep", methods=["POST"])
def debug_context_sleep_upsert():
    denied = require_internal_api_key()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    local_date = (payload.get("local_date") or get_local_date_string()).strip()
    upsert_sleep_daily_context(local_date, payload)
    return app.response_class(
        response=json.dumps({"ok": True, "local_date": local_date, "row": get_sleep_daily_context(local_date)}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/context/weather/refresh", methods=["POST"])
def debug_context_weather_refresh():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    payload = fetch_open_meteo_daily_context(local_date=local_date)
    row = get_weather_daily_context(local_date=local_date)
    return app.response_class(
        response=json.dumps({"ok": bool(payload or row), "local_date": local_date, "row": row}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/poll", methods=["GET", "POST"])
def task_poll():
    denied = require_internal_api_key()
    if denied:
        return denied
    force_currents = (request.args.get("force_currents") or request.form.get("force_currents") or "").strip() == "1"
    readable = (request.args.get("readable") or request.form.get("readable") or "").strip() == "1"
    payload = run_poll_cycle(log_to_alerts=True, send_messages=True, force_currents=force_currents)
    if readable:
        return app.response_class(
            response=build_readable_poll_summary(payload, limit=25),
            status=200,
            mimetype="text/plain",
        )
    return app.response_class(
        response=json.dumps(payload, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/portfolio-sync", methods=["GET", "POST"])
def task_portfolio_sync():
    denied = require_internal_api_key()
    if denied:
        return denied
    days_window = int((request.args.get("days") or request.form.get("days") or "14").strip() or "14")
    result = sync_trusted_portfolio_from_gmail(days_window=days_window)
    return app.response_class(
        response=json.dumps(result, indent=2),
        status=200 if result.get("ok") else 400,
        mimetype="application/json",
    )


@app.route("/tasks/daily-brief", methods=["GET", "POST"])
def task_daily_brief():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    force = (request.args.get("force") or request.form.get("force") or "").strip() == "1"
    if not force and has_scheduled_task_run("daily_brief", local_date=local_date):
        return app.response_class(
            response=json.dumps({"ok": True, "skipped": "already_sent_today", "local_date": local_date}, indent=2),
            status=200,
            mimetype="application/json",
        )
    # Daily brief path gets one local-inclusive poll pass before composing.
    # This keeps local context available in the brief while excluding it from live polls.
    run_poll_cycle(log_to_alerts=True, send_messages=False, force_currents=False, include_local=True)
    brief = compose_daily_brief(include_debug=False)
    result = send_whatsapp_message(brief)
    if result.get("ok"):
        mark_scheduled_task_run("daily_brief", local_date=local_date)
        log_outbound_message("daily_brief", brief)
    return app.response_class(
        response=json.dumps({"message": brief, "send_result": result}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/gratitude", methods=["GET", "POST"])
def task_gratitude():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    force = (request.args.get("force") or request.form.get("force") or "").strip() == "1"
    if not force and has_scheduled_task_run("gratitude", local_date=local_date):
        return app.response_class(
            response=json.dumps({"ok": True, "skipped": "already_sent_today", "local_date": local_date}, indent=2),
            status=200,
            mimetype="application/json",
        )
    run_nightly_memory_consolidation()
    prompt = "What is one thing you were grateful for today?"
    result = send_whatsapp_message(prompt)
    if result.get("ok"):
        mark_scheduled_task_run("gratitude", local_date=local_date)
        log_outbound_message("gratitude", prompt)
    return app.response_class(
        response=json.dumps({"message": prompt, "send_result": result}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/memory-consolidation", methods=["GET", "POST"])
def task_memory_consolidation():
    denied = require_internal_api_key()
    if denied:
        return denied
    run_nightly_memory_consolidation()
    return app.response_class(
        response=json.dumps({"ok": True, "memory": get_memory_debug_summary()}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/scheduled-check", methods=["GET", "POST"])
def task_scheduled_check():
    denied = require_internal_api_key()
    if denied:
        return denied
    now_local = get_local_now()
    local_date = get_local_date_string(now_local)
    results = {
        "now_local": now_local.isoformat(),
        "daily_brief": {"due": False, "sent": False},
        "gratitude": {"due": False, "sent": False},
    }

    if scheduled_task_due("daily_brief", DAILY_BRIEF_HOUR, DAILY_BRIEF_MINUTE, now_local=now_local):
        results["daily_brief"]["due"] = True
        run_poll_cycle(log_to_alerts=True, send_messages=False, force_currents=False, include_local=True)
        brief = compose_daily_brief(include_debug=False)
        send_result = send_whatsapp_message(brief)
        results["daily_brief"]["send_result"] = send_result
        if send_result.get("ok"):
            mark_scheduled_task_run("daily_brief", local_date=local_date)
            log_outbound_message("daily_brief", brief)
            results["daily_brief"]["sent"] = True

    if scheduled_task_due("gratitude", GRATITUDE_HOUR, GRATITUDE_MINUTE, now_local=now_local):
        results["gratitude"]["due"] = True
        run_nightly_memory_consolidation()
        prompt = "What is one thing you were grateful for today?"
        send_result = send_whatsapp_message(prompt)
        results["gratitude"]["send_result"] = send_result
        if send_result.get("ok"):
            mark_scheduled_task_run("gratitude", local_date=local_date)
            log_outbound_message("gratitude", prompt)
            results["gratitude"]["sent"] = True

    return app.response_class(
        response=json.dumps(results, indent=2),
        status=200,
        mimetype="application/json",
    )

@app.route("/sms", methods=["POST"])
def sms():
    msg = request.form.get("Body","")
    from_number = request.form.get("From","").replace("whatsapp:","")

    resp = MessagingResponse()

    if not verify_twilio_request():
        log_security_event("invalid_twilio_signature", source_number=from_number, message_text=msg)
        warning = build_unauthorized_message_warning(from_number or "unknown", f"[invalid signature]\n{msg}")
        result = send_whatsapp_message(warning)
        if result.get("ok"):
            log_outbound_message("security_warning", warning)
        return ""

    if from_number != MY_NUMBER:
        log_security_event("unauthorized_message", source_number=from_number, message_text=msg)
        warning = build_unauthorized_message_warning(from_number, msg)
        result = send_whatsapp_message(warning)
        if result.get("ok"):
            log_outbound_message("security_warning", warning)
        return ""

    memory_result = process_memory_updates(msg)
    if should_attempt_multi_task(msg):
        tasks = split_multi_tasks(msg)
        if len(tasks) >= 2:
            task_replies = []
            for idx, task_text in enumerate(tasks, start=1):
                task_intent, task_value = route(task_text)
                log_interaction_event(task_intent, task_text)
                upsert_daily_log(task_intent, task_text, memory_result=memory_result)
                task_reply = build_reply_for_intent(task_intent, task_value, task_text, memory_result=memory_result)
                if task_reply is None:
                    continue
                task_replies.append(f"{idx}. {task_reply}")

            if task_replies:
                combined_reply = "\n".join(task_replies)
                resp.message(combined_reply)
                return str(resp)

    intent, value = route(msg)
    log_interaction_event(intent, msg)
    upsert_daily_log(intent, msg, memory_result=memory_result)
    reply = build_reply_for_intent(intent, value, msg, memory_result=memory_result)
    if reply is None:
        return ""
    resp.message(reply)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port)
