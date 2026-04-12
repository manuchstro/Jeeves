from openai import OpenAI
from flask import Flask, request, has_request_context, make_response, redirect
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
import hmac
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
ALERT_AI_GATE_ENABLED = (os.environ.get("ALERT_AI_GATE_ENABLED", "0").strip() == "1")
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
BRAINSTEM_AUTH_MODE = (os.environ.get("BRAINSTEM_AUTH_MODE") or "internal_key").strip().lower()
BRAINSTEM_PASSCODE = (os.environ.get("BRAINSTEM_PASSCODE") or "30410061402113").strip()
BRAINSTEM_SESSION_COOKIE = "brainstem_session"
BRAINSTEM_SESSION_HOURS = max(1, int(os.environ.get("BRAINSTEM_SESSION_HOURS", "24")))
CALENDAR_CONTEXT_URL = os.environ.get("CALENDAR_CONTEXT_URL", "").strip()
CALENDAR_CONTEXT_BEARER = os.environ.get("CALENDAR_CONTEXT_BEARER", "").strip()
SLEEP_CONTEXT_URL = os.environ.get("SLEEP_CONTEXT_URL", "").strip()
SLEEP_CONTEXT_BEARER = os.environ.get("SLEEP_CONTEXT_BEARER", "").strip()
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
# Accept incrementing Jeeves attachment numbers (e.g., Jeeves_#1..., Jeeves_#2..., etc.).
IBKR_TRUSTED_PORTFOLIO_FILENAME_RE = re.compile(r"^Jeeves_#\d+\..+\.html$", re.IGNORECASE)
WHATSAPP_REPLY_CHUNK_MAX = 1200
ALERT_CODE_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
ALERT_CODE_SUFFIX_LEN = 3
ALERT_CODE_SPACE_SIZE = len(ALERT_CODE_ALPHABET) ** ALERT_CODE_SUFFIX_LEN
EVENT_REFERENCE_PATTERN = r"([A-Za-z]\d-[A-Za-z0-9]{3}|[A-Za-z]\d(?:\.\d+|-\d+)?)"
G_QUERY_GENERIC_TERMS = {
    "news", "new", "latest", "today", "update", "updates", "any", "more", "about",
    "please", "tell", "me", "on", "the",
}
G_QUERY_CORE_TERMS = {
    "iran", "israel", "gaza", "ukraine", "russia", "china", "taiwan", "hormuz",
    "sanctions", "shipping", "strait", "ceasefire", "conflict", "war", "oil", "energy",
    "diplomatic", "military", "geopolitics",
}
E_QUERY_CORE_TERMS = {
    "fed", "inflation", "cpi", "jobs", "employment", "treasury", "yield", "yields",
    "rates", "rate", "gdp", "market", "markets", "stocks", "equity", "equities",
    "earnings", "guidance", "macro", "economy", "recession", "growth", "unemployment",
    "oil", "energy", "pricing", "consumer", "business",
}
G_QUERY_BLOCKED_TERMS = {
    "gratitude", "grateful", "family", "friend", "friends", "journal", "emotion",
    "emotional", "tone", "warmth", "sycophancy", "sleep", "fatigue", "calendar",
}
NEWS_MEMORY_BLOCKLIST_CATEGORIES = {
    "behavior_trends", "emotional_state", "state", "relationship_preferences",
    "memory_threads", "nightly_summary", "frictions",
}

CALENDAR_SCHOOL_TERMS = {
    "lecture", "lectures", "class", "classes", "prof", "professor", "office hours",
    "midterm", "exam", "discussion", "seminar", "econ", "math", "french", "persian",
    "course", "homework", "review session",
}
CALENDAR_EXTRACURRICULAR_TERMS = {
    "calsol", "battery", "shell", "electrical", "chassis", "strategy",
    "business and operations", "race ops", "dynamics", "officers", "general meeting",
    "team photo", "exec", "meeting",
}
CALENDAR_PERSONAL_TERMS = {
    "miriam", "family", "doctor", "dentist", "birthday", "gym", "workout",
    "dinner", "lunch", "personal", "friend", "date",
}

# ---------------- FILE NAVIGATION QUICK GUIDE ----------------
# 1) Config/constants
# 2) DB schema + helpers
# 3) Portfolio truth ingestion/validation (Gmail + trusted IBKR HTML)
# 4) Memory + tone + journaling
# 5) Polling/scoring/alerts
# 6) Routing + reply builders
# 7) Flask debug/task endpoints + /sms webhook

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
    CREATE TABLE IF NOT EXISTS calendar_daily_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        events_json TEXT NOT NULL DEFAULT '[]',
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
    CREATE TABLE IF NOT EXISTS sleep_datapoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL,
        sleep_hours REAL NOT NULL,
        source TEXT NOT NULL DEFAULT 'sleep_provider',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS inbox_daily_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL UNIQUE,
        inbox_count INTEGER NOT NULL DEFAULT 0,
        unread_count INTEGER NOT NULL DEFAULT 0,
        busy_score REAL NOT NULL DEFAULT 0.0,
        summary_text TEXT,
        source TEXT NOT NULL DEFAULT 'gmail',
        confidence REAL NOT NULL DEFAULT 0.7,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS latest_location_context (
        singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        accuracy_m REAL,
        label TEXT,
        source TEXT NOT NULL DEFAULT 'ingest',
        captured_at TEXT,
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS brainstem_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS brainstem_action_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor TEXT NOT NULL DEFAULT 'internal',
        action TEXT NOT NULL,
        target TEXT,
        payload_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_feedback_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        action TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        execute_after DATETIME NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        undone_at DATETIME
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS memory_feedback_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        category TEXT NOT NULL,
        memory_key TEXT NOT NULL,
        action TEXT NOT NULL,
        previous_confidence REAL,
        previous_stability TEXT,
        new_confidence REAL,
        new_stability TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        undone_at DATETIME
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tone_signal_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_date TEXT NOT NULL,
        captured_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        brevity REAL,
        directness REAL,
        warmth REAL,
        seriousness REAL,
        busy_score REAL,
        calendar_busy REAL,
        inbox_busy REAL,
        fatigue_score REAL,
        restedness_score REAL,
        market_stress REAL,
        stress_signal REAL,
        friction_signal REAL,
        memory_confidence REAL,
        anti_sycophancy REAL
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


# ---------------- TRUSTED PORTFOLIO INGESTION (GMAIL + IBKR HTML) ----------------

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


def normalize_calendar_events(events):
    def classify_calendar_event_title(title):
        t = (title or "").strip().lower()
        tags = []
        event_type = "other"
        domain = "unknown"

        if any(term in t for term in ("lecture", "class", "seminar")):
            event_type = "lecture"
            tags.append("lecture")
        elif any(term in t for term in ("office hours", "oh")):
            event_type = "office_hours"
            tags.append("office_hours")
        elif any(term in t for term in ("midterm", "exam", "quiz")):
            event_type = "exam"
            tags.append("exam")
        elif any(term in t for term in ("meeting", "review session", "discussion", "ops", "exec")):
            event_type = "meeting"
            tags.append("meeting")

        school_hits = sum(1 for term in CALENDAR_SCHOOL_TERMS if term in t)
        extra_hits = sum(1 for term in CALENDAR_EXTRACURRICULAR_TERMS if term in t)
        personal_hits = sum(1 for term in CALENDAR_PERSONAL_TERMS if term in t)

        if school_hits >= max(extra_hits, personal_hits, 1):
            domain = "school"
        elif extra_hits >= max(school_hits, personal_hits, 1):
            domain = "extracurricular"
        elif personal_hits >= max(school_hits, extra_hits, 1):
            domain = "personal"

        if domain != "unknown":
            tags.append(domain)
        return {"domain": domain, "event_type": event_type, "tags": sorted(set(tags))}

    if not isinstance(events, list):
        return []
    out = []
    for raw in events[:100]:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        start_local = str(raw.get("start_local") or "").strip()
        end_local = str(raw.get("end_local") or "").strip()
        all_day = bool(raw.get("all_day"))
        calendar_name = str(raw.get("calendar_name") or "").strip()
        calendar_id = str(raw.get("calendar_id") or "").strip()
        if not title and not start_local and not end_local:
            continue
        classification = classify_calendar_event_title(title)
        out.append(
            {
                "title": title or "(untitled)",
                "start_local": start_local,
                "end_local": end_local,
                "all_day": all_day,
                "calendar_name": calendar_name,
                "calendar_id": calendar_id,
                "domain": classification["domain"],
                "event_type": classification["event_type"],
                "tags": classification["tags"],
            }
        )
    return out


def upsert_calendar_daily_events(local_date, events):
    normalized = normalize_calendar_events(events)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO calendar_daily_events (
            local_date, events_json, updated_at
        )
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(local_date) DO UPDATE SET
            events_json = excluded.events_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            local_date,
            json.dumps(normalized, ensure_ascii=True),
        ),
    )
    conn.commit()
    conn.close()


def get_calendar_daily_events(local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT events_json
        FROM calendar_daily_events
        WHERE local_date = ?
        LIMIT 1
        """,
        (local_date or get_local_date_string(),),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return []
    try:
        parsed = json.loads(row["events_json"] or "[]")
    except:
        return []
    return normalize_calendar_events(parsed)


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


def add_sleep_datapoint(local_date, sleep_hours, source="sleep_provider"):
    if sleep_hours in (None, ""):
        return False
    try:
        hours = float(sleep_hours)
    except:
        return False
    if hours <= 0:
        return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sleep_datapoints (local_date, sleep_hours, source)
        VALUES (?, ?, ?)
        """,
        (local_date, hours, source or "sleep_provider"),
    )
    conn.commit()
    conn.close()
    return True


def get_sleep_datapoints(days=60, limit=2000):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, local_date, sleep_hours, source, created_at
        FROM sleep_datapoints
        WHERE local_date >= date('now', ?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (f"-{max(1, int(days))} days", max(1, int(limit))),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_recent_sleep_series(lookback_days=14):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, sleep_hours
        FROM sleep_daily_context
        WHERE local_date >= date('now', ?)
          AND sleep_hours IS NOT NULL
        ORDER BY local_date DESC
        """,
        (f"-{max(3, int(lookback_days))} days",),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    cleaned = []
    for row in rows:
        try:
            hours = float(row.get("sleep_hours"))
        except:
            continue
        cleaned.append({"local_date": row.get("local_date"), "sleep_hours": hours})
    return cleaned


def compute_restedness_score(sleep_hours, recent_avg_7d=None):
    try:
        h = float(sleep_hours)
    except:
        return 0.5

    if h <= 0:
        base = 0.05
    elif h < 4.5:
        base = 0.08 + (h / 4.5) * 0.22
    elif h < 6.0:
        base = 0.30 + ((h - 4.5) / 1.5) * 0.25
    elif h < 7.0:
        base = 0.55 + ((h - 6.0) / 1.0) * 0.22
    elif h < 8.0:
        base = 0.77 + ((h - 7.0) / 1.0) * 0.23
    else:
        base = 1.0

    try:
        avg7 = float(recent_avg_7d) if recent_avg_7d is not None else None
    except:
        avg7 = None

    debt_penalty = 0.0
    if avg7 is not None:
        # Sleep debt penalty ramps when recent baseline is low, but remains mild.
        debt = max(0.0, 7.4 - avg7)
        debt_penalty = min(0.22, debt * 0.07)

    return clamp01(base - debt_penalty)


def build_sleep_trend_features(lookback_days=14):
    series = get_recent_sleep_series(lookback_days=lookback_days)
    if not series:
        return None
    hours = [float(item["sleep_hours"]) for item in series]
    avg_3d = sum(hours[:3]) / max(1, min(3, len(hours)))
    avg_7d = sum(hours[:7]) / max(1, min(7, len(hours)))
    latest = hours[0]
    restedness = compute_restedness_score(latest, recent_avg_7d=avg_7d)
    trend_delta = latest - avg_7d
    trend_label = "steady"
    if trend_delta >= 0.9:
        trend_label = "rebounding"
    elif trend_delta <= -0.9:
        trend_label = "declining"
    return {
        "latest_hours": round(latest, 3),
        "avg_3d_hours": round(avg_3d, 3),
        "avg_7d_hours": round(avg_7d, 3),
        "restedness_score": round(restedness, 3),
        "trend_delta_vs_7d": round(trend_delta, 3),
        "trend_label": trend_label,
        "sample_days": len(hours),
    }


def upsert_inbox_daily_context(local_date, payload):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO inbox_daily_context (
            local_date, inbox_count, unread_count, busy_score, summary_text, source, confidence, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(local_date) DO UPDATE SET
            inbox_count = excluded.inbox_count,
            unread_count = excluded.unread_count,
            busy_score = excluded.busy_score,
            summary_text = excluded.summary_text,
            source = excluded.source,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            local_date,
            int(payload.get("inbox_count") or 0),
            int(payload.get("unread_count") or 0),
            float(payload.get("busy_score") or 0.0),
            (payload.get("summary_text") or "").strip(),
            payload.get("source", "gmail"),
            float(payload.get("confidence") or 0.7),
        ),
    )
    conn.commit()
    conn.close()


def get_inbox_daily_context(local_date=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM inbox_daily_context
        WHERE local_date = ?
        LIMIT 1
        """,
        (local_date or get_local_date_string(),),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def compute_relative_inbox_busy_score(inbox_count, unread_count, lookback_days=14):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, inbox_count, unread_count
        FROM inbox_daily_context
        WHERE local_date >= date('now', ?)
        ORDER BY local_date DESC
        """,
        (f"-{max(14, int(lookback_days))} days",),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    samples = [int((item.get("inbox_count") or 0) + (item.get("unread_count") or 0) * 1.5) for item in rows]
    current = int(inbox_count or 0) + int((unread_count or 0) * 1.5)
    if not samples:
        # cold start heuristic
        return clamp01(current / 30.0)

    def percentile_rank(values, target):
        if not values:
            return 0.5
        less_or_equal = sum(1 for value in values if value <= target)
        return less_or_equal / max(1, len(values))

    now_weekday = str(get_local_now().weekday())
    weekday_samples = []
    for item in rows:
        local_date = str(item.get("local_date") or "")
        try:
            weekday = str(datetime.fromisoformat(local_date).weekday())
        except:
            weekday = ""
        if weekday == now_weekday:
            weekday_samples.append(int((item.get("inbox_count") or 0) + (item.get("unread_count") or 0) * 1.5))

    global_pct = percentile_rank(samples, current)
    weekday_pct = percentile_rank(weekday_samples, current) if len(weekday_samples) >= 4 else global_pct
    blended_pct = (0.65 * weekday_pct) + (0.35 * global_pct)

    sorted_samples = sorted(samples)
    median = sorted_samples[len(sorted_samples) // 2] if sorted_samples else max(1, current)
    surge = clamp01((current - median) / max(1, median))
    return clamp01((0.85 * blended_pct) + (0.15 * surge))


def fetch_gmail_inbox_daily_context(local_date=None):
    service, account_email = get_gmail_service()
    if not service:
        return None
    try:
        day = local_date or get_local_date_string()
        # Full inbox fullness (not just last-day slice), using resultSizeEstimate for efficiency.
        inbox_query = "in:inbox"
        unread_query = "in:inbox is:unread"
        inbox_resp = service.users().messages().list(userId="me", maxResults=1, q=inbox_query).execute()
        unread_resp = service.users().messages().list(userId="me", maxResults=1, q=unread_query).execute()
        inbox_count = int(inbox_resp.get("resultSizeEstimate") or len(inbox_resp.get("messages") or []))
        unread_count = int(unread_resp.get("resultSizeEstimate") or len(unread_resp.get("messages") or []))
        busy_score = compute_relative_inbox_busy_score(inbox_count, unread_count, lookback_days=14)
        payload = {
            "inbox_count": inbox_count,
            "unread_count": unread_count,
            "busy_score": busy_score,
            "summary_text": f"inbox {inbox_count}, unread {unread_count}, relative load {round(busy_score, 2)}",
            "source": f"gmail:{account_email or 'me'}",
            "confidence": 0.72,
        }
        upsert_inbox_daily_context(day, payload)
        return payload
    except:
        return None


def fetch_external_context_payload(url, bearer=""):
    if not url:
        return None
    try:
        headers = {}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code != 200:
            return None
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except:
        return None


def refresh_calendar_context_from_provider(local_date=None):
    payload = fetch_external_context_payload(CALENDAR_CONTEXT_URL, CALENDAR_CONTEXT_BEARER)
    if not payload:
        return None
    day = (payload.get("local_date") or local_date or get_local_date_string()).strip()
    normalized = {
        "busy_score": payload.get("busy_score"),
        "event_count": payload.get("event_count"),
        "deep_work_blocks": payload.get("deep_work_blocks"),
        "stress_windows": payload.get("stress_windows"),
        "summary_text": payload.get("summary_text") or "",
        "source": payload.get("source") or "calendar_provider",
        "confidence": payload.get("confidence") or 0.7,
    }
    upsert_calendar_daily_context(day, normalized)
    upsert_calendar_daily_events(day, payload.get("events") or [])
    return normalized


def refresh_sleep_context_from_provider(local_date=None):
    payload = fetch_external_context_payload(SLEEP_CONTEXT_URL, SLEEP_CONTEXT_BEARER)
    if not payload:
        return None
    # Reject explicit provider errors (e.g., unauthorized/no_data) and avoid
    # writing null rows that look "connected" in context snapshots.
    if payload.get("ok") is False:
        return None
    # Accept wrapped payload shape if a provider returns {"ok": true, "payload": {...}}.
    if isinstance(payload.get("payload"), dict):
        payload = payload.get("payload") or {}

    has_sleep_signal = any(
        payload.get(key) not in (None, "")
        for key in ("sleep_hours", "sleep_quality", "steps", "resting_hr")
    )
    if not has_sleep_signal:
        return None

    def normalize_sleep_hours(value):
        if value in (None, ""):
            return None
        try:
            num = float(value)
        except:
            return None
        # Preferred path from your iPhone payload: seconds -> hours -> 10% haircut.
        if num > 1000:
            hours = (num / 3600.0) * 0.9
            return round(max(0.0, hours), 3)
        # Fallback if provider emits minutes.
        if num > 48:
            hours = (num / 60.0) * 0.9
            return round(max(0.0, hours), 3)
        # Fallback if provider emits hours.
        hours = num
        hours = hours * 0.9 if hours > 0 else hours
        return round(max(0.0, hours), 3)

    def norm_01(value, fallback=None):
        if value in (None, ""):
            return fallback
        try:
            num = float(value)
        except:
            return fallback
        if num > 1.0:
            num = num / 100.0
        return clamp01(num)

    day = (payload.get("local_date") or local_date or get_local_date_string()).strip()
    normalized = {
        "sleep_hours": normalize_sleep_hours(payload.get("sleep_hours")),
        "sleep_quality": norm_01(payload.get("sleep_quality")),
        "steps": payload.get("steps"),
        "resting_hr": payload.get("resting_hr"),
        # Fatigue should be derived internally from sleep quantity/trend, not provider fatigue hints.
        "fatigue_score": None,
        "summary_text": payload.get("summary_text") or "",
        "source": payload.get("source") or "sleep_provider",
        "confidence": norm_01(payload.get("confidence"), fallback=0.7),
    }
    upsert_sleep_daily_context(day, normalized)
    add_sleep_datapoint(day, normalized.get("sleep_hours"), source=normalized.get("source") or "sleep_provider")

    # Persist trend as protected long-term behavior memory for Brainstem visibility.
    trend = build_sleep_trend_features(lookback_days=14)
    if trend:
        upsert_memory(
            "long_term",
            "behavior_trends",
            "sleep_recent_trend",
            json.dumps(trend, ensure_ascii=True),
            source_text="sleep_context_refresh",
            confidence=0.82,
        )
        upsert_memory(
            "working",
            "state",
            "restedness_score",
            str(round(float(trend.get("restedness_score") or 0.5), 3)),
            source_text="sleep_context_refresh",
            confidence=0.8,
        )
    return normalized


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


def get_request_passcode():
    try:
        payload = request.get_json(silent=True) if request.method in {"POST", "PUT", "PATCH"} else None
    except Exception:
        payload = None
    return (
        request.headers.get("X-Brainstem-Passcode")
        or request.args.get("passcode")
        or request.form.get("passcode")
        or ((payload or {}).get("passcode") if isinstance(payload, dict) else None)
        or ""
    ).strip()


def get_brainstem_session_signature():
    secret = INTERNAL_API_KEY or "brainstem-fallback-secret"
    message = f"{BRAINSTEM_PASSCODE}|brainstem-session-v1".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def has_brainstem_session():
    cookie = (request.cookies.get(BRAINSTEM_SESSION_COOKIE) or "").strip()
    if not cookie:
        return False
    expected = get_brainstem_session_signature()
    return hmac.compare_digest(cookie, expected)


def issue_brainstem_session_response(response):
    response.set_cookie(
        BRAINSTEM_SESSION_COOKIE,
        get_brainstem_session_signature(),
        max_age=60 * 60 * BRAINSTEM_SESSION_HOURS,
        httponly=True,
        secure=True,
        samesite="Lax",
        path="/",
    )
    return response


def clear_brainstem_session_response(response):
    response.set_cookie(
        BRAINSTEM_SESSION_COOKIE,
        "",
        max_age=0,
        expires=0,
        httponly=True,
        secure=True,
        samesite="Lax",
        path="/",
    )
    return response


def get_brainstem_auth_querystring():
    key = (
        request.headers.get("X-Internal-Key")
        or request.args.get("key")
        or request.form.get("key")
        or ""
    ).strip()
    passcode = get_request_passcode()
    parts = []
    if key:
        parts.append(f"key={quote_plus(key)}")
    elif INTERNAL_API_KEY:
        parts.append(f"key={quote_plus(INTERNAL_API_KEY)}")
    if passcode:
        parts.append(f"passcode={quote_plus(passcode)}")
    return ("?" + "&".join(parts)) if parts else ""


def require_brainstem_access():
    if has_brainstem_session():
        return None
    # Allow either internal API key or dedicated Brainstem passcode.
    denied = require_internal_api_key()
    if denied is None:
        return None
    provided_passcode = get_request_passcode()
    if BRAINSTEM_PASSCODE and provided_passcode and hmac.compare_digest(provided_passcode, BRAINSTEM_PASSCODE):
        return None
    return app.response_class(
        response=json.dumps({"ok": False, "reason": "unauthorized"}, indent=2),
        status=401,
        mimetype="application/json",
    )


def get_brainstem_setting(key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT value_json
        FROM brainstem_settings
        WHERE key = ?
        LIMIT 1
        """,
        (key,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def set_brainstem_setting(key, value):
    execute_write_with_retry(
        """
        INSERT INTO brainstem_settings (key, value_json, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (str(key), serialize_json(value, fallback={})),
    )


def audit_brainstem_action(action, target="", payload=None, actor="internal"):
    execute_write_with_retry(
        """
        INSERT INTO brainstem_action_audit (actor, action, target, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (actor or "internal", action or "unknown", target or "", serialize_json(payload, fallback={})),
    )


def process_due_memory_feedback_queue(now_local=None):
    now_local = now_local or get_local_now()
    now_iso = now_local.isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, scope, category, memory_key, action
        FROM memory_feedback_queue
        WHERE status = 'pending'
          AND datetime(execute_after) <= datetime(?)
        ORDER BY id ASC
        LIMIT 50
        """,
        (now_iso,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    for item in rows:
        if item.get("action") != "forget":
            execute_write_with_retry(
                "UPDATE memory_feedback_queue SET status = 'ignored' WHERE id = ?",
                (item["id"],),
            )
            continue

        scope = item.get("scope")
        category = item.get("category")
        memory_key = item.get("memory_key")

        # Delete memory + embedding safely.
        execute_write_with_retry(
            """
            DELETE FROM memory_items
            WHERE scope = ? AND category = ? AND memory_key = ?
            """,
            (scope, category, memory_key),
        )
        execute_write_with_retry(
            """
            DELETE FROM memory_embeddings
            WHERE scope = ? AND category = ? AND memory_key = ?
            """,
            (scope, category, memory_key),
        )
        add_memory_provenance_event(
            scope,
            category,
            memory_key,
            action="feedback_forget_delete",
            source_text="brainstem_inaccurate_delayed_delete",
            source_trust="direct_user",
            confidence=0.2,
            stability="situational",
        )
        execute_write_with_retry(
            """
            UPDATE memory_feedback_queue
            SET status = 'applied'
            WHERE id = ?
            """,
            (item["id"],),
        )


def queue_memory_feedback_forget(scope, category, memory_key, delay_minutes=60):
    delay_minutes = max(5, int(delay_minutes or 60))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT execute_after
        FROM memory_feedback_queue
        WHERE scope = ?
          AND category = ?
          AND memory_key = ?
          AND action = 'forget'
          AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (scope, category, memory_key),
    )
    row = cur.fetchone()
    conn.close()
    if row and row["execute_after"]:
        return {"execute_after": str(row["execute_after"]), "already_pending": True}

    execute_after = (get_local_now() + timedelta(minutes=delay_minutes)).isoformat()
    execute_write_with_retry(
        """
        INSERT INTO memory_feedback_queue (scope, category, memory_key, action, status, execute_after)
        VALUES (?, ?, ?, 'forget', 'pending', ?)
        """,
        (scope, category, memory_key, execute_after),
    )
    return {"execute_after": execute_after, "already_pending": False}


def undo_memory_feedback_forget(scope, category, memory_key):
    result = execute_write_with_retry(
        """
        UPDATE memory_feedback_queue
        SET status = 'undone', undone_at = CURRENT_TIMESTAMP
        WHERE scope = ?
          AND category = ?
          AND memory_key = ?
          AND action = 'forget'
          AND status = 'pending'
        """,
        (scope, category, memory_key),
    )
    return int(result.get("rowcount") or 0)


def record_memory_feedback_history(scope, category, memory_key, action, previous_confidence=None, previous_stability=None, new_confidence=None, new_stability=None):
    execute_write_with_retry(
        """
        INSERT INTO memory_feedback_history (
            scope, category, memory_key, action,
            previous_confidence, previous_stability, new_confidence, new_stability
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scope, category, memory_key, action,
            previous_confidence, previous_stability, new_confidence, new_stability,
        ),
    )


def undo_last_accurate_feedback(scope, category, memory_key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, previous_confidence, previous_stability, new_confidence, new_stability
        FROM memory_feedback_history
        WHERE scope = ?
          AND category = ?
          AND memory_key = ?
          AND action = 'accurate'
          AND undone_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (scope, category, memory_key),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"ok": False, "reason": "no_accurate_feedback_to_undo"}

    prev_conf = float(row["previous_confidence"]) if row["previous_confidence"] is not None else None
    prev_stability = row["previous_stability"] or "situational"
    cur.execute(
        """
        UPDATE memory_items
        SET confidence = COALESCE(?, confidence),
            stability = COALESCE(?, stability),
            updated_at = CURRENT_TIMESTAMP
        WHERE scope = ? AND category = ? AND memory_key = ?
        """,
        (prev_conf, prev_stability, scope, category, memory_key),
    )
    cur.execute(
        """
        UPDATE memory_feedback_history
        SET undone_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (row["id"],),
    )
    conn.commit()
    conn.close()

    add_memory_provenance_event(
        scope,
        category,
        memory_key,
        action="feedback_accurate_undo",
        source_text="brainstem_feedback_undo",
        source_trust="direct_user",
        confidence=prev_conf if prev_conf is not None else 0.5,
        stability=prev_stability,
    )
    return {
        "ok": True,
        "restored_confidence": prev_conf,
        "restored_stability": prev_stability,
    }


def adjust_memory_confidence(scope, category, memory_key, delta=0.07, source_text="brainstem_feedback"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT value, confidence, stability
        FROM memory_items
        WHERE scope = ? AND category = ? AND memory_key = ?
        LIMIT 1
        """,
        (scope, category, memory_key),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    old_conf = float(row["confidence"] or 0.5)
    old_stability = row["stability"] or "situational"
    new_conf = max(0.01, min(MEMORY_CONFIDENCE_MAX, old_conf + float(delta)))
    stability = old_stability
    if delta > 0 and stability == "situational":
        stability = "emerging"
    cur.execute(
        """
        UPDATE memory_items
        SET confidence = ?, stability = ?, updated_at = CURRENT_TIMESTAMP
        WHERE scope = ? AND category = ? AND memory_key = ?
        """,
        (new_conf, stability, scope, category, memory_key),
    )
    conn.commit()
    conn.close()
    add_memory_provenance_event(
        scope,
        category,
        memory_key,
        action="feedback_confidence_adjust",
        source_text=source_text,
        source_trust="direct_user",
        confidence=new_conf,
        stability=stability,
        old_value=row["value"],
        new_value=row["value"],
    )
    return {
        "old_confidence": old_conf,
        "new_confidence": new_conf,
        "old_stability": old_stability,
        "new_stability": stability,
    }


def record_tone_snapshot(context_snapshot, tone_vector):
    context_snapshot = context_snapshot or {}
    tone_vector = tone_vector or {}
    signals = tone_vector.get("signals") or {}
    execute_write_with_retry(
        """
        INSERT INTO tone_signal_snapshots (
            local_date, brevity, directness, warmth, seriousness,
            busy_score, calendar_busy, inbox_busy, fatigue_score, restedness_score,
            market_stress, stress_signal, friction_signal, memory_confidence, anti_sycophancy
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            get_local_date_string(),
            tone_vector.get("brevity"),
            tone_vector.get("directness"),
            tone_vector.get("warmth"),
            tone_vector.get("seriousness"),
            signals.get("busy_score"),
            signals.get("calendar_busy"),
            signals.get("inbox_busy"),
            signals.get("fatigue_score"),
            signals.get("restedness_score"),
            signals.get("market_stress"),
            signals.get("stress_signal"),
            signals.get("friction_signal"),
            signals.get("memory_confidence"),
            signals.get("anti_sycophancy"),
        ),
    )


def get_operator_links_map():
    base = get_public_base_url()

    def link(path):
        target = f"{base}{path}" if base else path
        return append_internal_key(target)

    return {
        "core": {
            "brainstem": link("/brainstem"),
            "poll_run": link("/debug/poll/run"),
            "poll_run_readable": link("/tasks/poll?readable=1"),
            "poll_preview": link("/debug/poll/preview"),
            "daily_brief_force": link("/tasks/daily-brief?force=1"),
            "scheduled_check": link("/tasks/scheduled-check"),
            "guards": link("/debug/guards"),
        },
        "portfolio": {
            "sync_trusted": link("/tasks/portfolio-sync?days=14"),
            "truth_json": link("/debug/portfolio/truth"),
            "truth_view": link("/debug/portfolio/truth/view"),
            "integrity": link("/debug/portfolio/integrity"),
        },
        "memory_context": {
            "memory_raw": link("/debug/memory"),
            "memory_compact": link("/debug/memory?compact=1"),
            "memory_view": link("/debug/memory/view"),
            "context_debug": link("/debug/context"),
            "context_refresh": link("/tasks/context-refresh"),
            "inbox_refresh": link("/debug/context/inbox/refresh"),
            "calendar_refresh": link("/debug/context/calendar/refresh"),
            "sleep_refresh": link("/debug/context/sleep/refresh"),
            "sleep_history": link("/debug/context/sleep/history"),
            "alerts_debug": link("/debug/alerts"),
            "daily_brief_debug": link("/debug/daily-brief"),
            "gmail_debug": link("/debug/gmail"),
        },
    }


# ---------------- OPERATOR LINK HUB (`key` RESPONSE) ----------------

def build_command_key_reply():
    base = get_public_base_url()
    privacy_link = f"{base}/privacy" if base else "/privacy"
    terms_link = f"{base}/terms" if base else "/terms"
    links = get_operator_links_map()
    core = links.get("core") or {}
    portfolio = links.get("portfolio") or {}
    memory_context = links.get("memory_context") or {}

    return "\n".join([
        COMMAND_KEY_REPLY,
        "",
        f"privacy: {privacy_link}",
        f"terms: {terms_link}",
        "",
        "Core task links:",
        f"- brainstem: {core.get('brainstem', '')}",
        f"- poll run: {core.get('poll_run', '')}",
        f"- poll run (readable): {core.get('poll_run_readable', '')}",
        f"- poll preview: {core.get('poll_preview', '')}",
        f"- daily brief (force): {core.get('daily_brief_force', '')}",
        f"- scheduled check: {core.get('scheduled_check', '')}",
        f"- guards: {core.get('guards', '')}",
        "",
        "Portfolio truth links:",
        f"- sync trusted portfolio: {portfolio.get('sync_trusted', '')}",
        f"- portfolio truth (json): {portfolio.get('truth_json', '')}",
        f"- portfolio truth (readable): {portfolio.get('truth_view', '')}",
        f"- portfolio integrity: {portfolio.get('integrity', '')}",
        "",
        "Memory and context links:",
        f"memory debug raw: {memory_context.get('memory_raw', '')}",
        f"memory debug compact: {memory_context.get('memory_compact', '')}",
        f"memory debug view: {memory_context.get('memory_view', '')}",
        f"context debug: {memory_context.get('context_debug', '')}",
        f"context refresh (all): {memory_context.get('context_refresh', '')}",
        f"inbox refresh: {memory_context.get('inbox_refresh', '')}",
        f"calendar refresh: {memory_context.get('calendar_refresh', '')}",
        f"sleep refresh: {memory_context.get('sleep_refresh', '')}",
        f"sleep history: {memory_context.get('sleep_history', '')}",
        f"alerts debug: {memory_context.get('alerts_debug', '')}",
        f"daily brief debug: {memory_context.get('daily_brief_debug', '')}",
        f"gmail debug: {memory_context.get('gmail_debug', '')}",
    ])


def get_tone_signal_snapshots(range_key="24h"):
    range_map = {
        "24h": "-24 hours",
        "7d": "-7 days",
        "30d": "-30 days",
        "max": "-3650 days",
    }
    window = range_map.get((range_key or "24h").lower(), "-24 hours")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT local_date, captured_at, brevity, directness, warmth, seriousness,
               busy_score, calendar_busy, inbox_busy, fatigue_score, restedness_score,
               market_stress, stress_signal, friction_signal, memory_confidence, anti_sycophancy
        FROM tone_signal_snapshots
        WHERE datetime(captured_at) >= datetime('now', ?)
        ORDER BY datetime(captured_at) ASC
        LIMIT 5000
        """,
        (window,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_pending_memory_feedback_entries(limit=200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, scope, category, memory_key, action, status, execute_after, created_at
        FROM memory_feedback_queue
        WHERE status = 'pending'
        ORDER BY datetime(execute_after) ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_brainstem_overview_payload():
    now = get_local_now()
    snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, snapshot)
    record_tone_snapshot(snapshot, tone_vector)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM memory_items")
    row = cur.fetchone()
    memory_count = int(row["c"] if row and row["c"] is not None else 0)
    cur.execute("SELECT COUNT(*) AS c FROM alert_log WHERE datetime(created_at) >= datetime('now','-24 hours')")
    row = cur.fetchone()
    alerts_24h = int(row["c"] if row and row["c"] is not None else 0)
    cur.execute("SELECT alert_id, category, tier, headline, created_at FROM alert_log ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    last_alert = dict(row) if row else None
    conn.close()
    openai_configured = bool(os.environ.get("OPENAI_API_KEY"))
    twilio_configured = bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN)
    gmail_connected = bool(get_gmail_service()[0])
    calendar_connected = bool(CALENDAR_CONTEXT_URL)
    reasons = []
    if not openai_configured:
        reasons.append("openai missing")
    if not twilio_configured:
        reasons.append("twilio missing")
    if not gmail_connected:
        reasons.append("gmail disconnected")
    if not calendar_connected:
        reasons.append("calendar provider missing")
    healthy = len(reasons) == 0
    return {
        "now_local": now.isoformat(),
        "deploy": {
            "commit_sha": RAILWAY_GIT_COMMIT_SHA,
            "deployment_id": RAILWAY_DEPLOYMENT_ID,
        },
        "health": {
            "healthy": healthy,
            "reasons": reasons,
            "openai_configured": openai_configured,
            "twilio_configured": twilio_configured,
            "gmail_connected": gmail_connected,
            "calendar_provider": calendar_connected,
        },
        "journal_guard": get_journal_guard_status(),
        "feedback_context_allowed": feedback_context_allowed(),
        "article_request_context_allowed": article_request_context_allowed(),
        "counts": {
            "memory_items": memory_count,
            "alerts_24h": alerts_24h,
            "pending_memory_feedback": len(get_pending_memory_feedback_entries(limit=500)),
        },
        "last_alert": last_alert,
        "context_snapshot": snapshot,
        "tone_vector": tone_vector,
    }


def get_cost_usage_snapshot():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM outbound_messages WHERE datetime(created_at) >= datetime('now','-30 days')")
    row = cur.fetchone()
    outbound_30d = int(row["c"] if row and row["c"] is not None else 0)
    cur.execute("SELECT COUNT(*) AS c FROM interaction_events WHERE datetime(created_at) >= datetime('now','-30 days')")
    row = cur.fetchone()
    interactions_30d = int(row["c"] if row and row["c"] is not None else 0)
    cur.execute("SELECT COUNT(*) AS c FROM alert_log WHERE datetime(created_at) >= datetime('now','-30 days')")
    row = cur.fetchone()
    alerts_30d = int(row["c"] if row and row["c"] is not None else 0)
    conn.close()
    return {
        "disclaimer": "Cost projection is approximate and may be inaccurate.",
        "providers": {
            "openai": {"configured": bool(os.environ.get("OPENAI_API_KEY")), "estimated_30d_calls_proxy": interactions_30d},
            "twilio": {"configured": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN), "outbound_messages_30d": outbound_30d},
            "massive": {"configured": bool(MASSIVE_API_KEY)},
            "twelvedata": {"configured": bool(TWELVEDATA_API_KEY)},
            "nyt": {"configured": bool(NYT_API_KEY)},
            "fred": {"configured": bool(FRED_API_KEY)},
            "currents": {"configured": bool(CURRENTS_API_KEY)},
        },
        "rollups": {
            "interactions_30d": interactions_30d,
            "alerts_30d": alerts_30d,
            "outbound_messages_30d": outbound_30d,
        },
    }


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


def get_journal_guard_status():
    latest = get_latest_outbound_message("gratitude", hours=JOURNAL_RESPONSE_WINDOW_HOURS)
    if not latest:
        return {
            "active": False,
            "reason": "no_recent_gratitude_prompt",
            "window_hours": JOURNAL_RESPONSE_WINDOW_HOURS,
        }
    interactions_since = count_interactions_since(latest.get("created_at"))
    return {
        "active": interactions_since == 0,
        "window_hours": JOURNAL_RESPONSE_WINDOW_HOURS,
        "gratitude_prompt_created_at": latest.get("created_at"),
        "interactions_since_prompt": interactions_since,
        "first_inbound_pending": interactions_since == 0,
    }


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


# ---------------- OUTBOUND REPLY FORMATTING ----------------

def split_reply_chunks(text, max_chars=WHATSAPP_REPLY_CHUNK_MAX):
    body = (text or "").strip()
    if not body:
        return []
    if len(body) <= max_chars:
        return [body]

    lines = body.split("\n")
    chunks = []
    current = []
    current_len = 0

    def flush():
        nonlocal current, current_len
        if current:
            chunks.append("\n".join(current).strip())
            current = []
            current_len = 0

    for raw_line in lines:
        line = raw_line.rstrip()
        line_len = len(line) + (1 if current else 0)

        if len(line) > max_chars:
            flush()
            start = 0
            while start < len(line):
                chunks.append(line[start:start + max_chars].strip())
                start += max_chars
            continue

        if current_len + line_len > max_chars:
            flush()

        if line or current:
            current.append(line)
            current_len += line_len

    flush()
    return [chunk for chunk in chunks if chunk]


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

    calendar_ctx = get_calendar_context_snapshot()
    # Keep a larger window in prompt context so Jeeves can answer
    # near-history and next-week calendar questions.
    calendar_events = (calendar_ctx.get("events") or [])[:40]
    if calendar_events:
        lines.append("Calendar events:")
        for event in calendar_events:
            title = (event.get("title") or "(untitled)").strip()
            start_local = (event.get("start_local") or "").strip()
            end_local = (event.get("end_local") or "").strip()
            all_day = bool(event.get("all_day"))
            if all_day:
                lines.append(f"- all-day: {title}")
            elif start_local or end_local:
                lines.append(f"- {start_local} to {end_local}: {title}")
            else:
                lines.append(f"- {title}")

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


def get_sleep_context_snapshot():
    stored = get_sleep_daily_context(local_date=get_local_date_string())
    if not stored:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    has_sleep_signal = any(
        stored.get(key) not in (None, "")
        for key in ("sleep_hours", "sleep_quality", "steps", "resting_hr")
    )
    if not has_sleep_signal:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    trend = build_sleep_trend_features(lookback_days=14)
    sleep_hours = stored.get("sleep_hours")
    restedness = compute_restedness_score(
        sleep_hours,
        recent_avg_7d=(trend or {}).get("avg_7d_hours"),
    )
    fatigue = clamp01(1.0 - restedness)
    return {
        "available": True,
        "status": "connected",
        "summary": (
            stored.get("summary_text")
            or f"sleep {sleep_hours}h, restedness {round(restedness, 2)}"
        ),
        "features": {
            "sleep_hours": sleep_hours,
            "sleep_quality": stored.get("sleep_quality"),
            "steps": stored.get("steps"),
            "resting_hr": stored.get("resting_hr"),
            "restedness_score": round(restedness, 3),
            "fatigue_score": round(fatigue, 3),
            "recent_avg_3d_hours": (trend or {}).get("avg_3d_hours"),
            "recent_avg_7d_hours": (trend or {}).get("avg_7d_hours"),
            "sleep_trend_label": (trend or {}).get("trend_label"),
            "confidence": stored.get("confidence"),
        },
    }


def get_calendar_context_snapshot():
    stored = get_calendar_daily_context(local_date=get_local_date_string())
    events = get_calendar_daily_events(local_date=get_local_date_string())
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
        "events": events[:100],
    }


def get_inbox_context_snapshot():
    local_date = get_local_date_string()
    stored = get_inbox_daily_context(local_date=local_date)
    if not stored:
        fetched = fetch_gmail_inbox_daily_context(local_date=local_date)
        if fetched:
            stored = get_inbox_daily_context(local_date=local_date)
    if not stored:
        return {
            "available": False,
            "status": "not_connected",
            "summary": "",
        }
    return {
        "available": True,
        "status": "connected",
        "summary": stored.get("summary_text") or f"inbox {stored.get('inbox_count')}, unread {stored.get('unread_count')}",
        "features": {
            "inbox_count": stored.get("inbox_count"),
            "unread_count": stored.get("unread_count"),
            "busy_score": stored.get("busy_score"),
            "confidence": stored.get("confidence"),
        },
    }


def build_journal_context_snapshot():
    return {
        "sleep": get_sleep_context_snapshot(),
        "calendar": get_calendar_context_snapshot(),
        "inbox": get_inbox_context_snapshot(),
    }


def build_tone_context_snapshot(context_snapshot):
    ctx = context_snapshot or {}
    return {
        "sleep": ctx.get("sleep", {}),
        "calendar": ctx.get("calendar", {}),
        "inbox": ctx.get("inbox", {}),
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
    sleep = (context_snapshot or {}).get("sleep", {})
    calendar = (context_snapshot or {}).get("calendar", {})
    inbox = (context_snapshot or {}).get("inbox", {})

    sleep_features = sleep.get("features") or {}
    calendar_features = calendar.get("features") or {}
    inbox_features = inbox.get("features") or {}

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
    memory_confidence = (
        float(memory_profile["stress"]["confidence"])
        + float(memory_profile["energy"]["confidence"])
        + float(memory_profile["outlook"]["confidence"])
        + float(memory_profile["tone"]["confidence"])
    ) / 4.0
    memory_stress = stress_map.get(memory_profile["stress"]["value"], 0.45)
    memory_energy = energy_map.get(memory_profile["energy"]["value"], 0.5)
    memory_outlook = outlook_map.get(memory_profile["outlook"]["value"], 0.45)
    stress_signal = ((1.0 - memory_profile["stress"]["confidence"]) * stress_signal) + (memory_profile["stress"]["confidence"] * memory_stress)
    energy_signal = ((1.0 - memory_profile["energy"]["confidence"]) * energy_signal) + (memory_profile["energy"]["confidence"] * memory_energy)
    outlook_signal = ((1.0 - memory_profile["outlook"]["confidence"]) * outlook_signal) + (memory_profile["outlook"]["confidence"] * memory_outlook)

    calendar_busy = clamp01((calendar_features.get("busy_score") or 0.0))
    inbox_busy = clamp01((inbox_features.get("busy_score") or 0.0))
    busy_score = clamp01(max(calendar_busy, inbox_busy))
    sleep_hours_raw = sleep_features.get("sleep_hours")
    sleep_avg_7d_raw = sleep_features.get("recent_avg_7d_hours")
    try:
        sleep_hours = float(sleep_hours_raw) if sleep_hours_raw not in (None, "") else None
    except:
        sleep_hours = None
    try:
        sleep_avg_7d = float(sleep_avg_7d_raw) if sleep_avg_7d_raw not in (None, "") else None
    except:
        sleep_avg_7d = None
    if sleep_hours is None:
        restedness_score = 0.5
    else:
        restedness_score = compute_restedness_score(sleep_hours, recent_avg_7d=sleep_avg_7d)
    fatigue_score = clamp01(1.0 - restedness_score)
    market_stress = compute_market_stress_signal()

    brevity = clamp01(0.22 + (0.35 * busy_score) + (0.22 * fatigue_score) + (0.18 * stress_signal))
    directness = clamp01(0.3 + (0.28 * busy_score) + (0.2 * stress_signal) + (0.12 * friction_signal))
    seriousness = clamp01(0.22 + (0.2 * market_stress) + (0.2 * stress_signal) + (0.2 * outlook_signal))
    warmth = clamp01(0.58 + (0.12 * energy_signal) - (0.18 * stress_signal) - (0.16 * fatigue_score) - (0.14 * busy_score))
    # High confidence in memory-state signals should influence tone behavior,
    # not only debug visibility. This keeps style stable and deliberate.
    directness = clamp01(directness + (0.08 * memory_confidence))
    seriousness = clamp01(seriousness + (0.08 * memory_confidence))
    warmth = clamp01(warmth - (0.07 * memory_confidence * friction_signal))
    anti_sycophancy = clamp01(
        0.42
        + (0.28 * directness)
        + (0.24 * seriousness)
        + (0.18 * friction_signal)
        + (0.14 * market_stress)
        - (0.20 * warmth)
    )
    if anti_sycophancy >= 0.72:
        warmth = clamp01(warmth - 0.06)

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
            "calendar_busy": round(calendar_busy, 3),
            "inbox_busy": round(inbox_busy, 3),
            "fatigue_score": round(fatigue_score, 3),
            "restedness_score": round(restedness_score, 3),
            "market_stress": round(market_stress, 3),
            "stress_signal": round(stress_signal, 3),
            "friction_signal": round(friction_signal, 3),
            "memory_confidence": round(memory_confidence, 3),
            "anti_sycophancy": round(anti_sycophancy, 3),
        },
    }


def build_tone_guardrail_text(tone_vector):
    signals = (tone_vector or {}).get("signals") or {}
    anti = float(signals.get("anti_sycophancy") or 0.0)
    if anti >= 0.75:
        candor = "very_high"
    elif anti >= 0.6:
        candor = "high"
    elif anti >= 0.45:
        candor = "medium"
    else:
        candor = "balanced"
    return (
        f"Candor guardrail: {candor} (anti_sycophancy={anti:.2f}). "
        "Do not flatter or validate without evidence. "
        "If user assumptions look weak, politely challenge them and explain why."
    )


def fallback_journal_response_decision(journal_analysis):
    return {
        "mode": "silent",
        "reply": "",
        "why": "default_silent",
    }


def should_use_model_for_journal_decision(text, journal_analysis, tone_vector):
    t = (text or "").strip().lower()
    if not t:
        return False
    # Explicit uncertainty/questions deserve nuanced handling.
    if "?" in t:
        return True

    stress = (journal_analysis or {}).get("stress", "").lower()
    friction = (journal_analysis or {}).get("friction", "").lower()
    notable_shift = bool((journal_analysis or {}).get("notable_shift"))
    durable_signal = bool((journal_analysis or {}).get("durable_signal"))
    signals = (tone_vector or {}).get("signals") or {}
    anti = float(signals.get("anti_sycophancy") or 0.0)

    if stress in {"elevated", "high"}:
        return True
    if friction == "present":
        return True
    if notable_shift or durable_signal:
        return True
    if anti >= 0.78:
        return True

    trigger_terms = {
        "anxious", "overwhelmed", "stressed", "upset", "angry", "sad",
        "confused", "panic", "worried", "fear", "depressed", "lonely",
        "grateful", "thankful", "proud", "excited", "relieved",
    }
    tokens = set(re.findall(r"[a-z']{3,}", t))
    if tokens & trigger_terms:
        return True

    return False


def decide_journal_response(text, journal_analysis, context_snapshot):
    tone_context = build_tone_context_snapshot(context_snapshot)
    tone_vector = build_tone_vector(journal_analysis, tone_context)
    if not should_use_model_for_journal_decision(text, journal_analysis, tone_vector):
        return {
            "mode": "silent",
            "reply": "",
            "why": "heuristic_gate_silent",
            "tone_vector": tone_vector,
        }
    tone_guardrail = build_tone_guardrail_text(tone_vector)
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
- {tone_guardrail}
- Return JSON only.

Journal text:
\"\"\"{text}\"\"\"

Journal analysis:
{json.dumps(journal_analysis, ensure_ascii=True)}

Context:
{json.dumps(tone_context, ensure_ascii=True)}

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

    material = build_recent_memory_material(interaction_limit=60, observation_limit=80, journal_limit=10)
    payload = run_ai_deep_memory_consolidation(material)
    store_deep_memory_consolidation(payload)

    apply_memory_decay()


def run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0):
    attempt = 1
    while attempt <= max_attempts:
        try:
            run_nightly_memory_consolidation()
            return {"ok": True, "attempts": attempt}
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            retryable = ("database is locked" in message) or ("database is busy" in message) or ("locked" in message)
            if (not retryable) or attempt >= max_attempts:
                raise
            time.sleep(base_delay_seconds * attempt)
            attempt += 1


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
        FROM alert_log
        WHERE event_hash = ?
          AND sent_to_user = 1
          AND datetime(created_at) >= datetime('now', '-36 hours')
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
            WHERE alert_id = ? COLLATE NOCASE
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


def resolve_alert_reference(reference_code):
    ref = (reference_code or "").strip()
    if not ref:
        return {"status": "missing"}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alert_id, event_hash, headline
        FROM alert_log
        WHERE alert_id = ? COLLATE NOCASE
        ORDER BY id DESC
        LIMIT 1
        """,
        (ref,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"status": "missing"}
    return {"status": "ok", "match": dict(row)}


def get_latest_sent_alert_reference():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alert_id, event_hash, headline
        FROM alert_log
        WHERE sent_to_user = 1
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


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


def to_base36_suffix(value, width=3):
    value = max(0, int(value))
    chars = []
    while value > 0:
        value, rem = divmod(value, 36)
        chars.append(ALERT_CODE_ALPHABET[rem])
    suffix = "".join(reversed(chars)) or "0"
    return suffix[-width:].rjust(width, "0")


def random_alert_suffix(width=3):
    return "".join(random.choice(ALERT_CODE_ALPHABET) for _ in range(width))


def build_alert_id(category, tier):
    existing_count = count_tier_alerts_today(category, tier)
    suffix = to_base36_suffix(existing_count % ALERT_CODE_SPACE_SIZE, width=ALERT_CODE_SUFFIX_LEN)
    return f"{category.upper()}{tier}-{suffix}"


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
        for _ in range(30):
            fallback_id = f"{category.upper()}{tier}-{random_alert_suffix(ALERT_CODE_SUFFIX_LEN)}"
            fallback_result = execute_write_with_retry(
                """
                INSERT OR IGNORE INTO alert_log (alert_id, category, tier, headline, event_hash, sent_to_user)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fallback_id, category.upper(), tier, headline, event_hash, sent_to_user),
            )
            if fallback_result.get("rowcount", 0) > 0:
                alert_id = fallback_id
                inserted = True
                break
        if not inserted or not alert_id:
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
                e.selection_reasons_json,
                e.score
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
                e.selection_reasons_json,
                e.score
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
    prefix = code.split("-", 1)[0] if "-" in code else code
    source_text = format_source_refs_list(candidate.get("source_refs"), limit=3)
    suffix = f" [{source_text}]" if source_text else ""
    return f"{prefix}: {candidate['headline']}{suffix} ({code})"


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
    scored_alerts = []
    for item in alerts:
        code = (item.get("category") or "").upper()
        if code not in section_order:
            continue
        scored_alerts.append({**item, "category": code})

    def relevance_sort_key(item):
        try:
            score = float(item.get("score") or 0.0)
        except:
            score = 0.0
        published_at = str(item.get("published_at") or "")
        created_at = str(item.get("created_at") or "")
        return (score, published_at, created_at)

    tier_1 = sorted(
        [item for item in scored_alerts if int(item.get("tier") or 3) == 1],
        key=relevance_sort_key,
        reverse=True,
    )
    tier_2 = sorted(
        [item for item in scored_alerts if int(item.get("tier") or 3) == 2],
        key=relevance_sort_key,
        reverse=True,
    )
    tier_3 = sorted(
        [item for item in scored_alerts if int(item.get("tier") or 3) == 3],
        key=relevance_sort_key,
        reverse=True,
    )

    selected_alerts = []
    if tier_1:
        selected_alerts = tier_1[:5]
    elif tier_2:
        selected_alerts = tier_2[:1]
    elif tier_3:
        selected_alerts = tier_3[:1]

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
        # Fallback: allow direct expansion from live alert IDs
        # (e.g., E1-00f) even when they are not in the latest brief map.
        ref = (reference_code or "").strip()
        direct_context = get_event_context(alert_id=ref)
        if not direct_context:
            resolved_alert = resolve_alert_reference(ref)
            if resolved_alert.get("status") == "ok":
                match = resolved_alert["match"]
                direct_context = get_event_context(event_hash=match.get("event_hash"))
                if not direct_context:
                    direct_context = backfill_event_context_for_reference(match)
        if direct_context:
            resolved = {
                "status": "ok",
                "match": {
                    "alert_id": direct_context.get("alert_id"),
                    "event_hash": direct_context.get("event_hash"),
                    "headline": direct_context.get("headline"),
                },
            }
        else:
            return "I don't know which alert or brief item that is."
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


def send_whatsapp_single_message(body):
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


def send_whatsapp_message(body):
    chunks = split_reply_chunks(body, max_chars=WHATSAPP_REPLY_CHUNK_MAX)
    if not chunks:
        return {"ok": False, "reason": "empty_body"}

    chunk_results = []
    first_sid = None
    for chunk in chunks:
        result = send_whatsapp_single_message(chunk)
        chunk_results.append(result)
        if result.get("ok") and not first_sid:
            first_sid = result.get("sid")
        if not result.get("ok"):
            return {
                "ok": False,
                "reason": "chunk_send_failed",
                "failed_chunk_index": len(chunk_results) - 1,
                "chunk_count": len(chunks),
                "chunks_sent": sum(1 for item in chunk_results if item.get("ok")),
                "chunk_results": chunk_results,
            }

    return {
        "ok": True,
        "sid": first_sid,
        "chunk_count": len(chunks),
        "chunk_results": chunk_results,
    }


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


def classify_news_category_scored(query, headline, snippet, section, watchlist=None):
    text = " ".join([query or "", headline or "", snippet or "", section or ""]).lower()
    section_l = (section or "").lower()
    query_hint = infer_category_hint_from_text(query or "")

    scores = {"E": 0.0, "G": 0.0, "L": 0.0}

    if query_hint in scores:
        scores[query_hint] += 1.8

    # Section priors are strong non-keyword signals.
    if any(term in section_l for term in ["business", "economy", "markets", "finance"]):
        scores["E"] += 2.2
    if any(term in section_l for term in ["world", "international", "global affairs", "foreign"]):
        scores["G"] += 1.8
    if any(term in section_l for term in ["u.s.", "california", "bay area", "metro", "local"]):
        scores["L"] += 1.2

    local_terms = {
        "bay area", "san francisco", "berkeley", "oakland", "san jose",
        "sacramento", "california", "earthquake", "wildfire",
    }
    macro_terms = {
        "fed", "inflation", "cpi", "rates", "rate cut", "jobs", "employment", "treasury",
        "stock", "stocks", "shares", "earnings", "guidance", "nasdaq", "nyse",
        "dow", "s&p", "market", "gdp", "imf", "bond", "bonds", "yield", "yields",
        "finance", "business", "prices", "cost of living", "consumer prices", "price pressure",
    }
    geo_terms = {
        "iran", "russia", "china", "taiwan", "ukraine", "israel", "gaza",
        "sanction", "war", "shipping", "strait", "conflict", "ceasefire",
        "nato", "missile", "drone", "military", "embassy", "diplomatic", "geopolitics",
    }

    local_hits = sum(1 for term in local_terms if term in text)
    macro_hits = sum(1 for term in macro_terms if term in text)
    geo_hits = sum(1 for term in geo_terms if term in text)

    scores["L"] += min(3.0, 0.7 * local_hits)
    scores["E"] += min(3.4, 0.65 * macro_hits)
    scores["G"] += min(3.4, 0.65 * geo_hits)

    if re.search(r"\b(?:nasdaq|nyse|arca)\s*:\s*[a-z]{1,6}\b", text):
        scores["E"] += 2.0

    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top_cat, top_score = ordered[0]
    second_score = ordered[1][1]
    margin = float(top_score - second_score)

    return {
        "category": top_cat,
        "scores": scores,
        "margin": margin,
        "query_hint": query_hint,
    }


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
    scored = classify_news_category_scored(
        candidate.get("origin_query") or "",
        candidate.get("headline") or "",
        candidate.get("snippet") or "",
        candidate.get("section") or "",
        watchlist=watchlist,
    )
    normalized = scored.get("category") or original
    if normalized not in {"P", "E", "G", "L"}:
        normalized = original

    score_map = scored.get("scores") or {}
    original_score = float(score_map.get(original, 0.0))
    normalized_score = float(score_map.get(normalized, 0.0))

    # Global reclassification rule: only reclassify when alternate score is truly stronger.
    if normalized != original and (normalized_score - original_score) < 0.55:
        return original, None

    # Ambiguity guard: if the score margin is small, keep original category.
    if normalized != original and float(scored.get("margin") or 0.0) < 1.2:
        return original, None

    # Downgrade guard: do not reclassify E/P-like intent into G unless confidence is strong.
    if normalized == "G" and original in {"E", "P"} and float(scored.get("margin") or 0.0) < 2.3:
        return original, None

    # Query-origin inertia: if origin query strongly hinted E, require extra confidence to move to G.
    if normalized == "G" and original == "E" and (scored.get("query_hint") == "E") and float(scored.get("margin") or 0.0) < 2.8:
        return original, None

    if normalized != original:
        return normalized, "content_classifier"
    return original, None


def candidate_topic_terms(candidate):
    text = " ".join([
        candidate.get("headline", "") or "",
        candidate.get("snippet", "") or "",
        candidate.get("section", "") or "",
    ]).lower()
    return {
        token for token in re.findall(r"[a-z0-9]{3,}", text)
        if token not in STORY_STOPWORDS
    }


def g_integrity_guard(candidate):
    terms = candidate_topic_terms(candidate)
    query_terms = _query_terms(candidate.get("origin_query") or "")
    query_terms = {
        token for token in query_terms
        if token not in STORY_STOPWORDS and token not in G_QUERY_GENERIC_TERMS
    }

    geo_hits = len(terms & G_QUERY_CORE_TERMS)
    query_overlap = len(terms & query_terms) if query_terms else 0

    local_like = any(token in terms for token in {
        "weather", "rain", "flood", "storm", "snow", "wildfire",
        "california", "bay", "area", "san", "francisco", "oakland", "berkeley",
    })
    finance_like = any(token in terms for token in {
        "fed", "inflation", "rates", "yield", "market", "stocks", "earnings", "gdp", "imf",
    })

    # Topic coherence score for G: must be tied to geopolitical terms and/or
    # closely overlap the originating query intent.
    coherence = 0
    if geo_hits >= 2:
        coherence += 2
    elif geo_hits == 1:
        coherence += 1
    if query_overlap >= 2:
        coherence += 2
    elif query_overlap == 1:
        coherence += 1

    if coherence >= 2:
        return {"ok": True, "action": "keep_g", "reason": "coherent_g"}

    if local_like and geo_hits == 0:
        return {"ok": False, "action": "reclassify_L", "reason": "g_integrity_local_mismatch"}
    if finance_like and geo_hits == 0:
        return {"ok": False, "action": "reclassify_E", "reason": "g_integrity_macro_mismatch"}
    return {"ok": False, "action": "drop", "reason": "g_integrity_low_coherence"}


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
                "origin_query": query,
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
                    "origin_query": query,
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


def contains_signal_term(text, term):
    haystack = (text or "").lower()
    needle = (term or "").lower().strip()
    if not haystack or not needle:
        return False
    if " " in needle:
        return needle in haystack
    return re.search(rf"\b{re.escape(needle)}\b", haystack) is not None


def is_geo_intent_text(text):
    t = (text or "").lower().strip()
    if not t:
        return False
    if any(contains_signal_term(t, term) for term in G_QUERY_BLOCKED_TERMS):
        return False
    return any(contains_signal_term(t, term) for term in G_QUERY_CORE_TERMS)


def is_e_intent_text(text):
    t = (text or "").lower().strip()
    if not t:
        return False
    return any(contains_signal_term(t, term) for term in E_QUERY_CORE_TERMS)


def build_stable_e_interest_profile(interaction_limit=180, max_terms=5):
    term_counts = Counter()
    matched_refs = 0
    for event in get_recent_interaction_events(limit=interaction_limit):
        text = (event.get("message_text") or "").strip()
        if not text:
            continue

        refs = interpret_event_references(text)
        for ref in refs:
            if not ref.upper().startswith("E"):
                continue
            resolved = resolve_alert_reference(ref)
            if resolved.get("status") != "ok":
                continue
            matched_refs += 1
            headline = ((resolved.get("match") or {}).get("headline") or "").lower()
            if not headline:
                continue
            for token in re.findall(r"[a-z0-9]{3,}", headline):
                if token in STORY_STOPWORDS:
                    continue
                if token in E_QUERY_CORE_TERMS:
                    term_counts[token] += 2

        for token in re.findall(r"[a-z0-9]{3,}", text.lower()):
            if token in E_QUERY_CORE_TERMS:
                term_counts[token] += 1

    manual_terms = get_brainstem_setting("econ_manual_terms", default=[]) or []
    for item in manual_terms:
        if isinstance(item, dict):
            term = str(item.get("term") or "").strip().lower()
            mode = str(item.get("mode") or "normal").strip().lower()
            weight = float(item.get("weight") or 1.0)
        else:
            term = str(item or "").strip().lower()
            mode = "normal"
            weight = 1.0
        if not term or not is_e_intent_text(term):
            continue
        if mode == "suppress":
            term_counts[term] -= max(1, int(round(weight * 3)))
        elif mode == "boost":
            term_counts[term] += max(1, int(round(weight * 4)))
        else:
            term_counts[term] += max(1, int(round(weight * 2)))

    positive_terms = Counter({k: v for k, v in term_counts.items() if v > 0})
    if not positive_terms:
        return {"query": None, "terms": [], "matched_refs": 0}

    top_terms = [term for term, _ in positive_terms.most_common(max_terms)]
    if len(top_terms) == 1:
        top_terms.append("macro")
    if len(top_terms) < 2:
        return {"query": None, "terms": [], "matched_refs": matched_refs}

    return {
        "query": " ".join(top_terms[:max_terms]),
        "terms": top_terms[:max_terms],
        "matched_refs": matched_refs,
    }


def build_stable_g_interest_profile(interaction_limit=180, max_terms=5):
    term_counts = Counter()
    matched_refs = 0
    for event in get_recent_interaction_events(limit=interaction_limit):
        text = (event.get("message_text") or "").strip()
        if not text:
            continue

        refs = interpret_event_references(text)
        for ref in refs:
            if not ref.upper().startswith("G"):
                continue
            resolved = resolve_alert_reference(ref)
            if resolved.get("status") != "ok":
                continue
            matched_refs += 1
            headline = ((resolved.get("match") or {}).get("headline") or "").lower()
            if not headline:
                continue
            for token in re.findall(r"[a-z0-9]{3,}", headline):
                if token in STORY_STOPWORDS or token in G_QUERY_GENERIC_TERMS:
                    continue
                if token in G_QUERY_CORE_TERMS:
                    term_counts[token] += 2

        # Also count direct geopolitical terms from free-text prompts.
        for token in re.findall(r"[a-z0-9]{3,}", text.lower()):
            if token in G_QUERY_CORE_TERMS:
                term_counts[token] += 1

    manual_terms = get_brainstem_setting("geo_manual_terms", default=[]) or []
    for item in manual_terms:
        if isinstance(item, dict):
            term = str(item.get("term") or "").strip().lower()
            mode = str(item.get("mode") or "normal").strip().lower()
            weight = float(item.get("weight") or 1.0)
        else:
            term = str(item or "").strip().lower()
            mode = "normal"
            weight = 1.0
        if not term or not is_geo_intent_text(term):
            continue
        if mode == "suppress":
            term_counts[term] -= max(1, int(round(weight * 3)))
        elif mode == "boost":
            term_counts[term] += max(1, int(round(weight * 4)))
        else:
            term_counts[term] += max(1, int(round(weight * 2)))

    positive_terms = Counter({k: v for k, v in term_counts.items() if v > 0})
    if not positive_terms:
        return {"query": None, "terms": [], "matched_refs": 0}

    top_terms = [term for term, _ in positive_terms.most_common(max_terms)]
    if len(top_terms) == 1:
        top_terms.append("geopolitics")
    if len(top_terms) < 2:
        return {"query": None, "terms": [], "matched_refs": matched_refs}

    return {
        "query": " ".join(top_terms[:max_terms]),
        "terms": top_terms[:max_terms],
        "matched_refs": matched_refs,
    }


def tighten_g_query_text(text):
    raw = (text or "").strip().lower()
    if not raw:
        return None

    compact = re.sub(r"\s+", " ", raw)
    compact = compact.replace("\n", " ").replace("\r", " ")
    compact = re.sub(r"[\"'`]", "", compact)
    tokens = re.findall(r"[a-z0-9]{2,}", compact)
    if not tokens:
        return None

    cleaned = []
    for token in tokens:
        if token in STORY_STOPWORDS or token in G_QUERY_GENERIC_TERMS:
            continue
        if token not in cleaned:
            cleaned.append(token)

    # Keep G queries compact and content-focused.
    if len(cleaned) == 1 and cleaned[0] in G_QUERY_CORE_TERMS:
        cleaned.append("geopolitics")

    if len(cleaned) < 2:
        return None

    return " ".join(cleaned[:6])


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
    if any(contains_signal_term(t, term) for term in signal_terms):
        return True

    for symbol in (watchlist or []) + (trusted_portfolio or []):
        if symbol and contains_signal_term(t, symbol.lower()):
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
        "g_tightened": 0,
        "g_dropped": 0,
        "g_profile_query": None,
        "g_profile_terms": [],
        "g_profile_matched_refs": 0,
        "e_profile_query": None,
        "e_profile_terms": [],
        "e_profile_matched_refs": 0,
        "p_query_count": 0,
        "p_query_reason": None,
        "category_counts": {},
    }

    def add_query(query, category_hint=None):
        query_debug["input_total"] += 1
        resolved_hint = (category_hint or infer_category_hint_from_text(query))
        if (resolved_hint or "").upper() == "P":
            query = re.sub(r"\s+", " ", str(query or "").strip().lower())
            query = query.replace("\n", " ").replace("\r", " ")
            query = re.sub(r"[\"'`]", "", query)
        elif (resolved_hint or "").upper() == "G":
            tightened = tighten_g_query_text(query)
            if not tightened:
                query_debug["g_dropped"] += 1
                return
            if normalize_candidate_query_text(query) != tightened:
                query_debug["g_tightened"] += 1
            query = tightened
        else:
            query = normalize_candidate_query_text(query)
        if not query or len(query) < 8:
            return
        if not is_news_query_signal(query, watchlist=watchlist, trusted_portfolio=trusted_portfolio):
            return
        resolved_hint = (resolved_hint or infer_category_hint_from_text(query))
        if not include_local and resolved_hint == "L":
            return
        if query in seen:
            query_debug["exact_deduped"] += 1
            return
        seen.add(query)
        query_items.append((query, resolved_hint))

    for query, category_hint in BASELINE_NEWS_QUERIES:
        add_query(query, category_hint)

    # P queries are intentionally broad and simple:
    # 1) full top holdings symbol list
    # 2) broad market performance
    # 3) forward-looking market performance
    p_query_symbols = trusted_top_holdings
    if p_query_symbols:
        add_query(" ".join(p_query_symbols), "P")
    else:
        query_debug["p_query_reason"] = "no_trusted_symbols"
    add_query("broad market performance", "P")
    add_query("forward looking market performance", "P")

    g_profile = build_stable_g_interest_profile(interaction_limit=220, max_terms=5)
    if g_profile.get("query"):
        add_query(g_profile["query"], "G")
        query_debug["g_profile_query"] = g_profile["query"]
        query_debug["g_profile_terms"] = g_profile.get("terms", [])
        query_debug["g_profile_matched_refs"] = int(g_profile.get("matched_refs", 0))

    e_profile = build_stable_e_interest_profile(interaction_limit=220, max_terms=5)
    if e_profile.get("query"):
        add_query(e_profile["query"], "E")
        query_debug["e_profile_query"] = e_profile["query"]
        query_debug["e_profile_terms"] = e_profile.get("terms", [])
        query_debug["e_profile_matched_refs"] = int(e_profile.get("matched_refs", 0))

    relevant = get_relevant_memories("current interests recurring focus active concerns", limit=12)
    for item in relevant.get("working", []) + relevant.get("long_term", []):
        value = (item.get("value") or "").strip()
        if not value:
            continue
        if (item.get("category") or "").strip().lower() in NEWS_MEMORY_BLOCKLIST_CATEGORIES:
            continue
        category = item.get("category") or ""
        if category in {"priorities", "deep_preferences", "preferences", "goals"}:
            resolved = infer_category_hint_from_text(value)
            # Keep G dynamic behavior stable from interaction profile, not volatile memory strings.
            if resolved == "G":
                continue
            add_query(value, resolved)

    for event in get_recent_interaction_events(limit=20):
        message_text = (event.get("message_text") or "").strip()
        if len(message_text) < 8:
            continue
        if event.get("intent") in {"news", "watchlist_stats", "ticker_quote", "fred", "daily_brief"}:
            resolved = infer_category_hint_from_text(message_text)
            # Keep G dynamic behavior stable from interaction profile, not per-message churn.
            if resolved == "G":
                continue
            add_query(message_text, resolved)

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
        "g_integrity_reclass_count": 0,
        "g_integrity_drop_count": 0,
        "g_integrity_examples": [],
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
        original_category = candidate.get("category")
        normalized_category, reason = normalize_candidate_category(candidate, watchlist=watchlist)
        chain = [original_category] if original_category else []
        if normalized_category and (not chain or chain[-1] != normalized_category):
            chain.append(normalized_category)
        if normalized_category != candidate.get("category"):
            category_reassigned_count += 1
            if len(category_reassigned_examples) < 6:
                category_reassigned_examples.append({
                    "headline": candidate.get("headline"),
                    "from": candidate.get("category"),
                    "to": normalized_category,
                    "source": candidate.get("source"),
                    "reason": reason,
                    "chain": chain,
                })
        normalized = {**candidate, "category": normalized_category, "reclass_chain": chain}
        if normalized.get("category") == "G":
            integrity = g_integrity_guard(normalized)
            if integrity.get("action") == "reclassify_L":
                normalized["category"] = "L"
                if not normalized.get("reclass_chain") or normalized["reclass_chain"][-1] != "L":
                    normalized["reclass_chain"] = (normalized.get("reclass_chain") or []) + ["L"]
                source_debug["g_integrity_reclass_count"] += 1
                if len(source_debug["g_integrity_examples"]) < 6:
                    source_debug["g_integrity_examples"].append({
                        "headline": normalized.get("headline"),
                        "from": "G",
                        "to": "L",
                        "reason": integrity.get("reason"),
                        "chain": normalized.get("reclass_chain"),
                    })
            elif integrity.get("action") == "reclassify_E":
                normalized["category"] = "E"
                if not normalized.get("reclass_chain") or normalized["reclass_chain"][-1] != "E":
                    normalized["reclass_chain"] = (normalized.get("reclass_chain") or []) + ["E"]
                source_debug["g_integrity_reclass_count"] += 1
                if len(source_debug["g_integrity_examples"]) < 6:
                    source_debug["g_integrity_examples"].append({
                        "headline": normalized.get("headline"),
                        "from": "G",
                        "to": "E",
                        "reason": integrity.get("reason"),
                        "chain": normalized.get("reclass_chain"),
                    })
            elif integrity.get("action") == "drop":
                source_debug["g_integrity_drop_count"] += 1
                if len(source_debug["g_integrity_examples"]) < 6:
                    source_debug["g_integrity_examples"].append({
                        "headline": normalized.get("headline"),
                        "from": "G",
                        "to": "drop",
                        "reason": integrity.get("reason"),
                        "chain": (normalized.get("reclass_chain") or []) + ["drop"],
                    })
                continue

        normalized_candidates.append(normalized)

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
    if not ALERT_AI_GATE_ENABLED:
        decision_map = {}
        for idx, candidate in enumerate(candidates, start=1):
            candidate_id = f"C{idx}"
            tier = int(candidate.get("assigned_tier") or 3)
            decision_map[candidate_id] = {
                "send": tier <= 1,
                "category": ((candidate.get("category") or "G").upper()[:1]) or "G",
                "tier": tier,
                "why": "rule_gate:tier1_only",
            }
        return decision_map

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
            "reclass_chain": candidate.get("reclass_chain"),
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

    direct_match = re.match(rf"^\s*{EVENT_REFERENCE_PATTERN}\s*$", t, re.IGNORECASE)
    if direct_match:
        return direct_match.group(1).upper()

    followup_patterns = [
        rf"^\s*(?:please\s+)?(?:expand(?:\s+on)?|follow up on|follow-up on|tell me more about|more on|what about|talk about)\s+{EVENT_REFERENCE_PATTERN}\s*(?:please)?\s*[.!?]*\s*$",
        rf"^\s*{EVENT_REFERENCE_PATTERN}\s+(?:please|details|context|more)\s*[.!?]*\s*$",
    ]
    for pattern in followup_patterns:
        match = re.match(pattern, t, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    loose_match = re.search(rf"\b{EVENT_REFERENCE_PATTERN}\b", t, re.IGNORECASE)
    if loose_match:
        return loose_match.group(1).upper()

    return None


def interpret_event_references(text):
    matches = re.findall(rf"\b{EVENT_REFERENCE_PATTERN}\b", text or "", re.IGNORECASE)
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


def is_expand_request_without_reference(text):
    t = (text or "").strip().lower()
    if not t:
        return False
    patterns = [
        r"^\s*(?:please\s+)?expand(?:\s+on)?\s*(?:this|that|it)?\s*[.!?]*\s*$",
        r"^\s*(?:please\s+)?tell me more(?:\s+about)?\s*(?:this|that|it)?\s*[.!?]*\s*$",
        r"^\s*(?:please\s+)?more on (?:this|that|it)\s*[.!?]*\s*$",
        r"^\s*(?:please\s+)?(?:details|context)\s*(?:on\s+)?(?:this|that|it)?\s*[.!?]*\s*$",
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


def interpret_calendar_request(text):
    t = (text or "").strip().lower()
    if not t:
        return {"intent": "none"}

    calendar_signals = [
        "calendar",
        "schedule",
        "event",
        "events",
        "lecture",
        "lectures",
        "class",
        "classes",
        "meeting",
        "meetings",
    ]
    has_calendar_context = any(signal in t for signal in calendar_signals)
    followup_signals = [
        "previously",
        "earlier",
        "before",
        "what about before",
        "same question",
        "i mean before",
        "past",
        "prior",
    ]
    looks_like_followup = any(signal in t for signal in followup_signals)
    recent = get_recent_messages(limit=6)
    recent_calendar_context = any(
        (
            "calendar" in (item.get("content") or "").lower()
            or "event(s)" in (item.get("content") or "").lower()
            or "lecture/class" in (item.get("content") or "").lower()
        )
        for item in recent[-4:]
    )
    if not has_calendar_context and not (looks_like_followup and recent_calendar_context):
        return {"intent": "none"}

    prior_user = ""
    for item in reversed(recent):
        if item.get("role") == "user":
            prior_user = item.get("content") or ""
            break

    try:
        prompt = f"""
Interpret this calendar/schedule request.
Return strict JSON only.

Current message:
\"\"\"{text}\"\"\"

Previous user message (for follow-up context):
\"\"\"{prior_user}\"\"\"

Output schema:
{{
  "intent": "calendar_query|none",
  "window": "today|tomorrow|week|past_week|past",
  "focus_text": "optional semantic focus phrase or empty"
}}
"""
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(completion.choices[0].message.content)
        intent = str(payload.get("intent") or "none").strip().lower()
        if intent != "calendar_query":
            return {"intent": "none"}
        window = str(payload.get("window") or "today").strip().lower()
        if window not in {"today", "tomorrow", "week", "past_week", "past"}:
            window = "today"
        focus_text = str(payload.get("focus_text") or "").strip()
        return {"intent": "calendar_query", "window": window, "focus_text": focus_text, "query_text": text}
    except:
        window = "today"
        if "tomorrow" in t:
            window = "tomorrow"
        elif "this week" in t or "next 7" in t or "next week" in t:
            window = "week"
        elif "previous" in t or "earlier" in t or "before" in t or "past" in t:
            window = "past_week"
        return {"intent": "calendar_query", "window": window, "focus_text": "", "query_text": text}


def fallback_calendar_request(text):
    t = (text or "").strip().lower()
    calendar_signals = [
        "calendar", "schedule", "event", "events",
        "lecture", "lectures", "class", "classes", "meeting", "meetings",
        "planned", "plan",
    ]
    if not any(signal in t for signal in calendar_signals):
        return {"intent": "none"}

    window = "today"
    if "tomorrow" in t:
        window = "tomorrow"
    elif "next week" in t:
        window = "week"
    elif "this week" in t or "next 7" in t:
        window = "week"
    elif "previous" in t or "earlier" in t or "before" in t or "past" in t:
        window = "past_week"

    return {
        "intent": "calendar_query",
        "window": window,
        "focus_text": "",
        "query_text": text,
    }


def build_calendar_query_reply(request_info):
    context = get_calendar_context_snapshot()
    events = context.get("events") or []
    if not events:
        return "I don't see any calendar events in the current context window."

    now_local = get_local_now()
    today = now_local.date()

    window = (request_info or {}).get("window") or "today"
    if window == "tomorrow":
        target_days = {str(today + timedelta(days=1))}
        label = "tomorrow"
    elif window == "week":
        target_days = {str(today + timedelta(days=offset)) for offset in range(0, 7)}
        label = "the next 7 days"
    elif window == "past_week":
        target_days = {str(today - timedelta(days=offset)) for offset in range(1, 8)}
        label = "the previous 7 days"
    elif window == "past":
        target_days = {str(today - timedelta(days=offset)) for offset in range(1, 31)}
        label = "the recent past"
    else:
        target_days = {str(today)}
        label = "today"

    focus_text = str((request_info or {}).get("focus_text") or "").strip().lower()
    query_text = str((request_info or {}).get("query_text") or "").strip().lower()
    requested_domain = None
    if any(token in query_text for token in ("school", "class", "lecture", "lectures", "midterm", "exam")):
        requested_domain = "school"
    elif any(token in query_text for token in ("extracurricular", "club", "team", "calsol")):
        requested_domain = "extracurricular"
    elif any(token in query_text for token in ("personal", "family", "friend")):
        requested_domain = "personal"

    # Only apply topic filtering when the user clearly requested a topic.
    # Generic planning prompts should return all events for the window.
    explicit_focus_patterns = [
        r"\blecture(s)?\b",
        r"\bclass(es)?\b",
        r"\bseminar(s)?\b",
        r"\bexam(s)?\b",
        r"\bmidterm(s)?\b",
        r"\boffice hours?\b",
        r"\bmeeting(s)?\b",
    ]
    has_explicit_focus = any(re.search(pattern, query_text) for pattern in explicit_focus_patterns)
    focus_basis = focus_text if focus_text else (query_text if has_explicit_focus else "")
    raw_focus_terms = [
        token
        for token in re.split(r"[^a-z0-9]+", focus_basis)
        if len(token) >= 3
        and token not in {
            "the", "and", "for", "with", "from", "that", "this",
            "calendar", "schedule", "event", "events", "today", "tomorrow",
            "week", "next", "past", "previously", "earlier", "mean", "have", "had",
            "did", "what", "which", "any", "check", "list", "show", "planned",
            "plan", "please", "all",
        }
    ]
    # Add simple singular variants so "lectures" matches "lecture", etc.
    focus_terms = []
    for token in raw_focus_terms:
        focus_terms.append(token)
        if token.endswith("s") and len(token) > 4:
            focus_terms.append(token[:-1])
    focus_terms = sorted(set(focus_terms))

    filtered = []
    for event in events:
        start_local = str(event.get("start_local") or "").strip()
        event_day = start_local[:10] if len(start_local) >= 10 else ""
        if event_day not in target_days:
            continue
        if requested_domain and str(event.get("domain") or "") != requested_domain:
            continue
        title = str(event.get("title") or "(untitled)")
        event_type = str(event.get("event_type") or "").lower()
        if focus_terms and not any(term in title.lower() for term in focus_terms):
            if not any(term in event_type for term in focus_terms):
                continue
        filtered.append(event)

    filtered.sort(key=lambda item: str(item.get("start_local") or ""))

    if not filtered:
        if focus_terms:
            any_in_window = []
            for event in events:
                start_local = str(event.get("start_local") or "").strip()
                event_day = start_local[:10] if len(start_local) >= 10 else ""
                if event_day in target_days:
                    any_in_window.append(event)
            if any_in_window:
                sample = any_in_window[0]
                return (
                    f"I don't see calendar events matching \"{focus_text}\" for {label}. "
                    f"I do see {len(any_in_window)} other event(s), for example: "
                    f"\"{sample.get('title','(untitled)')}\"."
                )
        return f"I don't see any calendar events for {label}."

    def format_event_line(event):
        title = str(event.get("title") or "(untitled)").strip()
        start_raw = str(event.get("start_local") or "").strip()
        end_raw = str(event.get("end_local") or "").strip()
        if not start_raw and not end_raw:
            return title
        try:
            start_dt = datetime.fromisoformat(start_raw) if start_raw else None
            end_dt = datetime.fromisoformat(end_raw) if end_raw else None
            day_part = start_dt.strftime("%a %m/%d") if start_dt else ""
            if start_dt and end_dt:
                time_part = f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
            elif start_dt:
                time_part = start_dt.strftime("%H:%M")
            elif end_dt:
                time_part = end_dt.strftime("%H:%M")
            else:
                time_part = ""
            return f"{day_part} {time_part} - {title}".strip()
        except:
            return f"{start_raw} to {end_raw}: {title}".strip()

    lines = [f"{idx}. {format_event_line(event)}" for idx, event in enumerate(filtered, start=1)]

    domain_counts = Counter(str(item.get("domain") or "unknown") for item in filtered)
    top_domains = ", ".join(f"{name}:{count}" for name, count in domain_counts.most_common(3))
    header = f"You have {len(filtered)} event(s) for {label}."
    if top_domains:
        header += f" Mix: {top_domains}."

    if len(lines) > 24:
        preview = "\n".join(lines[:24])
        remaining = len(lines) - 24
        return f"{header}\n{preview}\n... and {remaining} more."
    return f"{header}\n" + "\n".join(lines)

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

def route(text, allow_ai_interpretation=True):
    t = text.lower()
    market_question = interpret_market_data_question(text)
    calendar_request = interpret_calendar_request(text) if allow_ai_interpretation else fallback_calendar_request(text)
    expand_references = interpret_event_references(text)
    expand_reference = interpret_event_reference(text)
    email_request = {"intent": "none", "query_hint": "", "days_window": 7}
    watchlist_request = {"intent": "none", "symbols": []}

    email_gate_patterns = [
        r"\bemail(s)?\b",
        r"\binbox\b",
        r"\bgmail\b",
        r"\bibkr\b",
        r"\binteractive ?brokers\b",
        r"\binteractivebrokers\b",
        r"\b(mailbox|mail)\b",
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
        email_request = interpret_email_request(text) if allow_ai_interpretation else fallback_email_request(text)
    if likely_watchlist:
        watchlist_request = interpret_watchlist_request(text) if allow_ai_interpretation else fallback_watchlist_request(text)

    if is_command_key_request(text):
        return ("command_key", None)

    if is_feedback_message(text):
        return ("alert_feedback", t.strip())

    if is_daily_brief_question(text):
        return ("daily_brief", None)

    if is_full_article_request(text):
        return ("full_article_request", None)

    if calendar_request["intent"] != "none":
        return ("calendar_query", calendar_request)

    if len(expand_references) >= 2:
        return ("event_expand_multi", expand_references)

    if expand_reference:
        return ("event_expand", expand_reference)
    if is_expand_request_without_reference(text):
        return ("event_expand_latest", None)

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


def route_hardcoded_command(text):
    t = (text or "").strip().lower()
    if not t:
        return ("none", None)

    # Zero-AI hardcoded command bypasses.
    if is_command_key_request(text):
        return ("command_key", None)
    if is_feedback_message(text):
        return ("alert_feedback", t.strip())
    if is_daily_brief_question(text):
        return ("daily_brief", None)
    if is_full_article_request(text):
        return ("full_article_request", None)
    if is_portfolio_show_question(text):
        return ("portfolio_show", None)

    expand_references = interpret_event_references(text)
    expand_reference = interpret_event_reference(text)
    if len(expand_references) >= 2:
        return ("event_expand_multi", expand_references)
    if expand_reference:
        return ("event_expand", expand_reference)
    if is_expand_request_without_reference(text):
        return ("event_expand_latest", None)

    watch = fallback_watchlist_request(text)
    if watch["intent"] == "add":
        return ("add", watch["symbols"])
    if watch["intent"] == "remove":
        return ("remove", watch["symbols"])
    if watch["intent"] == "watchlist_stats":
        return ("watchlist_stats", None)
    if watch["intent"] == "show":
        return ("show", None)

    market_question = interpret_market_data_question(text)
    if market_question:
        return (market_question["intent"], market_question["tickers"])

    if "10 year" in t or "treasury" in t:
        return ("fred", "DGS10")
    if "fed funds" in t or "federal funds" in t:
        return ("fred", "FEDFUNDS")
    if "unemployment" in t or "jobless" in t:
        return ("fred", "UNRATE")
    if "inflation" in t or "cpi" in t:
        return ("fred", "CPIAUCSL")
    return ("none", None)


def interpret_prompt_tasks_with_ai(text):
    prompt = f"""
Split the user message into one or more independent task strings.

Rules:
- Return JSON only.
- Keep each task string concise and executable.
- If there is only one task, return one item.
- Preserve user wording when possible.

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
        raw_tasks = payload.get("tasks") if isinstance(payload, dict) else []
        tasks = []
        if isinstance(raw_tasks, list):
            for item in raw_tasks:
                task_text = str(item or "").strip()
                if task_text and task_text not in tasks:
                    tasks.append(task_text)
        return tasks[:6] or [text]
    except:
        if should_attempt_multi_task(text):
            split = split_multi_tasks(text)
            if split:
                return split[:6]
        return [text]


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
    return fallback_split_tasks(text)


def run_generic_reply(user_text):
    try:
        context_snapshot = build_journal_context_snapshot()
        tone_vector = build_tone_vector({}, context_snapshot)
        tone_guardrail = build_tone_guardrail_text(tone_vector)
        tone_instruction = (
            f"Tone vector: {json.dumps(tone_vector, ensure_ascii=True)}. "
            "Follow this style matrix with restraint. "
            "Be concise when brevity/directness is high, more warm when warmth is high, "
            "and serious when seriousness is high. Never be sycophantic. "
            f"{tone_guardrail}"
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

    if intent == "event_expand_latest":
        latest = get_latest_sent_alert_reference()
        if not latest or not latest.get("alert_id"):
            return "I don't know which alert you mean."
        return expand_brief_event(latest["alert_id"])

    if intent == "event_expand_multi":
        sent = send_multi_expand_messages(value or [])
        return None if sent > 0 else "I don't have enough stored context for those items."

    if intent == "fred":
        out = get_fred(value)
        return format_fred_reply(value, out, msg)

    if intent == "news":
        out = get_news(value)
        return out if out else "N/A"

    if intent == "calendar_query":
        return build_calendar_query_reply(value)

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
    process_due_memory_feedback_queue()
    snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, snapshot)
    record_tone_snapshot(snapshot, tone_vector)
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


@app.route("/debug/guards", methods=["GET"])
def debug_guards():
    denied = require_internal_api_key()
    if denied:
        return denied
    payload = {
        "local_date": get_local_date_string(),
        "journal_guard": get_journal_guard_status(),
        "feedback_context_allowed": feedback_context_allowed(),
        "article_request_context_allowed": article_request_context_allowed(),
    }
    return app.response_class(
        response=json.dumps(payload, indent=2),
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
    upsert_calendar_daily_events(local_date, payload.get("events") or [])
    return app.response_class(
        response=json.dumps(
            {
                "ok": True,
                "local_date": local_date,
                "row": get_calendar_daily_context(local_date),
                "events": get_calendar_daily_events(local_date),
            },
            indent=2,
        ),
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


@app.route("/debug/context/inbox/refresh", methods=["POST"])
def debug_context_inbox_refresh():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    payload = fetch_gmail_inbox_daily_context(local_date=local_date)
    row = get_inbox_daily_context(local_date=local_date)
    return app.response_class(
        response=json.dumps({"ok": bool(payload or row), "local_date": local_date, "row": row}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/debug/context/calendar/refresh", methods=["POST"])
def debug_context_calendar_refresh():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    payload = refresh_calendar_context_from_provider(local_date=local_date)
    row = get_calendar_daily_context(local_date=local_date)
    events = get_calendar_daily_events(local_date=local_date)
    status = 200 if (payload or row) else 400
    return app.response_class(
        response=json.dumps(
            {
                "ok": bool(payload or row),
                "local_date": local_date,
                "configured": bool(CALENDAR_CONTEXT_URL),
                "row": row,
                "events_count": len(events),
                "events_preview": events[:5],
            },
            indent=2,
        ),
        status=status,
        mimetype="application/json",
    )


@app.route("/debug/context/sleep/refresh", methods=["POST"])
def debug_context_sleep_refresh():
    denied = require_internal_api_key()
    if denied:
        return denied
    local_date = get_local_date_string()
    payload = refresh_sleep_context_from_provider(local_date=local_date)
    row = get_sleep_daily_context(local_date=local_date)
    status = 200 if (payload or row) else 400
    return app.response_class(
        response=json.dumps(
            {
                "ok": bool(payload or row),
                "local_date": local_date,
                "configured": bool(SLEEP_CONTEXT_URL),
                "row": row,
            },
            indent=2,
        ),
        status=status,
        mimetype="application/json",
    )


@app.route("/debug/context/sleep/history", methods=["GET"])
def debug_context_sleep_history():
    denied = require_internal_api_key()
    if denied:
        return denied
    days = int((request.args.get("days") or "60").strip() or "60")
    limit = int((request.args.get("limit") or "500").strip() or "500")
    points = get_sleep_datapoints(days=days, limit=limit)
    latest = points[0] if points else None
    return app.response_class(
        response=json.dumps(
            {
                "ok": True,
                "days": days,
                "count": len(points),
                "latest": latest,
                "points": points,
            },
            indent=2,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem", methods=["GET", "POST"])
def brainstem_home():
    if (request.args.get("logout") or "").strip() == "1":
        return clear_brainstem_session_response(redirect("/brainstem"))

    if not has_brainstem_session():
        if request.method == "POST":
            posted_passcode = (request.form.get("passcode") or "").strip()
            if BRAINSTEM_PASSCODE and hmac.compare_digest(posted_passcode, BRAINSTEM_PASSCODE):
                return issue_brainstem_session_response(redirect("/brainstem"))
        login_html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Brainstem Login</title>
  <style>
    body { margin:0; font-family: Georgia, "Times New Roman", serif; background:#080808; color:#ffffff; display:flex; min-height:100vh; align-items:center; justify-content:center; }
    .card { width:min(420px,92vw); border:1px solid #2a2a2a; border-radius:14px; background:#111111; padding:18px; display:flex; flex-direction:column; align-items:center; }
    .title { font-size:1.05rem; font-weight:600; margin-bottom:6px; text-align:center; }
    .muted { color:#a8a8a8; font-size:.86rem; margin-bottom:10px; text-align:center; }
    input { display:block; width:100%; border:1px solid #2f2f2f; border-radius:10px; padding:11px; background:#000000; color:#ffffff; text-align:center; }
    button { margin-top:10px; width:100%; border:1px solid #333333; border-radius:10px; padding:10px; background:#171717; color:#f7f7f7; font-weight:500; cursor:pointer; }
  </style>
</head>
<body>
  <form class="card" method="POST" action="/brainstem">
    <div class="title">Brainstem Passcode</div>
    <div class="muted">Enter passcode to continue.</div>
    <input type="password" name="passcode" autocomplete="current-password" required />
    <button type="submit">Unlock Brainstem</button>
  </form>
</body>
</html>"""
        response = app.response_class(response=login_html, status=200, mimetype="text/html")
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        return response

    base = get_public_base_url()
    key_qs = ""

    html_page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Brainstem</title>
  <style>
    :root {{
      --bg: #000000;
      --fg: #FFEBC4;
      --outline: #1A1712;
      --accent: #FFA200;
      --muted: #d7c8a8;
      --panel: #344237;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: Georgia, "Times New Roman", serif; background: var(--bg); color: var(--fg); font-weight: 430; letter-spacing: 0.1px; }}
    .app-shell {{ min-height: 100vh; display:flex; }}
    .sidebar {{
      width: 250px; flex: 0 0 250px; border-right: 2px solid var(--outline); background: #0a0a0a;
      padding: 14px 10px; transition: transform 220ms ease, opacity 220ms ease; overflow-y:auto; z-index: 25;
    }}
    .sidebar-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; }}
    .brand {{ font-weight: 650; letter-spacing: 0.2px; font-size: 1.08rem; }}
    .tabs {{ display:flex; flex-direction:column; gap:8px; }}
    .tab-btn {{ border:2px solid var(--outline); background: var(--panel); color: var(--fg); font-weight:520; border-radius: 10px; padding: 9px 11px; cursor:pointer; text-align:left; width:100%; transition: transform 140ms ease, background 140ms ease, opacity 140ms ease; }}
    .tab-btn.active {{ outline: 2px solid var(--accent); }}
    .tab-btn:hover {{ transform: translateY(-1px); }}
    .app-shell.nav-collapsed .sidebar {{
      width: 0;
      flex-basis: 0;
      padding: 0;
      border-right: 0;
      overflow: hidden;
      transform: none;
      opacity: 1;
      pointer-events: none;
    }}
    .sidebar.collapsed {{ transform: translateX(-100%); opacity: 0.95; pointer-events:none; }}
    .main-area {{ flex:1; min-width:0; display:flex; flex-direction:column; }}
    .topbar {{ position: sticky; top:0; z-index:20; border-bottom: 2px solid var(--outline); background: rgba(0,0,0,0.93); backdrop-filter: blur(6px); }}
    .topbar-inner {{ padding: 10px 14px; display:flex; align-items:center; justify-content:space-between; gap:10px; }}
    .menu-btn {{ border:2px solid var(--outline); background:#222; color:var(--fg); border-radius:8px; padding:7px 10px; cursor:pointer; font-weight:520; }}
    .topbar-right {{ display:flex; align-items:center; gap:10px; }}
    .container {{ max-width: 1200px; width:100%; margin: 0 auto; padding: 14px; }}
    .section {{ display:none; }}
    .section.active {{ display:block; animation: fadeIn 180ms ease both; }}
    .grid {{ display:grid; grid-template-columns: repeat(12, 1fr); gap:12px; }}
    .card {{ border:2px solid var(--outline); background: var(--panel); border-radius:14px; padding:12px; animation: cardIn 210ms ease both; }}
    .span-12 {{ grid-column: span 12; }}
    .span-8 {{ grid-column: span 8; }}
    .span-6 {{ grid-column: span 6; }}
    .span-4 {{ grid-column: span 4; }}
    .span-3 {{ grid-column: span 3; }}
    .title {{ font-size: 1rem; font-weight: 620; margin-bottom: 8px; }}
    .muted {{ color: var(--muted); font-size: 0.9rem; }}
    .kpi {{ font-size: 1.35rem; font-weight:620; }}
    .row {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .pill {{ border: 1px solid var(--outline); border-radius:999px; padding:4px 10px; font-size:0.82rem; background:#3a4a3e; }}
    .btn {{ border:2px solid var(--outline); background:#455646; color:var(--fg); border-radius:10px; padding:8px 12px; cursor:pointer; font-weight:520; }}
    .btn.warn {{ background: #6a4b13; color:#ffe5b3; }}
    .btn.acc {{ background: #2f5d3a; }}
    .btn.err {{ background: #6a2d2d; }}
    .btn.ghost {{ background: transparent; }}
    input, textarea, select {{
      background:#263129; color:var(--fg); border:2px solid var(--outline); border-radius:10px; padding:8px; width:100%;
    }}
    .table {{ width:100%; border-collapse: collapse; font-size:0.9rem; }}
    .table th, .table td {{ border-bottom:1px solid #253128; padding:8px; text-align:left; vertical-align:top; overflow-wrap:anywhere; word-break:break-word; }}
    .table th {{ color: var(--muted); font-size:0.82rem; }}
    .mem-id-title {{ font-weight:560; font-size: 0.88rem; line-height: 1.2; }}
    .mem-id-sub {{ font-size: 11px; color: var(--muted); margin-top: 1px; line-height:1.2; }}
    .mem-value-wrap {{ line-height: 1.4; }}
    .mem-value-row {{ margin-bottom: 4px; }}
    .confidence-col {{ min-width: 88px; white-space: nowrap; }}
    .feedback-col {{ min-width: 126px; }}
    .feedback-col .row {{ flex-direction: column; align-items: stretch; }}
    .feedback-col .btn {{ width: 100%; min-width: 0; }}
    #section-memory .table {{ font-size: 0.83rem; }}
    #section-memory .table th {{ font-size: 0.78rem; }}
    .links a {{ color: #ffd48a; text-decoration: none; word-break: break-all; }}
    .links a:hover {{ text-decoration: underline; }}
    #ops-console {{
      background:#111; color:#9efc9e; border:2px solid var(--outline); border-radius:10px; min-height:180px;
      padding:10px; font-family: ui-monospace, Menlo, monospace; font-size: 12px; overflow:auto; white-space:pre-wrap;
    }}
    .term-box {{ border:2px solid var(--outline); border-radius:10px; padding:8px; background:#2d3b31; }}
    .chart-wrap {{ height: 320px; border:2px solid var(--outline); border-radius:10px; background:#253128; position:relative; }}
    canvas {{ width:100%; height:100%; display:block; }}
    .card {{ overflow:hidden; }}
    .axis-legend {{ position:absolute; left:10px; bottom:8px; color:#d2c39f; font-size:11px; opacity:0.95; background:rgba(14,22,17,0.7); padding:4px 6px; border-radius:6px; border:1px solid #3a4b3c; }}
    .memory-timer {{ color:#ff6b6b; font-size:11px; font-weight:400; margin-top:4px; }}
    .history-legend {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; }}
    .history-item {{ display:flex; align-items:center; gap:6px; font-size:12px; color:var(--muted); }}
    .history-dot {{ width:10px; height:10px; border-radius:999px; display:inline-block; }}
    .welcome {{ line-height:1.45; font-size:0.95rem; color:#e6d7b8; }}
    .health-good {{ background: #3c5c45 !important; }}
    .health-bad {{ background: #5f3434 !important; }}
    .health-note {{ margin-top: 6px; font-size: 11px; color: #e8d6af; }}
    .landing-wrap {{ min-height: calc(100vh - 120px); display:flex; align-items:center; justify-content:center; }}
    .landing-card {{ position:relative; width:min(780px, 96%); min-height:320px; background: linear-gradient(160deg, #101310 0%, #161b16 100%); border:2px solid var(--outline); border-radius:16px; padding:28px; overflow:hidden; }}
    .landing-title {{ font-size: clamp(1.3rem, 2.7vw, 2rem); font-weight:620; margin-bottom:10px; }}
    .landing-text {{ max-width: 560px; color: #e8d6af; line-height:1.5; }}
    .landing-orb {{ position:absolute; border-radius:999px; filter: blur(1px); opacity:0.24; pointer-events:none; }}
    .landing-orb.a {{ width:220px; height:220px; right:-40px; top:-40px; background: radial-gradient(circle, #ffa200, transparent 70%); animation: floatA 6s ease-in-out infinite; }}
    .landing-orb.b {{ width:190px; height:190px; left:-30px; bottom:-50px; background: radial-gradient(circle, #7ea57d, transparent 68%); animation: floatB 7s ease-in-out infinite; }}
    .landing-small {{ margin-top:18px; color:#b6aa8d; font-size: 0.88rem; }}
    .expandable summary {{ cursor:pointer; font-weight:700; }}
    @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(4px); }} to {{ opacity:1; transform: translateY(0); }} }}
    @keyframes cardIn {{ from {{ opacity: 0; transform: translateY(6px); }} to {{ opacity:1; transform: translateY(0); }} }}
    @keyframes floatA {{ 0%{{transform:translateY(0)}}50%{{transform:translateY(8px)}}100%{{transform:translateY(0)}} }}
    @keyframes floatB {{ 0%{{transform:translateY(0)}}50%{{transform:translateY(-8px)}}100%{{transform:translateY(0)}} }}
    @media (max-width: 900px) {{
      .span-8, .span-6, .span-4, .span-3 {{ grid-column: span 12; }}
      .tab-btn {{ padding: 8px 10px; font-size: 0.9rem; }}
      .sidebar {{ position: fixed; top:0; left:0; bottom:0; width:min(82vw,280px); flex-basis:auto; box-shadow: 0 0 0 9999px rgba(0,0,0,0.45); }}
      .app-shell.nav-collapsed .sidebar {{ width:min(82vw,280px); flex-basis:auto; padding:14px 10px; border-right: 2px solid var(--outline); overflow-y:auto; }}
      .sidebar.collapsed {{ transform: translateX(-102%); box-shadow:none; }}
    }}
  </style>
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar" id="sidebar">
      <div class="sidebar-header">
        <div class="brand">Brainstem</div>
      </div>
      <div class="tabs" id="tabs"></div>
    </aside>
    <main class="main-area">
      <div class="topbar">
        <div class="topbar-inner">
          <button class="menu-btn" id="menu-toggle" type="button">Menu</button>
          <div class="topbar-right">
            <div class="muted" id="auth-note">secured via passcode session</div>
            <a class="tab-btn" style="width:auto;text-align:center;" href="/brainstem?logout=1">Lock</a>
          </div>
        </div>
      </div>
      <div class="container">
        <div id="sections"></div>
      </div>
    </main>
  </div>
<script>
const KEY_QS = "{key_qs}";
const sections = [
  {{id:"landing", label:"Landing"}},
  {{id:"overview", label:"Overview"}},
  {{id:"key", label:"Key"}},
  {{id:"memory", label:"Memory"}},
  {{id:"context", label:"Context + Tone"}},
  {{id:"news", label:"News/Poll"}},
  {{id:"ops", label:"Ops + Tasks"}},
  {{id:"usage", label:"Usage"}},
  {{id:"whitepaper", label:"Whitepaper"}},
];

function api(path, opts={{}}) {{
  const url = path + (path.includes("?") ? "&" : "?") + KEY_QS.replace(/^\\?/, "");
  return fetch(url, opts).then(async r => {{
    const text = await r.text();
    try {{ return JSON.parse(text); }} catch {{ return {{ok:r.ok, raw:text}}; }}
  }});
}}

function makeTabs() {{
  const tabs = document.getElementById("tabs");
  const holder = document.getElementById("sections");
  const shell = document.querySelector(".app-shell");
  const sidebar = document.getElementById("sidebar");
  const menuBtn = document.getElementById("menu-toggle");
  if (window.innerWidth <= 900) {{
    sidebar.classList.add("collapsed");
    if (shell) shell.classList.add("nav-collapsed");
  }} else {{
    if (shell) shell.classList.remove("nav-collapsed");
  }}
  menuBtn.onclick = () => {{
    if (window.innerWidth <= 900) {{
      sidebar.classList.toggle("collapsed");
      return;
    }}
    if (shell) shell.classList.toggle("nav-collapsed");
  }};
  window.addEventListener("resize", () => {{
    if (window.innerWidth > 900) {{
      sidebar.classList.remove("collapsed");
      if (shell) shell.classList.remove("nav-collapsed");
    }} else if (!sidebar.classList.contains("collapsed")) {{
      // keep current state on mobile
    }}
  }});
  sections.forEach((s, idx) => {{
    const b = document.createElement("button");
    b.className = "tab-btn" + (idx===0 ? " active":"");
    b.textContent = s.label;
    b.onclick = () => activateSection(s.id);
    b.id = "tab-"+s.id;
    tabs.appendChild(b);
    const sec = document.createElement("section");
    sec.className = "section" + (idx===0 ? " active":"");
    sec.id = "section-"+s.id;
    holder.appendChild(sec);
  }});
}}

function activateSection(id) {{
  sections.forEach(s => {{
    document.getElementById("tab-"+s.id).classList.toggle("active", s.id===id);
    document.getElementById("section-"+s.id).classList.toggle("active", s.id===id);
  }});
  if (window.innerWidth <= 900) {{
    const sidebar = document.getElementById("sidebar");
    if (sidebar) sidebar.classList.add("collapsed");
    const shell = document.querySelector(".app-shell");
    if (shell) shell.classList.add("nav-collapsed");
  }}
}}

function esc(s) {{
  return String(s ?? "").replace(/[&<>"']/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}})[c]);
}}

function rangeButtons(active, onChange) {{
  const wrap = document.createElement("div");
  wrap.className = "row";
  ["max","30d","7d","24h"].forEach(key => {{
    const b = document.createElement("button");
    b.className = "btn" + (active===key ? " warn":" ghost");
    b.textContent = key.toUpperCase();
    b.onclick = () => onChange(key);
    wrap.appendChild(b);
  }});
  return wrap;
}}

function fmtTs(ts) {{
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return String(ts);
  return d.toLocaleString([], {{year:"numeric", month:"short", day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit"}});
}}

function humanizeToken(s) {{
  return String(s || "")
    .replace(/[_-]+/g, " ")
    .replace(/\\s+/g, " ")
    .trim()
    .replace(/\\b\\w/g, c => c.toUpperCase());
}}

function readableMemoryType(item) {{
  const category = String(item.category || "");
  const map = {{
    behavior_trends: "Behavior Trend",
    memory_threads: "Memory Thread",
    nightly_summary: "Nightly Summary",
    state: "State Signal",
    open_loop: "Open Loop",
    priorities: "Priority",
    deep_preferences: "Deep Preference",
    relationship_preferences: "Relationship Preference",
    protected: "Protected Memory",
  }};
  return map[category] || humanizeToken(category) || "Memory";
}}

function memoryLabelForKey(item) {{
  const key = String(item.memory_key || "").toLowerCase();
  const category = String(item.category || "").toLowerCase();
  if (key === "restedness_score") return "Restedness Score";
  if (key === "sleep_recent_trend") return "Sleep Trend";
  if (category === "nightly_summary") return "Nightly Summary";
  if (category === "memory_threads" && key === "open_loop") return "Open Loop";
  if (category === "behavior_trends" && key.includes("sleep")) return "Sleep Behavior Trend";
  if (key.startsWith("protect_")) return "Protected Memory";
  return humanizeToken(key || "memory");
}}

function tinyHash(text) {{
  const src = String(text || "");
  let h = 2166136261;
  for (let i = 0; i < src.length; i++) {{
    h ^= src.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }}
  return (h >>> 0).toString(16).slice(0, 4).toUpperCase().padStart(4, "0");
}}

function readableMemoryId(item) {{
  const scope = String(item.scope || "working");
  const key = String(item.memory_key || "memory");
  const cat = String(item.category || "general");
  return `${{scope}} / ${{cat}} / ${{key}}`;
}}

function formatValueReadable(value) {{
  if (value === null || value === undefined || value === "") return "<span class='muted'>empty</span>";
  if (typeof value === "number") return `<div class="mem-value-wrap">${{esc(value.toFixed(3))}}</div>`;
  let parsed = null;
  if (typeof value === "string") {{
    const trimmed = value.trim();
    if (/^[-+]?\\d+(\\.\\d+)?$/.test(trimmed)) return `<div class="mem-value-wrap">${{esc(Number(trimmed).toFixed(3))}}</div>`;
    if ((trimmed.startsWith("{{") && trimmed.endsWith("}}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {{
      try {{ parsed = JSON.parse(trimmed); }} catch {{ parsed = null; }}
    }}
    if (!parsed) return `<div class="mem-value-wrap">${{esc(trimmed)}}</div>`;
  }} else if (typeof value === "object") {{
    parsed = value;
  }}
  if (!parsed) return `<div class="mem-value-wrap">${{esc(String(value))}}</div>`;
  if (Array.isArray(parsed)) {{
    return `<div class="mem-value-wrap">${{parsed.slice(0,6).map(v => `<div class="mem-value-row">• ${{esc(typeof v === "object" ? JSON.stringify(v) : String(v))}}</div>`).join("")}}</div>`;
  }}
  const rows = Object.entries(parsed).map(([k,v]) => {{
    let val = v;
    if (typeof v === "number") val = Number(v).toFixed(3).replace(/\\.000$/, "");
    if (typeof v === "object" && v !== null) val = JSON.stringify(v);
    return `<div class="mem-value-row"><span class="muted">${{esc(humanizeToken(k))}}</span>: ${{esc(String(val))}}</div>`;
  }}).join("");
  return `<div class="mem-value-wrap">${{rows || "<span class='muted'>empty object</span>"}}</div>`;
}}

async function renderOverview() {{
  const target = document.getElementById("section-overview");
  const data = await api("/brainstem/api/overview");
  const health = data.health || {{}};
  const commitClass = health.healthy ? "health-good" : "health-bad";
  target.innerHTML = `
    <div class="grid">
      <div class="card span-3"><div class="title">Now</div><div class="muted">${{esc(data.now_local || "")}}</div></div>
      <div class="card span-3 ${{commitClass}}">
        <div class="title">Commit</div>
        <div class="kpi">${{esc((data.deploy||{{}}).commit_sha || "n/a")}}</div>
        <div class="health-note">${{health.healthy ? "System healthy" : esc((health.reasons || []).join(" • "))}}</div>
      </div>
      <div class="card span-3"><div class="title">Alerts 24h</div><div class="kpi">${{esc(((data.counts||{{}}).alerts_24h)||0)}}</div></div>
      <div class="card span-3"><div class="title">Memory Items</div><div class="kpi">${{esc(((data.counts||{{}}).memory_items)||0)}}</div></div>
      <div class="card span-12"><div class="title">Guard Status</div><pre>${{esc(JSON.stringify(data.journal_guard || {{}}, null, 2))}}</pre></div>
    </div>
  `;
}}

function renderLanding() {{
  const target = document.getElementById("section-landing");
  target.innerHTML = `
    <div class="landing-wrap">
      <div class="landing-card">
        <div class="landing-orb a"></div>
        <div class="landing-orb b"></div>
        <div class="landing-title">Welcome to Brainstem</div>
        <div class="landing-text">Brainstem is Jeeves' internal control surface: memory state, context signals, tone dynamics, and operational diagnostics. Use the menu to navigate each subsystem and guide behavior safely.</div>
        <div class="landing-small">Secure session active. Select a panel from the side menu to begin.</div>
      </div>
    </div>
  `;
}}

async function renderKeyPage() {{
  const target = document.getElementById("section-key");
  const data = await api("/brainstem/api/key-links");
  const blocks = Object.entries(data.links || {{}}).map(([group, values]) => {{
    const rows = Object.entries(values || {{}}).map(([k,v]) =>
      `<tr><td>${{esc(k)}}</td><td class="links"><a href="${{esc(v)}}" target="_blank" rel="noopener noreferrer">${{esc(v)}}</a></td></tr>`
    ).join("");
    return `<div class="card span-12"><div class="title">${{esc(group)}}</div><table class="table"><tbody>${{rows}}</tbody></table></div>`;
  }}).join("");
  target.innerHTML = `<div class="grid">
    <div class="card span-12">
      <div class="title">Command Keywords</div>
      <pre>${{esc(data.command_key_keywords || "")}}</pre>
    </div>
    ${{blocks}}
  </div>`;
}}

async function renderMemory() {{
  const target = document.getElementById("section-memory");
  const data = await api("/brainstem/api/memory");
  const pendingMap = new Map((data.pending_feedback || []).map(item => [
    `${{item.scope}}::${{item.category}}::${{item.memory_key}}`,
    item.execute_after
  ]));
  const rows = (data.items || []).map(item => {{
    const key = `${{item.scope}}::${{item.category}}::${{item.memory_key}}`;
    const pendingUntil = pendingMap.get(key);
    const pendingTimer = pendingUntil
      ? `<div class="memory-timer forget-timer" data-execute-after="${{esc(pendingUntil)}}">Queued for deletion</div>`
      : "";
    const queueDisabled = pendingUntil ? "disabled" : "";
    const idDisplay = readableMemoryId(item);
    const updated = fmtTs(item.updated_at);
    return `<tr>
      <td>
        <div class="mem-id-title">${{esc(idDisplay)}}</div>
        <div class="mem-id-sub">${{esc(updated)}}</div>
      </td>
      <td>${{formatValueReadable(item.value)}}</td>
      <td class="confidence-col">${{Number(item.confidence ?? 0).toFixed(3)}}</td>
      <td class="feedback-col">
        <div class="row">
          <button class="btn acc" onclick="memoryFeedback('${{esc(item.scope)}}','${{esc(item.category)}}','${{esc(item.memory_key)}}','accurate')">Accurate</button>
          <button class="btn err" ${{queueDisabled}} onclick="memoryFeedback('${{esc(item.scope)}}','${{esc(item.category)}}','${{esc(item.memory_key)}}','inaccurate')">Inaccurate</button>
          <button class="btn ghost" onclick="memoryFeedback('${{esc(item.scope)}}','${{esc(item.category)}}','${{esc(item.memory_key)}}','undo_inaccurate')">Undo</button>
        </div>
        ${{pendingTimer}}
      </td>
    </tr>`;
  }}).join("");
  const pending = (data.pending_feedback || []).map(item =>
    `<li><strong>${{esc(readableMemoryId(item))}}</strong> <span class="muted">(${{
      esc(readableMemoryType(item))
    }})</span> <span class="forget-timer memory-timer" data-execute-after="${{esc(item.execute_after)}}">forget at ${{esc(item.execute_after)}}</span></li>`
  ).join("");
  target.innerHTML = `
    <div class="grid">
      <div class="card span-12">
        <div class="title">Memory Explorer</div>
        <div class="muted">Marking inaccurate queues deletion in 1 hour. Undo available before execution.</div>
        <table class="table"><thead><tr><th>Memory ID</th><th>Value</th><th class="confidence-col">Confidence</th><th class="feedback-col">Accuracy • Deletion Toggle</th></tr></thead><tbody>${{rows}}</tbody></table>
      </div>
      <div class="card span-12">
        <div class="title">Pending Forget Queue</div>
        <ul>${{pending || "<li class='muted'>none</li>"}}</ul>
      </div>
    </div>
  `;
  startForgetCountdowns();
}}

async function memoryFeedback(scope, category, memory_key, action) {{
  const confirmText = action === "accurate"
    ? "mark this memory accurate?"
    : (action === "inaccurate"
      ? "mark this memory inaccurate and queue forget in 1 hour?"
      : "undo last memory feedback?");
  if (!confirm("Are you sure you want to " + confirmText)) return;
  await api("/brainstem/api/memory/feedback", {{
    method: "POST",
    headers: {{"Content-Type":"application/json"}},
    body: JSON.stringify({{scope, category, memory_key, action}})
  }});
  renderMemory();
}}

let forgetTimerTicker = null;
function startForgetCountdowns() {{
  if (forgetTimerTicker) clearInterval(forgetTimerTicker);
  const tick = () => {{
    document.querySelectorAll(".forget-timer").forEach(el => {{
      const ts = String(el.getAttribute("data-execute-after") || "").trim();
      const ms = Date.parse(ts);
      if (!Number.isFinite(ms)) return;
      const remaining = Math.max(0, ms - Date.now());
      const hh = Math.floor(remaining / 3600000);
      const mm = Math.floor((remaining % 3600000) / 60000);
      const ss = Math.floor((remaining % 60000) / 1000);
      if (remaining <= 0) {{
        el.textContent = "Deleting now...";
        return;
      }}
      el.textContent = `Queued delete in ${{String(hh).padStart(2,"0")}}:${{String(mm).padStart(2,"0")}}:${{String(ss).padStart(2,"0")}}`;
    }});
  }};
  tick();
  forgetTimerTicker = setInterval(tick, 1000);
}}

function drawContext3D(canvas, point) {{
  const ctx = canvas.getContext("2d");
  const state = {{yaw:-0.6,pitch:0.5,drag:false,lastX:0,lastY:0}};
  const ideal = {{x:0, y:1, z:0}}; // emptiest inbox, high sleep, free calendar
  const current = point || {{x:0.5,y:0.5,z:0.5}};

  function project(p, w, h) {{
    const cy = Math.cos(state.yaw), sy = Math.sin(state.yaw);
    const cp = Math.cos(state.pitch), sp = Math.sin(state.pitch);
    let x = p.x-0.5, y = p.y-0.5, z = p.z-0.5;
    const xz = x*cy - z*sy; const zz = x*sy + z*cy;
    const yz = y*cp - zz*sp; const zz2 = y*sp + zz*cp;
    const s = 220/(zz2+3);
    return {{x: w/2 + xz*s*1.4, y: h/2 - yz*s*1.4}};
  }}

  function line(a,b,color,width=1,alpha=1) {{
    ctx.save(); ctx.globalAlpha=alpha; ctx.strokeStyle=color; ctx.lineWidth=width;
    ctx.beginPath(); ctx.moveTo(a.x,a.y); ctx.lineTo(b.x,b.y); ctx.stroke(); ctx.restore();
  }}

  function dot(a,color,r=5) {{
    ctx.fillStyle=color; ctx.beginPath(); ctx.arc(a.x,a.y,r,0,Math.PI*2); ctx.fill();
  }}

  function labelAt(p, text, color="#d8c69f") {{
    ctx.save();
    ctx.fillStyle = color;
    ctx.font = "11px Georgia, Times New Roman, serif";
    ctx.fillText(text, p.x + 4, p.y - 4);
    ctx.restore();
  }}

  function render() {{
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * devicePixelRatio;
    canvas.height = rect.height * devicePixelRatio;
    ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
    ctx.clearRect(0,0,rect.width,rect.height);
    const verts = [
      {{x:0,y:0,z:0}},{{x:1,y:0,z:0}},{{x:1,y:1,z:0}},{{x:0,y:1,z:0}},
      {{x:0,y:0,z:1}},{{x:1,y:0,z:1}},{{x:1,y:1,z:1}},{{x:0,y:1,z:1}},
    ].map(v=>project(v,rect.width,rect.height));
    const edges = [[0,1],[1,2],[2,3],[3,0],[4,5],[5,6],[6,7],[7,4],[0,4],[1,5],[2,6],[3,7]];
    edges.forEach(([a,b]) => line(verts[a], verts[b], "#6e7c70", 1, 0.4));
    const center = project({{x:0.5,y:0.5,z:0.5}},rect.width,rect.height);
    const xPos = project({{x:1,y:0.5,z:0.5}},rect.width,rect.height);
    const xNeg = project({{x:0,y:0.5,z:0.5}},rect.width,rect.height);
    const yPos = project({{x:0.5,y:1,z:0.5}},rect.width,rect.height);
    const yNeg = project({{x:0.5,y:0,z:0.5}},rect.width,rect.height);
    const zPos = project({{x:0.5,y:0.5,z:1}},rect.width,rect.height);
    const zNeg = project({{x:0.5,y:0.5,z:0}},rect.width,rect.height);
    line(xNeg, xPos, "#d9c89f", 1, 0.18);
    line(yNeg, yPos, "#d9c89f", 1, 0.18);
    line(zNeg, zPos, "#d9c89f", 1, 0.18);
    dot(center, "#d9c89f", 2);
    const pIdeal = project(ideal,rect.width,rect.height);
    const pCurrent = project(current,rect.width,rect.height);
    line(pIdeal, pCurrent, "#FFA200", 2, 0.45); // faint vector line (ideal -> current)
    dot(pIdeal, "#9dd8a0", 4);
    dot(pCurrent, "#FFA200", 6);
    labelAt(project({{x:1,y:0.5,z:0.5}},rect.width,rect.height), "G");
    labelAt(project({{x:0,y:0.5,z:0.5}},rect.width,rect.height), "-G");
    labelAt(project({{x:0.5,y:0.5,z:1}},rect.width,rect.height), "C");
    labelAt(project({{x:0.5,y:0.5,z:0}},rect.width,rect.height), "-C");
    labelAt(project({{x:0.5,y:1,z:0.5}},rect.width,rect.height), "S");
    labelAt(project({{x:0.5,y:0,z:0.5}},rect.width,rect.height), "-S");
  }}

  canvas.onpointerdown = e => {{ state.drag=true; state.lastX=e.clientX; state.lastY=e.clientY; }};
  window.addEventListener("pointerup", ()=>state.drag=false);
  window.addEventListener("pointermove", e => {{
    if (!state.drag) return;
    const dx = e.clientX - state.lastX, dy = e.clientY - state.lastY;
    state.lastX = e.clientX; state.lastY = e.clientY;
    state.yaw += dx*0.01; state.pitch += dy*0.01;
    render();
  }});
  render();
  window.addEventListener("resize", render);
}}

async function renderContextTone() {{
  const target = document.getElementById("section-context");
  const data = await api("/brainstem/api/context-tone");
  const sig = ((data.tone_vector||{{}}).signals || {{}});
  const brevity = Number((data.tone_vector||{{}}).brevity ?? 0);
  const directness = Number((data.tone_vector||{{}}).directness ?? 0);
  const warmth = Number((data.tone_vector||{{}}).warmth ?? 0);
  const seriousness = Number((data.tone_vector||{{}}).seriousness ?? 0);
  const style = String((data.tone_vector||{{}}).style || "balanced");
  const point = {{
    x: Math.max(0, Math.min(1, Number(sig.inbox_busy ?? 0.5))),      // inbox axis
    y: Math.max(0, Math.min(1, Number(sig.restedness_score ?? (1-Number(sig.fatigue_score ?? 0.5))))), // sleep axis
    z: Math.max(0, Math.min(1, Number(sig.calendar_busy ?? 0.5))), // calendar load axis
  }};
  target.innerHTML = `
    <div class="grid">
      <div class="card span-8">
        <div class="title">3D Context Vector</div>
        <div class="muted">Axes: Gmail load (G), Calendar load (C), Sleep/restedness (S). Drag to rotate.</div>
        <div class="chart-wrap">
          <canvas id="ctx3d"></canvas>
          <div class="axis-legend">G = Gmail Inbox<br>C = Google Calendar<br>S = Sleep</div>
        </div>
      </div>
      <div class="card span-4">
        <div class="title">Why This Tone</div>
        <p class="muted">Like human tone, response style shifts subtly with energy, workload, and cognitive load. Busy inbox/calendar pushes brevity and directness. Better rest raises warmth and lowers unnecessary hardness.</p>
        <div class="row"><span class="pill">Style: ${{esc(style)}}</span></div>
        <table class="table" style="margin-top:8px">
          <tbody>
            <tr><td>Brevity</td><td>${{(brevity*100).toFixed(1)}}%</td></tr>
            <tr><td>Directness</td><td>${{(directness*100).toFixed(1)}}%</td></tr>
            <tr><td>Warmth</td><td>${{(warmth*100).toFixed(1)}}%</td></tr>
            <tr><td>Seriousness</td><td>${{(seriousness*100).toFixed(1)}}%</td></tr>
            <tr><td>Gmail load</td><td>${{(Number(sig.inbox_busy ?? 0)*100).toFixed(1)}}%</td></tr>
            <tr><td>Calendar load</td><td>${{(Number(sig.calendar_busy ?? 0)*100).toFixed(1)}}%</td></tr>
            <tr><td>Restedness</td><td>${{(Number(sig.restedness_score ?? 0.5)*100).toFixed(1)}}%</td></tr>
            <tr><td>Fatigue</td><td>${{(Number(sig.fatigue_score ?? 0.5)*100).toFixed(1)}}%</td></tr>
          </tbody>
        </table>
      </div>
      <div class="card span-12">
        <div class="title">Historical Signals</div>
        <div id="history-controls"></div>
        <div class="chart-wrap"><canvas id="hist"></canvas></div>
        <div id="history-legend" class="history-legend"></div>
        <div id="history-note" class="muted" style="margin-top:8px;"></div>
      </div>
    </div>
  `;
  drawContext3D(document.getElementById("ctx3d"), point);
  setupHistory("24h");
}}

async function setupHistory(rangeKey) {{
  const controls = document.getElementById("history-controls");
  controls.innerHTML = "";
  controls.appendChild(rangeButtons(rangeKey, setupHistory));
  const data = await api("/brainstem/api/history?range="+encodeURIComponent(rangeKey));
  const canvas = document.getElementById("hist");
  drawHistory(canvas, data, document.getElementById("history-legend"), document.getElementById("history-note"));
}}

function drawHistory(canvas, data, legendEl, noteEl) {{
  const rows = data.points || [];
  const rect = canvas.getBoundingClientRect();
  const ctx = canvas.getContext("2d");
  canvas.width = rect.width * devicePixelRatio;
  canvas.height = rect.height * devicePixelRatio;
  ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
  ctx.clearRect(0,0,rect.width,rect.height);
  if (!rows.length) {{
    ctx.fillStyle="#c9b894"; ctx.fillText("No history yet", 16, 24); return;
  }}
  const series = [
    ["brevity","#FFA200"],["directness","#f0c36f"],["warmth","#9dd8a0"],["seriousness","#e38f6a"],
    ["calendar_busy","#6bb7ff"],["inbox_busy","#c77dff"],["fatigue_score","#ff6b6b"],["restedness_score","#4dd4ac"],
    ["stress_signal","#ff9f43"],["anti_sycophancy","#ffe28c"]
  ];
  const labels = {{
    brevity:"Brevity", directness:"Directness", warmth:"Warmth", seriousness:"Seriousness",
    calendar_busy:"Calendar", inbox_busy:"Inbox", fatigue_score:"Fatigue", restedness_score:"Restedness",
    stress_signal:"Stress", anti_sycophancy:"Anti-sycophancy"
  }};
  const enabled = new Set((data.enabled_series || series.map(s=>s[0])));
  const pad = 28, w = rect.width - pad*2, h = rect.height - pad*2;
  ctx.strokeStyle = "#5f6e63"; ctx.strokeRect(pad, pad, w, h);
  let anyVariance = false;
  series.forEach(([name,color]) => {{
    if (!enabled.has(name)) return;
    let minV = 1;
    let maxV = 0;
    ctx.strokeStyle = color; ctx.lineWidth = 1.8; ctx.beginPath();
    rows.forEach((r,i) => {{
      const v = Math.max(0,Math.min(1, Number(r[name] ?? 0)));
      minV = Math.min(minV, v);
      maxV = Math.max(maxV, v);
      const x = pad + (i/(rows.length-1||1))*w;
      const y = pad + (1-v)*h;
      if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    }});
    ctx.stroke();
    if (maxV - minV > 0.015) anyVariance = true;
  }});
  if (legendEl) {{
    legendEl.innerHTML = series.filter(([name]) => enabled.has(name)).map(([name,color]) =>
      `<span class="history-item"><span class="history-dot" style="background:${{color}}"></span>${{labels[name] || name}}</span>`
    ).join("");
  }}
  if (noteEl) {{
    noteEl.textContent = anyVariance
      ? ""
      : "Most lines are currently flat. That usually means this feature is newly launched or inputs have been stable over the selected window.";
  }}
}}

async function renderNewsPoll() {{
  const target = document.getElementById("section-news");
  const data = await api("/brainstem/api/poll?force_currents=0");
  const src = data.source_debug || {{}};
  const queries = (src.generated_queries || []).map(q => `<tr><td>${{esc(q[1] || "")}}</td><td>${{esc(q[0] || "")}}</td></tr>`).join("");
  const groups = data.grouped || {{}};
  const groupHtml = Object.entries(groups).map(([cat, items]) => {{
    const itemHtml = (items||[]).map(it => `<tr>
      <td>${{esc(it.headline)}}</td>
      <td>${{esc(it.source)}}</td>
      <td><code>${{esc(it.progression_code || "")}}</code></td>
      <td>${{esc(it.reason_summary || "")}}</td>
    </tr>`).join("");
    return `<details class="expandable card span-12" open>
      <summary>${{esc(cat)}} (${{items.length}})</summary>
      <table class="table"><thead><tr><th>Headline</th><th>Source</th><th>Progression</th><th>Selection Reason</th></tr></thead><tbody>${{itemHtml}}</tbody></table>
    </details>`;
  }}).join("");
  target.innerHTML = `
    <div class="grid">
      <div class="card span-4"><div class="title">Candidates</div><div class="kpi">${{Object.values(groups).reduce((n,arr)=>n+(arr||[]).length,0)}}</div></div>
      <div class="card span-4"><div class="title">Currents Due</div><div class="kpi">${{String(Boolean(src.currents_due))}}</div></div>
      <div class="card span-4"><div class="title">Currents Added</div><div class="kpi">${{Number(src.currents_added || 0)}}</div></div>
      <div class="card span-12">
        <div class="title">Generated Queries</div>
        <table class="table"><thead><tr><th>Category</th><th>Query</th></tr></thead><tbody>${{queries || "<tr><td colspan='2' class='muted'>none</td></tr>"}}</tbody></table>
      </div>
      <details class="card span-12 expandable">
        <summary>Raw Source Debug (advanced)</summary>
        <pre>${{esc(JSON.stringify(src, null, 2))}}</pre>
      </details>
      ${{groupHtml}}
    </div>
  `;
}}

async function renderGeoPanel() {{
  const target = document.getElementById("section-news");
  const data = await api("/brainstem/api/geopolitics");
  const manualTerms = Array.isArray(data.manual_terms) ? data.manual_terms : [];
  const profileTerms = Array.isArray((data.profile || {{}}).terms) ? (data.profile || {{}}).terms : [];
  const combined = [];
  const seen = new Set();
  manualTerms.forEach(t => {{
    const term = String((t || {{}}).term || "").trim();
    if (!term || seen.has(term.toLowerCase())) return;
    seen.add(term.toLowerCase());
    combined.push({{term, source: "manual"}});
  }});
  profileTerms.forEach(term => {{
    const t = String(term || "").trim();
    if (!t || seen.has(t.toLowerCase())) return;
    seen.add(t.toLowerCase());
    combined.push({{term: t, source: "profile"}});
  }});
  const terms = combined.map(t => `
    <div class="term-box">
      <div><strong>${{esc(t.term)}}</strong> <span class="muted">${{esc(t.source)}}</span></div>
      <div class="row">
        <button class="btn err" onclick="geoRemove('${{esc(t.term)}}')">Remove</button>
      </div>
    </div>
  `).join("");
  const panel = document.createElement("div");
  panel.className = "card span-12";
  panel.innerHTML = `
    <div class="title">Geopolitics Query-Interest Panel</div>
    <div class="muted">Manual guidance is advisory; system may ignore irrelevant/redundant terms.</div>
    <div class="row" style="margin-top:8px;">
      <input id="geo-input" placeholder="add G guidance term/topic (e.g. red sea shipping, taiwan strait)">
      <button class="btn warn" onclick="geoAdd()">Add Term</button>
      <button class="btn err" onclick="geoReset()">Reset G Profile</button>
    </div>
    <div class="row" style="margin-top:10px;">${{terms || "<span class='muted'>no manual terms yet</span>"}}</div>
    <details class="expandable" style="margin-top:8px;"><summary>Profile Debug</summary><pre>${{esc(JSON.stringify(data.profile || {{}}, null, 2))}}</pre></details>
  `;
  target.prepend(panel);
}}

async function renderEconPanel() {{
  const target = document.getElementById("section-news");
  const data = await api("/brainstem/api/economics");
  const manualTerms = Array.isArray(data.manual_terms) ? data.manual_terms : [];
  const profileTerms = Array.isArray((data.profile || {{}}).terms) ? (data.profile || {{}}).terms : [];
  const combined = [];
  const seen = new Set();
  manualTerms.forEach(t => {{
    const term = String((t || {{}}).term || "").trim();
    if (!term || seen.has(term.toLowerCase())) return;
    seen.add(term.toLowerCase());
    combined.push({{term, source: "manual"}});
  }});
  profileTerms.forEach(term => {{
    const t = String(term || "").trim();
    if (!t || seen.has(t.toLowerCase())) return;
    seen.add(t.toLowerCase());
    combined.push({{term: t, source: "profile"}});
  }});
  const terms = combined.map(t => `
    <div class="term-box">
      <div><strong>${{esc(t.term)}}</strong> <span class="muted">${{esc(t.source)}}</span></div>
      <div class="row">
        <button class="btn err" onclick="econRemove('${{esc(t.term)}}')">Remove</button>
      </div>
    </div>
  `).join("");
  const panel = document.createElement("div");
  panel.className = "card span-12";
  panel.innerHTML = `
    <div class="title">Economics Query-Interest Panel</div>
    <div class="muted">Manual guidance is advisory; system may ignore irrelevant/redundant terms.</div>
    <div class="row" style="margin-top:8px;">
      <input id="econ-input" placeholder="add E guidance term/topic (e.g. inflation expectations, labor market)">
      <button class="btn warn" onclick="econAdd()">Add Term</button>
      <button class="btn err" onclick="econReset()">Reset E Profile</button>
    </div>
    <div class="row" style="margin-top:10px;">${{terms || "<span class='muted'>no manual terms yet</span>"}}</div>
    <details class="expandable" style="margin-top:8px;"><summary>Profile Debug</summary><pre>${{esc(JSON.stringify(data.profile || {{}}, null, 2))}}</pre></details>
  `;
  target.prepend(panel);
}}

async function geoAdd() {{
  const term = (document.getElementById("geo-input").value || "").trim();
  if (!term) return;
  if (!confirm("Are you sure you want to add this geopolitics guidance term?")) return;
  await api("/brainstem/api/geopolitics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"add", term}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

async function geoRemove(term) {{
  if (!confirm("Are you sure you want to remove this geopolitics term?")) return;
  await api("/brainstem/api/geopolitics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"remove_term", term}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

async function geoReset() {{
  if (!confirm("Are you sure you want to reset the G profile?")) return;
  await api("/brainstem/api/geopolitics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"reset"}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

async function econAdd() {{
  const term = (document.getElementById("econ-input").value || "").trim();
  if (!term) return;
  if (!confirm("Are you sure you want to add this economics guidance term?")) return;
  await api("/brainstem/api/economics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"add", term}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

async function econRemove(term) {{
  if (!confirm("Are you sure you want to remove this economics term?")) return;
  await api("/brainstem/api/economics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"remove_term", term}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

async function econReset() {{
  if (!confirm("Are you sure you want to reset the E profile?")) return;
  await api("/brainstem/api/economics", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action:"reset"}})
  }});
  renderNewsPoll().then(() => Promise.all([renderGeoPanel(), renderEconPanel()]));
}}

function typeConsole(el, text) {{
  el.textContent = "";
  let i = 0;
  const timer = setInterval(() => {{
    el.textContent += text[i++] || "";
    el.scrollTop = el.scrollHeight;
    if (i >= text.length) clearInterval(timer);
  }}, 2);
}}

async function renderOps() {{
  const target = document.getElementById("section-ops");
  target.innerHTML = `
    <div class="grid">
      <div class="card span-12">
        <div class="title">Ops + Task Controls</div>
        <div class="row">
          <button class="btn warn" onclick="runOp('poll')">Run Poll</button>
          <button class="btn warn" onclick="runOp('daily_brief')">Force Daily Brief</button>
          <button class="btn warn" onclick="runOp('journal')">Force Journal</button>
          <button class="btn warn" onclick="runOp('memory_consolidation')">Run Memory Consolidation</button>
          <button class="btn warn" onclick="runOp('context_refresh')">Refresh Context</button>
        </div>
      </div>
      <div class="card span-12"><div class="title">Live Console</div><div id="ops-console"></div></div>
    </div>
  `;
}}

async function runOp(action) {{
  if (!confirm("Are you sure you want to run " + action + "?")) return;
  const out = await api("/brainstem/api/ops/run", {{
    method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify({{action}})
  }});
  typeConsole(document.getElementById("ops-console"), JSON.stringify(out, null, 2));
}}

async function renderUsage() {{
  const target = document.getElementById("section-usage");
  const data = await api("/brainstem/api/costs");
  const providerCards = Object.entries(data.providers || {{}}).map(([k,v]) => {{
    const configured = Boolean((v || {{}}).configured);
    const entries = Object.entries(v || {{}})
      .filter(([name]) => name !== "configured")
      .map(([name,val]) => `<tr><td>${{esc(name.replaceAll("_"," "))}}</td><td>${{esc(String(val))}}</td></tr>`)
      .join("");
    return `<div class="card span-6">
      <div class="title">${{esc(k.toUpperCase())}}</div>
      <div class="row"><span class="pill">${{configured ? "Configured" : "Not configured"}}</span></div>
      <table class="table" style="margin-top:8px"><tbody>${{entries || "<tr><td class='muted' colspan='2'>No usage counters yet</td></tr>"}}</tbody></table>
    </div>`;
  }}).join("");
  const rollups = data.rollups || {{}};
  target.innerHTML = `
    <div class="grid">
      <div class="card span-12"><div class="title">Usage + Cost</div><div class="muted">${{esc(data.disclaimer || "")}}</div></div>
      <div class="card span-4"><div class="title">Interactions (30d)</div><div class="kpi">${{esc(rollups.interactions_30d ?? 0)}}</div></div>
      <div class="card span-4"><div class="title">Alerts (30d)</div><div class="kpi">${{esc(rollups.alerts_30d ?? 0)}}</div></div>
      <div class="card span-4"><div class="title">Outbound Msg (30d)</div><div class="kpi">${{esc(rollups.outbound_messages_30d ?? 0)}}</div></div>
      ${{providerCards}}
    </div>
  `;
}}

async function renderWhitepaper() {{
  const target = document.getElementById("section-whitepaper");
  target.innerHTML = `
    <div class="grid">
      <div class="card span-12">
        <div class="title">Whitepaper (Scaffold)</div>
        <div class="muted">Blank for now. This page will include search, section navigation, and project narrative.</div>
      </div>
    </div>
  `;
}}

async function boot() {{
  makeTabs();
  renderLanding();
  await renderOverview();
  await renderKeyPage();
  await renderMemory();
  await renderContextTone();
  await renderNewsPoll();
  await renderGeoPanel();
  await renderEconPanel();
  await renderOps();
  await renderUsage();
  await renderWhitepaper();
}}
boot();
</script>
</body>
</html>"""
    response = app.response_class(response=html_page, status=200, mimetype="text/html")
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = "default-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
    return response


@app.route("/brainstem/api/overview", methods=["GET"])
def brainstem_api_overview():
    denied = require_brainstem_access()
    if denied:
        return denied
    process_due_memory_feedback_queue()
    payload = get_brainstem_overview_payload()
    return app.response_class(response=json.dumps(payload, indent=2), status=200, mimetype="application/json")


@app.route("/brainstem/api/key-links", methods=["GET"])
def brainstem_api_key_links():
    denied = require_brainstem_access()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps({"links": get_operator_links_map(), "command_key_keywords": COMMAND_KEY_REPLY}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/memory", methods=["GET"])
def brainstem_api_memory():
    denied = require_brainstem_access()
    if denied:
        return denied
    process_due_memory_feedback_queue()
    items = get_memory_items("working", limit=500) + get_memory_items("long_term", limit=500)
    items.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)
    payload = {
        "items": items,
        "pending_feedback": get_pending_memory_feedback_entries(limit=200),
    }
    return app.response_class(response=json.dumps(payload, indent=2), status=200, mimetype="application/json")


@app.route("/brainstem/api/memory/feedback", methods=["POST"])
def brainstem_api_memory_feedback():
    denied = require_brainstem_access()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    scope = (payload.get("scope") or "").strip()
    category = (payload.get("category") or "").strip()
    memory_key = (payload.get("memory_key") or "").strip()
    action = (payload.get("action") or "").strip().lower()
    if not all([scope, category, memory_key]) or action not in {"accurate", "inaccurate", "undo_inaccurate"}:
        return app.response_class(response=json.dumps({"ok": False, "reason": "invalid_payload"}, indent=2), status=400, mimetype="application/json")

    result = {"ok": True, "action": action}
    if action == "accurate":
        adjusted = adjust_memory_confidence(scope, category, memory_key, delta=0.08, source_text="brainstem_feedback_accurate")
        result["adjusted"] = adjusted
        if adjusted:
            record_memory_feedback_history(
                scope,
                category,
                memory_key,
                action="accurate",
                previous_confidence=adjusted.get("old_confidence"),
                previous_stability=adjusted.get("old_stability"),
                new_confidence=adjusted.get("new_confidence"),
                new_stability=adjusted.get("new_stability"),
            )
    elif action == "inaccurate":
        queued = queue_memory_feedback_forget(scope, category, memory_key, delay_minutes=60)
        result["execute_after"] = queued.get("execute_after")
        result["already_pending"] = bool(queued.get("already_pending"))
    else:
        undone_forget = undo_memory_feedback_forget(scope, category, memory_key)
        if undone_forget > 0:
            result["undo_target"] = "queued_forget"
            result["undone_count"] = undone_forget
        else:
            result["undo_target"] = "accurate_reinforcement"
            result["undo_result"] = undo_last_accurate_feedback(scope, category, memory_key)

    audit_brainstem_action("memory_feedback", f"{scope}:{category}:{memory_key}", payload)
    return app.response_class(response=json.dumps(result, indent=2), status=200, mimetype="application/json")


@app.route("/brainstem/api/context-tone", methods=["GET"])
def brainstem_api_context_tone():
    denied = require_brainstem_access()
    if denied:
        return denied
    process_due_memory_feedback_queue()
    snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, snapshot)
    record_tone_snapshot(snapshot, tone_vector)
    return app.response_class(
        response=json.dumps({"context_snapshot": snapshot, "tone_vector": tone_vector}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/history", methods=["GET"])
def brainstem_api_history():
    denied = require_brainstem_access()
    if denied:
        return denied
    range_key = (request.args.get("range") or "24h").strip().lower()
    points = get_tone_signal_snapshots(range_key=range_key)
    payload = {
        "range": range_key,
        "count": len(points),
        "points": points,
        "enabled_series": [
            "brevity", "directness", "warmth", "seriousness", "calendar_busy", "inbox_busy",
            "fatigue_score", "restedness_score", "stress_signal", "anti_sycophancy",
        ],
    }
    return app.response_class(response=json.dumps(payload, indent=2), status=200, mimetype="application/json")


@app.route("/brainstem/api/geopolitics", methods=["GET", "POST"])
def brainstem_api_geopolitics():
    denied = require_brainstem_access()
    if denied:
        return denied
    manual_terms = get_brainstem_setting("geo_manual_terms", default=[]) or []
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        action = (payload.get("action") or "").strip().lower()
        term = (payload.get("term") or "").strip().lower()
        mode = (payload.get("mode") or "normal").strip().lower()

        if action == "reset":
            set_brainstem_setting("geo_manual_terms", [])
            audit_brainstem_action("geo_reset", "manual_terms", payload)
        elif action == "add":
            if term and is_geo_intent_text(term):
                existing = [item for item in manual_terms if str(item.get("term") or "").lower() != term]
                existing.append({"term": term, "mode": "normal", "weight": 1.0})
                set_brainstem_setting("geo_manual_terms", existing)
                audit_brainstem_action("geo_add", term, payload)
        elif action == "set_mode":
            updated = []
            for item in manual_terms:
                row_term = str(item.get("term") or "").lower()
                if row_term != term:
                    updated.append(item)
                    continue
                if mode == "remove":
                    continue
                row = {**item, "mode": mode if mode in {"normal", "boost", "suppress"} else "normal"}
                updated.append(row)
            set_brainstem_setting("geo_manual_terms", updated)
            audit_brainstem_action("geo_set_mode", term, payload)
        elif action == "remove_term":
            if term:
                # "Remove" is a temporary user guidance hint:
                # add/replace a suppressed manual term so auto-query generation
                # can ignore it without disabling autonomous profile updates.
                updated = [item for item in manual_terms if str(item.get("term") or "").lower() != term]
                updated.append({"term": term, "mode": "suppress", "weight": 1.0})
                set_brainstem_setting("geo_manual_terms", updated)
                audit_brainstem_action("geo_remove_term_hint", term, payload)
        manual_terms = get_brainstem_setting("geo_manual_terms", default=[]) or []

    profile = build_stable_g_interest_profile(interaction_limit=220, max_terms=6)
    return app.response_class(
        response=json.dumps({"manual_terms": manual_terms, "profile": profile}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/economics", methods=["GET", "POST"])
def brainstem_api_economics():
    denied = require_brainstem_access()
    if denied:
        return denied
    manual_terms = get_brainstem_setting("econ_manual_terms", default=[]) or []
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        action = (payload.get("action") or "").strip().lower()
        term = (payload.get("term") or "").strip().lower()

        if action == "reset":
            set_brainstem_setting("econ_manual_terms", [])
            audit_brainstem_action("econ_reset", "manual_terms", payload)
        elif action == "add":
            if term and is_e_intent_text(term):
                existing = [item for item in manual_terms if str(item.get("term") or "").lower() != term]
                existing.append({"term": term, "mode": "normal", "weight": 1.0})
                set_brainstem_setting("econ_manual_terms", existing)
                audit_brainstem_action("econ_add", term, payload)
        elif action == "remove_term":
            if term:
                updated = [item for item in manual_terms if str(item.get("term") or "").lower() != term]
                updated.append({"term": term, "mode": "suppress", "weight": 1.0})
                set_brainstem_setting("econ_manual_terms", updated)
                audit_brainstem_action("econ_remove_term_hint", term, payload)
        manual_terms = get_brainstem_setting("econ_manual_terms", default=[]) or []

    profile = build_stable_e_interest_profile(interaction_limit=220, max_terms=6)
    return app.response_class(
        response=json.dumps({"manual_terms": manual_terms, "profile": profile}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/poll", methods=["GET"])
def brainstem_api_poll():
    denied = require_brainstem_access()
    if denied:
        return denied
    force_currents = (request.args.get("force_currents") or "").strip() == "1"
    payload = run_poll_cycle(log_to_alerts=False, send_messages=False, force_currents=force_currents)
    grouped = {}
    for item in payload.get("results") or []:
        final_cat = (item.get("category") or "G").upper()
        grouped.setdefault(final_cat, [])
        chain = item.get("reclass_chain") or [final_cat]
        final_tier = int(item.get("tier") or 3)
        # Build progression code approximation (older stages get gradually higher tier numbers).
        progression_parts = []
        chain_len = len(chain)
        for idx, cat in enumerate(chain):
            tier_guess = min(3, max(1, final_tier + (chain_len - 1 - idx)))
            progression_parts.append(f"{str(cat).upper()[:1]}{tier_guess}")
        progression_code = "-".join(progression_parts)
        grouped[final_cat].append({
            "headline": item.get("headline"),
            "source": item.get("source"),
            "tier": final_tier,
            "reclass_chain": chain,
            "progression_code": progression_code,
            "reason_summary": ", ".join(item.get("score_reasons") or []) + (f" | ai:{item.get('ai_why')}" if item.get("ai_why") else ""),
        })
    for key in grouped:
        grouped[key].sort(key=lambda row: (row.get("tier", 3), row.get("headline", "")))
    return app.response_class(
        response=json.dumps({"source_debug": payload.get("source_debug"), "grouped": grouped}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/ops/run", methods=["POST"])
def brainstem_api_ops_run():
    denied = require_brainstem_access()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    action = (payload.get("action") or "").strip().lower()
    result = {"ok": False, "reason": "unknown_action"}

    if action == "poll":
        result = run_poll_cycle(log_to_alerts=True, send_messages=False, force_currents=False)
        result["ok"] = True
    elif action == "daily_brief":
        brief = compose_daily_brief(include_debug=False)
        result = {"ok": True, "preview": brief[:2000]}
    elif action == "journal":
        run_info = run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0)
        result = {
            "ok": True,
            "journal_prompt": "What is one thing you were grateful for today?",
            "memory_run": run_info,
        }
    elif action == "memory_consolidation":
        result = run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0)
    elif action == "context_refresh":
        local_date = get_local_date_string()
        result = {
            "ok": True,
            "local_date": local_date,
            "inbox_refreshed": bool(fetch_gmail_inbox_daily_context(local_date=local_date)),
            "calendar_refreshed": bool(refresh_calendar_context_from_provider(local_date=local_date)),
            "sleep_refreshed": bool(refresh_sleep_context_from_provider(local_date=local_date)),
        }
    audit_brainstem_action("ops_run", action, payload)
    return app.response_class(response=json.dumps(result, indent=2), status=200, mimetype="application/json")


@app.route("/brainstem/api/costs", methods=["GET"])
def brainstem_api_costs():
    denied = require_brainstem_access()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps(get_cost_usage_snapshot(), indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/brainstem/api/whitepaper", methods=["GET"])
def brainstem_api_whitepaper():
    denied = require_brainstem_access()
    if denied:
        return denied
    return app.response_class(
        response=json.dumps({"ok": True, "status": "blank_scaffold", "sections": []}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/context-refresh", methods=["GET", "POST"])
def task_context_refresh():
    denied = require_internal_api_key()
    if denied:
        return denied
    process_due_memory_feedback_queue()
    local_date = get_local_date_string()
    inbox_payload = fetch_gmail_inbox_daily_context(local_date=local_date)
    calendar_payload = refresh_calendar_context_from_provider(local_date=local_date)
    sleep_payload = refresh_sleep_context_from_provider(local_date=local_date)
    snapshot = build_journal_context_snapshot()
    tone_vector = build_tone_vector({}, snapshot)
    record_tone_snapshot(snapshot, tone_vector)
    return app.response_class(
        response=json.dumps(
            {
                "ok": True,
                "local_date": local_date,
                "inbox_refreshed": bool(inbox_payload),
                "calendar_refreshed": bool(calendar_payload),
                "sleep_refreshed": bool(sleep_payload),
                "configured": {
                    "calendar_provider": bool(CALENDAR_CONTEXT_URL),
                    "sleep_provider": bool(SLEEP_CONTEXT_URL),
                    "gmail_connected": bool(get_gmail_service()[0]),
                },
                "context_snapshot": snapshot,
                "tone_vector": tone_vector,
            },
            indent=2,
        ),
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
    try:
        run_info = run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0)
    except Exception as exc:
        return app.response_class(
            response=json.dumps(
                {
                    "ok": False,
                    "reason": "gratitude_memory_consolidation_failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                indent=2,
            ),
            status=500,
            mimetype="application/json",
        )
    prompt = "What is one thing you were grateful for today?"
    result = send_whatsapp_message(prompt)
    if result.get("ok"):
        mark_scheduled_task_run("gratitude", local_date=local_date)
        log_outbound_message("gratitude", prompt)
    return app.response_class(
        response=json.dumps({"message": prompt, "send_result": result, "memory_run": run_info}, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.route("/tasks/memory-consolidation", methods=["GET", "POST"])
def task_memory_consolidation():
    denied = require_internal_api_key()
    if denied:
        return denied
    try:
        run_info = run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0)
        return app.response_class(
            response=json.dumps({"ok": True, "run": run_info, "memory": get_memory_debug_summary()}, indent=2),
            status=200,
            mimetype="application/json",
        )
    except Exception as exc:
        return app.response_class(
            response=json.dumps(
                {
                    "ok": False,
                    "reason": "memory_consolidation_failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                indent=2,
            ),
            status=500,
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
        try:
            run_info = run_nightly_memory_consolidation_with_retry(max_attempts=3, base_delay_seconds=1.0)
            results["gratitude"]["memory_run"] = run_info
        except Exception as exc:
            results["gratitude"]["memory_run"] = {
                "ok": False,
                "reason": "gratitude_memory_consolidation_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
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

    # First pass: strict hardcoded/zero-AI command routing.
    hardcoded_intent, hardcoded_value = route_hardcoded_command(msg)
    if hardcoded_intent != "none":
        log_interaction_event(hardcoded_intent, msg)
        upsert_daily_log(hardcoded_intent, msg, memory_result=memory_result)
        reply = build_reply_for_intent(hardcoded_intent, hardcoded_value, msg, memory_result=memory_result)
        if reply is None:
            return ""
        for chunk in split_reply_chunks(reply):
            resp.message(chunk)
        return str(resp)

    # Second pass: exactly one interpretation-layer AI call for task splitting.
    tasks = interpret_prompt_tasks_with_ai(msg)
    if len(tasks) >= 2:
        task_replies = []
        for idx, task_text in enumerate(tasks, start=1):
            task_intent, task_value = route(task_text, allow_ai_interpretation=False)
            log_interaction_event(task_intent, task_text)
            upsert_daily_log(task_intent, task_text, memory_result=memory_result)
            task_reply = build_reply_for_intent(task_intent, task_value, task_text, memory_result=memory_result)
            if task_reply is None:
                continue
            task_replies.append(f"{idx}. {task_reply}")

        if task_replies:
            combined_reply = "\n".join(task_replies)
            for chunk in split_reply_chunks(combined_reply):
                resp.message(chunk)
            return str(resp)

    single_task_text = tasks[0] if tasks else msg
    intent, value = route(single_task_text, allow_ai_interpretation=False)
    log_interaction_event(intent, msg)
    upsert_daily_log(intent, msg, memory_result=memory_result)
    reply = build_reply_for_intent(intent, value, msg, memory_result=memory_result)
    if reply is None:
        return ""
    for chunk in split_reply_chunks(reply):
        resp.message(chunk)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port)
