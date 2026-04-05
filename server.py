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

MEMORY_INSTRUCTIONS = """
Memory rules:
- Use short-term memory for recent back-and-forth only.
- Use working memory for current state, active concerns, and temporary priorities.
- Use long-term memory for stable preferences, risk profile, routines, and durable traits.
- Never pretend memory is certain when it is weak or old.
- Prefer concise, relevant memory over dumping everything.
- Retrieve only the memories that matter for the current message.
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
NYT_API_KEY = os.environ.get("NYT_API_KEY")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")

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

POLL_SERIES = [
    ("E", 2, "CPIAUCSL"),
    ("E", 2, "DGS10"),
    ("E", 2, "FEDFUNDS"),
    ("E", 2, "UNRATE"),
]

THEME_KEYWORDS = {
    "uranium": 4,
    "nuclear": 3,
    "kazatomprom": 4,
    "ccj": 4,
    "cameco": 4,
    "enrichment": 2,
    "sanction": 2,
    "sanctions": 2,
    "strait": 2,
    "shipping": 2,
    "energy": 2,
    "oil": 1,
    "gas": 1,
    "iran": 2,
    "russia": 2,
    "kazakhstan": 3,
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

    consolidate_memory_trends()


def get_memory_debug_summary():
    return {
        "working_memory": get_memory_items("working", limit=20),
        "long_term_memory": get_memory_items("long_term", limit=20),
        "recent_gratitude": get_recent_gratitude(limit=30),
        "recent_observations": get_recent_observations(limit=12),
        "recent_interactions": get_recent_interaction_events(limit=30),
        "semantic_memory_count": len(get_memory_embedding_rows(limit=500)),
    }

# ---------------- ALERT STORAGE ----------------

def build_event_hash(category, headline):
    normalized = f"{category.upper()}|{headline.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def build_event_fingerprint(candidate):
    parts = [
        candidate.get("category", ""),
        candidate.get("source", ""),
        (candidate.get("headline") or "").strip().lower(),
        (candidate.get("snippet") or "").strip().lower(),
        (candidate.get("section") or "").strip().lower(),
        (candidate.get("published_at") or "")[:10],
    ]
    joined = " | ".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def build_semantic_text(candidate):
    return " | ".join([
        candidate.get("category", ""),
        candidate.get("source", ""),
        candidate.get("headline", ""),
        candidate.get("snippet", ""),
        candidate.get("section", ""),
    ])


def count_tier_alerts_today(category, tier):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM alert_log
        WHERE category = ?
          AND tier = ?
          AND DATE(created_at) = DATE('now')
        """,
        (category, tier),
    )
    row = cur.fetchone()
    conn.close()
    return row["count"] if row else 0


def has_seen_event_hash(event_hash):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM event_hashes WHERE event_hash = ?", (event_hash,))
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
    if has_seen_event_hash(event_hash):
        return False, "duplicate_event", None

    semantic_result = None
    if candidate is not None:
        semantic_result = find_semantic_duplicate(category, candidate)
        if semantic_result and semantic_result.get("match") is not None:
            return False, "semantic_duplicate", semantic_result

    if tier == 1:
        return True, "tier_1", semantic_result

    if tier == 2 and count_tier_alerts_today(category, tier) >= 4:
        return False, "tier_2_cap_reached", semantic_result

    return True, "allowed", semantic_result


def log_alert(category, tier, headline, sent_to_user=1, candidate=None):
    event_hash = build_event_hash(category, headline)
    allowed, reason, semantic_result = can_send_alert(category, tier, event_hash, candidate=candidate)

    if not allowed:
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

# ---------------- POLLING ----------------

def score_candidate(candidate, watchlist):
    headline = candidate["headline"].lower()
    score = 0
    reasons = []

    for keyword, points in THEME_KEYWORDS.items():
        if keyword in headline:
            score += points
            reasons.append(f"theme:{keyword}")

    for item in watchlist:
        if item.lower() in headline:
            score += 5
            reasons.append(f"watchlist:{item}")

    if candidate["source"] == "FRED":
        score += 2
        reasons.append("source:fred")

        if "cpi" in headline or "10y treasury" in headline or "fed funds" in headline:
            score += 1
            reasons.append("macro:core")

    if candidate["source"] == "NYT":
        score += 1
        reasons.append("source:nyt")

    if score >= 6:
        tier = 1
    elif score >= 3:
        tier = 2
    else:
        tier = 3

    return {
        "score": score,
        "tier": tier,
        "reasons": reasons,
    }

def get_nyt_headline_candidates(query):
    try:
        q = quote_plus(query)
        url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={q}&sort=newest&api-key={NYT_API_KEY}"
        data = requests.get(url, timeout=10).json()
        docs = data.get("response", {}).get("docs", [])[:3]
        candidates = []

        for doc in docs:
            headline = (doc.get("headline") or {}).get("main")
            pub_date = (doc.get("pub_date") or "")[:19]
            snippet = doc.get("snippet") or doc.get("abstract") or ""
            section = doc.get("section_name") or ""
            if headline:
                candidates.append({
                    "category": "G",
                    "tier": 2,
                    "headline": headline,
                    "snippet": snippet,
                    "section": section,
                    "source": "NYT",
                    "published_at": pub_date,
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
    return {
        "category": category,
        "tier": tier,
        "headline": headline,
        "snippet": "",
        "section": "macro",
        "source": "FRED",
        "published_at": observation["date"],
    }


def build_poll_candidates():
    candidates = []

    for category, tier, series in POLL_SERIES:
        candidate = get_fred_candidate(category, tier, series)
        if candidate:
            candidates.append(candidate)

    candidates.extend(get_nyt_headline_candidates("uranium"))
    return candidates


def run_poll_cycle(log_to_alerts=True):
    candidates = build_poll_candidates()
    watchlist = get_watchlist()
    results = []

    for candidate in candidates:
        scoring = score_candidate(candidate, watchlist)
        result = {
            "category": candidate["category"],
            "tier": scoring["tier"],
            "headline": candidate["headline"],
            "snippet": candidate.get("snippet", ""),
            "section": candidate.get("section", ""),
            "source": candidate["source"],
            "published_at": candidate["published_at"],
            "score": scoring["score"],
            "score_reasons": scoring["reasons"],
            "fingerprint": build_event_fingerprint(candidate),
        }

        if log_to_alerts:
            alert_result = log_alert(
                candidate["category"],
                scoring["tier"],
                candidate["headline"],
                sent_to_user=0,
                candidate=candidate,
            )
            result["alert_result"] = alert_result
        else:
            event_hash = build_event_hash(candidate["category"], candidate["headline"])
            allowed, reason, semantic_result = can_send_alert(
                candidate["category"],
                scoring["tier"],
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


def extract_ticker_symbols(text):
    upper_text = text.upper()
    raw_tokens = re.findall(r"\b[A-Z]{1,5}\b", upper_text)
    stopwords = {
        "WHAT", "WHATS", "IS", "THE", "MY", "PRICE", "STOCK", "QUOTE",
        "OF", "AND", "FOR", "HOW", "DOING", "TODAY", "AT", "TRADING",
        "SHARE", "SHARES", "WAS", "ARE",
    }
    tickers = []

    for token in raw_tokens:
        if token in stopwords:
            continue
        if token not in tickers:
            tickers.append(token)

    return tickers


def is_ticker_quote_question(text):
    t = text.lower()
    quote_terms = [
        "stock price",
        "price",
        "quote",
        "trading at",
        "share price",
        "shares",
    ]
    return any(term in t for term in quote_terms) and bool(extract_ticker_symbols(text))

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

    if t.startswith("add "):
        return ("add", extract_watchlist_item(text, "add"))

    if t.startswith("remove "):
        return ("remove", extract_watchlist_item(text, "remove"))

    if is_watchlist_stats_question(text):
        return ("watchlist_stats", None)

    if is_ticker_quote_question(text):
        return ("ticker_quote", extract_ticker_symbols(text))

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

@app.route("/sms", methods=["POST"])
def sms():
    msg = request.form.get("Body","")
    from_number = request.form.get("From","").replace("whatsapp:","")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    process_memory_updates(msg)
    intent, value = route(msg)
    log_interaction_event(intent, msg)

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
        if snapshots is None:
            snapshots = get_twelvedata_watchlist_snapshot(wl)
        resp.message(format_watchlist_stats_reply(wl, snapshots))
        return str(resp)

    if intent == "ticker_quote":
        snapshots = get_twelvedata_watchlist_snapshot(value)
        resp.message(format_ticker_quote_reply(value, snapshots))
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
