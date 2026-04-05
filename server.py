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
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = os.environ.get("DB_PATH", "jeeves.db")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
NYT_API_KEY = os.environ.get("NYT_API_KEY")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
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


def get_recent_gratitude(limit=7):
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


def format_memory_context():
    working = get_memory_items("working", limit=8)
    long_term = get_memory_items("long_term", limit=12)

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


def process_memory_updates(text):
    updates, gratitude_entry = extract_memory_updates(text)

    for update in updates:
        upsert_memory(
            update["scope"],
            update["category"],
            update["memory_key"],
            update["value"],
            source_text=text,
            confidence=update["confidence"],
        )

    if gratitude_entry:
        add_gratitude_entry(gratitude_entry)
        upsert_memory(
            "working",
            "gratitude",
            "latest_gratitude",
            gratitude_entry,
            source_text=text,
            confidence=0.9,
        )


def get_memory_debug_summary():
    return {
        "working_memory": get_memory_items("working", limit=20),
        "long_term_memory": get_memory_items("long_term", limit=20),
        "recent_gratitude": get_recent_gratitude(limit=10),
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
    from_number = request.form.get("From",""
        ).replace("whatsapp:","")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    process_memory_updates(msg)
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
        messages = [
            {"role":"system","content":SYSTEM_PROMPT},
            {"role":"system","content":format_memory_context()},
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
