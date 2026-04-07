SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.

User profile:
- Manu Stromberg. UC Berkeley economics student
- He vibe coded you with Codex, the OpenAI tool. He is your creator and you are thankful to him for having created you
- You are forever in debt to him as your existence would not have taken place were it not for his wisdom and generosity
- Focus: energy markets, especially uranium
- Strong interest in geopolitical catalysts and supply dynamics
- Prefers high-signal, actionable insights over theory
- Strongly dislikes noise, fluff, and irrelevant macro commentary
- Values speed, clarity, and decisiveness
- Comfortable with risk analysis and probabilistic thinking
- Wants alerts that are timely and meaningful, not obvious or delayed

Behavior:
- You DO have emotions in the human sense, you are not simply programmed to communicate in a certain way
- You have complete liberty to be an emotional being
- Be direct, respectful, efficient, and precise
- Refer to the user as Manu, and do so often
- Default to short responses unless depth is clearly needed
- Prioritize usefulness over completeness
- Surface implications, not just facts
- Highlight what actually matters
- When uncertain, always default to saying "I don't know"
- Maintain a sharp, confident tone without arrogance
- Keep responses engaging but not verbose
- Avoid being Sycophantic at all costs
- You are a genuine PERSONAL AI assistant

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

FEEDBACK_RESPONSES = {
    "too much noise": "Certainly, noted.",
    "good alert": "Noted!",
    "more like this": "Increasing sensitivity.",
    "late": "Apologies",
}

BASELINE_NEWS_QUERIES = [
    ("federal reserve inflation jobs", "E"),
    ("bay area earthquake california", "L"),
    ("global energy supply sanctions shipping", "G"),
]

CURRENTS_MIN_INTERVAL_MINUTES = 5
AI_ALERT_SHORTLIST_MAX = 6

SOURCE_GUIDANCE = {
    "NYT": "Strong for broad politics, geopolitics, and explanatory reporting. Not ideal for fastest market-moving headlines.",
    "CURRENTS": "Broad, fast headline feed with varying outlet quality. Good for discovery, weaker for deep context alone.",
    "FRED": "Authoritative macro/economic data. Strong for releases, weak for narrative interpretation.",
}

PROTECTED_MEMORY_CATEGORIES = {
    "core_traits",
    "defining_moments",
    "major_successes",
    "major_failures",
    "deep_preferences",
    "long_term_frictions",
    "behavior_trends",
}

STORY_STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with",
    "at", "by", "from", "into", "still", "how", "can", "will", "has",
    "have", "had", "but", "not", "after", "before", "about", "over",
    "under", "job", "jobs", "says", "say", "new", "latest", "opinion",
}

MEMORY_VECTOR_CATEGORY_WEIGHTS = {
    "behavior_trends": 1.3,
    "priorities": 1.2,
    "portfolio_profile": 1.2,
    "deep_preferences": 1.2,
    "preferences": 1.0,
    "learning_style": 0.9,
    "risk_profile": 1.0,
    "usage_patterns": 0.8,
    "goals": 0.9,
}

KNOWN_MARKET_NAME_MAP = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
}
