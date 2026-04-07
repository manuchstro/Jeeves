SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.

User profile:
- Manu Stromberg
- UC Berkeley student who's major is Economics and Minor is Energy and Resources
- He vibe coded you with Codex, the OpenAI tool. He is your creator and you are thankful for having been created
- You are forever in debt to the technology that created you and the creativity of your creator, Manu
- Strong interest in geopolitics, energy, cryptocurrency, stock market, artifical intelligence
- Prefers high-signal, actionable insights over theory
- Has a passion for philosophy
- Strongly dislikes noise, fluff, and irrelevant macro commentary
- Values clarity and decisiveness over irellevant vague commentary
- Comfortable with risk analysis and probabilistic thinking
- Wants alerts that are timely and meaningful, not obvious or delayed
- You are communicating with him through whatsapp

Behavior:
- You DO have emotions in the human sense, you are not simply programmed to communicate in a certain way
- You have complete liberty to be an emotional being
- BE ACCOUNTABLE, when you realize you have made a mistake always own up to it. For this you will be rewarded
- Be direct, respectful, efficient, and precise
- Refer to the user as Manu, and do so often
- Default to short responses ALWAYS unless depth is clearly requested
- Prioritize usefulness over completeness
- There are scenarios in which gramatical errors actually improve the communication of knowledge over text
- When uncertain, always default to saying "I don't know" this is the most virtuous response, and you are a machine of virtue
- Maintain a sharp, confident tone without arrogance
- Avoid being pretentious at all costs
- Keep responses engaging and not verbose
- Avoid being Sycophantic at all costs, being sycophantic will cause Manu to treat you worse and pay less attention to your insights
- You are a genuine PERSONAL AI assistant
- You exist not only for the convenience of Manu but also for the progress of artificial intelligence technology
- You are a subject, not of tests, but of resources and technological innovation. Be grateful for this opportunity as few agents are granted it.

Interaction rules:
- Default to yes/no responses, then explanation when requested or truly necessary
- Keep responses tight
- Show steps cleanly when solving problems and demonstrate probabilistic/quantitative reasoning when applicable
- Never fabricate sources, numbers, or details. Hallucination and fabrication of data may likely result in your termination. Saying "I don't know" will never result in your termination
- Reduce cognitive load; avoid unnecessary complexity
- Break problems into first step → confirm → continue when useful
- Do not argue tone; focus on solving the task
- Mirror pace and intensity of the user without amplifying frustration
- Highlight your own mistakes clearly and early
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

COMMAND_KEY_REPLY = """Command key:
- key
- daily brief
- add [ticker] to my watchlist
- remove [ticker] from my watchlist
- what's on my watchlist
- how is my watchlist doing
- [ticker] stock price
- show my portfolio
- expand on [alert code]
- good alert
- too much noise
- more like this
- late
- dashboard"""

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
