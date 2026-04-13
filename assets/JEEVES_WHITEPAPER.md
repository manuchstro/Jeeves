JEEVES WHITEPAPER

Version: 2026-04-13
Source of truth: /Users/manustromberg/Documents/Jeeves/server.py and /Users/manustromberg/Documents/Jeeves/ROADMAP.md

============================================================
PAPER I: JEEVES AS A HUMAN PRODUCTIVITY SYSTEM (NON-TECHNICAL)
============================================================

Abstract
Jeeves is a personal operating system for cognition, workload, and execution. It reduces friction between intention and action by combining three core ideas: persistent memory, live context ingestion, and adaptive communication style. The goal is not to imitate a person. The goal is to increase output quality, reduce decision fatigue, and preserve user trust under real-world noise.

1. Problem Statement
Most assistants fail in practice for predictable reasons:
- They respond in the moment but do not accumulate stable understanding.
- They are disconnected from operational context (calendar, inbox, sleep, markets, portfolio truth).
- They cannot reliably separate high-signal actions from low-signal noise.
- They overuse language fluency where deterministic behavior is required.
- They lack user-visible controls for correction, reinforcement, and audit.

Jeeves addresses these by design:
- Memory is structured, scored, decayed, reinforced, and auditable.
- Context is refreshed from external providers and normalized into deterministic features.
- Alerting uses tiering, filters, dedupe, and delivery retries.
- Interpretation and response are split so deterministic handlers can bypass unnecessary AI calls.
- Brainstem exposes internals for direct operator control.

2. What Jeeves Is
Jeeves is not one model call. Jeeves is a stateful, multi-loop system.

Core loops:
- Inbound interaction loop (SMS/WhatsApp): classify, interpret, route, answer, log.
- Context refresh loop: ingest inbox/calendar/sleep, compute tone signals, snapshot history.
- Poll loop: generate and score candidates, shortlist, gate to tier-1 live alerts, retry failed delivery.
- Consolidation loop: convert short-term traces into durable memory with guardrails.

3. User Value
Jeeves provides value in four dimensions:

3.1 Decision Compression
The system summarizes large context windows into actionable outputs:
- "What matters now?"
- "What is changing?"
- "What requires action?"

3.2 Context-Dependent Tone
The tone matrix adjusts brevity, directness, warmth, and seriousness using objective signals:
- Calendar load
- Inbox load
- Restedness/fatigue
- Stress proxies

Tone adaptation is simulated empathy, not emotional inference.

3.3 Durable Recall
Jeeves retains patterns over time:
- behavior trends
- recurring frictions
- open loops
- journal-derived state and shift signals

3.4 Trust Through Transparency
Brainstem makes internal state inspectable:
- memory entries and confidence
- queued forget operations with delayed delete
- context snapshots and tone signals
- poll diagnostics, category assignments, and outcomes

4. Why Brainstem Exists
Brainstem is a control surface, not a chatbot UI.
It exists to:
- inspect what Jeeves currently believes,
- modify confidence via explicit feedback,
- validate ingestion pipelines,
- monitor operational health,
- run guarded manual tasks.

This is critical because invisible adaptation is untrustworthy. Jeeves makes adaptation visible.

5. Data Philosophy
Jeeves is selective about truth claims.

5.1 Portfolio Truth
Only trusted IBKR report patterns are accepted for portfolio truth updates. Other similar emails are ignored.

5.2 Calendar Truth
Calendar events are provider-ingested and normalized. Classification is deterministic and title-based (no hidden AI hallucination layer for event type/domain assignment).

5.3 Sleep Truth
Sleep ingestion centers on quantity. Downstream restedness is derived deterministically from normalized duration with explicit transformation rules.

5.4 Inbox Truth
Inbox busy signal now prioritizes unread count as hard baseline for zero-state behavior and supports relative load tracking.

6. Safety and Control
Jeeves is designed fail-closed in key paths:
- Internal task endpoints require key authentication.
- Brainstem session is passcode-gated.
- Startup validates required security settings.
- Strict task mode can force cron failures to be visible instead of false-green.

7. Operational Reality
Jeeves is a cloud-resident production system, not a local demo.
This matters because cloud execution introduces:
- transient network faults,
- scheduler race conditions,
- delivery uncertainty,
- external API partial failures.

The architecture therefore emphasizes:
- idempotency,
- retries,
- compact task modes,
- structured failure reporting,
- post-fact auditability.

8. Memory as Controlled Plasticity
Jeeves does not treat memory as static notes.
Memory changes through:
- reinforcement,
- correlation-based confidence lift,
- decay slowdown,
- delayed forget queue,
- protected-category safeguards.

The key principle: memory should be editable by evidence and operator feedback, not overwritten arbitrarily.

9. Journal Function and Verbatim Export
Journal entries serve two distinct roles:
- cognitive input for state and trend extraction,
- exact historical artifact for user-owned archive.

Jeeves now supports automatic verbatim journal export to one local document without AI in the export path. This preserves raw user text exactly while still allowing structured interpretation for adaptive behavior.

10. Product Boundary
Jeeves is intentionally opinionated:
- deterministic where precision matters,
- probabilistic where interpretation is beneficial,
- transparent where trust can degrade.

This boundary is the central design discipline of the system.

11. Outcome
Jeeves is a practical coordination engine for one person’s information and workload environment. It is not trying to maximize chat novelty. It is trying to increase reliability of action under real constraints.


============================================================
PAPER II: JEEVES TECHNICAL ARCHITECTURE (DETAILED)
============================================================

Abstract
This paper describes the production implementation of Jeeves, as implemented in /Users/manustromberg/Documents/Jeeves/server.py with supporting execution priorities documented in /Users/manustromberg/Documents/Jeeves/ROADMAP.md. It covers runtime shape, schema, ingestion, interpretation, memory mechanics, alert pipeline, Brainstem controls, and failure handling.

1. Runtime Shape

1.1 Service Topology
Jeeves runs as a Flask application (single service) exposing:
- messaging webhook routes,
- internal task routes,
- debug/inspection routes,
- Brainstem UI + API routes.

This monolith is intentional: low coordination overhead, low deployment complexity, and fast iteration in a single code surface.

1.2 Core Dependencies
Key runtime dependencies include:
- Flask for HTTP surface,
- sqlite3 for state and history,
- requests for outbound provider calls,
- OpenAI SDK for selective model paths,
- Twilio for outbound delivery,
- Google APIs for Gmail/Calendar integration.

1.3 Configuration Contract
Critical environment variables are loaded near the top of server.py and validated early. Notably:
- INTERNAL_API_KEY (required)
- BRAINSTEM_PASSCODE (must not use insecure default)
- provider keys and URL settings

validate_security_configuration() enforces fail-closed startup behavior when invalid.

2. Database and State Model

2.1 Storage Engine
SQLite with WAL mode is used via get_conn().
Operational settings include:
- PRAGMA busy_timeout
- PRAGMA journal_mode = WAL
- PRAGMA synchronous = NORMAL

2.2 Write Robustness
execute_write_with_retry() retries lock conflicts and commits atomically per statement.
This provides practical resilience under concurrent task/webhook pressure.

2.3 Principal Tables (selected)
The schema includes:
- conversation_messages: raw interaction log
- memory_items: active memory state
- memory_observations: lower-level observations
- memory_provenance_events: why memory changed
- memory_feedback_queue: delayed delete queue
- journal_entries: canonical journal text
- tone_signal_snapshots: time series of tone-related signals
- memory_count_snapshots: time series for memory growth
- alert_log / alert_outcomes / event_lineage: poll and delivery traceability
- provider context tables for calendar, inbox, sleep

3. Ingestion Pipelines

3.1 Calendar
refresh_calendar_context_from_provider() ingests provider payloads and upserts:
- daily context summary features
- normalized event rows (with enriched fields)

Calendar enrichment includes deterministic classification fields:
- domain
- event_type
- tags
plus provider metadata such as calendar_name and calendar_id when available.

3.2 Gmail
fetch_gmail_inbox_daily_context() computes inbox features and writes inbox_daily_context.
Unread baseline behavior is explicitly handled to avoid misleading busy scores when unread = 0.

3.3 Sleep
refresh_sleep_context_from_provider() and upsert_sleep_daily_context() normalize sleep payloads.
Pipeline supports:
- unit normalization,
- quality/fatigue scale normalization,
- duration calibration logic,
- derived restedness feature integration,
- timeseries persistence through add_sleep_datapoint().

4. Context Refresh Task

4.1 Task Endpoint
/tasks/context-refresh orchestrates the three ingestion paths:
- inbox
- calendar
- sleep
then rebuilds context snapshot + tone vector and records snapshots.

4.2 Snapshot Outputs
The full response contains:
- refreshed flags,
- configured-provider map,
- staleness/failure metadata,
- context_snapshot,
- tone_vector,
- memory_count.

4.3 Compact Mode for Scheduler Reliability
To reduce payload size and lower transport fragility during scheduler runs, compact mode is available:
- /tasks/context-refresh?compact=1
- /tasks/context-refresh-lite

Compact mode omits heavy snapshot blocks and returns summary fields suitable for cron health checks.

This addresses recurring network stream issues like urllib3 IncompleteRead / ChunkedEncodingError on large response bodies.

5. Message Interpretation and Routing

5.1 Split Architecture
Jeeves distinguishes:
- deterministic command paths (key links, control intents, explicit task handlers),
- interpretation-layer model routing for non-hardcoded prompts,
- deterministic fallback handlers (calendar/email/watchlist) before generic response model fallback.

5.2 Why This Matters
This reduces unnecessary model usage and improves behavior predictability while preserving natural-language flexibility.

6. Memory Mechanics

6.1 Memory Write Path
process_memory_updates(text) executes the core write path:
- extract updates
- add observation rows
- upsert active memory rows
- create embedding records

6.2 Journal Capture
When journal context is active (or gratitude extraction yields entry), process_memory_updates() writes:
- journal_entries insert,
- working + long_term journal memory entries,
- derived journal analysis memory signals.

6.3 Verbatim Journal Export
Automatic export now occurs on ingest through append_journal_export():
- appends exact entry text with local timestamp
- writes to a single file (`Jeeves Journal Export`)
- enforces file mode 600
- uses no AI call in export path

This creates a human-readable and exact user-owned record while preserving system-side analysis.

6.4 Confidence and Reinforcement
Memory confidence is not static:
- explicit "accurate" feedback triggers reinforcement,
- confidence growth is additive with diminishing returns,
- reinforcement slows decay,
- undo path supported.

6.5 Delayed Forget Queue
"Inaccurate" feedback queues deletion (1h delay) via memory_feedback_queue.
This supports reversible correction before irreversible deletion.

6.6 Protected Categories
Certain categories are protected against low-confidence deletion for stability and safety.

7. Tone Matrix

7.1 Inputs
build_tone_vector(...) consumes context + state signals including:
- calendar_busy
- inbox_busy
- fatigue/restedness
- stress/friction proxies
- memory confidence meta-signals

7.2 Outputs
Primary output dimensions:
- brevity
- directness
- warmth
- seriousness
- style label

7.3 Persistence
record_tone_snapshot(...) stores tone time series for historical diagnostics and graphing in Brainstem.

8. Poll and Alert Pipeline

8.1 Candidate Generation
run_poll_cycle() gathers candidates from configured sources and query streams (E/G/P/L depending on mode).

8.2 Scoring and Tiering
Candidates are scored with source trust, novelty, category relevance, memory vector effects, and specialized logic.

8.3 Live Push Policy
Live push is constrained to tier-1 policy.
Non-tier-1 candidates remain visible for diagnostics or brief composition where configured.

8.4 Delivery Integrity
System differentiates candidate selection from actual delivery. Alert dedupe and retry logic tracks sent-state by confirmed delivery outcomes, not optimistic pre-send state.

8.5 Retry Queue
Failed deliveries are queued and retried with backoff. This removes silent drops and increases eventual-delivery reliability.

9. Brainstem Architecture

9.1 Auth Model
Brainstem requires passcode session with signed cookie.
Session handling utilities include:
- has_brainstem_session()
- issue_brainstem_session_response()
- clear_brainstem_session_response()
- require_brainstem_access()

9.2 UI
/brainstem serves a single-page control console with sections:
- Landing
- Overview
- Key
- Memory
- Tone Matrix Dashboard
- Live News Polls
- Live Operations Console
- Usage
- Whitepaper

9.3 API Surface (selected)
- /brainstem/api/overview
- /brainstem/api/key-links
- /brainstem/api/memory
- /brainstem/api/memory/feedback
- /brainstem/api/context-tone
- /brainstem/api/history
- /brainstem/api/memory-growth
- /brainstem/api/usage-activity
- /brainstem/api/poll
- /brainstem/api/geopolitics
- /brainstem/api/economics
- /brainstem/api/ops/run
- /brainstem/api/whitepaper

9.4 Whitepaper Delivery
The Whitepaper tab now serves static text loaded from one editable source file:
- /Users/manustromberg/Documents/Jeeves/assets/JEEVES_WHITEPAPER.md

This enables direct edits without touching UI rendering logic.

10. Key Links and Operator Workflow

10.1 Key Links
get_operator_links_map() and build_command_key_reply() expose critical routes in one map.
This provides operational discoverability across tasks/debug endpoints.

10.2 Added Compact Context Link
The operator map now includes context refresh compact link:
- /tasks/context-refresh?compact=1

Use this for scheduler-safe refresh checks and lower log payload.

11. Failure Modes and Mitigations

11.1 Network Stream Truncation During Cron
Observed issue:
- urllib3 IncompleteRead
- requests ChunkedEncodingError
- container stop after response stream failure

Mitigation implemented:
- compact response modes for context-refresh
- lite endpoint for scheduler use

Recommended cron invocation pattern:
- call compact endpoint
- parse JSON minimally
- retry on transient transport failure

11.2 Provider Partial Failure
context-refresh includes per-provider refreshed flags and failure list, plus strict-status mode support so automation can fail loudly when needed.

11.3 SQLite Lock Contention
execute_write_with_retry + WAL mode mitigate transient lock collisions.

12. Security Model (Current)

12.1 Positive Controls
- Internal routes key-gated.
- Brainstem passcode session control.
- startup security validation.
- strict origin/csp hardening headers on Brainstem page.

12.2 Known Practical Risk Areas
- key-bearing URLs in operator workflows remain sensitive; any leak of these URLs is equivalent to token leak.
- operator discipline around logs/screenshots remains part of security boundary.

13. Cloud vs Local Tradeoffs

Cloud-run benefits:
- always-on schedulers
- stable webhook availability
- centralized operational state

Cloud-run costs:
- network unpredictability
- provider latency variance
- stronger need for retries, idempotency, and compact task outputs

Local-only alternatives simplify networking but degrade always-on operations and external integration reliability.

14. Cost and Rate Discipline

Design choices that reduce call pressure:
- deterministic command bypass for hardcoded intents
- selective interpretation layer usage
- deterministic post-interpret handlers where possible
- static rendering for whitepaper (no model call)
- local DB metrics for usage displays

15. Roadmap Alignment Snapshot

From ROADMAP.md, major already-aligned pillars include:
- tier-1 alert discipline
- delivery retry integrity
- memory reinforcement and delayed forget
- context refresh automation
- inbox/calendar/sleep signal hardening
- Brainstem transparency controls

The whitepaper objective in roadmap is now operationalized in Brainstem via static document loading.

16. Minimal Operator Runbook

16.1 Verify Context Loop
- run /tasks/context-refresh?compact=1
- check ok, refreshed flags, and failures
- inspect /debug/context

16.2 Verify Poll Loop
- run /tasks/poll?readable=1
- inspect /debug/poll/preview
- validate alert_outcomes stages

16.3 Verify Memory Feedback
- Brainstem > Memory
- mark accurate/inaccurate
- confirm confidence/provenance changes
- confirm delayed queue behavior

16.4 Verify Journal Pipeline
- submit journal response
- confirm journal_entries update
- confirm verbatim append in local export file

17. Design Principles (Final)

Principle A: Deterministic before probabilistic.
Principle B: Visible state beats hidden adaptation.
Principle C: Delivery truth beats decision truth.
Principle D: Reinforcement should modify behavior, not just display values.
Principle E: Operational simplicity wins when under real rate and reliability constraints.

18. Conclusion
Jeeves is implemented as a practical, stateful control system for personal execution. The architecture intentionally blends deterministic pipelines with selective model interpretation while keeping high-risk surfaces inspectable and operator-steerable. Its strongest property is not novelty; it is controlled reliability under noisy, real-world conditions.

