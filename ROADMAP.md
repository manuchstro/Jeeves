# Jeeves Roadmap

Last updated: 2026-04-10

## Recently Deployed (Completed)
- ✅ Removed `portfolio_profile` from active logic paths and removed manual `portfolio_update` intent path; portfolio truth now stays with `portfolio_holdings` + `portfolio_snapshots` flow.
- ✅ Changed low-confidence deletion age gate from `120` to `80` days.
- ✅ Fixed duplicate `recent_journal` display bug caused by legacy union behavior in debug query.
- ✅ Added refresh gating: memory `refresh` is now skipped when there is no real new signal since last update (`refresh_skipped_no_new_signal` provenance action).
- ✅ Added memory debug usability endpoints and key links:
  - `/debug/memory` (raw)
  - `/debug/memory?compact=1` (compact)
  - `/debug/memory/view` (readable page)
- ✅ Upgraded memory debug view readability:
  - grouped journal rows
  - provenance rollups
  - potential-issues diagnostics
  - plain-English memory cards with value-first display and numeric confidence
- 🧪 Wired confidence into behavior influence paths:
  - confidence-weighted memory influence in scoring vector
  - confidence-weighted carryover into tone-state blending
- ✅ Hotfixed compact debug endpoint SQL ordering after join alias regression.
- ✅ Added provenance idempotency window for `refresh_skipped_no_new_signal` events to reduce duplicate spam during rapid repeated consolidation runs.
- ✅ Improved poll architecture:
  - query diversity budgeting
  - near-duplicate query dedupe (non-`P`)
  - category normalization (including `FRED -> E`)
  - poll preview diagnostics for missing `P`, reassignment counts, and Currents filter stats
- ✅ Adjusted live poll vs brief split:
  - live polls exclude `L` and reallocate capacity to `P`
  - daily brief path performs a local-inclusive poll pass (`include_local=true`)
- ✅ Deployed and tested trusted IBKR HTML portfolio sync:
  - only `Jeeves_#1*.html` attachments are trusted for portfolio truth
  - Daily Trade Report and monthly statements are ignored for truth ingestion
  - sync endpoint added: `/tasks/portfolio-sync`
  - debug endpoint added: `/debug/portfolio/sync`
- ✅ Deployed and tested portfolio-truth inspection links (for dashboard-forward visibility):
  - `/debug/portfolio/truth` (JSON)
  - `/debug/portfolio/truth/view` (readable)
- 🧪 Updated `P` query symbol selection to use top 10 trusted holdings (ETF-inclusive) instead of non-ETF-only.
- 🧪 Poll quality hotfix deployed to branch:
  - force `FRED` category to remain `E` after AI decision layer
  - replace misleading `portfolio:awaiting_trusted_data` with `portfolio:no_direct_match` when trusted holdings exist
  - preserve full `P` query strings to prevent 3 `P` variants from collapsing into 1 during normalization

1. **Stabilize Messaging Cost + Alert Discipline (Now)**
- ✅ Keep `Tier 1 only` live-alert behavior hard-enforced (Tier 2 never pushed).
- ✅ Add a quick debug check you can run anytime to confirm push behavior after deploy.
- Keep your manual testing loop: you test, report issues, I patch.

2. **Memory Model Upgrade (Now / First Improvement)**
- ✅ Move from confidence overwrite behavior toward confidence accrual behavior (with caps).
- ✅ Use additive (not multiplicative) confidence bonuses.
- ✅ Set maximum confidence cap to `0.99`.
- ✅ Apply correlation-based reinforcement only for strong matches (threshold `>= 0.8`).
- ✅ For high-correlation pairs (`>= 0.8`), apply additive confidence bonus to both linked memories.
- ✅ For high-correlation pairs (`>= 0.8`), slow decay for both linked memories (linear slowdown model).
- ✅ Add low-confidence deletion for non-protected memories after threshold/aging rules.
- ✅ Remove/replace the effective confidence-floor limitation as part of that redesign.
- ✅ Make deep AI consolidation nightly non-optional (always attempted), with strict anti-hallucination guardrails and safe fallback behavior.
- ✅ Run memory decay at most once per local day (daily-gated), not on every inbound message.
- ✅ Expand protected memory behavior to preserve `journal` and `risk_profile` from auto-forgetting.
- ✅ Keep contradiction handling as a recorded human-like signal, without automatically decreasing old-memory confidence.
- ✅ Remove/deprecate inferred `portfolio_profile` memory signals from decision-critical logic.
- ✅ Keep portfolio truth sourced from `portfolio_holdings` + `portfolio_snapshots`.
- Add health/integrity checks and safeguards for `portfolio_holdings` + `portfolio_snapshots` update/replace flow.

3. **A2P Go-Live and SMS Production Validation (Now)**
- Finish campaign approval flow.
- Bind approved campaign + messaging service + US number.
- Run end-to-end SMS tests on key flows (`key`, daily brief, alerts, feedback).

4. **IBKR Statement Hard-Parser (Tomorrow/Immediate Next)**
- ✅ You shared real daily statement samples.
- ✅ Hardcoded recognition to the trusted statement format (`Jeeves_#1*.html`, not generic IBKR email parsing).
- ✅ Parse holdings into trusted portfolio snapshot.
- ✅ Use trusted portfolio truth in `P` scoring/query logic paths.
- ✅ Parser + trusted-source gate deployed and verified live on Railway (`/tasks/portfolio-sync`, `/debug/portfolio/truth`, `/debug/portfolio/truth/view`).

4.1 **P Query Expansion Mode (Future / Optional)**
- Add optional `P` query mode switch:
  - default: `top10` trusted holdings (current behavior)
  - optional: `all` trusted holdings (for broader scan)
- Keep `top10` as default for live polling quality; evaluate `all` mode after API refresh cycle and query-quality benchmarking.
- Add explicit debug fields to preview:
  - `p_symbols_used`
  - `p_symbols_count`
  - `p_query_mode`

5. **Locked Decisions and Behavior Guards (Immediate After IBKR)**
- Keep both market providers (`Massive` + `TwelveData`) in place.
- Journal-lock behavior must remain unlimited until your first reply, even if alerts are sent meanwhile.
- Journal prompt response window: use `12 hours` (not `8 hours`).
- Pile-up handling: no backlog queue; only one journal response is expected even if prompts pile up.
- Alert messages must not break journal-context capture.
- First inbound message within the active `12-hour` journal window is treated as the journal response.
- ✅ Keep unauthorized warning behavior with full verbatim transcript included (and protect against regressions).
- Portfolio state logic: if no new IBKR activity statement is received, assume holdings, position sizes, and cash are unchanged.
- Dashboard access must be restricted to only your Google account.

6. **Tone Matrix Expansion (Near-Term / Moved Up)**
- Confirm current tone engine deployment status.
- Treat `jeeves_config.py` personality/prompt edits as sensitive: require explicit deep review before any deploy.
- Expand matrix so response length, warmth, empathy, and directness adapt to context inputs (weather, inbox, calendar, portfolio performance, sleep).
- Reduce sycophancy by defaulting to higher candor/directness and lower baseline warmth.
- Add an explicit anti-sycophancy control signal in the tone matrix (used in both generic and journal reply flows).
- Enforce tone rules that avoid default praise/validation and prefer respectful disagreement when needed.
- Preserve purpose-aware behavior: clear understanding of role, direct usefulness, and accountable correction when wrong.
- 🧪 Add transparent debug output so you can see why a tone was chosen.

7. **API Attachments for Context Engine (Near-Term)**
- Add Weather API (fully configured, not scaffold-only).
- Add Calendar API.
- Add Health API (sleep, steps, recovery-style signals from Apple sources if feasible).
- Verify each context source is visible in debug and actually influencing behavior.

8. **Voice Input to Text Thread (Mid-Term)**
- Implement voice intake where your speech is transcribed.
- Jeeves replies back as text in the same message thread (not voice-to-voice).
- Add fallback handling for low-confidence transcription.

9. **Dashboard (Mid/Late-Term, Major Build)**
- Google sign-in auth.
- API spend metrics, usage graphs, and cost totals.
- “Calls used / remaining” visibility where provider data allows it.
- Full readable memory explorer from foundation.
- Per-memory accuracy feedback controls.
- Memory visibility toggles.
- Kill switch with confirmation modal.
- Trigger controls for core functions (daily brief, journal, 5-min poll, nightly consolidation).
- Add cheap/fun metrics panel.

10. **Safe Fresh-Start Memory Reset (Post-Stability, Pre-Whitepaper)**
- Perform reset only after: A2P/SMS is fully live/tested, IBKR parser is validated on real statements, context APIs are attached/verified, and tone/memory behavior is no longer in daily churn.
- Require a 7–14 day stable run window before reset.
- Backup database first (mandatory).
- Execute targeted memory reset (memory/history-focused tables), not full DB destruction.
- Preserve operational truth/config tables during reset (auth/config/ops, portfolio/watchlist truth).
- Validate post-reset behavior with a controlled smoke test sequence before resuming normal operation.

11. **Whitepaper + Full Ops Recipe (Final Major Milestone)**
- Canonical “Jeeves whitepaper” in cloud storage.
- Complete technical and non-technical system description.
- Full change/progress history.
- Reproducible setup guide: Twilio sole-prop flow, GitHub/Railway deploy, projected costs, operating instructions.

12. **PDF Read-to-You Capability (Post-Whitepaper / Late-Late)**
- Add document ingestion + chunking + narration/summarization path.
- Keep this explicitly after whitepaper completion.
