# CVM in Digital Banking: A Year-One Playbook (Data + Campaigns + ML/AI)

A straight guide for someone who already runs **data-informed CVM**, wants **revenue and healthy customer growth**, and is **leveling up ML/AI** without drowning in presentation tooling.

---

## 1. Thesis for the year

**Your job is not “more models.”** Your job is a **reliable growth system**:

- **Retention** — keep the right customers active, funded, and digitally engaged; reduce value leak and silent churn.
- **Upgrade** — cross-sell and upsell with **economics** and **incrementality**, not vanity attach rates.
- **Acquisition** — bring **healthy** customers (quality + early behavior + compliance), not just top-of-funnel volume.

**ML/AI earns its seat** when it improves **decisions under constraints** (budget, fatigue, channel capacity, risk), and when **measurement** is clean enough for the CFO and risk partners to trust.

---

## 2. The three layers (keep them separate)

| Layer | What it is | Failure mode if mixed |
|------|------------|------------------------|
| **Definitions** | Churn, active, healthy, LTV horizon, “exposed,” conversion | Everyone argues about numbers |
| **Operating metrics** | Funnel, response, cost-to-serve, early quality | Pretty charts, wrong incentives |
| **Decisions** | Who gets what, when, under which guardrails | Models that nobody acts on |

**Rule:** publish a **metric dictionary** (even a one-pager + versioned YAML). If two teams use the same word (“active”) with different SQL, you lose the year.

---

## 3. Campaign creation: the non-negotiables

### 3.1 One campaign = one hypothesis

Every campaign should answer:

- **Who** (eligible universe; exclusions)
- **What** (offer, message, channel)
- **Why we believe it works** (insight, segment rationale)
- **How we’ll know** (primary KPI + guardrails + horizon)
- **Risk / conduct** notes (vulnerable, affordability, consent)

If you can’t state the hypothesis, you’re doing broadcast, not CVM.

### 3.2 Minimum viable **campaign tracking record**

Make exposure and assignment **boringly traceable**. At minimum, persist:

- `campaign_id`, `cell_id` (treatment vs control), `variant` (offer/message)
- `customer_id`, `decision_date` (or eligibility snapshot date)
- `channel`, `send_ts` / `exposure_ts` (best available)
- `cost` (even rough) when relevant
- **Holdout / control** flag and assignment mechanism

**Bad tracking = dead ML.** Good models on bad attribution produce confident lies.

### 3.3 Controls and incremental thinking

Prefer:

- **Random holdout** (cleanest), or
- **Matched / stratified** control (document bias risks), or
- **Geo / time stagger** (only when you must—analysis gets harder)

Your headline metric should trend toward **incremental lift** (treatment vs control), not “conversion rate among people we messaged.”

---

## 4. “Model the decision, not just the score”

### 4.1 What a score does

A typical propensity model answers: “likelihood of X under observational patterns.”

### 4.2 What a decision needs

A CVM decision answers: **given costs and limits, who gets which action for maximum incremental value?**

So you need:

- **Economic lens** — margin, NPV, payback; avoid optimizing clicks that destroy margin.
- **Constraints** — contact caps, channel capacity, operational readiness.
- **Incrementality** — uplift / causal framing where possible; experiments to validate.

### 4.3 Practical ML menu (what to learn next)

Prioritize tools that change targeting:

- **Baseline**: ranking + calibration + monitored drift
- **Uplift / HTE**: models that separate “persuadables” from “always/never”
- **Policy**: top-N selection under budget (knapsack-ish thinking)
- **Bandits** (when appropriate): allocate learnings across message/offer variants with governance

If leadership only sees AUC, teach them **incremental profit per 1,000 contacted** and **lift curves**.

---

## 5. Churn + LTV: how to stay credible

### Churn

- Define **churn for digital**: inactivity, balance collapse, digital dormancy, product closure—pick ONE primary for targeting, others as diagnostics.
- Separate **early warning** (engagement decay) from **hard churn** (account-level).
- Always show **base rate + precision/recall tradeoffs** at chosen contact volume.

### LTV

- LTV is a **horizon + definition** problem before it is a model problem.
- Prefer **simple, transparent baselines** (RFM-ish + margins) while you earn trust for fancier survival/GBDT stacks.
- Pair LTV with **quality** guardrails (early fraud flags, early credit stress, complaints) so you don’t grow “bad revenue.”

---

## 6. Portfolio operating cadence (how you “shine” visibly)

### Weekly (internal engine)

- Pipeline health: eligibility counts, sends, failures, latency
- Early readouts: exposure vs outcomes (with caveats)
- Model monitoring: drift, calibration, feature freshness

### Monthly (business rhythm)

- Retention / upgrade / acquisition **scorecards** tied to definitions
- Campaign postmortems: hypothesis vs outcome; what to kill/scale
- **One insight** leadership can repeat (your narrative currency)

### Quarterly (strategy)

- Refresh roadmap: three horizons (quick wins, platform bets, experiments)
- Retire low-value campaigns; codify playbooks that repeat

**Visibility isn’t self-promotion—it’s structured proof.**

---

## 7. Reporting: don’t let tools steal your year

You already have BI elsewhere. For your personal stack:

- **Python + notebooks**: exploration, QC, prototypes
- **SQLite**: fine for personal sandboxes—not the system of record for bank metrics
- **Executive vehicle is often PPT**: automate **template fills + chart PNGs** (e.g., `python-pptx`) and treat decks as **outputs**, not the product
- **Excel**: straightforward exports for stress-testing tables—add a **Definitions** sheet always
- **Time discipline**: store **UTC instants** in databases; use local TZ for scheduling and human-readable reporting where needed

**You do not need React/shadcn** for analysis or board storytelling unless you’re building a durable internal product.

Optional later: **Quarto** for clean PDF/HTML appendices (methods, caveats)—useful when risk/compliance asks “show your work.”

---

## 8. Governance that accelerates (instead of blocking)

Bring these early:

- **Data lineage-lite**: where the cohort came from, as-of timestamp
- **Model cards-lite**: intent, data, known limitations, monitoring
- **Fallbacks**: if scoring fails, what safe rule runs?
- **Conduct**: consent, frequency caps, vulnerability handling—baked into design, not appended

Teams that shine pair **speed with auditability**.

---

## 9. A 90-day plan (concrete)

### Days 0–30: make measurement trustworthy

- Publish v1 **metric dictionary** (even if imperfect—version it)
- Standardize **campaign tracking fields** for all new campaigns
- Run **one clean experiment** end-to-end (even small) with documented analysis

### Days 31–60: ship one decision upgrade

- Replace a “sort by score” list with **ranking under a budget + fatigue rule**
- Add **uplift/experiment evidence** to one flagship program (upgrade or save)

### Days 61–90: scale what’s proven

- Automate the monthly pack (PPT/Excel generation from the same tables)
- Establish a **model monitoring** ritual (drift + outcomes)
- Kill or refactor two campaigns with weak incrementality—executives notice judgment

---

## 10. Anti-patterns that waste a year

- Chasing model complexity before **tracking + definitions** are fixed
- Optimizing response rate while margin and risk worsen
- No holdouts → endless arguments about “what would have happened”
- ML outputs nobody can operationalize in CRM/CDP within 48 hours
- Decks that look good but hide **as-of** dates, exclusions, and definitions

---

## 11. What “great” looks like at year-end

- **Revenue/quality**: measurable portfolio impact on retention, upgrade, acquisition—documented
- **Operating leverage**: faster campaign cycles because tracking + templates exist
- **Credibility**: risk/finance trusts your readouts because definitions and controls are consistent
- **Decisions**: a library of policies (not just notebooks of scores)

---

## 12. One-line north star you can repeat

**Grow revenue by growing healthy, engaged relationships—prove it with clean exposure data, controlled comparisons, and decisions optimized under real bank constraints.**

---

*This playbook is meant to be operational. Revise quarterly: what improved targeting, what improved learning speed, and what reduced regret.*
