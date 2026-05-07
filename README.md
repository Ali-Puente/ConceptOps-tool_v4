# Data Cleaning & Enrichment Tool — V4

Streamlit app for cleaning, validating, and enriching CSV entity data with AI-powered mention rules and descriptions.

## Features

- **Parallel URL validation** — 2-layer HTTP check (HEAD + GET) with timeout handling
- **Name normalization** — strips codes, suffixes, geographic qualifiers; handles abbreviations
- **Entity research** — scrapes `/about` pages and runs targeted Google searches via Serper.dev
- **AI rule generation** — Claude Haiku produces up to 5 mention rules + entity descriptions
- **Confidence gating** — suppresses low-confidence or ambiguous rules (prevents false positives)
- **Division & ambiguity detection** — flags parent companies and shared-name entities
- **Non-Latin filtering** — automatically excludes Cyrillic, Arabic, CJK names
- **Anti-blocking** — User-Agent rotation, request throttling, optional proxy support

## Requirements

- Python 3.9+
- **[Anthropic API key](https://console.anthropic.com)** — Claude Haiku for rule generation
- **[Serper.dev API key](https://serper.dev)** — Google Search API (free tier: 2,500 queries/month)

## Installation & Setup

```bash
pip install -r requirements.txt
```

Create `.env` file in project root:

```env
ANTHROPIC_API_KEY=your_key_here
SERPER_API_KEY=your_key_here

# Optional: IP rotation via proxies
# PROXY_LIST=http://user:pass@host1:8080,http://user:pass@host2:8080
```

## Running

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501)

## Input CSV

Required columns (exact names):
- `Name` — entity name
- `URL` — website URL

Optional:
- `Entity Type` — "org" or "person" (default: "org")
- `Role` — role/company title (for people)

## Output

Three tabs + downloadable CSV:

| Tab | Purpose |
|-----|---------|
| **OK** | High-confidence rules ready to use |
| **Needs Revision** | Low confidence, ambiguity, or broken links |
| **Incorrect Requests** | Non-Latin names or blocked domains |

Key output columns:
- `Rule 1–5` — mention rules for news matching
- `Rule Type 1–5` — "rule" (high-confidence) or "alias" (manual verification needed)
- `Entity Description` — 2–3 sentence summary
- `Confidence` — Claude's self-assessed score (0–100)
- `Ambiguity Flag` — true if division/shared-name risk detected
- `Has Divisions` — parent company with named sub-divisions
- `Needs Manual Revision` — true if rules suppressed by confidence gate
- `Website Status` — OK / Broken / Need revision / Social media

## Confidence Gating

Rules are suppressed if:
- Confidence < 70 (or 85 for division-risk names)
- Ambiguity flag detected
- Rule is too broad (e.g., first word only of multi-word entity)
- Name contains known ambiguous tokens

## Pipeline Steps

1. **Validate URL** — 2-layer HTTP check (3s HEAD, 10s GET retry)
2. **Clean name** — remove codes, suffixes, abbreviations
3. **Extract base name** — strip geographic qualifiers for variant matching
4. **Scrape `/about`** — extract title, meta description, H1, body text
5. **Google search** — 3–5 targeted Serper queries
6. **Claude Haiku** — generate rules + description + confidence score
7. **Gate rules** — suppress if confidence low or ambiguity detected
8. **Output** — categorize into OK / Needs Revision / Incorrect

## Caching

In-memory caches per session (reset on each app restart):
- `rules_cache` — by raw URL (avoids duplicate API calls)
- `base_name_cache` — by base name (shared across variants)
- `about_cache` — by root domain (avoids re-scraping)
- `search_cache` — by query string

## Sidebar Controls

- **Standard confidence threshold** (default: 70) — gate for standard entities
- **Group / holdings threshold** (default: 85) — higher bar for division-risk names

## Anti-blocking Measures

- Random User-Agent per request (7-entry pool)
- Randomized Accept-Language headers
- Request semaphore (max 15 concurrent)
- 0.2–0.5s jitter between validation layers
- Optional proxy rotation

## Notes

- Broken URLs and social media / blocked domains skip rule generation
- Non-Latin names skip rule generation but appear in output
- No evidence (no `/about` + no search results) → flagged for manual revision
- Caches are in-memory; restart app to clear
- Proxy feature requires third-party service (Bright Data, Oxylabs, Webshare, etc.)
