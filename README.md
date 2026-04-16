# ConceptOps tool — V3
## What it does

1. **Name cleaning** — strips leading numeric/alphanumeric codes, collapses dotted corporate abbreviations (S.L. → SL), removes business suffixes (Inc, LLC, Ltd, SL, SA, GmbH…), normalizes to Title Case
2. **Base name extraction** — strips trailing geographic/branch qualifiers (e.g. `(Francia)`, `(UK)`) to derive a core entity name used for research sharing
3. **Domain extraction** — pulls the registered domain from each URL
4. **Duplicate detection** — flags rows sharing the same raw URL
5. **URL validation** — concurrent HTTP checks using browser TLS fingerprinting (`curl_cffi`) to bypass bot detection
6. **Latin-script filter** — skips non-Latin entity names
7. **Name↔URL matching** — fuzzy match between normalized name and domain (Yes / Similar / No)
8. **Rule + description generation** — evidence-first research pipeline (single Haiku call per unique entity):
   - Scrapes `/about` then `/about-us`; if both 404, proceeds with empty website content
   - Match = **Yes**: about-page content → Haiku (rules + description)
   - Match = **No/Similar**: about-page + Serper.dev Google Search → Haiku (rules + description)
   - No evidence found: rules and description left blank, reason recorded in Notes
   - Duplicate URLs inherit results from their first occurrence at zero extra API cost
   - Same base-entity variants (e.g. `Chemieuro SL` / `Chemieuro (Francia)`) share one research call via base-name cache

---

## Requirements

- Python 3.10+
- An Anthropic API key
- A Serper.dev API key

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file in the project root:

```
ANTHROPIC_API_KEY=your_anthropic_key_here
SERPER_API_KEY=your_serper_key_here
```

**Getting your Serper.dev API key:** sign up at [https://serper.dev](https://serper.dev) and copy the key from the dashboard. Free tier: **2,500 queries/month**.

---

## Running the app

```bash
streamlit run app.py
```

Then open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Input CSV format

Your CSV must have at minimum these two columns (exact names):

| Column | Description |
|--------|-------------|
| `Name` | Entity name |
| `URL`  | Website URL for that entity |

---

## Output columns

| Column | Description |
|--------|-------------|
| `Name` | Original name |
| `URL` | Original URL |
| `Normalized Name` | Cleaned, Title-Cased name with suffixes stripped |
| `Base Name` | Core entity name with geographic/branch qualifiers removed |
| `Normalized Website` | Registered domain (e.g. `example.com`) |
| `Duplicate Flag` | `True` if this raw URL appears more than once in the input |
| `Website Status` | OK / Broken / Need revision / Social media |
| `Match` | Yes / Similar / No / Unknown |
| `Entity Description` | 2–3 sentence factual summary of the entity (blank if no evidence found) |
| `Rule 1–5` | Mention surface forms for news matching (blank if no evidence found) |
| `Rules Evidence Notes` | Short note on evidence used, or reason rules were skipped |

---

## Caching behaviour

Four in-memory caches are reset at the start of each run:

| Cache | Key | Purpose |
|-------|-----|---------|
| `rules_cache` | Raw URL | Exact duplicate rows — zero extra cost |
| `base_name_cache` | Base name | Same entity, different regional variant — zero extra cost |
| `about_cache` | Root URL (scheme + host) | Avoids re-scraping the same domain |
| `search_cache` | Query string | Avoids duplicate Serper calls |

---

## Project structure

```
.
├── app.py            # Main application
├── requirements.txt  # Python dependencies
├── .env              # API keys (never commit this)
└── README.md
```

---

## Notes

- Rows with `Website Status` of Broken or Social media are skipped for rule/description generation
- Non-Latin entity names are skipped entirely
- If both `/about` and `/about-us` return 404, the row proceeds without website evidence; Match = No/Similar rows still attempt a Serper search
- Caches are in-memory per session; restarting the app clears them
