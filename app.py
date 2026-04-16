# ============================================================
# DATA CLEANING & ENRICHMENT TOOL  —  V3
# ============================================================
# Changes vs V2:
#   - Removed entity classification (Person/Organization)
#     entity_cache, classify_entity_haiku, get_entity_type dropped
#   - Removed memory-pass gate; evidence phase runs unconditionally
#     get_rules_from_haiku_memory, rules_are_weak dropped
#   - Scrape target changed from homepage to /about then /about-us
#     (if both 404 → proceed with empty website content)
#   - Extended suffix list with European corporate forms (SL, SA, GmbH…)
#   - Dotted abbreviation normalisation: S.L. → SL before suffix strip
#   - Base-name extraction strips geographic/branch qualifiers (Francia)
#     so entity variants share one research call via base_name_cache
#   - Added Entity Description column (same Haiku call as rules)
#   - No-evidence short-circuit: skips LLM when nothing to analyse
#   - Caches reset on each run; output filename → cleaned_data_v3.csv
# ============================================================
# Setup:
#   ANTHROPIC_API_KEY  — https://console.anthropic.com
#   SERPER_API_KEY     — https://serper.dev (2 500 free queries/month)
# ============================================================

# ── stdlib ──────────────────────────────────────────────────
import os
import re
import json
import time
from urllib.parse import urlparse
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── third-party ─────────────────────────────────────────────
import streamlit as st
import pandas as pd
import tldextract
from rapidfuzz import fuzz
from dotenv import load_dotenv
import anthropic
from bs4 import BeautifulSoup

from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import (
    ConnectionError  as CffiConnectionError,
    Timeout          as CffiTimeout,
    RequestException as CffiRequestException,
)

# ============================================================
# CONSTANTS & CONFIG
# ============================================================

BLOCKED = [
    "facebook.com", "instagram.com", "x.com",
    "twitter.com", "youtube.com", "ebay.", "dictionary.",
]

HAIKU_MODEL = "claude-haiku-4-5-20251001"

_LEADING_CODE_RE = re.compile(
    r"^\s*"
    r"(?:"
    r"[A-Z0-9]{2,}[\s]*[-–][\s]*"
    r"|"
    r"\d+\s+"
    r")"
)

_SUFFIXES = [
    # English
    "inc", "llc", "ltd", "corp", "co", "company", "group", "plc",
    # European corporate forms
    "sl", "sa", "sas", "srl", "spa", "bv", "nv", "ag", "gmbh",
    "ab", "oy", "as", "kft", "sro",
]
_SUFFIX_RE = re.compile(
    r"\b(?:" + "|".join(_SUFFIXES) + r")\b",
    re.IGNORECASE,
)

# Collapses dotted abbreviations before suffix stripping: S.L. → SL, S.A.S. → SAS
_DOTTED_ABBR_RE = re.compile(r"\b((?:[A-Z]\.){2,})", re.IGNORECASE)

# Matches a trailing parenthetical used as a geographic/branch qualifier: (Francia), (UK)
_GEO_QUALIFIER_RE = re.compile(r"\s*\([^)]+\)\s*$")

_LATIN_RE = re.compile(r"[\u0000-\u024F\u1E00-\u1EFF]")
_ALPHA_RE  = re.compile(r"[^\W\d_]", re.UNICODE)

# ============================================================
# CACHES
# ============================================================

rules_cache     : dict = {}   # raw_url   → rule dict
base_name_cache : dict = {}   # base name → rule dict (shared across entity variants)
about_cache     : dict = {}   # root url  → scraped about-page content
search_cache    : dict = {}   # query     → list[dict]

# ============================================================
# API CLIENTS
# ============================================================

load_dotenv(dotenv_path=".env")
_api_key        = os.getenv("ANTHROPIC_API_KEY")
_serper_api_key = os.getenv("SERPER_API_KEY")

claude_client = anthropic.Anthropic(api_key=_api_key)

# ============================================================
# PURE HELPER FUNCTIONS
# ============================================================

def is_latin_script(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    alpha_chars = _ALPHA_RE.findall(text)
    if not alpha_chars:
        return False
    latin_chars = [c for c in alpha_chars if _LATIN_RE.match(c)]
    return (len(latin_chars) / len(alpha_chars)) >= 0.80


def clean_name(name) -> str:
    if pd.isna(name):
        return ""
    text = str(name).strip()
    text = _LEADING_CODE_RE.sub("", text).strip()
    # Collapse dotted abbreviations so suffix regex fires: S.L. → SL, S.A.S. → SAS
    text = _DOTTED_ABBR_RE.sub(lambda m: m.group(1).replace(".", ""), text)
    text = _SUFFIX_RE.sub("", text)
    text = re.sub(r"[^\w\s()]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.title()


def extract_base_name(normalized_name: str) -> str:
    """
    Strip trailing geographic/branch qualifiers to get the core entity name.
    Used as the cache key for sharing research across entity variants.

    Examples:
      "Chemieuro (Francia)"  →  "Chemieuro"
      "Acme Group (Uk)"      →  "Acme Group"
      "Reuters"              →  "Reuters"  (no change)
    """
    if not normalized_name:
        return normalized_name
    base = _GEO_QUALIFIER_RE.sub("", normalized_name).strip()
    return base if base else normalized_name


def extract_domain(url) -> str:
    if pd.isna(url) or not str(url).strip():
        return ""
    raw = str(url)
    if not raw.startswith("http"):
        raw = "https://" + raw
    ext = tldextract.extract(raw)
    return f"{ext.domain}.{ext.suffix}"


def normalize_url_for_match(url) -> str:
    if pd.isna(url) or not str(url).strip():
        return ""
    raw = str(url).strip().lower()
    raw = re.sub(r"^https?://", "", raw)
    raw = re.sub(r"^www\.", "", raw)
    raw = raw.split("/")[0]
    return raw


def normalize_url(url) -> str:
    raw = str(url).strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    return raw


def is_blocked(url: str) -> bool:
    return any(kw in url.lower() for kw in BLOCKED)


def match_name_url(normalized_name: str, normalized_url_str: str) -> str:
    if not normalized_name or not normalized_url_str:
        return "Unknown"
    name_slug = normalized_name.replace(" ", "").lower()
    score = fuzz.partial_ratio(name_slug, normalized_url_str)
    if score > 80:
        return "Yes"
    elif score > 50:
        return "Similar"
    return "No"


def parse_rules_response(text: str):
    try:
        start = text.find("{")
        end   = text.rfind("}")
        if start == -1 or end == -1:
            return [], "", ""

        payload = json.loads(text[start : end + 1])
        rules   = payload.get("rules", [])
        notes   = payload.get("notes", "")

        cleaned: list[str] = []
        seen: set[str] = set()
        for r in rules:
            r = re.sub(r"\s+", " ", str(r)).strip()
            if r and r not in seen:
                seen.add(r)
                cleaned.append(r)

        _title_re = re.compile(
            r"\b(ceo|chief|president|founder|chairman|chairwoman|mr|mrs|ms|dr)\b",
            re.IGNORECASE,
        )
        final_rules: list[str] = []
        for r in cleaned:
            if _title_re.search(r):
                continue
            redundant = any(
                other != r and other.lower() in r.lower() and len(other) < len(r)
                for other in cleaned
            )
            if not redundant:
                final_rules.append(r)

        description = str(payload.get("description", "")).strip()[:600]
        return final_rules[:5], str(notes).strip()[:500], description

    except Exception:
        return [], "", ""


# ============================================================
# NETWORK / IO FUNCTIONS
# ============================================================

def check_url(url) -> str:
    if pd.isna(url) or not str(url).strip():
        return "Need revision"
    url = normalize_url(url)
    if is_blocked(url):
        return "Social media"
    try:
        response = cffi_requests.get(
            url, impersonate="chrome120", timeout=15, allow_redirects=True,
        )
        status = response.status_code
        if 200 <= status < 400 or status == 403:
            return "OK"
        elif status == 404:
            return "Broken"
        return "Need revision"
    except CffiTimeout:
        return "OK"
    except (CffiConnectionError, CffiRequestException):
        return "Broken"
    except Exception:
        return "Need revision"


def check_urls_concurrent(urls: List[str], max_workers: int = 20) -> Dict[str, str]:
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(check_url, url): url for url in urls}
        for future in as_completed(future_to_url):
            raw = future_to_url[future]
            try:
                results[raw] = future.result()
            except Exception:
                results[raw] = "Need revision"
    return results


def fetch_about_page(url) -> dict:
    """
    Try /about then /about-us on the entity's root domain.
    Returns the first sub-page that responds with a non-404/non-error status.
    If both return 404 (or error), returns an empty content dict so the
    caller can still proceed (search results will carry the evidence instead).

    Cached per root URL (scheme + netloc) to avoid re-scraping the same
    domain across multiple rows.
    """
    empty = {"title": "", "meta_description": "", "h1": "", "text": ""}
    if pd.isna(url) or not str(url).strip():
        return empty

    raw    = normalize_url(str(url))
    parsed = urlparse(raw)
    root   = f"{parsed.scheme}://{parsed.netloc}"

    if root in about_cache:
        return about_cache[root]

    for path in ("/about", "/about-us"):
        about_url = root + path
        try:
            response = cffi_requests.get(
                about_url, impersonate="chrome120", timeout=15, allow_redirects=True
            )
            if response.status_code == 404:
                continue
            if response.status_code >= 400:
                continue

            soup   = BeautifulSoup(response.text, "html.parser")
            result = {"title": "", "meta_description": "", "h1": "", "text": ""}

            if soup.title and soup.title.string:
                result["title"] = soup.title.string.strip()
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                result["meta_description"] = meta.get("content").strip()[:500]
            h1 = soup.find("h1")
            if h1:
                result["h1"] = h1.get_text(" ", strip=True)[:300]
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = " ".join(soup.stripped_strings)
            result["text"] = re.sub(r"\s+", " ", text).strip()[:1200]

            about_cache[root] = result
            return result

        except Exception:
            continue

    about_cache[root] = empty
    return empty


def serper_search(query: str, count: int = 5) -> list[dict]:
    """
    Query Serper.dev (Google Search API).
    Sign up: https://serper.dev  |  Free tier: 2,500 queries/month
    Add to .env: SERPER_API_KEY=your_key_here
    """
    if not _serper_api_key:
        return []

    cache_key = f"{query}|{count}"
    if cache_key in search_cache:
        return search_cache[cache_key]

    headers = {
        "X-API-KEY":    _serper_api_key,
        "Content-Type": "application/json",
    }
    payload = {"q": query, "num": count}
    results: list[dict] = []

    try:
        resp = cffi_requests.post(
            "https://google.serper.dev/search",
            headers=headers,
            json=payload,
            timeout=20,
        )
        data = resp.json()

        for r in data.get("organic", []):
            results.append({
                "title":          r.get("title", ""),
                "description":    r.get("snippet", ""),
                "url":            r.get("link", ""),
                "extra_snippets": [],
            })

        kg = data.get("knowledgeGraph", {})
        if kg:
            kg_text = " | ".join(filter(None, [
                kg.get("title", ""),
                kg.get("type", ""),
                kg.get("description", ""),
            ]))
            if kg_text:
                results.insert(0, {
                    "title":          kg.get("title", "Knowledge Graph"),
                    "description":    kg_text,
                    "url":            kg.get("website", ""),
                    "extra_snippets": [],
                })

    except Exception:
        results = []

    search_cache[cache_key] = results
    return results[:count]


# ============================================================
# AI / LLM FUNCTIONS  (Haiku)
# ============================================================

def get_rules_from_haiku_website_only(
    name: str,
    domain: str,
    website_info: dict,
):
    """
    Match = "Yes": entity name closely matches its domain.
    About-page content is the sole evidence source; no search needed.
    Returns (rules, notes, description).
    """
    prompt = f"""You are helping build entity mention rules for English-language news matching.

Target entity:
Name: {name}
Domain: {domain}

The entity name closely matches its domain. Use the about-page content as primary evidence.

Tasks:
1. Extract mention rules (surface forms used to identify this entity in news text).
2. Write a concise entity description: 2-3 sentences covering what the entity is, what it does,
   and where it operates. Base it only on the evidence below — do not speculate.

Rules constraints:
- Return 0 to 5 rules only
- No generic phrases, no context-dependent phrases
- No longer wrappers around a shorter included rule
- No speculative aliases
- Surname-only for people only if clearly distinctive and evidenced
- Short name or acronym for organizations only if clearly evidenced

Website title: {website_info.get("title", "")}
Website meta description: {website_info.get("meta_description", "")}
Website H1: {website_info.get("h1", "")}
Website text excerpt: {website_info.get("text", "")}

Return strict JSON only:
{{
  "rules": ["rule1", "rule2"],
  "notes": "Very short note",
  "description": "2-3 sentence factual description of the entity"
}}"""

    retries = 0
    while retries < 3:
        try:
            response = claude_client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=450,
                messages=[{"role": "user", "content": prompt}],
            )
            return parse_rules_response(response.content[0].text.strip())
        except anthropic.RateLimitError:
            retries += 1
            wait = 60 * retries
            print(f"Rate limit hit for '{name}'. Waiting {wait}s …")
            time.sleep(wait)
        except Exception as e:
            print(f"Haiku API error for '{name}': {e}")
            return [], "", ""
    return [], "", ""


def get_rules_from_haiku_evidence(
    name: str,
    domain: str,
    website_info: dict,
    search_results: list[dict],
):
    """
    Match = "No" / "Similar": entity name doesn't match domain well.
    About-page content + Serper search results are both used as evidence.
    Returns (rules, notes, description).
    """
    search_text_parts = []
    for i, r in enumerate(search_results[:5], start=1):
        search_text_parts.append(f"{i}. {r['title']}\n{r['description']}\n{r['url']}")
        for extra in r.get("extra_snippets", [])[:2]:
            search_text_parts.append(f"Extra: {extra}")

    prompt = f"""You are helping build entity mention rules for English-language news matching.

Target entity:
Name: {name}
Domain: {domain}

The entity name does NOT closely match its domain. Use search results to identify the real
entity and confirm its aliases.

Use website evidence to confirm identity.
Use search results only if they clearly refer to the same entity.
Ignore mismatched or ambiguous results.

Tasks:
1. Extract mention rules (surface forms used to identify this entity in news text).
2. Write a concise entity description: 2-3 sentences covering what the entity is, what it does,
   and where it operates. Base it only on the evidence below — do not speculate.

Rules constraints:
- Return 0 to 5 rules only
- No generic phrases, no context-dependent phrases
- No longer wrappers around a shorter included rule
- No speculative aliases
- Surname-only for people only if clearly distinctive and evidenced
- Short name or acronym for organizations only if clearly evidenced

Website title: {website_info.get("title", "")}
Website meta description: {website_info.get("meta_description", "")}
Website H1: {website_info.get("h1", "")}
Website text excerpt: {website_info.get("text", "")}

Search results:
{chr(10).join(search_text_parts) if search_text_parts else "(none)"}

Return strict JSON only:
{{
  "rules": ["rule1", "rule2"],
  "notes": "Very short note",
  "description": "2-3 sentence factual description of the entity"
}}"""

    retries = 0
    while retries < 3:
        try:
            response = claude_client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )
            return parse_rules_response(response.content[0].text.strip())
        except anthropic.RateLimitError:
            retries += 1
            wait = 60 * retries
            print(f"Rate limit hit for '{name}'. Waiting {wait}s …")
            time.sleep(wait)
        except Exception as e:
            print(f"Haiku API error for '{name}': {e}")
            return [], "", ""
    return [], "", ""


# ============================================================
# ORCHESTRATION FUNCTIONS
# ============================================================

def build_search_queries(name: str, domain: str, website_info: Dict[str, str]) -> List[str]:
    queries = []
    if name and domain:
        queries.append(f'"{name}" "{domain}"')
    if name:
        queries.append(f'"{name}" company')
    if name and website_info.get("title"):
        queries.append(f'"{name}" "{website_info["title"][:80]}"')
    seen: set[str] = set()
    deduped = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append(q)
    return deduped[:3]


def generate_rules_for_row(row) -> dict:
    """
    V3 pipeline — no memory pass, straight to evidence:

    1. Cache hit on raw URL  → free for exact duplicate rows
    2. Cache hit on base name → free for same-entity variants
       (e.g. "Chemieuro" and "Chemieuro (Francia)" share one research call)
    3. Fetch /about (fall back to /about-us; if both 404 → empty content)
    4. No-evidence short-circuit: skip LLM when there is nothing to analyse;
       leave rules and description blank, record the reason in Notes
    5. Match = "Yes"          → about-page → Haiku (rules + description)
       Match = "No/Similar"   → about-page + Serper search → Haiku (rules + description)
    """
    raw_url = str(row["URL"])

    # ── Level 1: exact URL duplicate ──────────────────────────
    if raw_url in rules_cache:
        return rules_cache[raw_url]

    name      = row["Normalized Name"]
    base_name = extract_base_name(name)
    domain    = row["Normalized Website"]
    match     = row.get("Match", "Unknown")

    # ── Level 2: same entity, different regional variant ──────
    if base_name and base_name in base_name_cache:
        cached     = base_name_cache[base_name].copy()
        prior_note = cached.get("Rules Evidence Notes") or ""
        cached["Rules Evidence Notes"] = (
            (prior_note + " [shared from base entity]").strip()
        )
        rules_cache[raw_url] = cached
        return cached

    _no_rules = {
        "Rule 1": "", "Rule 2": "", "Rule 3": "",
        "Rule 4": "", "Rule 5": "", "Entity Description": "",
    }

    about_info = fetch_about_page(raw_url)
    has_about  = any(about_info.values())

    if match == "Yes":
        # ── No-evidence short-circuit ─────────────────────────
        if not has_about:
            output = {**_no_rules, "Rules Evidence Notes": "No about page found"}
            rules_cache[raw_url]       = output
            base_name_cache[base_name] = output
            return output
        rules, notes, description = get_rules_from_haiku_website_only(
            name, domain, about_info
        )

    else:
        queries = build_search_queries(name, domain, about_info)
        search_results: list[dict] = []
        for q in queries:
            search_results.extend(serper_search(q, count=5))
            if len(search_results) >= 5:
                break

        # ── No-evidence short-circuit ─────────────────────────
        if not has_about and not search_results:
            output = {**_no_rules, "Rules Evidence Notes": "No evidence found"}
            rules_cache[raw_url]       = output
            base_name_cache[base_name] = output
            return output

        rules, notes, description = get_rules_from_haiku_evidence(
            name, domain, about_info, search_results[:5]
        )

    padded = (rules + ["", "", "", "", ""])[:5]
    output = {
        "Rule 1":               padded[0],
        "Rule 2":               padded[1],
        "Rule 3":               padded[2],
        "Rule 4":               padded[3],
        "Rule 5":               padded[4],
        "Rules Evidence Notes": notes,
        "Entity Description":   description,
    }

    rules_cache[raw_url]       = output
    base_name_cache[base_name] = output
    return output


# ============================================================
# STREAMLIT UI
# ============================================================

st.title("Data Cleaning Tool  —  V3")
st.caption("Haiku + Serper.dev  |  Direct evidence phase  |  Sub-page scraping (/about)")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write("Preview:", df.head())

    if st.button("Run Cleaning"):
        # Reset all in-memory caches so each run is independent
        rules_cache.clear()
        base_name_cache.clear()
        about_cache.clear()
        search_cache.clear()

        progress_bar = st.progress(0)
        status_text  = st.empty()

        # ── 1. Clean Name ──────────────────────────────────────
        status_text.text("Cleaning names…")
        df["Normalized Name"] = df["Name"].apply(clean_name)
        progress_bar.progress(8)

        # ── 2. Base Name (for research sharing across entity variants) ──
        df["Base Name"] = df["Normalized Name"].apply(extract_base_name)
        progress_bar.progress(12)

        # ── 3. Normalize Website ───────────────────────────────
        status_text.text("Extracting domains…")
        df["Normalized Website"] = df["URL"].apply(extract_domain)
        progress_bar.progress(20)

        # ── 4. Duplicate Flag on RAW URL ───────────────────────
        status_text.text("Checking duplicates…")
        df["Duplicate Flag"] = df["URL"].duplicated(keep=False)
        progress_bar.progress(30)

        # ── 5. Website Status (concurrent) ─────────────────────
        status_text.text("Validating URLs concurrently…")
        url_statuses = check_urls_concurrent(df["URL"].tolist(), max_workers=20)
        df["Website Status"] = df["URL"].map(url_statuses).fillna("Need revision")
        progress_bar.progress(50)

        # ── 6. Latin-Script Filter ─────────────────────────────
        status_text.text("Checking script/language…")
        latin_mask = df["Normalized Name"].apply(is_latin_script)
        progress_bar.progress(58)

        # ── 7. Match Column ────────────────────────────────────
        # Must run before rule generation so generate_rules_for_row
        # can use the Match value to choose its evidence strategy.
        status_text.text("Matching names to URLs…")
        df["Match"] = df.apply(
            lambda row: match_name_url(
                row["Normalized Name"],
                normalize_url_for_match(row["URL"]),
            ),
            axis=1,
        )
        progress_bar.progress(70)

        # ── 8. Generate Rules + Descriptions (V3: direct evidence) ──
        status_text.text("Generating rules and descriptions (/about scrape)…")
        rule_cols = [
            "Rule 1", "Rule 2", "Rule 3", "Rule 4", "Rule 5",
            "Rules Evidence Notes", "Entity Description",
        ]
        for col in rule_cols:
            df[col] = ""

        eligible_indices = [
            idx for idx, row in df.iterrows()
            if row["Website Status"] == "OK"
            and latin_mask[idx]
            and row["Normalized Website"]
        ]
        total_eligible = len(eligible_indices)

        for processed, idx in enumerate(eligible_indices, start=1):
            row = df.loc[idx]
            rules_data = generate_rules_for_row(row)
            for col, value in rules_data.items():
                df.at[idx, col] = value
            progress = 70 + int((processed / max(total_eligible, 1)) * 28)
            progress_bar.progress(min(progress, 98))

        progress_bar.progress(100)
        status_text.text("")
        st.success("✅ Processing completed!")

        output_columns = [
            "Name",            "URL",
            "Normalized Name", "Base Name",
            "Normalized Website",
            "Duplicate Flag",
            "Website Status",
            "Match",
            "Entity Description",
            "Rule 1", "Rule 2", "Rule 3", "Rule 4", "Rule 5",
            "Rules Evidence Notes",
        ]
        df_output = df[output_columns]
        st.write(df_output.head(10))

        csv = df_output.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Cleaned CSV",
            csv,
            "cleaned_data_v3.csv",
            "text/csv",
        )