"""
server.py — Rebuild Digital Co
FastAPI server running on Railway.

Original endpoints (Google Maps business pipeline):
  POST /scrape          — run the full Maps→email→analyze→DB pipeline
  POST /send-followup   — send a follow-up email
  POST /send-proposal   — send a proposal to a specific business
  GET  /preview-proposal — preview proposal HTML
  POST /reset-db        — wipe all data (irreversible)
  GET  /status          — current scrape status + log
  GET  /health          — health check

Universal scraper endpoints (all job types):
  POST /universal-scrape      — run any job type with a full config dict
  POST /scrape/google-maps    — shortcut: Google Maps business scraper
  POST /scrape/prices         — shortcut: price scraper
  POST /scrape/trivia         — shortcut: trivia Q&A scraper
  POST /scrape/emails         — shortcut: email harvester
  POST /scrape/content        — shortcut: content / article scraper
  POST /scrape/paginated      — shortcut: paginated list scraper

Results & session history:
  GET  /scrape-sessions       — list all past scrape sessions
  GET  /results               — query scraped results (filter by job_type, session_id)
  GET  /results/{session_id}  — all results for a specific session
"""

import os
import asyncio
import json
import threading
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from supabase import create_client
import uvicorn

load_dotenv()

app = FastAPI(title="Rebuild Digital Co — Universal Scraper API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
db = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

# ── Shared state ──────────────────────────────────────────────────────────────
# Tracks the currently running scrape (only one at a time)
scrape_state = {
    "running":  False,
    "log":      [],
    "run_id":   None,
    "job_type": None,
}


# ═════════════════════════════════════════════════════════════════════════════
#  REQUEST MODELS
# ═════════════════════════════════════════════════════════════════════════════

class ScrapeRequest(BaseModel):
    """Original Google Maps business pipeline (backward compat)."""
    location:    str  = "Dallas, TX"
    limit:       int  = 10
    send_emails: bool = False
    categories:  list = []


class UniversalScrapeRequest(BaseModel):
    """
    Universal job config. Fields used depend on job_type.
    Matches the job dict schema in universal_scraper.py.
    """
    job_type: str = Field(
        ...,
        description=(
            "One of: google_maps_business | price_scraper | trivia_scraper | "
            "email_harvester | content_scraper | paginated_scraper"
        ),
    )
    # ── Google Maps ─────────────────────────────────
    location:   str  = "Dallas TX"
    limit:      int  = 10
    categories: list = []          # list of [query, label] pairs or label strings

    # ── URL-based scrapers ──────────────────────────
    urls:       list = []          # list of URLs to visit

    # ── CSS selectors (price / content / trivia) ────
    selectors:  dict = {}          # e.g. {"row": ".item", "name": ".name", "price": ".price"}
    each_item:  str  = ""          # repeating container selector for content scraper
    fields:     dict = {}          # {field_name: css_selector} for paginated scraper

    # ── Metadata ────────────────────────────────────
    category:   str  = ""
    label:      str  = ""

    # ── Trivia ──────────────────────────────────────
    use_default_trivia_sources: bool = True

    # ── Email harvester ─────────────────────────────
    dig_contact: bool = True

    # ── Paginated ───────────────────────────────────
    start_url:  str  = ""
    next_sel:   str  = 'a[rel="next"]'
    item_sel:   str  = ""
    max_pages:  int  = 10

    # ── Destination ─────────────────────────────────
    destination_table: str           = "scrape_results"  # table in home project
    external_db_id:    Optional[str] = None              # UUID of an external_databases row


class PriceScrapeRequest(BaseModel):
    urls:       list
    category:   str  = ""
    selectors:  dict = {}


class TriviaScrapeRequest(BaseModel):
    use_default_sources: bool = True
    urls:                list = []
    selectors:           dict = {}
    category:            str  = "trivia"
    limit:               int  = 100


class EmailHarvestRequest(BaseModel):
    urls:        list
    label:       str  = ""
    dig_contact: bool = True


class ContentScrapeRequest(BaseModel):
    urls:      list
    selectors: dict = {}
    each_item: str  = ""
    category:  str  = "content"


class PaginatedScrapeRequest(BaseModel):
    start_url: str
    item_sel:  str
    fields:    dict = {}
    next_sel:  str  = 'a[rel="next"]'
    max_pages: int  = 10
    category:  str  = ""


class FollowUpRequest(BaseModel):
    business_id:    str
    follow_up_id:   str
    to_email:       str
    business_name:  str


class SendProposalRequest(BaseModel):
    business_id: str


# ═════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def log(msg: str):
    ts  = datetime.now().strftime("%H:%M:%S")
    msg = f"[{ts}] {msg}"
    print(msg)
    scrape_state["log"].append(msg)


def _create_run_record(job_type: str, label: str = "", location: str = "") -> Optional[str]:
    """Insert a scrape_runs row and return its id."""
    if not db:
        return None
    try:
        result = db.table("scrape_runs").insert({
            "location":    location or job_type,
            "categories":  label or job_type,
            "job_type":    job_type,
            "status":      "running",
            "triggered_by": "app",
        }).execute()
        return result.data[0]["id"] if result.data else None
    except Exception as e:
        print(f"[!] Could not create scrape_runs record: {e}")
        return None


def _finish_run(run_id: Optional[str], result_count: int, error: str = ""):
    if not db or not run_id:
        return
    try:
        payload = {
            "status":       "failed" if error else "completed",
            "completed_at": datetime.now().isoformat(),
            "result_count": result_count,
        }
        if error:
            payload["error_message"] = error[:500]
        else:
            # backward compat — businesses_found was the old field
            payload["businesses_found"] = result_count
        db.table("scrape_runs").update(payload).eq("id", run_id).execute()
    except Exception as e:
        print(f"[!] Could not update scrape_runs record: {e}")


def _save_results_to_db(results: list, job_type: str, run_id: Optional[str]):
    """
    Upsert all scraped rows into scrape_results table.
    Also re-saves google_maps_business results to the businesses table
    for backward compatibility with the tracker app.
    """
    if not db or not results:
        return

    # ── scrape_results (all job types) ───────────────────────────────────────
    rows = []
    for row in results:
        # Pick the best "name" field depending on job type
        name = (
            row.get("name")
            or row.get("question")
            or row.get("email")
            or row.get("title")
            or ""
        )
        rows.append({
            "job_type":   job_type,
            "session_id": run_id,
            "data":       row,
            "name":       name[:500] if name else "",
            "category":   str(row.get("category", ""))[:200],
            "source_url": str(row.get("source_url", ""))[:1000],
            "scraped_at": row.get("scraped_at", datetime.now().isoformat()),
        })

    try:
        # Insert in batches of 100 to avoid payload limits
        for i in range(0, len(rows), 100):
            db.table("scrape_results").insert(rows[i:i+100]).execute()
        log(f"Saved {len(rows)} rows → scrape_results")
    except Exception as e:
        log(f"[!] DB insert error (scrape_results): {e}")

    # ── businesses table (Google Maps job only — tracker app compatibility) ──
    if job_type == "google_maps_business":
        from analyzer import analyze_website
        for biz in results:
            try:
                analysis = analyze_website(biz.get("website", ""))
                row = {
                    "name":              biz.get("name"),
                    "category":          biz.get("category"),
                    "website":           biz.get("website"),
                    "phone":             biz.get("phone"),
                    "address":           biz.get("address", ""),
                    "email":             analysis.get("email_found") or biz.get("email", ""),
                    "opportunity_score": analysis["score"],
                    "issues":            " | ".join(analysis["issues"]),
                    "has_ssl":           analysis["has_ssl"],
                    "has_mobile":        analysis["has_mobile_meta"],
                    "has_booking":       analysis["has_booking"],
                    "has_portal":        analysis["has_client_portal"],
                    "has_pricing":       analysis["has_pricing"],
                    "status":            "new",
                    "yelp_url":          biz.get("yelp_url", ""),
                    "scrape_run_id":     run_id,
                }
                existing = db.table("businesses").select("id").eq("name", row["name"]).execute()
                if existing.data:
                    db.table("businesses").update(row).eq("name", row["name"]).execute()
                else:
                    db.table("businesses").insert(row).execute()
            except Exception as e:
                log(f"  [!] businesses table error for {biz.get('name')}: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  UNIVERSAL PIPELINE  (background task)
# ═════════════════════════════════════════════════════════════════════════════

def _build_job_dict(req: UniversalScrapeRequest) -> dict:
    """Convert a UniversalScrapeRequest into the job dict universal_scraper expects."""
    from universal_scraper import MAPS_CATEGORIES_REBUILD, DEFAULT_TRIVIA_SOURCES

    job = {"type": req.job_type}

    if req.job_type == "google_maps_business":
        if req.categories:
            # Support both [[query, label], ...] and [label, ...] formats
            cats = []
            for c in req.categories:
                if isinstance(c, list) and len(c) == 2:
                    cats.append(tuple(c))
                elif isinstance(c, str):
                    cats.append((c.lower(), c))
            job["categories"] = cats or MAPS_CATEGORIES_REBUILD
        else:
            job["categories"] = MAPS_CATEGORIES_REBUILD
        job["location"] = req.location
        job["limit"]    = req.limit

    elif req.job_type == "price_scraper":
        job["urls"]      = req.urls
        job["category"]  = req.category or req.label
        job["selectors"] = req.selectors

    elif req.job_type == "trivia_scraper":
        job["sources"]   = DEFAULT_TRIVIA_SOURCES if req.use_default_trivia_sources else []
        job["urls"]      = req.urls
        job["selectors"] = req.selectors
        job["category"]  = req.category or "trivia"
        job["limit"]     = req.limit

    elif req.job_type == "email_harvester":
        job["urls"]        = req.urls
        job["label"]       = req.label or req.category
        job["dig_contact"] = req.dig_contact

    elif req.job_type == "content_scraper":
        job["urls"]      = req.urls
        job["selectors"] = req.selectors
        job["each_item"] = req.each_item
        job["category"]  = req.category

    elif req.job_type == "paginated_scraper":
        job["start_url"] = req.start_url or (req.urls[0] if req.urls else "")
        job["item_sel"]  = req.item_sel
        job["fields"]    = req.fields
        job["next_sel"]  = req.next_sel
        job["max_pages"] = req.max_pages
        job["category"]  = req.category

    # Always save to DB via this pipeline (not via universal_scraper's own output handler)
    job["output"] = "print"
    return job


def _save_to_destination_table(table: str, results: list, ext_db=None):
    """
    Save scraped rows into a user-chosen table.
    ext_db: a supabase client for an external project, or None to use the default db.
    Queries column list first so only matching fields are inserted.
    """
    client = ext_db or db
    if not client or not results or not table or (table == "scrape_results" and ext_db is None):
        return

    try:
        cols_resp = client.rpc("get_table_columns", {"p_table_name": table}).execute()
        if not cols_resp.data:
            # Fall back: try information_schema directly
            log(f"  [!] get_table_columns RPC not found on external DB — inserting all fields")
            rows = results
        else:
            valid_cols = {r["column_name"] for r in cols_resp.data}
            log(f"  → Destination '{table}' columns: {', '.join(sorted(valid_cols))}")
            rows = [{k: v for k, v in row.items() if k in valid_cols} for row in results]
            rows = [r for r in rows if r]

        if not rows:
            log(f"  [!] No scraped fields matched columns in '{table}' — nothing inserted")
            return

        for i in range(0, len(rows), 100):
            client.table(table).insert(rows[i:i + 100]).execute()

        log(f"  ✓ {len(rows)} rows → {table}")

    except Exception as e:
        log(f"  [!] Could not save to '{table}': {e}")


def _get_external_db_client(ext_db_id: str):
    """Fetch external DB credentials from Supabase and return a client, or None."""
    if not db or not ext_db_id:
        return None, None
    try:
        resp = db.table("external_databases").select("*").eq("id", ext_db_id).single().execute()
        if not resp.data:
            log(f"  [!] External DB '{ext_db_id}' not found")
            return None, None
        rec = resp.data
        ext_client = create_client(rec["supabase_url"], rec["supabase_key"])
        return ext_client, rec
    except Exception as e:
        log(f"  [!] Could not load external DB: {e}")
        return None, None


async def run_universal_pipeline(
    job: dict,
    run_id: Optional[str],
    destination_table: str = "scrape_results",
    external_db_id: Optional[str] = None,
):
    scrape_state["running"]  = True
    scrape_state["job_type"] = job["type"]
    scrape_state["log"]      = [f"[{datetime.now().strftime('%H:%M:%S')}] Starting {job['type']} job..."]

    results = []
    try:
        from universal_scraper import _dispatch_job
        from playwright.async_api import async_playwright

        log("Launching browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            results = await _dispatch_job(job, browser)
            await browser.close()

        log(f"Scrape complete — {len(results)} results")

        if db:
            # Always save audit trail to scrape_results in the home project
            log("Saving to scrape_results (audit trail)...")
            _save_results_to_db(results, job["type"], run_id)

            if external_db_id:
                # Save to an external Supabase project
                log(f"Loading external database connection...")
                ext_client, ext_rec = _get_external_db_client(external_db_id)
                if ext_client:
                    tbl = destination_table or ext_rec.get("default_table", "scrape_results")
                    log(f"Saving to external DB '{ext_rec['label']}' → table '{tbl}'...")
                    _save_to_destination_table(tbl, results, ext_db=ext_client)
                else:
                    log("[!] Could not connect to external DB — data is in audit trail only")
            elif destination_table and destination_table != "scrape_results":
                # Save to a different table in the home project
                log(f"Saving to destination table: {destination_table}...")
                _save_to_destination_table(destination_table, results)
        else:
            log("[!] No DB connection — results not persisted")

        _finish_run(run_id, len(results))
        dest_label = (
            f"external DB '{external_db_id}'" if external_db_id
            else destination_table if destination_table != "scrape_results"
            else "scrape_results"
        )
        log(f"Done. Session {run_id} complete. Results in: scrape_results + {dest_label}")

    except Exception as e:
        log(f"[!] Pipeline error: {e}")
        _finish_run(run_id, len(results), error=str(e))

    scrape_state["running"] = False


# ═════════════════════════════════════════════════════════════════════════════
#  ORIGINAL GOOGLE MAPS PIPELINE  (backward compat — unchanged logic)
# ═════════════════════════════════════════════════════════════════════════════

async def run_scrape_pipeline(location, limit, send_emails, run_id, categories=None):
    scrape_state["running"]  = True
    scrape_state["job_type"] = "google_maps_business"
    scrape_state["log"]      = [f"[{datetime.now().strftime('%H:%M:%S')}] Starting scrape for {location}..."]

    try:
        from scraper import scrape_category, ALL_CATEGORIES
        from playwright.async_api import async_playwright

        log("Launching browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            if categories:
                cat_map  = {label: query for query, label in ALL_CATEGORIES}
                run_cats = []
                for c in categories:
                    if c in cat_map:
                        run_cats.append((cat_map[c], c))
                    else:
                        run_cats.append((c.lower(), c))
            else:
                run_cats = ALL_CATEGORIES

            log(f"Categories: {', '.join(l for _, l in run_cats)}")
            all_businesses = []

            for query, label in run_cats:
                log(f"Scraping {label}...")
                try:
                    bizs = await scrape_category(browser, query, label, limit=limit, location=location)
                    all_businesses.extend(bizs)
                    log(f"  Found {len(bizs)} businesses")
                except Exception as e:
                    log(f"  Error: {e}")
                await asyncio.sleep(2)

            await browser.close()

        # Deduplicate
        seen, unique = set(), []
        for b in all_businesses:
            key = b["name"].lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(b)

        log(f"Total unique businesses: {len(unique)}")

        # Analyze
        from analyzer import analyze_website
        analyzed = []
        for biz in unique:
            log(f"Analyzing {biz['name']}...")
            try:
                analysis = analyze_website(biz.get("website", ""))
                biz["email"]             = analysis.get("email_found") or biz.get("email", "")
                biz["opportunity_score"] = analysis["score"]
                biz["issues"]            = " | ".join(analysis["issues"])
                biz["has_ssl"]           = analysis["has_ssl"]
                biz["has_mobile"]        = analysis["has_mobile_meta"]
                biz["has_booking"]       = analysis["has_booking"]
                biz["has_portal"]        = analysis["has_client_portal"]
                biz["has_pricing"]       = analysis["has_pricing"]
            except Exception as e:
                log(f"  Analysis error: {e}")
            analyzed.append(biz)

        # Write to Supabase
        if db:
            log("Writing to Supabase...")
            emails_queued = 0
            high_opp      = 0
            for biz in analyzed:
                row = {
                    "name":              biz.get("name"),
                    "category":          biz.get("category"),
                    "website":           biz.get("website"),
                    "phone":             biz.get("phone"),
                    "address":           biz.get("address"),
                    "email":             biz.get("email"),
                    "opportunity_score": biz.get("opportunity_score", 0),
                    "issues":            biz.get("issues", ""),
                    "has_ssl":           biz.get("has_ssl", False),
                    "has_mobile":        biz.get("has_mobile", False),
                    "has_booking":       biz.get("has_booking", False),
                    "has_portal":        biz.get("has_portal", False),
                    "has_pricing":       biz.get("has_pricing", False),
                    "status":            "new",
                    "yelp_url":          biz.get("yelp_url", ""),
                    "scrape_run_id":     run_id,
                }
                try:
                    existing = db.table("businesses").select("id").eq("name", row["name"]).execute()
                    if existing.data:
                        db.table("businesses").update(row).eq("name", row["name"]).execute()
                    else:
                        db.table("businesses").insert(row).execute()
                    if biz.get("opportunity_score", 0) >= 5:
                        high_opp += 1
                    if biz.get("email"):
                        emails_queued += 1
                except Exception as e:
                    log(f"  DB error for {biz.get('name')}: {e}")

            # Also write to scrape_results for unified session view
            _save_results_to_db(
                [{"name": b["name"], "category": b["category"],
                  "website": b.get("website"), "email": b.get("email"),
                  "phone": b.get("phone"), "opportunity_score": b.get("opportunity_score", 0),
                  "source_url": b.get("website", ""), "scraped_at": datetime.now().isoformat()}
                 for b in analyzed],
                "google_maps_business", run_id
            )

            db.table("scrape_runs").update({
                "status":           "completed",
                "completed_at":     datetime.now().isoformat(),
                "businesses_found": len(analyzed),
                "result_count":     len(analyzed),
                "emails_queued":    emails_queued,
                "high_opp_count":   high_opp,
            }).eq("id", run_id).execute()

            log(f"Saved {len(analyzed)} businesses to Supabase")

        # Send emails
        emails_sent = 0
        if send_emails:
            log("Generating proposals and sending emails...")
            from proposal_gen import generate_html_email, generate_subject_line
            from mockup_gen import run_mockup
            from emailer import send_proposal

            targets = [b for b in analyzed if b.get("email") and b.get("opportunity_score", 0) >= 3]
            for biz in targets[:50]:
                try:
                    shots   = run_mockup(biz)
                    html    = generate_html_email(biz, mockup_screenshots=shots)
                    subject = generate_subject_line(biz)
                    result  = send_proposal(biz["name"], biz["email"], subject, html, dry_run=False)
                    if db and result.get("success"):
                        biz_row = db.table("businesses").select("id").eq("name", biz["name"]).execute()
                        if biz_row.data:
                            biz_id = biz_row.data[0]["id"]
                            db.table("email_logs").insert({
                                "business_id": biz_id, "subject": subject,
                                "resend_id":   result.get("id", ""),
                                "to_email":    biz["email"],
                                "status":      "sent", "email_type": "initial",
                            }).execute()
                            db.table("businesses").update({"status": "emailed"}).eq("id", biz_id).execute()
                            fu_date = (datetime.now() + timedelta(days=5)).isoformat()
                            db.table("follow_ups").insert({
                                "business_id": biz_id, "scheduled_for": fu_date,
                                "follow_up_number": 1,
                            }).execute()
                        emails_sent += 1
                        log(f"  Sent to {biz['name']}")
                except Exception as e:
                    log(f"  Email error for {biz.get('name')}: {e}")

            if db:
                db.table("scrape_runs").update({"emails_sent": emails_sent}).eq("id", run_id).execute()

        log(f"Pipeline complete. {len(analyzed)} businesses, {emails_sent} emails sent.")

    except Exception as e:
        log(f"Pipeline error: {e}")
        if db and scrape_state.get("run_id"):
            db.table("scrape_runs").update({
                "status": "failed",
                "completed_at": datetime.now().isoformat(),
                "error_message": str(e),
            }).eq("id", run_id).execute()

    scrape_state["running"] = False


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTES — STATUS / HEALTH / TABLES
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {
        "status":   "online",
        "db":       bool(db),
        "db_url":   bool(SUPABASE_URL),
        "running":  scrape_state["running"],
        "job_type": scrape_state.get("job_type"),
    }


@app.get("/tables")
async def list_tables():
    """
    Return all user-created tables in the Supabase public schema.
    The React Scraper tab calls this to populate the 'Save Results To' dropdown.
    """
    if not db:
        return {"tables": [], "error": "No database connection"}
    try:
        resp = db.rpc("get_user_tables").execute()
        tables = [r["table_name"] for r in (resp.data or [])]
        return {"tables": tables}
    except Exception as e:
        return {"tables": [], "error": str(e)}


# ── External Databases ───────────────────────────────────────────────────────

class ExternalDBCreate(BaseModel):
    label:         str
    supabase_url:  str
    supabase_key:  str
    default_table: str = ""


@app.get("/external-databases")
async def list_external_databases():
    if not db:
        return {"databases": [], "error": "No DB connection"}
    try:
        resp = db.table("external_databases").select("id,label,supabase_url,default_table,created_at").order("created_at").execute()
        return {"databases": resp.data or []}
    except Exception as e:
        return {"databases": [], "error": str(e)}


@app.post("/external-databases")
async def add_external_database(payload: ExternalDBCreate):
    if not db:
        return {"error": "No DB connection"}
    # Validate the credentials actually work before saving
    try:
        test_client = create_client(payload.supabase_url, payload.supabase_key)
        test_client.table("_nonexistent_").select("*").limit(1).execute()
    except Exception:
        pass  # Any response (even 404) means we can connect — only network failures are a problem
    try:
        resp = db.table("external_databases").insert({
            "label":         payload.label,
            "supabase_url":  payload.supabase_url,
            "supabase_key":  payload.supabase_key,
            "default_table": payload.default_table,
        }).execute()
        return {"ok": True, "database": resp.data[0] if resp.data else {}}
    except Exception as e:
        return {"error": str(e)}


@app.get("/external-databases/{db_id}/tables")
async def list_external_db_tables(db_id: str):
    """List tables in an external Supabase project."""
    ext_client, rec = _get_external_db_client(db_id)
    if not ext_client:
        return {"tables": [], "error": "Could not connect to external database"}
    try:
        resp = ext_client.rpc("get_user_tables").execute()
        tables = [r["table_name"] for r in (resp.data or [])]
        return {"tables": tables, "label": rec["label"]}
    except Exception as e:
        return {"tables": [], "error": f"get_user_tables RPC not available: {e}"}


@app.delete("/external-databases/{db_id}")
async def delete_external_database(db_id: str):
    if not db:
        return {"error": "No DB connection"}
    try:
        db.table("external_databases").delete().eq("id", db_id).execute()
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


@app.get("/status")
def status():
    return {
        "running":  scrape_state["running"],
        "job_type": scrape_state.get("job_type"),
        "run_id":   scrape_state["run_id"],
        "log":      scrape_state["log"][-100:],
    }


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTES — ORIGINAL SCRAPE (backward compat)
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/scrape")
async def trigger_scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    if scrape_state["running"]:
        return {"error": "A scrape is already running", "log": scrape_state["log"]}

    run_id = _create_run_record("google_maps_business", "all" if not req.categories else ",".join(req.categories), req.location)
    scrape_state["run_id"] = run_id

    background_tasks.add_task(
        asyncio.run,
        run_scrape_pipeline(req.location, req.limit, req.send_emails, run_id, req.categories or []),
    )
    return {"status": "started", "run_id": run_id, "job_type": "google_maps_business"}


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTES — UNIVERSAL SCRAPE
# ═════════════════════════════════════════════════════════════════════════════

def _start_universal_job(
    job: dict,
    label: str = "",
    background_tasks: BackgroundTasks = None,
    destination_table: str = "scrape_results",
    external_db_id: Optional[str] = None,
):
    """Shared logic: create DB record, enqueue background task, return response."""
    if scrape_state["running"]:
        return {"error": "A scrape is already running. Check /status for progress.", "log": scrape_state["log"]}

    run_id = _create_run_record(job["type"], label)
    scrape_state["run_id"] = run_id

    background_tasks.add_task(
        asyncio.run,
        run_universal_pipeline(job, run_id, destination_table, external_db_id=external_db_id),
    )
    return {
        "status":            "started",
        "run_id":            run_id,
        "job_type":          job["type"],
        "destination_table": destination_table,
        "external_db_id":    external_db_id,
        "message":           f"Job started. Poll /status for live log. Results at /results/{run_id}",
    }


@app.post("/universal-scrape")
async def universal_scrape(req: UniversalScrapeRequest, background_tasks: BackgroundTasks):
    """
    Run any scraper job type with a full config.

    Example body (price scraper):
    {
      "job_type": "price_scraper",
      "urls": ["https://example.com/beer-menu"],
      "category": "Beer",
      "selectors": {"row": ".menu-item", "name": ".name", "price": ".price"}
    }

    Example body (trivia scraper):
    {
      "job_type": "trivia_scraper",
      "use_default_trivia_sources": true,
      "limit": 200,
      "category": "General Knowledge"
    }
    """
    try:
        job = _build_job_dict(req)
        return _start_universal_job(
            job,
            label=req.category or req.label or req.job_type,
            background_tasks=background_tasks,
            destination_table=req.destination_table,
            external_db_id=req.external_db_id,
        )
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})


@app.post("/scrape/google-maps")
async def scrape_google_maps(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """Convenience shortcut — identical to /scrape but goes through the universal pipeline."""
    from universal_scraper import MAPS_CATEGORIES_REBUILD
    cats = []
    if req.categories:
        for c in req.categories:
            cats.append((c.lower(), c))
    job = {
        "type":       "google_maps_business",
        "categories": cats or MAPS_CATEGORIES_REBUILD,
        "location":   req.location,
        "limit":      req.limit,
        "output":     "print",
    }
    return _start_universal_job(job, label=req.location, background_tasks=background_tasks)


@app.post("/scrape/prices")
async def scrape_prices(req: PriceScrapeRequest, background_tasks: BackgroundTasks):
    """
    Scrape item + price pairs from menu/product pages.

    Example:
    {
      "urls": ["https://peticolas.com/beer"],
      "category": "Craft Beer - Dallas",
      "selectors": {"row": ".beer-item", "name": ".beer-name", "price": ".price"}
    }
    Leave selectors empty to use regex fallback.
    """
    job = {
        "type":      "price_scraper",
        "urls":      req.urls,
        "category":  req.category,
        "selectors": req.selectors,
        "output":    "print",
    }
    return _start_universal_job(job, label=req.category or "price_scraper", background_tasks=background_tasks)


@app.post("/scrape/trivia")
async def scrape_trivia(req: TriviaScrapeRequest, background_tasks: BackgroundTasks):
    """
    Pull trivia Q&A pairs. Uses Open Trivia DB by default (no URLs needed).

    Example (default API sources):
    {"use_default_sources": true, "limit": 200, "category": "Sports"}

    Example (custom HTML pages):
    {
      "use_default_sources": false,
      "urls": ["https://triviasite.com/questions"],
      "selectors": {"question": ".question", "answer": ".answer"}
    }
    """
    from universal_scraper import DEFAULT_TRIVIA_SOURCES
    job = {
        "type":     "trivia_scraper",
        "sources":  DEFAULT_TRIVIA_SOURCES if req.use_default_sources else [],
        "urls":     req.urls,
        "selectors": req.selectors,
        "category": req.category,
        "limit":    req.limit,
        "output":   "print",
    }
    return _start_universal_job(job, label=req.category or "trivia", background_tasks=background_tasks)


@app.post("/scrape/emails")
async def scrape_emails(req: EmailHarvestRequest, background_tasks: BackgroundTasks):
    """
    Collect every email address from a list of websites.
    Also digs /contact, /about, /contact-us automatically.

    Example:
    {
      "urls": ["https://somerestaurant.com", "https://anotherplace.com"],
      "label": "Dallas Restaurant Leads"
    }
    """
    job = {
        "type":        "email_harvester",
        "urls":        req.urls,
        "label":       req.label,
        "dig_contact": req.dig_contact,
        "output":      "print",
    }
    return _start_universal_job(job, label=req.label or "email_harvest", background_tasks=background_tasks)


@app.post("/scrape/content")
async def scrape_content(req: ContentScrapeRequest, background_tasks: BackgroundTasks):
    """
    Pull headlines, articles, or any repeating content using CSS selectors.

    Example (news headlines):
    {
      "urls": ["https://dallasnews.com"],
      "each_item": "article.story",
      "selectors": {"title": "h2", "body": "p.summary", "link": "a", "date": "time"},
      "category": "Dallas News"
    }
    """
    job = {
        "type":      "content_scraper",
        "urls":      req.urls,
        "selectors": req.selectors,
        "each_item": req.each_item,
        "category":  req.category,
        "output":    "print",
    }
    return _start_universal_job(job, label=req.category or "content", background_tasks=background_tasks)


@app.post("/scrape/paginated")
async def scrape_paginated(req: PaginatedScrapeRequest, background_tasks: BackgroundTasks):
    """
    Follow "next page" links across multiple pages and collect items.

    Example (job board):
    {
      "start_url": "https://jobs.example.com/listings",
      "item_sel": ".job-card",
      "fields": {"title": ".job-title", "company": ".company", "location": ".location"},
      "next_sel": "a.next-page",
      "max_pages": 5,
      "category": "Jobs"
    }
    """
    job = {
        "type":      "paginated_scraper",
        "start_url": req.start_url,
        "item_sel":  req.item_sel,
        "fields":    req.fields,
        "next_sel":  req.next_sel,
        "max_pages": req.max_pages,
        "category":  req.category,
        "output":    "print",
    }
    return _start_universal_job(job, label=req.category or "paginated", background_tasks=background_tasks)


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTES — RESULTS & SESSION HISTORY
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/scrape-sessions")
async def get_scrape_sessions(limit: int = Query(20, le=100), offset: int = 0):
    """
    List all past scrape sessions, newest first.
    Shows job_type, status, result count, and timestamps.
    """
    if not db:
        return {"error": "No database connection"}
    try:
        resp = (
            db.table("scrape_runs")
            .select("id, job_type, location, categories, status, result_count, businesses_found, emails_queued, created_at, completed_at, error_message")
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return {"sessions": resp.data, "count": len(resp.data)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/results")
async def get_results(
    job_type:   Optional[str] = Query(None, description="Filter by job type"),
    session_id: Optional[str] = Query(None, description="Filter by session/run ID"),
    category:   Optional[str] = Query(None, description="Filter by category"),
    limit:      int           = Query(100, le=500),
    offset:     int           = 0,
):
    """
    Query scraped results. Filter by job type, session, or category.

    Examples:
      /results?job_type=trivia_scraper&limit=50
      /results?session_id=abc-123
      /results?job_type=price_scraper&category=Beer
    """
    if not db:
        return {"error": "No database connection"}
    try:
        q = db.table("scrape_results").select("*").order("created_at", desc=True)
        if job_type:
            q = q.eq("job_type", job_type)
        if session_id:
            q = q.eq("session_id", session_id)
        if category:
            q = q.ilike("category", f"%{category}%")
        resp = q.range(offset, offset + limit - 1).execute()

        # Flatten JSONB data field into each row for easier reading
        rows = []
        for r in resp.data:
            flat = {**r.get("data", {}), **{k: v for k, v in r.items() if k != "data"}}
            rows.append(flat)

        return {
            "results":  rows,
            "count":    len(rows),
            "job_type": job_type,
            "session_id": session_id,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/results/{session_id}")
async def get_session_results(session_id: str, limit: int = Query(500, le=1000), offset: int = 0):
    """
    Get all scraped results for a specific session.
    Returns flattened rows — data field is merged into each row.
    """
    if not db:
        return {"error": "No database connection"}
    try:
        # Get session info
        run_resp = db.table("scrape_runs").select("*").eq("id", session_id).execute()
        session  = run_resp.data[0] if run_resp.data else {}

        # Get results
        resp = (
            db.table("scrape_results")
            .select("*")
            .eq("session_id", session_id)
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )

        rows = []
        for r in resp.data:
            flat = {**r.get("data", {}), **{k: v for k, v in r.items() if k != "data"}}
            rows.append(flat)

        return {
            "session":  session,
            "results":  rows,
            "count":    len(rows),
        }
    except Exception as e:
        return {"error": str(e)}


# ═════════════════════════════════════════════════════════════════════════════
#  ROUTES — EMAIL / PROPOSAL (unchanged from original)
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/send-followup")
async def send_followup(req: FollowUpRequest):
    try:
        from emailer import send_proposal

        if db:
            biz_row = db.table("businesses").select("*").eq("id", req.business_id).execute()
            if not biz_row.data:
                return {"error": "Business not found"}
            biz = biz_row.data[0]
        else:
            biz = {"name": req.business_name, "email": req.to_email, "category": "", "issues": ""}

        subject = f"Following up — {req.business_name}"
        html = f"""
        <div style="font-family:sans-serif;max-width:600px;margin:auto;padding:32px;background:#f8f9fa;border-radius:12px;">
          <h2 style="color:#1a1a2e;">Still thinking it over?</h2>
          <p>Hi {req.business_name} team,</p>
          <p>Just wanted to follow up on my last message about your website. Happy to hop on a quick
          15-minute call — no pressure, just an honest look at what could be improved.</p>
          <p>Reply here to schedule, or just let me know you're not interested and I won't bother you again.</p>
          <p>— Jimmy Cannon<br>Rebuild Digital Co</p>
          <p style="font-size:11px;color:#aaa;">You received this because we reached out as part of our
          Dallas small business outreach. Reply 'unsubscribe' to opt out.</p>
        </div>"""

        result = send_proposal(req.business_name, req.to_email, subject, html, dry_run=False)

        if db and result.get("success"):
            db.table("email_logs").insert({
                "business_id": req.business_id, "subject": subject,
                "resend_id":   result.get("id", ""),
                "to_email":    req.to_email, "status": "sent", "email_type": "follow_up_1",
            }).execute()
            db.table("follow_ups").update({
                "status": "sent", "sent_at": datetime.now().isoformat(),
            }).eq("id", req.follow_up_id).execute()
            db.table("businesses").update({"status": "follow_up_sent"}).eq("id", req.business_id).execute()

        return {"success": result.get("success"), "resend_id": result.get("id")}
    except Exception as e:
        return {"error": str(e)}


def _get_biz(business_id: str):
    if not db:
        return None
    row = db.table("businesses").select("*").eq("id", business_id).execute()
    return row.data[0] if row.data else None


@app.get("/preview-proposal")
async def preview_proposal(business_id: str):
    try:
        biz = _get_biz(business_id)
        if not biz:
            return HTMLResponse("<h2>Business not found</h2>", status_code=404)
        from proposal_gen import generate_html_email
        return HTMLResponse(content=generate_html_email(biz))
    except Exception as e:
        return HTMLResponse(f"<h2>Error: {e}</h2>", status_code=500)


@app.post("/send-proposal")
async def send_proposal_endpoint(req: SendProposalRequest):
    try:
        biz = _get_biz(req.business_id)
        if not biz:
            return {"error": "Business not found"}
        if not biz.get("email"):
            return {"error": "No email address on file for this business"}

        from proposal_gen import generate_html_email, generate_subject_line
        from emailer import send_proposal as send_email

        html    = generate_html_email(biz)
        subject = generate_subject_line(biz)
        result  = send_email(biz["name"], biz["email"], subject, html, dry_run=False)

        if db and result.get("success"):
            db.table("email_logs").insert({
                "business_id": req.business_id, "subject": subject,
                "resend_id":   result.get("id", ""), "to_email": biz["email"],
                "status": "sent", "email_type": "initial",
            }).execute()
            db.table("businesses").update({"status": "emailed"}).eq("id", req.business_id).execute()
            fu_date = (datetime.now() + timedelta(days=5)).isoformat()
            db.table("follow_ups").insert({
                "business_id": req.business_id, "scheduled_for": fu_date,
                "follow_up_number": 1,
            }).execute()

        return {"success": result.get("success"), "resend_id": result.get("id"), "subject": subject}
    except Exception as e:
        return {"error": str(e)}


@app.post("/reset-db")
async def reset_db():
    if not db:
        return {"error": "No database connection"}
    try:
        tables = ["email_logs", "follow_ups", "feedback", "scrape_results", "businesses", "scrape_runs"]
        for table in tables:
            db.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        return {"success": True, "message": "All data deleted."}
    except Exception as e:
        return {"error": str(e)}


# ═════════════════════════════════════════════════════════════════════════════
#  SERVE REACT TRACKER APP (must be last)
# ═════════════════════════════════════════════════════════════════════════════

_DIST = os.path.join(os.path.dirname(__file__), "tracker-app", "dist")
if os.path.isdir(_DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(_DIST, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        file_path = os.path.join(_DIST, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(_DIST, "index.html"))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print("Rebuild Digital Co — Universal Scraper API")
    print(f"  Supabase : {'connected' if db else 'NOT configured'}")
    print(f"  Port     : {port}")
    print()
    print("Endpoints:")
    print("  POST /scrape               — original Google Maps pipeline")
    print("  POST /universal-scrape     — any job type")
    print("  POST /scrape/prices        — price scraper")
    print("  POST /scrape/trivia        — trivia Q&A scraper")
    print("  POST /scrape/emails        — email harvester")
    print("  POST /scrape/content       — content scraper")
    print("  POST /scrape/paginated     — paginated list scraper")
    print("  GET  /scrape-sessions      — session history")
    print("  GET  /results              — query results")
    print("  GET  /results/{session_id} — results for one session")
    print("  GET  /status               — live log")
    print()
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
