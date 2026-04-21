"""
server.py — Rebuild Digital Co
Local FastAPI server. Run this on your machine.
The tracker app calls this to trigger the scraper, analyzer, and emailer.

Usage:
  pip install fastapi uvicorn supabase
  python server.py
"""

import os
import asyncio
import threading
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from supabase import create_client
import uvicorn

load_dotenv()

app = FastAPI(title="Rebuild Digital Co — Local API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from Netlify app
    allow_methods=["*"],
    allow_headers=["*"],
)

# Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
db = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

# Scrape state tracker
scrape_state = {"running": False, "log": [], "run_id": None}


class ScrapeRequest(BaseModel):
    location: str = "Dallas, TX"
    limit: int = 10
    send_emails: bool = False
    categories: list = []  # empty = all categories


class FollowUpRequest(BaseModel):
    business_id: str
    follow_up_id: str
    to_email: str
    business_name: str


@app.get("/health")
def health():
    return {"status": "online", "db": bool(db)}


@app.get("/status")
def status():
    return {
        "running": scrape_state["running"],
        "log": scrape_state["log"][-50:],
        "run_id": scrape_state["run_id"],
    }


def log(msg):
    print(msg)
    scrape_state["log"].append(msg)
    # Update scrape run log in DB (best-effort)
    if db and scrape_state.get("run_id"):
        pass  # Could update a log column here


async def run_scrape_pipeline(location, limit, send_emails, run_id, categories=None):
    scrape_state["running"] = True
    scrape_state["log"] = [f"[{datetime.now().strftime('%H:%M:%S')}] Starting scrape for {location}..."]

    try:
        # ── Step 1: Scrape ──────────────────────────────────
        from scraper import run_scraper_async
        from playwright.async_api import async_playwright

        log("Launching browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            from scraper import scrape_category, ALL_CATEGORIES
            # Filter to selected categories if provided
            if categories:
                cat_map = {label: query for query, label in ALL_CATEGORIES}
                run_cats = []
                for c in categories:
                    if c in cat_map:
                        run_cats.append((cat_map[c], c))
                    else:
                        # Custom vertical: use label as the Google Maps search query
                        run_cats.append((c.lower(), c))
            else:
                run_cats = ALL_CATEGORIES

            log(f"Categories: {', '.join(l for _, l in run_cats)}")
            all_businesses = []

            for query, label in run_cats:
                log(f"Scraping {label}...")
                try:
                    bizs = await scrape_category(browser, query, label, limit=limit)
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

        # ── Step 2: Analyze ─────────────────────────────────
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

        # ── Step 3: Write to Supabase ────────────────────────
        if db:
            log("Writing to Supabase...")
            emails_queued = 0
            high_opp = 0
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
                # Upsert by name (avoid duplicates)
                try:
                    # Check if business already exists by name
                    existing = db.table("businesses").select("id").eq("name", row["name"]).execute()
                    if existing.data:
                        # Update existing record
                        db.table("businesses").update(row).eq("name", row["name"]).execute()
                    else:
                        # Insert new record
                        db.table("businesses").insert(row).execute()
                    if biz.get("opportunity_score", 0) >= 5:
                        high_opp += 1
                    if biz.get("email"):
                        emails_queued += 1
                except Exception as e:
                    log(f"  DB error for {biz.get('name')}: {e}")

            # Update scrape run
            db.table("scrape_runs").update({
                "status":           "completed",
                "completed_at":     datetime.now().isoformat(),
                "businesses_found": len(analyzed),
                "emails_queued":    emails_queued,
                "high_opp_count":   high_opp,
            }).eq("id", run_id).execute()

            log(f"Saved {len(analyzed)} businesses to Supabase")

        # ── Step 4: Send emails (if requested) ──────────────
        emails_sent = 0
        if send_emails:
            log("Generating proposals and sending emails...")
            from proposal_gen import generate_html_email, generate_subject_line
            from mockup_gen import run_mockup
            from emailer import send_proposal

            targets = [b for b in analyzed if b.get("email") and b.get("opportunity_score", 0) >= 3]
            for biz in targets[:50]:  # Cap at 50 per run
                try:
                    shots = run_mockup(biz)
                    html = generate_html_email(biz, mockup_screenshots=shots)
                    subject = generate_subject_line(biz)
                    result = send_proposal(biz["name"], biz["email"], subject, html, dry_run=False)

                    if db and result.get("success"):
                        # Log to email_logs
                        biz_row = db.table("businesses").select("id").eq("name", biz["name"]).execute()
                        if biz_row.data:
                            biz_id = biz_row.data[0]["id"]
                            db.table("email_logs").insert({
                                "business_id": biz_id,
                                "subject":     subject,
                                "resend_id":   result.get("id", ""),
                                "to_email":    biz["email"],
                                "status":      "sent",
                                "email_type":  "initial",
                            }).execute()
                            # Update business status
                            db.table("businesses").update({"status": "emailed"}).eq("id", biz_id).execute()
                            # Schedule follow-up in 5 days
                            fu_date = datetime.now()
                            fu_date = fu_date.replace(day=fu_date.day + 5)
                            db.table("follow_ups").insert({
                                "business_id":      biz_id,
                                "scheduled_for":    fu_date.isoformat(),
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


@app.post("/scrape")
async def trigger_scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    if scrape_state["running"]:
        return {"error": "Scrape already running", "log": scrape_state["log"]}

    # Create scrape run record
    run_id = None
    if db:
        result = db.table("scrape_runs").insert({
            "location": req.location,
            "categories": ", ".join(req.categories) if req.categories else "all",
            "status": "running",
            "triggered_by": "app",
        }).execute()
        if result.data:
            run_id = result.data[0]["id"]

    scrape_state["run_id"] = run_id
    background_tasks.add_task(
        asyncio.run,
        run_scrape_pipeline(req.location, req.limit, req.send_emails, run_id, req.categories or [])
    )
    return {"status": "started", "run_id": run_id, "log": scrape_state["log"]}


@app.post("/send-followup")
async def send_followup(req: FollowUpRequest):
    """Send a follow-up email to a specific business."""
    try:
        from emailer import send_proposal

        # Get business from DB
        if db:
            biz_row = db.table("businesses").select("*").eq("id", req.business_id).execute()
            if not biz_row.data:
                return {"error": "Business not found"}
            biz = biz_row.data[0]
        else:
            biz = {"name": req.business_name, "email": req.to_email, "category": "", "issues": ""}

        subject = f"Following up — {req.business_name}"
        html = f"""
        <div style="font-family:sans-serif; max-width:600px; margin:auto; padding:32px; background:#f8f9fa; border-radius:12px;">
          <h2 style="color:#1a1a2e;">Still thinking it over?</h2>
          <p>Hi {req.business_name} team,</p>
          <p>Just wanted to follow up on my last message about your website. Happy to hop on a quick 15-minute call — no pressure, just an honest look at what could be improved.</p>
          <p>Reply here to schedule, or just let me know you're not interested and I won't bother you again.</p>
          <p>— Jimmy Cannon<br>Rebuild Digital Co</p>
          <p style="font-size:11px;color:#aaa;">You received this because we reached out as part of our Dallas small business outreach. Reply 'unsubscribe' to opt out.</p>
        </div>"""

        result = send_proposal(req.business_name, req.to_email, subject, html, dry_run=False)

        if db and result.get("success"):
            db.table("email_logs").insert({
                "business_id": req.business_id,
                "subject":     subject,
                "resend_id":   result.get("id", ""),
                "to_email":    req.to_email,
                "status":      "sent",
                "email_type":  "follow_up_1",
            }).execute()
            db.table("follow_ups").update({
                "status": "sent",
                "sent_at": datetime.now().isoformat(),
            }).eq("id", req.follow_up_id).execute()
            db.table("businesses").update({"status": "follow_up_sent"}).eq("id", req.business_id).execute()

        return {"success": result.get("success"), "resend_id": result.get("id")}

    except Exception as e:
        return {"error": str(e)}


class SendProposalRequest(BaseModel):
    business_id: str


@app.post("/reset-db")
async def reset_db():
    """Wipe all data from all tracker tables. Irreversible."""
    if not db:
        return {"error": "No database connection"}
    try:
        tables = ["email_logs", "follow_ups", "feedback", "businesses", "scrape_runs"]
        for table in tables:
            db.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        return {"success": True, "message": "All data deleted."}
    except Exception as e:
        return {"error": str(e)}


def _get_biz(business_id):
    """Fetch a business dict from Supabase."""
    if not db:
        return None
    row = db.table("businesses").select("*").eq("id", business_id).execute()
    return row.data[0] if row.data else None


@app.get("/preview-proposal")
async def preview_proposal(business_id: str):
    """Return the proposal HTML so the dashboard can open it in a new tab."""
    try:
        biz = _get_biz(business_id)
        if not biz:
            return HTMLResponse("<h2>Business not found</h2>", status_code=404)
        from proposal_gen import generate_html_email, generate_subject_line
        html = generate_html_email(biz)
        return HTMLResponse(content=html)
    except Exception as e:
        return HTMLResponse(f"<h2>Error generating proposal: {e}</h2>", status_code=500)


@app.post("/send-proposal")
async def send_proposal_endpoint(req: SendProposalRequest):
    """Generate and send a proposal email to a specific business."""
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
                "business_id": req.business_id,
                "subject":     subject,
                "resend_id":   result.get("id", ""),
                "to_email":    biz["email"],
                "status":      "sent",
                "email_type":  "initial",
            }).execute()
            db.table("businesses").update({"status": "emailed"}).eq("id", req.business_id).execute()
            # Schedule follow-up in 5 days
            from datetime import timedelta
            fu_date = (datetime.now() + timedelta(days=5)).isoformat()
            db.table("follow_ups").insert({
                "business_id":      req.business_id,
                "scheduled_for":    fu_date,
                "follow_up_number": 1,
            }).execute()

        return {"success": result.get("success"), "resend_id": result.get("id"), "subject": subject}

    except Exception as e:
        return {"error": str(e)}


# ── Serve the React tracker app ──────────────────────────────
# Must be defined AFTER all API routes so they take priority.
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
    ssl_keyfile  = "localhost-key.pem" if os.path.exists("localhost-key.pem") else None
    ssl_certfile = "localhost.pem"     if os.path.exists("localhost.pem")     else None
    protocol = "https" if ssl_certfile else "http"

    print("🔨 Rebuild Digital Co — Local API Server")
    print(f"   Supabase: {'✓ connected' if db else '✗ not configured'}")
    print(f"   Running at {protocol}://localhost:8000")
    print()
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False,
                ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile)
