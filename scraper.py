"""
scraper.py — Rebuild Digital Co
Three-stage pipeline per business:
  1. Google Maps search  → collect name + profile URL (no fragile clicks)
  2. Google Maps profile → extract website URL
  3. Google Search       → fallback if Maps has no website listed
  4. Website visit       → dig homepage + /contact + /about for email

Outputs: output/businesses_raw.csv  (also writes to Supabase via analyzer)
"""

import asyncio
import csv
import os
import re
import random
import urllib.parse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

ALL_CATEGORIES = [
    ("hair salons",         "Hair Salon"),
    ("restaurants",         "Restaurant"),
    ("general contractors", "Contractor"),
    ("medical offices",     "Medical Office"),
    ("plumbers",            "Plumber"),
    ("electricians",        "Electrician"),
    ("dentists",            "Dentist"),
    ("gyms fitness",        "Gym / Fitness"),
    ("auto repair shops",   "Auto Repair"),
    ("landscaping",         "Landscaping"),
]

CATEGORIES = ALL_CATEGORIES   # alias for server.py imports

LOCATION          = "Dallas TX"
RESULTS_PER_CATEGORY = 10
SCROLL_PAUSE      = 1500       # ms between scrolls in results panel

SOCIAL_SKIP = [
    "google.com", "goo.gl", "facebook.com", "instagram.com",
    "twitter.com", "linkedin.com", "youtube.com", "tiktok.com",
    "yelp.com", "javascript:", "#",
]
BOOKING_SKIP = [
    "booking", "onlinebooking", "schedule", "appointment",
    "zenoti", "meevo", "boulevard", "vagaro", "mindbody",
    "booksy", "calendly", "squareup", "fresha", "toasttab",
    "resy", "opentable", "rwg_token", "xapp.ai",
]
SEARCH_SKIP = [
    "google.com", "facebook.com", "instagram.com", "twitter.com",
    "linkedin.com", "youtube.com", "tiktok.com",
    "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
    "thumbtack.com", "homeadvisor.com", "angieslist.com",
    "manta.com", "superpages.com", "mapquest.com", "nextdoor.com",
    "porch.com", "houzz.com", "bark.com", "expertise.com",
    "chamberofcommerce.com", "dexknows.com",
]
EMAIL_SKIP = [
    "example", "youremail", "email@", "sentry", "wix.com",
    "wordpress", "schema", ".png", ".jpg", ".svg",
    "placeholder", "noreply", "no-reply", "donotreply",
]
DOMAIN_RE    = re.compile(r'^[\w\-]+\.[\w\-]{2,}$')
EMAIL_RE     = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE     = re.compile(r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}')
ADDRESS_RE   = re.compile(
    r'\d+\s+[\w\s\.]+(?:Ave|St|Blvd|Dr|Rd|Ln|Way|Pkwy|Ct|Pl|Hwy|Fwy)'
    r'[^\<]{0,40}Dallas,?\s*TX(?:\s*\d{5})?'
)


# ── Helpers ───────────────────────────────────────────────────

def clean_url(href):
    """Strip UTM / tracking params and trailing slash."""
    from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
    p = urlparse(href)
    qs = {k: v for k, v in parse_qs(p.query).items()
          if not k.lower().startswith(("utm_", "rwg_", "gbp", "gclid"))}
    return urlunparse(p._replace(query=urlencode(qs, doseq=True))).rstrip("/")


def extract_emails_from_html(html):
    """Return cleaned list of real email addresses found in HTML."""
    found = EMAIL_RE.findall(html)
    return [
        e for e in found
        if not any(s in e.lower() for s in EMAIL_SKIP)
    ]


def is_good_url(href):
    """True if href looks like a real business website."""
    if not href.startswith("http"):
        return False
    if any(s in href for s in SOCIAL_SKIP + BOOKING_SKIP):
        return False
    return True


# ── Multi-strategy URL extractor ─────────────────────────────

def _extract_url_from_page(html, soup):
    """Try every known strategy to pull a business website URL from a Maps page."""

    # 1. Google's canonical authority link
    auth = soup.find("a", attrs={"data-item-id": "authority"})
    if auth:
        href = auth.get("href", "")
        if is_good_url(href):
            return clean_url(href)

    # 2. Any <a> whose aria-label or text says "website"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            continue
        aria = re.sub(r'[^\x20-\x7E]', '', a.get("aria-label") or "").strip().lower()
        text = re.sub(r'[^\x20-\x7E]', '', a.get_text(strip=True)).strip().lower()
        if "website" in aria or text == "website":
            if is_good_url(href):
                return clean_url(href)

    # 3. Visible text looks like a bare domain (e.g. "mybiz.com")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = re.sub(r'[^\x20-\x7E]', '', a.get_text(strip=True)).strip()
        if is_good_url(href) and DOMAIN_RE.match(text):
            return clean_url(href)

    # 4. JSON / data-attribute scan — URL embedded in page source
    for pattern in [
        r'"url"\s*:\s*"(https?://[^"]{5,120})"',
        r'website["\s:\']+\s*(https?://[^\s"\'<>]{5,120})',
    ]:
        for url in re.findall(pattern, html):
            if is_good_url(url) and "google" not in url and "gstatic" not in url:
                return clean_url(url)

    return ""


# ── Stage 2: Extract website from a Maps profile page ────────

async def website_from_maps_profile(page, profile_url):
    """Navigate to a Google Maps business profile, return the website URL or ''."""
    try:
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(4000)   # give JS panel time to render

        content = await page.content()
        soup    = BeautifulSoup(content, "html.parser")

        title_el = soup.find("title")
        if title_el and any(x in title_el.get_text().lower()
                            for x in ["captcha", "unusual traffic", "before you continue"]):
            print("[Maps blocked]", end=" ", flush=True)
            return ""

        return _extract_url_from_page(content, soup)
    except Exception as e:
        print(f"[profile err: {e}]", end=" ", flush=True)
        return ""


# ── Stage 3: Google Search fallback ──────────────────────────

async def website_from_google_search(page, business_name, location=None):
    """Search Google for the business name, return first non-directory organic URL."""
    loc = location or LOCATION
    try:
        query = urllib.parse.quote_plus(f'"{business_name}" {loc}')
        await page.goto(
            f"https://www.google.com/search?q={query}",
            wait_until="domcontentloaded", timeout=15000,
        )
        await page.wait_for_timeout(2500)

        content = await page.content()
        soup    = BeautifulSoup(content, "html.parser")

        # Bot-detection check
        title_el = soup.find("title")
        if title_el and any(x in title_el.get_text().lower()
                            for x in ["captcha", "unusual traffic", "before you continue"]):
            print("[Google blocked]", end=" ", flush=True)
            return ""

        # Modern Google: <cite> tags contain the visible domain of each result
        for cite in soup.find_all("cite"):
            raw = cite.get_text(strip=True)
            domain = raw.split("›")[0].split("»")[0].strip()
            if domain and "." in domain and not any(
                s in domain.lower() for s in [
                    "google", "yelp", "facebook", "instagram", "twitter",
                    "linkedin", "youtube", "bbb.org", "angi", "thumbtack",
                    "yellowpages", "mapquest", "nextdoor",
                ]
            ):
                url = domain if domain.startswith("http") else f"https://{domain}"
                if is_good_url(url):
                    return clean_url(url)

        # Classic Google: /url?q= redirect links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/url?q=" in href:
                m = re.search(r'/url\?q=([^&]+)', href)
                if m:
                    href = urllib.parse.unquote(m.group(1))
            if not href.startswith("http"):
                continue
            if any(s in href for s in SEARCH_SKIP):
                continue
            return clean_url(href)

        return ""
    except Exception as e:
        print(f"[search err: {e}]", end=" ", flush=True)
        return ""


# ── Stage 4: Visit website and dig for emails ─────────────────

async def email_from_website(page, url):
    """Visit homepage + /contact + /about and return first real email found."""
    if not url or not url.startswith("http"):
        return ""

    slugs = ["", "/contact", "/contact-us", "/about", "/about-us", "/reach-us"]
    for slug in slugs:
        try:
            target = url.rstrip("/") + slug
            await page.goto(target, wait_until="domcontentloaded", timeout=12000)
            await page.wait_for_timeout(1500)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # mailto: links are the most reliable
            for a in soup.find_all("a", href=re.compile(r"^mailto:")):
                addr = a["href"].replace("mailto:", "").split("?")[0].strip()
                if addr and "@" in addr and not any(s in addr.lower() for s in EMAIL_SKIP):
                    return addr

            # Regex scan of raw HTML
            emails = extract_emails_from_html(html)
            if emails:
                return emails[0]

        except Exception:
            continue

    return ""


# ── Phone / address helpers ───────────────────────────────────

def extract_phone(html):
    soup = BeautifulSoup(html, "html.parser")
    tel = soup.find("a", href=re.compile(r"^tel:"))
    if tel:
        return tel["href"].replace("tel:", "").strip()
    m = PHONE_RE.search(html)
    return m.group(0) if m else ""


def extract_address(html):
    m = ADDRESS_RE.search(html)
    return re.sub(r"\s+", " ", m.group(0)).strip() if m else ""


# ── Per-business pipeline ─────────────────────────────────────

async def get_business_details(page, name, profile_url, category_label, location=None):
    """Full 4-stage pipeline for one business."""
    loc = location or LOCATION
    print(f"    → {name}")

    # Stage 2: Maps profile — load ONCE, extract everything from the same page
    print(f"      Maps profile...", end=" ", flush=True)
    try:
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(4000)
    except Exception as e:
        print(f"[load error: {e}]", end=" ", flush=True)

    maps_html = await page.content()
    soup      = BeautifulSoup(maps_html, "html.parser")
    phone     = extract_phone(maps_html)
    address   = extract_address(maps_html)

    # Try to get website from the already-loaded page (no second navigation)
    website = _extract_url_from_page(maps_html, soup)

    if website:
        print(f"✓ {website}")
    else:
        # Stage 3: Google Search fallback
        print(f"no Maps URL → Google search...", end=" ", flush=True)
        website = await website_from_google_search(page, name, location=loc)
        if website:
            print(f"✓ {website}")
        else:
            print("no URL found")

    # Stage 4: Email extraction from website
    email = ""
    if website:
        print(f"      Checking website for email...", end=" ", flush=True)
        email = await email_from_website(page, website)
        print(f"✓ {email}" if email else "no email found")

    return {
        "name":     name,
        "category": category_label,
        "website":  website,
        "phone":    phone,
        "address":  address,
        "email":    email,
        "yelp_url": "",
    }


# ── Category scraper ──────────────────────────────────────────

async def scrape_category(browser, category_query, category_label, limit=10, location=None):
    """
    Stage 1: Load Google Maps search results, collect names + profile URLs.
    Then run the full pipeline on each.
    """
    loc   = location or LOCATION
    page  = await browser.new_page()
    await page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    })
    await page.set_viewport_size({"width": 1280, "height": 800})

    query = urllib.parse.quote_plus(f"{category_query} in {loc}")
    url   = f"https://www.google.com/maps/search/{query}"
    print(f"\n  → Searching Maps: {category_label}")

    try:
        await asyncio.wait_for(
            page.goto(url, wait_until="domcontentloaded", timeout=20000),
            timeout=25
        )
    except Exception as e:
        print(f"  [Maps load error: {e}]")
        await page.close()
        return []
    await page.wait_for_timeout(2000)

    # Detect Google blocking
    title_el = await page.title()
    if any(x in title_el.lower() for x in ["captcha", "unusual traffic", "before you continue", "verify"]):
        print(f"  [Google blocked scrape for {category_label}]")
        await page.close()
        return []

    # Scroll to load more listings
    panel = await page.query_selector('div[role="feed"]')
    if panel:
        for _ in range(4):
            await panel.evaluate("el => el.scrollBy(0, 600)")
            await page.wait_for_timeout(SCROLL_PAUSE)

    # Collect (name, profile_url) — no clicking, just read hrefs
    raw = await page.query_selector_all("a[aria-label]")
    listing_data, seen_names = [], set()
    for el in raw:
        label = await el.get_attribute("aria-label") or ""
        href  = await el.get_attribute("href")  or ""
        if label and "/maps/place/" in href and href.startswith("http"):
            key = label.lower().strip()
            if key not in seen_names:
                seen_names.add(key)
                listing_data.append((label, href))

    print(f"  Found {len(listing_data)} listings, processing up to {limit}...")

    businesses = []
    for name, profile_url in listing_data[:limit * 2]:
        try:
            details = await get_business_details(page, name, profile_url, category_label, location=loc)
            businesses.append(details)
        except Exception as e:
            print(f"      [!] Error: {e}")
            businesses.append({
                "name": name, "category": category_label,
                "website": "", "phone": "", "address": "", "email": "", "yelp_url": ""
            })

        if len(businesses) >= limit:
            break

        await page.wait_for_timeout(random.randint(1500, 3000))

    await page.close()
    return businesses


# ── Top-level runner ──────────────────────────────────────────

async def run_scraper_async(output_path="output/businesses_raw.csv", limit_per_category=10, categories=None):
    if categories is None:
        categories = ALL_CATEGORIES

    print("=== Rebuild Digital Co — Dallas Business Scraper ===\n")
    print(f"  Categories : {', '.join(l for _, l in categories)}")
    print(f"  Results    : up to {limit_per_category} per category")
    print(f"  Location   : {LOCATION}\n")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    all_businesses = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for query, label in categories:
            print(f"\nScraping: {label}...")
            try:
                bizs = await scrape_category(browser, query, label, limit=limit_per_category)
                print(f"  Collected {len(bizs)} businesses")
                all_businesses.extend(bizs)
            except Exception as e:
                print(f"  [!] Error on {label}: {e}")
            await asyncio.sleep(random.uniform(2, 4))

        await browser.close()

    # Deduplicate by name
    seen, unique = set(), []
    for biz in all_businesses:
        key = biz["name"].lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(biz)

    fieldnames = ["name", "category", "website", "phone", "address", "email", "yelp_url"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unique)

    with_site  = sum(1 for b in unique if b.get("website"))
    with_email = sum(1 for b in unique if b.get("email"))
    print(f"\n✓ Saved {len(unique)} businesses → {output_path}")
    print(f"  Websites found : {with_site}/{len(unique)}")
    print(f"  Emails found   : {with_email}/{len(unique)}")
    return unique


def run_scraper(output_path="output/businesses_raw.csv", limit_per_category=10, categories=None):
    return asyncio.run(run_scraper_async(output_path, limit_per_category, categories))


# ── Category selector (terminal) ──────────────────────────────

def select_categories():
    print("\n╔══════════════════════════════════════════════════╗")
    print("║   Rebuild Digital Co — Category Selector        ║")
    print("╠══════════════════════════════════════════════════╣")
    for i, (_, label) in enumerate(ALL_CATEGORIES, 1):
        print(f"║  {i:>2}. {label:<43}║")
    print("╠══════════════════════════════════════════════════╣")
    print("║  Enter numbers (e.g. 1,3,5) or Enter for ALL   ║")
    print("╚══════════════════════════════════════════════════╝")

    raw = input("\nCategories to scrape: ").strip()
    if not raw:
        print(f"\n→ Scraping ALL {len(ALL_CATEGORIES)} categories\n")
        return ALL_CATEGORIES

    selected, seen = [], set()
    for part in raw.split(","):
        part = part.strip()
        if not part.isdigit():
            continue
        idx = int(part) - 1
        if 0 <= idx < len(ALL_CATEGORIES) and idx not in seen:
            selected.append(ALL_CATEGORIES[idx])
            seen.add(idx)

    if not selected:
        print("  No valid selections — scraping ALL.\n")
        return ALL_CATEGORIES

    print(f"\n→ Scraping: {', '.join(l for _, l in selected)}\n")
    return selected


if __name__ == "__main__":
    chosen    = select_categories()
    raw_limit = input(f"Results per category [default {RESULTS_PER_CATEGORY}]: ").strip()
    limit     = int(raw_limit) if raw_limit.isdigit() else RESULTS_PER_CATEGORY
    run_scraper(limit_per_category=limit, categories=chosen)
