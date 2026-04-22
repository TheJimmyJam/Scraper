"""
universal_scraper.py — Rebuild Digital Co
==========================================
A plug-and-play scraper engine. Define a JOB, run it, get data.

JOB TYPES
---------
  google_maps_business  → scrape Google Maps listings (original Rebuild Digital logic)
  price_scraper         → extract item + price pairs from menu / product pages
  trivia_scraper        → pull Q&A pairs from trivia sites
  email_harvester       → collect emails from a list of URLs
  content_scraper       → grab article text, headlines, or any CSS-targeted content
  paginated_scraper     → follow "next page" links across multiple pages
  auto_email            → scrape a source, then pipe matching results into the email pipeline

OUTPUT TYPES
------------
  csv          → writes a .csv file
  json         → writes a .json file
  supabase     → upserts rows into a Supabase table (requires env vars)
  email_trigger → passes scraped rows to the email pipeline (auto_email job type only)
  print        → pretty-prints to console (great for testing)

QUICK START (CLI)
-----------------
  python universal_scraper.py

  ...then pick a job from the interactive menu, fill in the prompts, and go.

QUICK START (API / Railway)
---------------------------
  from universal_scraper import run_job

  results = run_job({
      "type":    "price_scraper",
      "urls":    ["https://example.com/beer-menu"],
      "selectors": {
          "row":   ".menu-item",
          "name":  ".item-name",
          "price": ".item-price",
      },
      "output":  "csv",
      "output_path": "output/beer_prices.csv",
  })
"""

import asyncio
import csv
import json
import os
import re
import random
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Optional Supabase ─────────────────────────────────────────────────────────
try:
    from supabase import create_client
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
    _supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if (SUPABASE_URL and SUPABASE_KEY) else None
except ImportError:
    _supabase = None

# ── Shared constants ──────────────────────────────────────────────────────────
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

DOMAIN_RE = re.compile(r'^[\w\-]+\.[\w\-]{2,}$')
EMAIL_RE  = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE  = re.compile(r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}')
PRICE_RE  = re.compile(r'\$\s?\d+(?:\.\d{1,2})?')


# ═════════════════════════════════════════════════════════════════════════════
#  SHARED UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

def clean_url(href):
    from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
    p  = urlparse(href)
    qs = {k: v for k, v in parse_qs(p.query).items()
          if not k.lower().startswith(("utm_", "rwg_", "gbp", "gclid"))}
    return urlunparse(p._replace(query=urlencode(qs, doseq=True))).rstrip("/")


def is_good_url(href):
    if not href.startswith("http"):
        return False
    if any(s in href for s in SOCIAL_SKIP + BOOKING_SKIP):
        return False
    return True


def extract_emails_from_html(html):
    found = EMAIL_RE.findall(html)
    return [e for e in found if not any(s in e.lower() for s in EMAIL_SKIP)]


def extract_phone(html):
    soup = BeautifulSoup(html, "html.parser")
    tel  = soup.find("a", href=re.compile(r"^tel:"))
    if tel:
        return tel["href"].replace("tel:", "").strip()
    m = PHONE_RE.search(html)
    return m.group(0) if m else ""


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _new_page(browser, stealth=True):
    page = await browser.new_page()
    if stealth:
        await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        await page.set_viewport_size({"width": 1280, "height": 900})
    return page


async def _safe_goto(page, url, timeout=20000):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(random.randint(1800, 3200))
        return await page.content()
    except Exception as e:
        print(f"  [!] Load error {url}: {e}")
        return ""


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 1 — GOOGLE MAPS BUSINESS SCRAPER  (original Rebuild Digital logic)
# ═════════════════════════════════════════════════════════════════════════════

# Pre-built category lists
MAPS_CATEGORIES_REBUILD = [
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


def _extract_url_from_maps_page(html, soup):
    auth = soup.find("a", attrs={"data-item-id": "authority"})
    if auth:
        href = auth.get("href", "")
        if is_good_url(href):
            return clean_url(href)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            continue
        aria = re.sub(r'[^\x20-\x7E]', '', a.get("aria-label") or "").strip().lower()
        text = re.sub(r'[^\x20-\x7E]', '', a.get_text(strip=True)).strip().lower()
        if "website" in aria or text == "website":
            if is_good_url(href):
                return clean_url(href)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = re.sub(r'[^\x20-\x7E]', '', a.get_text(strip=True)).strip()
        if is_good_url(href) and DOMAIN_RE.match(text):
            return clean_url(href)

    for pattern in [
        r'"url"\s*:\s*"(https?://[^"]{5,120})"',
        r'website["\s:\']+\s*(https?://[^\s"\'<>]{5,120})',
    ]:
        for url in re.findall(pattern, html):
            if is_good_url(url) and "google" not in url and "gstatic" not in url:
                return clean_url(url)

    return ""


async def _website_from_google_search(page, business_name, location="Dallas TX"):
    try:
        query = urllib.parse.quote_plus(f'"{business_name}" {location}')
        await page.goto(
            f"https://www.google.com/search?q={query}",
            wait_until="domcontentloaded", timeout=15000,
        )
        await page.wait_for_timeout(2500)
        content = await page.content()
        soup    = BeautifulSoup(content, "html.parser")
        title_el = soup.find("title")
        if title_el and any(x in title_el.get_text().lower()
                            for x in ["captcha", "unusual traffic", "before you continue"]):
            return ""
        for cite in soup.find_all("cite"):
            raw    = cite.get_text(strip=True)
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
        print(f"  [search err: {e}]")
        return ""


async def _email_from_website(page, url):
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
            for a in soup.find_all("a", href=re.compile(r"^mailto:")):
                addr = a["href"].replace("mailto:", "").split("?")[0].strip()
                if addr and "@" in addr and not any(s in addr.lower() for s in EMAIL_SKIP):
                    return addr
            emails = extract_emails_from_html(html)
            if emails:
                return emails[0]
        except Exception:
            continue
    return ""


async def _scrape_maps_business(page, name, profile_url, category_label, location="Dallas TX"):
    print(f"    → {name}")
    try:
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(4000)
    except Exception as e:
        print(f"      [load error: {e}]")

    maps_html = await page.content()
    soup      = BeautifulSoup(maps_html, "html.parser")
    phone     = extract_phone(maps_html)

    website = _extract_url_from_maps_page(maps_html, soup)
    if not website:
        website = await _website_from_google_search(page, name, location)

    email = await _email_from_website(page, website) if website else ""

    return {
        "name": name, "category": category_label,
        "website": website, "phone": phone, "email": email,
        "scraped_at": _now(),
    }


async def _run_google_maps_job(job, browser):
    """
    job keys:
      categories  : list of (query, label) tuples  [default: MAPS_CATEGORIES_REBUILD]
      location    : str  [default: "Dallas TX"]
      limit       : int  [default: 10]
    """
    categories = job.get("categories", MAPS_CATEGORIES_REBUILD)
    location   = job.get("location", "Dallas TX")
    limit      = job.get("limit", 10)

    results = []
    for query, label in categories:
        page  = await _new_page(browser)
        enc   = urllib.parse.quote_plus(f"{query} in {location}")
        await page.goto(
            f"https://www.google.com/maps/search/{enc}",
            wait_until="domcontentloaded", timeout=25000,
        )
        await page.wait_for_timeout(3000)

        panel = await page.query_selector('div[role="feed"]')
        if panel:
            for _ in range(4):
                await panel.evaluate("el => el.scrollBy(0, 600)")
                await page.wait_for_timeout(1500)

        raw = await page.query_selector_all("a[aria-label]")
        listings, seen = [], set()
        for el in raw:
            label_text = await el.get_attribute("aria-label") or ""
            href       = await el.get_attribute("href") or ""
            if label_text and "/maps/place/" in href and href.startswith("http"):
                key = label_text.lower().strip()
                if key not in seen:
                    seen.add(key)
                    listings.append((label_text, href))

        print(f"  {label}: {len(listings)} listings found")
        count = 0
        for name, profile_url in listings[:limit * 2]:
            if count >= limit:
                break
            try:
                row = await _scrape_maps_business(page, name, profile_url, label, location)
                results.append(row)
                count += 1
            except Exception as e:
                print(f"    [!] {name}: {e}")
            await page.wait_for_timeout(random.randint(1500, 3000))

        await page.close()
        await asyncio.sleep(random.uniform(2, 4))

    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 2 — PRICE SCRAPER
#  Extracts item name + price pairs from menus, product pages, etc.
# ═════════════════════════════════════════════════════════════════════════════

async def _run_price_scraper(job, browser):
    """
    job keys:
      urls        : list of URLs to visit
      selectors   : {
                      "row":   CSS selector for each item container  (optional)
                      "name":  CSS selector for item name (relative to row)
                      "price": CSS selector for price (relative to row)
                    }
                    If no selectors given, falls back to regex price extraction.
      category    : label to tag all rows with  [default: ""]
    """
    urls      = job.get("urls", [])
    sel       = job.get("selectors", {})
    category  = job.get("category", "")
    results   = []

    page = await _new_page(browser)
    for url in urls:
        print(f"  → {url}")
        html = await _safe_goto(page, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        if sel.get("row") and sel.get("name"):
            # Structured CSS extraction
            for row in soup.select(sel["row"]):
                name_el  = row.select_one(sel["name"])
                price_el = row.select_one(sel.get("price", ""))
                name  = name_el.get_text(strip=True)  if name_el  else ""
                price = price_el.get_text(strip=True) if price_el else ""
                if not price:
                    # Try regex fallback within the row
                    m = PRICE_RE.search(row.get_text())
                    price = m.group(0) if m else ""
                if name:
                    results.append({
                        "name": name, "price": price,
                        "category": category, "source_url": url,
                        "scraped_at": _now(),
                    })
        else:
            # Regex fallback — scan entire page for price-like patterns
            text_blocks = soup.find_all(["p", "li", "td", "div", "span"])
            for block in text_blocks:
                text = block.get_text(strip=True)
                price_match = PRICE_RE.search(text)
                if price_match and len(text) < 200:
                    price = price_match.group(0)
                    name  = text.replace(price, "").strip(" -–:|")
                    if name and len(name) > 2:
                        results.append({
                            "name": name, "price": price,
                            "category": category, "source_url": url,
                            "scraped_at": _now(),
                        })

        print(f"    Found {len([r for r in results if r['source_url'] == url])} items")

    await page.close()
    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 3 — TRIVIA SCRAPER
#  Pulls question + answer pairs from trivia / quiz sites.
# ═════════════════════════════════════════════════════════════════════════════

# Default trivia sources (free, no auth required)
DEFAULT_TRIVIA_SOURCES = [
    {
        "url":      "https://opentdb.com/api.php?amount=50&type=multiple",
        "type":     "json_api",
        "q_key":    "question",
        "a_key":    "correct_answer",
        "category_key": "category",
        "results_key": "results",
    },
    {
        "url":      "https://the-trivia-api.com/api/questions?limit=50",
        "type":     "json_api",
        "q_key":    "question",
        "a_key":    "correctAnswer",
        "category_key": "category",
        "results_key": None,   # top-level list
    },
]


async def _run_trivia_scraper(job, browser):
    """
    job keys:
      sources     : list of source configs (see DEFAULT_TRIVIA_SOURCES for schema)
                    If omitted, uses DEFAULT_TRIVIA_SOURCES (Open Trivia DB + Trivia API)
      urls        : simple list of HTML trivia page URLs to scrape with CSS selectors
      selectors   : {
                      "question": CSS selector for question text
                      "answer":   CSS selector for answer text
                    }
      category    : label to tag rows  [default: "trivia"]
      limit       : max questions to collect  [default: 100]
    """
    sources  = job.get("sources", DEFAULT_TRIVIA_SOURCES)
    html_urls = job.get("urls", [])
    sel      = job.get("selectors", {})
    category = job.get("category", "trivia")
    limit    = job.get("limit", 100)
    results  = []

    page = await _new_page(browser)

    # ── JSON API sources ──────────────────────────────────────────────────
    for src in sources:
        if src.get("type") != "json_api":
            continue
        print(f"  → API: {src['url']}")
        try:
            html = await _safe_goto(page, src["url"])
            # Browsers wrap JSON in <html><body><pre>...</pre></body></html>
            # Strip that wrapper before parsing
            raw = html.strip()
            if not (raw.startswith("{") or raw.startswith("[")):
                soup_pre = BeautifulSoup(raw, "html.parser")
                pre = soup_pre.find("pre")
                raw = pre.get_text(strip=True) if pre else raw
            data = json.loads(raw) if (raw.startswith("{") or raw.startswith("[")) else {}

            rows_key = src.get("results_key")
            rows     = data.get(rows_key, data) if rows_key else data
            if isinstance(rows, dict):
                rows = rows.get("results", [])

            for item in rows:
                if len(results) >= limit:
                    break
                q = item.get(src["q_key"], "")
                a = item.get(src["a_key"], "")
                # Strip HTML entities from Open Trivia DB
                q = BeautifulSoup(q, "html.parser").get_text()
                a = BeautifulSoup(a, "html.parser").get_text()
                if q and a:
                    results.append({
                        "question":   q,
                        "answer":     a,
                        "category":   item.get(src.get("category_key", ""), category),
                        "difficulty": item.get("difficulty", ""),
                        "source_url": src["url"],
                        "scraped_at": _now(),
                    })
        except Exception as e:
            print(f"    [!] API error: {e}")

    # ── HTML page sources (CSS selectors) ────────────────────────────────
    for url in html_urls:
        if len(results) >= limit:
            break
        print(f"  → HTML: {url}")
        html = await _safe_goto(page, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        q_sel = sel.get("question", ".question")
        a_sel = sel.get("answer", ".answer")
        questions = soup.select(q_sel)
        answers   = soup.select(a_sel)
        for q_el, a_el in zip(questions, answers):
            if len(results) >= limit:
                break
            results.append({
                "question":   q_el.get_text(strip=True),
                "answer":     a_el.get_text(strip=True),
                "category":   category,
                "difficulty": "",
                "source_url": url,
                "scraped_at": _now(),
            })

    await page.close()
    print(f"  Collected {len(results)} trivia Q&As")
    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 4 — EMAIL HARVESTER
#  Given a list of URLs, return every email found on each page.
# ═════════════════════════════════════════════════════════════════════════════

async def _run_email_harvester(job, browser):
    """
    job keys:
      urls        : list of URLs to harvest from
      dig_contact : bool — also try /contact, /about, /contact-us  [default: True]
      label       : tag to attach to each row  [default: ""]
    """
    urls        = job.get("urls", [])
    dig_contact = job.get("dig_contact", True)
    label       = job.get("label", "")
    results     = []

    extra_slugs = ["", "/contact", "/contact-us", "/about", "/about-us"] if dig_contact else [""]

    page = await _new_page(browser)
    for url in urls:
        print(f"  → {url}")
        found_emails = set()
        for slug in extra_slugs:
            target = url.rstrip("/") + slug
            html   = await _safe_goto(page, target, timeout=12000)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            # Prefer mailto: links
            for a in soup.find_all("a", href=re.compile(r"^mailto:")):
                addr = a["href"].replace("mailto:", "").split("?")[0].strip()
                if addr and "@" in addr and not any(s in addr.lower() for s in EMAIL_SKIP):
                    found_emails.add(addr)
            # Regex scan
            for e in extract_emails_from_html(html):
                found_emails.add(e)

        for email in found_emails:
            results.append({
                "email":      email,
                "source_url": url,
                "label":      label,
                "scraped_at": _now(),
            })
        print(f"    Found {len(found_emails)} email(s)")

    await page.close()
    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 5 — CONTENT SCRAPER
#  Pull headlines, articles, blog posts, or any text content using CSS selectors.
# ═════════════════════════════════════════════════════════════════════════════

async def _run_content_scraper(job, browser):
    """
    job keys:
      urls        : list of URLs to visit
      selectors   : {
                      "title":   CSS selector for title/headline  (optional)
                      "body":    CSS selector for body content    (optional)
                      "link":    CSS selector for links           (optional)
                      "image":   CSS selector for image src       (optional)
                      "date":    CSS selector for publish date    (optional)
                      "custom":  dict of {field_name: css_selector} for anything else
                    }
      each_item   : CSS selector for the repeating container element (optional)
                    — if set, extract fields relative to each container
      category    : tag for all rows  [default: "content"]
    """
    urls      = job.get("urls", [])
    sel       = job.get("selectors", {})
    each_item = job.get("each_item", "")
    category  = job.get("category", "content")
    results   = []

    std_fields = {
        "title":  sel.get("title", "h1"),
        "body":   sel.get("body", ""),
        "link":   sel.get("link", ""),
        "image":  sel.get("image", ""),
        "date":   sel.get("date", ""),
    }
    custom_fields = sel.get("custom", {})

    page = await _new_page(browser)
    for url in urls:
        print(f"  → {url}")
        html = await _safe_goto(page, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        def _extract(container, css, attr=None):
            if not css:
                return ""
            el = container.select_one(css)
            if not el:
                return ""
            if attr:
                return el.get(attr, "")
            return el.get_text(strip=True)

        if each_item:
            containers = soup.select(each_item)
            for container in containers:
                row = {"category": category, "source_url": url, "scraped_at": _now()}
                row["title"] = _extract(container, std_fields["title"])
                row["body"]  = _extract(container, std_fields["body"])
                row["link"]  = _extract(container, std_fields["link"], attr="href")
                row["image"] = _extract(container, std_fields["image"], attr="src")
                row["date"]  = _extract(container, std_fields["date"])
                for fname, fcss in custom_fields.items():
                    row[fname] = _extract(container, fcss)
                if any(v for v in row.values() if v not in (category, url, _now(), "")):
                    results.append(row)
        else:
            row = {"category": category, "source_url": url, "scraped_at": _now()}
            row["title"] = _extract(soup, std_fields["title"])
            row["body"]  = _extract(soup, std_fields["body"])
            row["link"]  = _extract(soup, std_fields["link"], attr="href")
            row["image"] = _extract(soup, std_fields["image"], attr="src")
            row["date"]  = _extract(soup, std_fields["date"])
            for fname, fcss in custom_fields.items():
                row[fname] = _extract(soup, fcss)
            results.append(row)

        print(f"    Extracted {len([r for r in results if r['source_url'] == url])} items")

    await page.close()
    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 6 — PAGINATED SCRAPER
#  Follow "next page" links and collect items across multiple pages.
# ═════════════════════════════════════════════════════════════════════════════

async def _run_paginated_scraper(job, browser):
    """
    job keys:
      start_url   : first page URL
      next_sel    : CSS selector for the "next page" link  [default: 'a[rel="next"]']
      item_sel    : CSS selector for each item on the page
      fields      : dict of {field_name: css_selector} extracted relative to each item
      max_pages   : max pages to follow  [default: 10]
      category    : tag for all rows  [default: ""]
    """
    start_url = job.get("start_url", "")
    next_sel  = job.get("next_sel", 'a[rel="next"]')
    item_sel  = job.get("item_sel", "")
    fields    = job.get("fields", {})
    max_pages = job.get("max_pages", 10)
    category  = job.get("category", "")
    results   = []

    if not start_url or not item_sel:
        print("  [!] paginated_scraper requires start_url and item_sel")
        return results

    page        = await _new_page(browser)
    current_url = start_url
    page_num    = 0

    while current_url and page_num < max_pages:
        page_num += 1
        print(f"  → Page {page_num}: {current_url}")
        html = await _safe_goto(page, current_url)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")

        for item in soup.select(item_sel):
            row = {"category": category, "source_url": current_url,
                   "page": page_num, "scraped_at": _now()}
            for fname, fcss in fields.items():
                el = item.select_one(fcss)
                row[fname] = el.get_text(strip=True) if el else ""
            results.append(row)

        print(f"    {len(results)} total items so far")

        # Find next page link
        next_el = soup.select_one(next_sel)
        if next_el:
            href = next_el.get("href", "")
            if href and href.startswith("http"):
                current_url = href
            elif href:
                from urllib.parse import urljoin
                current_url = urljoin(current_url, href)
            else:
                break
        else:
            break

        await page.wait_for_timeout(random.randint(2000, 4000))

    await page.close()
    return results


# ═════════════════════════════════════════════════════════════════════════════
#  JOB TYPE 7 — AUTO EMAIL TRIGGER
#  Scrape leads then immediately pipe into the email pipeline.
# ═════════════════════════════════════════════════════════════════════════════

async def _run_auto_email_job(job, browser):
    """
    job keys:
      source_job  : a nested job config (google_maps_business or email_harvester)
                    that generates leads with an "email" field
      filter_fn   : optional callable(row) -> bool to filter which rows get emailed
      email_fn    : callable(row) -> None  — your send function
                    If omitted, prints what would be sent (dry run)
      dry_run     : bool  [default: True for safety]
    """
    source_job = job.get("source_job", {})
    filter_fn  = job.get("filter_fn", None)
    email_fn   = job.get("email_fn", None)
    dry_run    = job.get("dry_run", True)

    # Run the source scraper first
    source_type = source_job.get("type", "google_maps_business")
    rows = await _dispatch_job(source_job, browser)

    queued = 0
    for row in rows:
        email = row.get("email", "")
        if not email:
            continue
        if filter_fn and not filter_fn(row):
            continue

        if dry_run or not email_fn:
            print(f"  [DRY RUN] Would email: {row.get('name', '')} <{email}>")
        else:
            try:
                email_fn(row)
                queued += 1
                print(f"  ✓ Emailed: {email}")
            except Exception as e:
                print(f"  [!] Email error for {email}: {e}")

    print(f"\n  {'Dry run complete' if (dry_run or not email_fn) else f'Sent {queued} emails'}")
    return rows


# ═════════════════════════════════════════════════════════════════════════════
#  OUTPUT HANDLERS
# ═════════════════════════════════════════════════════════════════════════════

def _output_csv(results, job):
    if not results:
        print("  No results to write.")
        return
    path = job.get("output_path", f"output/scrape_{_now().replace(' ', '_').replace(':', '-')}.csv")
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    fieldnames = list(results[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"\n✓ CSV saved → {path}  ({len(results)} rows)")
    return path


def _output_json(results, job):
    if not results:
        print("  No results to write.")
        return
    path = job.get("output_path", f"output/scrape_{_now().replace(' ', '_').replace(':', '-')}.json")
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n✓ JSON saved → {path}  ({len(results)} records)")
    return path


def _output_supabase(results, job):
    if not _supabase:
        print("  [!] Supabase client not initialized. Check SUPABASE_URL / SUPABASE_KEY env vars.")
        return
    table = job.get("supabase_table", "scrape_results")
    try:
        resp = _supabase.table(table).upsert(results).execute()
        print(f"\n✓ Supabase upsert → {table}  ({len(results)} rows)")
        return resp
    except Exception as e:
        print(f"  [!] Supabase error: {e}")


def _output_print(results, job):
    print(f"\n{'─'*60}")
    for row in results:
        for k, v in row.items():
            print(f"  {k:<18}: {v}")
        print()
    print(f"  Total: {len(results)} rows")


def _write_output(results, job):
    output = job.get("output", "csv")
    if output == "csv":
        return _output_csv(results, job)
    elif output == "json":
        return _output_json(results, job)
    elif output == "supabase":
        return _output_supabase(results, job)
    elif output == "print":
        return _output_print(results, job)
    elif output == "email_trigger":
        print("  → Results handed off to email pipeline (see auto_email job type)")
        return results
    else:
        print(f"  [!] Unknown output type '{output}' — defaulting to print")
        return _output_print(results, job)


# ═════════════════════════════════════════════════════════════════════════════
#  DISPATCHER
# ═════════════════════════════════════════════════════════════════════════════

JOB_RUNNERS = {
    "google_maps_business": _run_google_maps_job,
    "price_scraper":        _run_price_scraper,
    "trivia_scraper":       _run_trivia_scraper,
    "email_harvester":      _run_email_harvester,
    "content_scraper":      _run_content_scraper,
    "paginated_scraper":    _run_paginated_scraper,
    "auto_email":           _run_auto_email_job,
}


async def _dispatch_job(job, browser):
    job_type = job.get("type", "google_maps_business")
    runner   = JOB_RUNNERS.get(job_type)
    if not runner:
        print(f"  [!] Unknown job type: '{job_type}'")
        return []
    print(f"\n{'═'*60}")
    print(f"  JOB: {job_type.upper()}")
    print(f"  Started: {_now()}")
    print(f"{'═'*60}")
    return await runner(job, browser)


async def run_job_async(job: dict) -> list:
    """Main entry point. Pass a job config dict, get results list back."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            results = await _dispatch_job(job, browser)
        finally:
            await browser.close()

    _write_output(results, job)
    return results


def run_job(job: dict) -> list:
    """Synchronous wrapper — safe to call from server.py or Railway."""
    return asyncio.run(run_job_async(job))


# ═════════════════════════════════════════════════════════════════════════════
#  INTERACTIVE CLI
# ═════════════════════════════════════════════════════════════════════════════

JOB_MENU = [
    ("Google Maps Business Scraper",  "google_maps_business"),
    ("Price Scraper (menus, products)", "price_scraper"),
    ("Trivia Q&A Scraper",            "trivia_scraper"),
    ("Email Harvester",               "email_harvester"),
    ("Content / Article Scraper",     "content_scraper"),
    ("Paginated List Scraper",        "paginated_scraper"),
]

OUTPUT_MENU = [
    ("CSV file",       "csv"),
    ("JSON file",      "json"),
    ("Supabase table", "supabase"),
    ("Print to console", "print"),
]


def _pick(menu, prompt="Pick one: "):
    for i, (label, _) in enumerate(menu, 1):
        print(f"  {i:>2}. {label}")
    raw = input(prompt).strip()
    if raw.isdigit() and 1 <= int(raw) <= len(menu):
        return menu[int(raw) - 1][1]
    return menu[0][1]


def _build_job_interactive():
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║         Universal Scraper — Job Builder              ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    print("SELECT JOB TYPE:")
    job_type = _pick(JOB_MENU, "Job type: ")
    job = {"type": job_type}

    # ── Google Maps ──────────────────────────────────────────────────────
    if job_type == "google_maps_business":
        print("\nUSE DEFAULT REBUILD DIGITAL CATEGORIES? (y/n) [y]")
        if input().strip().lower() not in ("n", "no"):
            job["categories"] = MAPS_CATEGORIES_REBUILD
        else:
            print("Enter search queries, one per line. Blank line to finish.")
            print("Format: search term | Label   e.g.  coffee shops | Coffee Shop")
            cats = []
            while True:
                line = input("> ").strip()
                if not line:
                    break
                parts = [p.strip() for p in line.split("|")]
                if len(parts) == 2:
                    cats.append((parts[0], parts[1]))
                else:
                    cats.append((line, line.title()))
            job["categories"] = cats or MAPS_CATEGORIES_REBUILD

        loc = input("\nLocation [Dallas TX]: ").strip() or "Dallas TX"
        job["location"] = loc
        lim = input("Results per category [10]: ").strip()
        job["limit"] = int(lim) if lim.isdigit() else 10

    # ── Price Scraper ─────────────────────────────────────────────────────
    elif job_type == "price_scraper":
        print("\nEnter URLs to scrape (one per line, blank to finish):")
        urls = []
        while True:
            u = input("> ").strip()
            if not u:
                break
            urls.append(u)
        job["urls"] = urls
        job["category"] = input("Category label (e.g. Beer, Food): ").strip()
        print("\nCSS selectors (leave blank to use regex fallback):")
        row_sel  = input("  Row container selector (e.g. .menu-item): ").strip()
        name_sel = input("  Name selector (e.g. .item-name): ").strip()
        price_sel = input("  Price selector (e.g. .price): ").strip()
        if row_sel or name_sel:
            job["selectors"] = {"row": row_sel, "name": name_sel, "price": price_sel}

    # ── Trivia Scraper ────────────────────────────────────────────────────
    elif job_type == "trivia_scraper":
        print("\nUSE DEFAULT API SOURCES (Open Trivia DB)? (y/n) [y]")
        if input().strip().lower() not in ("n", "no"):
            job["sources"] = DEFAULT_TRIVIA_SOURCES
        else:
            print("Enter trivia page URLs (one per line, blank to finish):")
            urls = []
            while True:
                u = input("> ").strip()
                if not u:
                    break
                urls.append(u)
            job["urls"]    = urls
            job["sources"] = []
            q_sel = input("Question CSS selector [.question]: ").strip() or ".question"
            a_sel = input("Answer CSS selector [.answer]: ").strip() or ".answer"
            job["selectors"] = {"question": q_sel, "answer": a_sel}

        lim = input("Max questions to collect [100]: ").strip()
        job["limit"]    = int(lim) if lim.isdigit() else 100
        job["category"] = input("Category label [trivia]: ").strip() or "trivia"

    # ── Email Harvester ───────────────────────────────────────────────────
    elif job_type == "email_harvester":
        print("\nEnter URLs to harvest (one per line, blank to finish):")
        urls = []
        while True:
            u = input("> ").strip()
            if not u:
                break
            urls.append(u)
        job["urls"]        = urls
        job["label"]       = input("Label for these emails: ").strip()
        job["dig_contact"] = input("Also check /contact and /about pages? (y/n) [y]: ").strip().lower() not in ("n", "no")

    # ── Content Scraper ───────────────────────────────────────────────────
    elif job_type == "content_scraper":
        print("\nEnter URLs to scrape (one per line, blank to finish):")
        urls = []
        while True:
            u = input("> ").strip()
            if not u:
                break
            urls.append(u)
        job["urls"]      = urls
        job["category"]  = input("Category label: ").strip()
        each_item = input("Repeating item container selector (blank if whole page): ").strip()
        if each_item:
            job["each_item"] = each_item
        title_sel = input("Title selector [h1]: ").strip() or "h1"
        body_sel  = input("Body selector (e.g. .article-body): ").strip()
        job["selectors"] = {"title": title_sel, "body": body_sel}

    # ── Paginated Scraper ─────────────────────────────────────────────────
    elif job_type == "paginated_scraper":
        job["start_url"]  = input("\nStart URL: ").strip()
        job["item_sel"]   = input("Item CSS selector (e.g. .listing): ").strip()
        job["next_sel"]   = input('Next page selector [a[rel="next"]]: ').strip() or 'a[rel="next"]'
        lim = input("Max pages [10]: ").strip()
        job["max_pages"]  = int(lim) if lim.isdigit() else 10
        job["category"]   = input("Category label: ").strip()
        print("Define fields to extract (field_name:css_selector, one per line, blank to finish):")
        fields = {}
        while True:
            line = input("> ").strip()
            if not line:
                break
            if ":" in line:
                k, v = line.split(":", 1)
                fields[k.strip()] = v.strip()
        job["fields"] = fields

    # ── Output ────────────────────────────────────────────────────────────
    print("\nSELECT OUTPUT:")
    job["output"] = _pick(OUTPUT_MENU, "Output: ")

    if job["output"] == "csv":
        path = input("Output file path [output/results.csv]: ").strip() or "output/results.csv"
        job["output_path"] = path
    elif job["output"] == "json":
        path = input("Output file path [output/results.json]: ").strip() or "output/results.json"
        job["output_path"] = path
    elif job["output"] == "supabase":
        table = input("Supabase table name: ").strip()
        job["supabase_table"] = table

    return job


def main():
    job = _build_job_interactive()
    print(f"\n  Starting job...\n")
    results = run_job(job)
    print(f"\n  Done. {len(results)} total records.")


if __name__ == "__main__":
    main()
