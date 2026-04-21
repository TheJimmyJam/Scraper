"""
analyzer.py — Rebuild Digital Co
Visits each business website and scores it across multiple dimensions.
Outputs: output/businesses_analyzed.csv
"""

import requests
import csv
import re
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

BOOKING_KEYWORDS = [
    "book now", "book appointment", "schedule", "reserve", "booking",
    "calendly", "acuity", "square appointments", "booksy", "vagaro",
    "fresha", "mindbody", "setmore", "appointy", "simplybook"
]

PORTAL_KEYWORDS = [
    "client login", "patient portal", "member login", "log in", "sign in",
    "my account", "customer portal", "login", "portal"
]

PRICING_KEYWORDS = [
    "pricing", "prices", "rates", "fee", "cost", "services & rates",
    "menu", "packages", "$", "per hour", "starting at"
]

OUTDATED_SIGNALS = [
    "flash", "internet explorer", "best viewed in", "adobe flash",
    "get adobe", "under construction", "coming soon"
]

MODERN_CMS = ["wordpress", "wix", "squarespace", "shopify", "webflow", "weebly"]


def analyze_website(url):
    result = {
        "has_ssl": False,
        "has_mobile_meta": False,
        "has_booking": False,
        "has_client_portal": False,
        "has_pricing": False,
        "has_contact_form": False,
        "copyright_year": None,
        "is_outdated": False,
        "cms_detected": None,
        "email_found": None,
        "phone_found": None,
        "issues": [],
        "score": 0,  # 0-10, higher = more opportunity for Rebuild Digital
        "fetch_error": None,
    }

    if not url or not url.startswith("http"):
        result["fetch_error"] = "No website found"
        result["score"] = 10
        result["issues"].append("No website — biggest opportunity: build from scratch")
        return result

    # SSL check
    result["has_ssl"] = url.startswith("https://")
    if not result["has_ssl"]:
        result["issues"].append("No SSL/HTTPS")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        # Check if redirect upgraded to HTTPS
        if resp.url.startswith("https://"):
            result["has_ssl"] = True
            result["issues"] = [i for i in result["issues"] if i != "No SSL/HTTPS"]

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        html_lower = html.lower()

    except Exception as e:
        result["fetch_error"] = str(e)[:100]
        result["score"] = 8  # Can't reach site = big opportunity
        result["issues"].append("Site unreachable or very slow")
        return result

    # Mobile viewport
    viewport = soup.find("meta", attrs={"name": re.compile("viewport", re.I)})
    result["has_mobile_meta"] = viewport is not None
    if not result["has_mobile_meta"]:
        result["issues"].append("Not mobile-friendly (no viewport meta)")

    # Booking
    result["has_booking"] = any(kw in html_lower for kw in BOOKING_KEYWORDS)
    if not result["has_booking"]:
        result["issues"].append("No online booking system detected")

    # Client portal
    result["has_client_portal"] = any(kw in html_lower for kw in PORTAL_KEYWORDS)

    # Pricing
    result["has_pricing"] = any(kw in html_lower for kw in PRICING_KEYWORDS)
    if not result["has_pricing"]:
        result["issues"].append("No visible pricing/rates page")

    # Contact form
    forms = soup.find_all("form")
    for form in forms:
        inputs = form.find_all("input")
        if any(i.get("type") in ["email", "text"] for i in inputs):
            result["has_contact_form"] = True
            break
    if not result["has_contact_form"]:
        result["issues"].append("No contact form detected")

    # Copyright year — detect stale sites
    copyright_match = re.search(r'©\s*(\d{4})|copyright\s+(\d{4})', html_lower)
    if copyright_match:
        year = int(copyright_match.group(1) or copyright_match.group(2))
        result["copyright_year"] = year
        if year < 2020:
            result["is_outdated"] = True
            result["issues"].append(f"Outdated copyright ({year}) — site likely stale")

    # Outdated tech signals
    for signal in OUTDATED_SIGNALS:
        if signal in html_lower:
            result["is_outdated"] = True
            result["issues"].append(f"Outdated tech detected: '{signal}'")
            break

    # CMS detection
    for cms in MODERN_CMS:
        if cms in html_lower:
            result["cms_detected"] = cms
            break
    if not result["cms_detected"]:
        result["issues"].append("Unknown/custom CMS — may be hard to maintain")

    # Email extraction
    def extract_emails(text):
        found = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text)
        return [
            e for e in found
            if not any(skip in e.lower() for skip in [
                "example", "youremail", "email@", "sentry", "wix.com",
                "wordpress", "schema", "png", "jpg", "svg", "placeholder",
                "noreply", "no-reply", "donotreply",
            ])
        ]

    # Check homepage first
    emails = extract_emails(html)

    # If no email found, try the contact page
    if not emails:
        from urllib.parse import urljoin
        for contact_slug in ["/contact", "/contact-us", "/about", "/about-us", "/reach-us"]:
            try:
                contact_url = urljoin(url, contact_slug)
                cr = requests.get(contact_url, headers=HEADERS, timeout=8, allow_redirects=True)
                if cr.status_code == 200:
                    contact_emails = extract_emails(cr.text)
                    if contact_emails:
                        emails = contact_emails
                        break
            except Exception:
                continue

    # Also check mailto: links specifically
    if not emails:
        for a in soup.find_all("a", href=re.compile(r"^mailto:")):
            addr = a["href"].replace("mailto:", "").split("?")[0].strip()
            if addr and "@" in addr:
                emails = [addr]
                break

    if emails:
        result["email_found"] = emails[0]

    # Phone extraction
    phones = re.findall(r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}', html)
    if phones:
        result["phone_found"] = phones[0]

    # Score: 0 = perfect site, 10 = total rebuild needed
    score = 0
    if not result["has_ssl"]:
        score += 2
    if not result["has_mobile_meta"]:
        score += 2
    if not result["has_booking"]:
        score += 2
    if result["is_outdated"]:
        score += 2
    if not result["has_contact_form"]:
        score += 1
    if not result["has_pricing"]:
        score += 1
    result["score"] = min(score, 10)

    return result


def run_analyzer(input_path="output/businesses_raw.csv", output_path="output/businesses_analyzed.csv"):
    print("=== Rebuild Digital Co — Website Analyzer ===\n")

    with open(input_path, newline="", encoding="utf-8") as f:
        businesses = list(csv.DictReader(f))

    print(f"Analyzing {len(businesses)} businesses...\n")

    results = []
    for i, biz in enumerate(businesses):
        name = biz.get("name", "Unknown")
        url = biz.get("website", "").strip()
        print(f"[{i+1}/{len(businesses)}] {name} — {url or 'NO WEBSITE'}")

        analysis = analyze_website(url)

        # Override email with one found on site if better
        email = analysis.get("email_found") or biz.get("email", "")
        phone = analysis.get("phone_found") or biz.get("phone", "")

        row = {**biz}
        row["email"] = email
        row["phone"] = phone
        row["has_ssl"] = analysis["has_ssl"]
        row["has_mobile"] = analysis["has_mobile_meta"]
        row["has_booking"] = analysis["has_booking"]
        row["has_portal"] = analysis["has_client_portal"]
        row["has_pricing"] = analysis["has_pricing"]
        row["has_contact_form"] = analysis["has_contact_form"]
        row["copyright_year"] = analysis["copyright_year"]
        row["is_outdated"] = analysis["is_outdated"]
        row["cms"] = analysis["cms_detected"]
        row["opportunity_score"] = analysis["score"]
        row["issues"] = " | ".join(analysis["issues"])
        row["fetch_error"] = analysis["fetch_error"]

        results.append(row)

        if analysis["score"] >= 5:
            print(f"  ★ HIGH OPPORTUNITY (score {analysis['score']}/10): {', '.join(analysis['issues'][:2])}")
        else:
            print(f"  Score: {analysis['score']}/10")

        time.sleep(random.uniform(1.0, 2.5))

    # Sort by opportunity score descending
    results.sort(key=lambda x: int(x.get("opportunity_score", 0) or 0), reverse=True)

    fieldnames = [
        "name", "category", "website", "email", "phone", "address",
        "opportunity_score", "has_ssl", "has_mobile", "has_booking",
        "has_portal", "has_pricing", "has_contact_form",
        "copyright_year", "is_outdated", "cms", "issues", "fetch_error", "yelp_url"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    high_opp = [r for r in results if int(r.get("opportunity_score", 0) or 0) >= 5]
    with_email = [r for r in high_opp if r.get("email")]

    print(f"\n✓ Analysis complete.")
    print(f"  Total analyzed: {len(results)}")
    print(f"  High opportunity (score ≥5): {len(high_opp)}")
    print(f"  High opp with email: {len(with_email)}")
    print(f"  Saved to: {output_path}")
    return results


if __name__ == "__main__":
    run_analyzer()
