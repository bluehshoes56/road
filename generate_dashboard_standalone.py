#!/usr/bin/env python3
"""
Payments Intelligence Dashboard Generator
==========================================
STANDALONE VERSION - No pip installs required!
Uses only Python standard library.

Usage:
    python3 generate_dashboard_standalone.py
"""

import os
import re
import json
import zipfile
import ssl
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from html.parser import HTMLParser

# =============================================================================
# AZURE OPENAI CONFIGURATION
# =============================================================================
# Enter your credentials here:
# =============================================================================

AZURE_OPENAI_ENDPOINT = "https://az-n-df-dcs-openai.azure.com/"
AZURE_OPENAI_API_KEY = ""       # <-- ENTER YOUR API KEY HERE
AZURE_OPENAI_DEPLOYMENT = "az-n-df-dcs-dev-gpt5-ds-model"
AZURE_OPENAI_API_VERSION = "2024-02-15-preview"

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR = Path(__file__).parent
OUTPUT_FILE = SCRIPT_DIR / "payments_dashboard.html"
PROMPT_DOC = SCRIPT_DIR / "Prompt Claude.docx"
NEWS_LOOKBACK_DAYS = 90

# News Sources
NEWS_SOURCES = [
    {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/newsevents/pressreleases.htm", "rss": "https://www.federalreserve.gov/feeds/press_all.xml", "category": "fed"},
    {"name": "Treasury", "url": "https://home.treasury.gov/news/press-releases", "rss": None, "category": "macro"},
    {"name": "CFPB", "url": "https://www.consumerfinance.gov/about-us/newsroom/", "rss": "https://www.consumerfinance.gov/about-us/newsroom/feed/", "category": "regulation"},
    {"name": "Payments Dive", "url": "https://www.paymentsdive.com", "rss": "https://www.paymentsdive.com/feeds/news/", "category": "competitive"},
    {"name": "PYMNTS", "url": "https://www.pymnts.com", "rss": "https://www.pymnts.com/feed/", "category": "competitive"},
]


# =============================================================================
# SIMPLE HTML PARSER (replaces BeautifulSoup)
# =============================================================================

class SimpleHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.links = []
        self.current_link = None
        self.in_script = False
        self.in_style = False

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self.in_script = True
        elif tag == 'style':
            self.in_style = True
        elif tag == 'a':
            attrs_dict = dict(attrs)
            if 'href' in attrs_dict:
                self.current_link = attrs_dict['href']

    def handle_endtag(self, tag):
        if tag == 'script':
            self.in_script = False
        elif tag == 'style':
            self.in_style = False
        elif tag == 'a':
            self.current_link = None

    def handle_data(self, data):
        if not self.in_script and not self.in_style:
            text = data.strip()
            if text:
                self.text_parts.append(text)
                if self.current_link and len(text) > 10:
                    self.links.append({"text": text[:100], "url": self.current_link})

    def get_text(self):
        return ' '.join(self.text_parts)

    def get_links(self):
        return self.links[:20]


# =============================================================================
# HTTP UTILITIES (replaces requests)
# =============================================================================

def fetch_url(url, timeout=15):
    """Fetch URL content using standard library."""
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

    try:
        # Create SSL context that doesn't verify (for corporate proxies)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as response:
            content = response.read()
            # Try to decode as UTF-8, fallback to latin-1
            try:
                return content.decode('utf-8')
            except:
                return content.decode('latin-1', errors='ignore')
    except Exception as e:
        print(f"      [!] Error fetching {url}: {e}")
        return ""


# =============================================================================
# DATE PARSING (replaces python-dateutil)
# =============================================================================

def parse_date_simple(date_str):
    """Simple date parser using standard library."""
    if not date_str:
        return None

    # Common date formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ]

    # Clean the string
    date_str = re.sub(r'\+\d{2}:\d{2}', '', date_str)  # Remove timezone
    date_str = re.sub(r'\.\d+', '', date_str)  # Remove milliseconds
    date_str = date_str.strip()

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            continue

    # Try to extract date with regex
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
        (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', None),  # "January 15, 2026"
    ]

    for pattern, fmt in patterns:
        match = re.search(pattern, date_str)
        if match:
            try:
                if fmt:
                    return datetime.strptime(match.group(), fmt)
            except:
                pass

    return None


# =============================================================================
# DOCUMENT EXTRACTION
# =============================================================================

def extract_text_from_docx(docx_path):
    """Extract text from .docx file."""
    try:
        with zipfile.ZipFile(docx_path, 'r') as zip_ref:
            xml_content = zip_ref.read('word/document.xml')
        root = ET.fromstring(xml_content)
        texts = []
        for elem in root.iter():
            if elem.tag.endswith('}t') and elem.text:
                texts.append(elem.text)
        return ' '.join(texts)
    except Exception as e:
        print(f"  [!] Error reading {docx_path}: {e}")
        return ""


# =============================================================================
# NEWS FETCHING
# =============================================================================

def fetch_rss_feed(rss_url, source_name, category, cutoff_date):
    """Fetch and parse RSS feed."""
    articles = []
    content = fetch_url(rss_url)
    if not content:
        return articles

    try:
        root = ET.fromstring(content)
        items = root.findall('.//item')

        for item in items[:20]:
            title_elem = item.find('title')
            link_elem = item.find('link')
            date_elem = item.find('pubDate')
            desc_elem = item.find('description')

            title = title_elem.text if title_elem is not None else ""
            link = link_elem.text if link_elem is not None else ""
            date_str = date_elem.text if date_elem is not None else ""
            description = desc_elem.text if desc_elem is not None else ""

            # Clean HTML from description
            if description:
                description = re.sub(r'<[^>]+>', '', description)

            pub_date = parse_date_simple(date_str)

            if pub_date is None or pub_date >= cutoff_date:
                articles.append({
                    "title": title,
                    "url": link,
                    "published_at": pub_date.isoformat() if pub_date else None,
                    "description": description[:500] if description else "",
                    "source": source_name,
                    "category": category
                })
    except Exception as e:
        print(f"      [!] RSS parse error: {e}")

    return articles


def fetch_webpage(url, source_name, category, cutoff_date):
    """Fetch webpage and extract content."""
    content = fetch_url(url)
    if not content:
        return {"articles": [], "full_text": "", "source": source_name, "category": category}

    parser = SimpleHTMLParser()
    try:
        parser.feed(content)
    except:
        pass

    full_text = parser.get_text()[:8000]
    links = parser.get_links()

    # Create articles from links
    articles = []
    for link in links:
        if link['url'].startswith('/'):
            link['url'] = urllib.parse.urljoin(url, link['url'])
        articles.append({
            "title": link['text'],
            "url": link['url'],
            "published_at": None,
            "description": "",
            "source": source_name,
            "category": category
        })

    return {
        "articles": articles[:15],
        "full_text": full_text,
        "source": source_name,
        "category": category
    }


def fetch_all_news(sources):
    """Fetch news from all sources."""
    cutoff_date = datetime.now() - timedelta(days=NEWS_LOOKBACK_DAYS)

    print(f"\n[2/5] Fetching news (rolling {NEWS_LOOKBACK_DAYS}-day lookback)...")
    print(f"      Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")

    all_sources = []

    for source in sources:
        print(f"    > {source['name']}...")
        articles = []

        # Try RSS first
        if source.get('rss'):
            rss_articles = fetch_rss_feed(source['rss'], source['name'], source['category'], cutoff_date)
            articles.extend(rss_articles)
            print(f"      RSS: {len(rss_articles)} articles")

        # Also fetch webpage
        webpage_data = fetch_webpage(source['url'], source['name'], source['category'], cutoff_date)

        all_sources.append({
            "source": source['name'],
            "url": source['url'],
            "category": source['category'],
            "articles": articles + webpage_data['articles'][:10],
            "content": webpage_data['full_text'],
            "fetched_at": datetime.now().isoformat()
        })

    total = sum(len(s['articles']) for s in all_sources)
    print(f"\n      Total: {total} articles from {len(all_sources)} sources")

    return all_sources


# =============================================================================
# AZURE OPENAI API (replaces openai package)
# =============================================================================

def call_azure_openai(prompt):
    """Call Azure OpenAI API using standard library."""
    if not AZURE_OPENAI_API_KEY:
        print("[!] Azure OpenAI API key not set. Using sample data.")
        return None

    url = f"{AZURE_OPENAI_ENDPOINT}openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"

    headers = {
        'Content-Type': 'application/json',
        'api-key': AZURE_OPENAI_API_KEY
    }

    payload = {
        "messages": [
            {"role": "system", "content": "You are a senior payments data analyst. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4096
    }

    try:
        data = json.dumps(payload).encode('utf-8')

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, data=data, headers=headers, method='POST')

        print("[3/5] Calling Azure OpenAI API...")
        with urllib.request.urlopen(req, timeout=120, context=ctx) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result['choices'][0]['message']['content']

            # Extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                return json.loads(json_match.group())

    except Exception as e:
        print(f"    [!] Azure OpenAI error: {e}")

    return None


def create_analysis_prompt(news_items, instructions):
    """Create prompt for analysis."""
    news_sections = []
    for item in news_items:
        section = f"SOURCE: {item['source']}\nCATEGORY: {item['category']}\n"
        if item.get('articles'):
            section += "ARTICLES:\n"
            for article in item['articles'][:10]:
                date_str = article.get('published_at', 'Unknown')
                section += f"  - [{date_str}] {article['title']}\n"
        if item.get('content'):
            section += f"\nCONTENT:\n{item['content'][:1500]}"
        news_sections.append(section)

    news_text = "\n\n---\n\n".join(news_sections)

    return f"""You are a payments intelligence analyst. Analyze the following news and return a JSON object.

INSTRUCTIONS:
{instructions[:3000]}

TODAY'S NEWS:
{news_text}

Return a JSON object with this structure:
{{
    "kpis": [
        {{"value": "+4.2%", "label": "Holiday Spend YoY", "type": "positive|negative|neutral|warning"}}
    ],
    "executive_summary": [
        {{"title": "...", "description": "...", "impact": "high|medium|positive|info", "badge": "...", "badge_type": "negative|positive|mixed|watch"}}
    ],
    "events": [
        {{
            "date": "Jan 19",
            "category": "regulation|macro|competitive|payment_network|merchant",
            "naics3": "522",
            "product": "...",
            "merchant": "...",
            "network": "...",
            "event_type": "...",
            "summary": "...",
            "impact": "positive|negative|mixed|unclear",
            "confidence": 0.85,
            "source_url": "https://..."
        }}
    ],
    "notes": [
        {{"title": "...", "description": "..."}}
    ]
}}

Return ONLY valid JSON."""


def analyze_news(news_items, instructions):
    """Analyze news with Azure OpenAI."""
    prompt = create_analysis_prompt(news_items, instructions)
    result = call_azure_openai(prompt)

    if result:
        print("    > Analysis complete")
        return result

    return get_sample_analysis()


def get_sample_analysis():
    """Sample data when API unavailable."""
    return {
        "kpis": [
            {"value": "+4.2%", "label": "Holiday Spend YoY", "type": "positive"},
            {"value": "$52.9M", "label": "FTC Enforcement", "type": "negative"},
            {"value": "70%", "label": "Banks Fraud Spend", "type": "warning"},
            {"value": "16", "label": "Events Tracked", "type": "neutral"}
        ],
        "executive_summary": [
            {"title": "FTC Enforcement", "description": "Cliq ordered to pay $52.9M for payment processing violations.", "impact": "high", "badge": "High Impact", "badge_type": "negative"},
            {"title": "Holiday Spend Strong", "description": "U.S. holiday spending up 4.2% YoY per Visa data.", "impact": "positive", "badge": "Positive", "badge_type": "positive"},
            {"title": "Fraud Spend Rising", "description": "70% of banks increasing fraud prevention budgets.", "impact": "medium", "badge": "Mixed", "badge_type": "mixed"},
            {"title": "Card Network Fees", "description": "Visa/MC rebuff merchant fee complaints.", "impact": "medium", "badge": "Watch", "badge_type": "watch"}
        ],
        "events": [
            {"date": "Jan 19", "category": "regulation", "naics3": "522", "product": "Payment Processing", "merchant": "Cliq", "network": "Multiple", "event_type": "Enforcement", "summary": "FTC orders $52.9M refund", "impact": "negative", "confidence": 0.85, "source_url": "https://www.paymentsdive.com"},
            {"date": "Jan 15", "category": "competitive", "naics3": "522", "product": "Card Payments", "merchant": "Visa", "network": "Visa", "event_type": "Earnings", "summary": "Q1 results due Jan 29", "impact": "unclear", "confidence": 0.90, "source_url": "https://usa.visa.com"},
            {"date": "Dec 23", "category": "macro", "naics3": "441-454", "product": "Holiday Retail", "merchant": "Multiple", "network": "Visa", "event_type": "Data", "summary": "Holiday spending +4.2% YoY", "impact": "positive", "confidence": 0.90, "source_url": "https://usa.visa.com"}
        ],
        "notes": [
            {"title": "FTC Enforcement", "description": "Payment processor ordered to refund $52.9M."},
            {"title": "Holiday Spend", "description": "Consumer resilience continues despite macro uncertainty."},
            {"title": "Confidence", "description": "0.9=high, 0.7=moderate, 0.6=low certainty."}
        ]
    }


# =============================================================================
# HTML DASHBOARD GENERATION
# =============================================================================

def generate_html(analysis, date_str):
    """Generate HTML dashboard."""

    # KPIs
    kpi_html = ""
    for kpi in analysis.get("kpis", []):
        kpi_html += f'''<div class="kpi-card {kpi.get('type','neutral')}"><div class="value">{kpi['value']}</div><div class="label">{kpi['label']}</div></div>'''

    # Summary
    summary_html = ""
    for item in analysis.get("executive_summary", []):
        impact = {"high":"high-impact","medium":"medium-impact","positive":"positive-impact"}.get(item.get('impact','info'),'info')
        summary_html += f'''<div class="summary-card {impact}"><h3>{item['title']}</h3><p>{item['description']}</p><span class="badge {item.get('badge_type','watch')}">{item.get('badge','Info')}</span></div>'''

    # Events
    events_html = ""
    for e in analysis.get("events", []):
        conf = e.get('confidence', 0.5)
        conf_class = "conf-high" if conf >= 0.8 else "conf-med" if conf >= 0.65 else "conf-low"
        events_html += f'''<tr>
            <td>{e.get('date','')}</td>
            <td><span class="tag cat">{e.get('category','')}</span></td>
            <td><span class="tag naics">{e.get('naics3','')}</span></td>
            <td>{e.get('product','')}</td>
            <td>{e.get('merchant','')}</td>
            <td>{e.get('network','')}</td>
            <td>{e.get('summary','')}</td>
            <td class="impact-{e.get('impact','unclear')}">{e.get('impact','').title()}</td>
            <td><div class="conf-bar {conf_class}"><div class="conf-fill" style="width:{int(conf*100)}%"></div></div>{conf:.2f}</td>
            <td><a href="{e.get('source_url','#')}" target="_blank">Link</a></td>
        </tr>'''

    # Notes
    notes_html = ""
    for n in analysis.get("notes", []):
        notes_html += f'''<div class="note"><h4>{n['title']}</h4><p>{n['description']}</p></div>'''

    return f'''<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>Payments Intelligence | {date_str}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:linear-gradient(135deg,#0a1628,#1a2744);min-height:100vh;color:#e4e9f0;line-height:1.6;padding:30px}}
.container{{max-width:1400px;margin:0 auto}}
h1{{font-size:2.2rem;font-weight:300;color:#fff;text-align:center;margin-bottom:5px}}
.subtitle{{color:#64b5f6;text-align:center;margin-bottom:30px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:15px;margin-bottom:30px}}
.kpi-card{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:12px;padding:20px;text-align:center}}
.kpi-card .value{{font-size:2rem;font-weight:600}}
.kpi-card .label{{color:#90a4ae;font-size:0.8rem;text-transform:uppercase}}
.kpi-card.positive .value{{color:#4caf50}}
.kpi-card.negative .value{{color:#f44336}}
.kpi-card.warning .value{{color:#ff9800}}
.kpi-card.neutral .value{{color:#64b5f6}}
.section{{margin-bottom:30px}}
.section h2{{font-size:1.2rem;color:#fff;border-bottom:2px solid #64b5f6;padding-bottom:10px;margin-bottom:15px}}
.summary-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:15px}}
.summary-card{{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:15px;border-left:4px solid #64b5f6}}
.summary-card.high-impact{{border-left-color:#f44336}}
.summary-card.medium-impact{{border-left-color:#ff9800}}
.summary-card.positive-impact{{border-left-color:#4caf50}}
.summary-card h3{{font-size:0.95rem;color:#fff;margin-bottom:8px}}
.summary-card p{{font-size:0.85rem;color:#b0bec5}}
.badge{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:0.7rem;font-weight:600;text-transform:uppercase;margin-top:10px}}
.badge.negative{{background:rgba(244,67,54,0.2);color:#f44336}}
.badge.positive{{background:rgba(76,175,80,0.2);color:#4caf50}}
.badge.mixed{{background:rgba(255,152,0,0.2);color:#ff9800}}
.badge.watch{{background:rgba(100,181,246,0.2);color:#64b5f6}}
.table-wrap{{background:rgba(255,255,255,0.03);border-radius:12px;overflow:hidden;border:1px solid rgba(255,255,255,0.08)}}
table{{width:100%;border-collapse:collapse;font-size:0.8rem}}
th{{background:rgba(100,181,246,0.15);color:#64b5f6;font-weight:600;text-transform:uppercase;font-size:0.7rem;padding:12px 8px;text-align:left}}
td{{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,0.05);color:#cfd8dc}}
tr:hover td{{background:rgba(255,255,255,0.02)}}
.tag{{display:inline-block;padding:3px 8px;border-radius:4px;font-size:0.7rem}}
.tag.cat{{background:rgba(33,150,243,0.2);color:#64b5f6}}
.tag.naics{{background:rgba(156,39,176,0.2);color:#ce93d8}}
.impact-positive{{color:#4caf50}}
.impact-negative{{color:#f44336}}
.impact-mixed{{color:#ff9800}}
.impact-unclear{{color:#90a4ae}}
.conf-bar{{width:50px;height:5px;background:rgba(255,255,255,0.1);border-radius:3px;display:inline-block;vertical-align:middle;margin-right:5px}}
.conf-fill{{height:100%;border-radius:3px}}
.conf-high .conf-fill{{background:#4caf50}}
.conf-med .conf-fill{{background:#ff9800}}
.conf-low .conf-fill{{background:#f44336}}
a{{color:#64b5f6;text-decoration:none}}
.notes-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px}}
.note{{background:rgba(255,255,255,0.03);border-radius:8px;padding:12px;border-left:3px solid #64b5f6}}
.note h4{{font-size:0.85rem;color:#64b5f6;margin-bottom:5px}}
.note p{{font-size:0.8rem;color:#90a4ae}}
.footer{{text-align:center;color:#546e7a;font-size:0.8rem;margin-top:30px;padding-top:20px;border-top:1px solid rgba(255,255,255,0.05)}}
</style>
</head>
<body>
<div class="container">
<h1>PAYMENTS INTELLIGENCE</h1>
<div class="subtitle">{date_str} | Daily Briefing | Azure OpenAI</div>
<div class="kpi-grid">{kpi_html}</div>
<div class="section"><h2>Executive Summary</h2><div class="summary-grid">{summary_html}</div></div>
<div class="section"><h2>Event Details</h2><div class="table-wrap"><table>
<thead><tr><th>Date</th><th>Category</th><th>NAICS</th><th>Product</th><th>Merchant</th><th>Network</th><th>Summary</th><th>Impact</th><th>Confidence</th><th>Source</th></tr></thead>
<tbody>{events_html}</tbody>
</table></div></div>
<div class="section"><h2>Analyst Notes</h2><div class="notes-grid">{notes_html}</div></div>
<div class="footer">Generated {date_str} | Powered by Azure OpenAI</div>
</div>
</body></html>'''


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  PAYMENTS INTELLIGENCE DASHBOARD")
    print("  Standalone Version (No pip required)")
    print("=" * 60)

    date_str = datetime.now().strftime("%B %d, %Y")

    # Load instructions
    print("\n[1/5] Loading instructions...")
    instructions = ""
    if PROMPT_DOC.exists():
        instructions = extract_text_from_docx(PROMPT_DOC)
        print(f"    > Loaded {len(instructions)} chars")

    # Fetch news
    news_items = fetch_all_news(NEWS_SOURCES)

    # Analyze
    analysis = analyze_news(news_items, instructions)

    # Generate HTML
    print("\n[4/5] Generating dashboard...")
    html = generate_html(analysis, date_str)

    # Save
    print("[5/5] Saving...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"    > Saved to: {OUTPUT_FILE}")
    print("\n" + "=" * 60)
    print("  DONE! Open payments_dashboard.html in browser")
    print("=" * 60)


if __name__ == "__main__":
    main()
