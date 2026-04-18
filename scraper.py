import asyncio
import csv
import json
import os
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import Page, async_playwright, Route

API_KEY = "sk-hc-v1-d6d61be66f1a4155b9cf45874602df5ad66355c8381d4fb0a4f025a45cd865ce"
AI_URL = "https://ai.hackclub.com/proxy/v1/chat/completions"

MAX_PAGES = 30
CONCURRENT_PAGES = 25
PAGE_TIMEOUT = 3000
MAX_STAFF_PAGE_ATTEMPTS = 3
MAX_QUEUE_LENGTH = 150
MAX_PAGE_ATTEMPTS = 2
STAFF_FETCH_ONLY = True
PAGE_WAIT_MS = 1500

STEM_KEYWORDS = [
    "math", "mathematics", "algebra", "geometry", "calculus",
    "trigonometry", "statistics", "pre-calculus", "precalculus",
    "science", "biology", "chemistry", "physics", "earth science",
    "environmental science", "anatomy", "physiology", "ecology",
    "geology", "astronomy", "botany", "zoology", "stem", "steam",
    "engineering", "computer science", "robotics", "technology",
    "coding", "programming", "information technology", "computer",
    "tech", "cyber", "cybersecurity", "data science", "maker", "digital", "aviation"
]

NEGATIVE_ROLES = [
    "registrar", "health", "nurse", "custodian", "janitor",
    "secretary", "clerk", "lunch", "cafeteria", "food", "bus",
    "transportation", "maintenance", "security", "resource officer",
    "police", "attendance", "bookkeeper", "accountant", "substitute",
    "coach", "athletic", "sports", "band", "choir", "music", "art",
    "drama", "theater", "pe", "physical education", "gym", "history",
    "english", "language", "spanish", "french", "german", "latin",
    "social studies", "reading", "special ed", "sped", "counselor",
    "psychologist", "social worker", "speech", "therapy", "assistant",
    "interventionist", "paraeducator", "paraprofessional", "aide"
]

BAD_FRAGMENTS = [
    "directory", "staff", "faculty", "department", "school",
    "district", "instruction", "contact", "search", "start over",
    "home", "email address", "office phone", "our district",
    "powerteacher", "powerschool", "schoolmessenger", "acceptable use",
    "agreement", "current topics", "score", "gradebook", "grading",
    "how to", "set up", "setup", "print", "reports", "help guides",
    "teacher help", "standards based", "recalculate", "showing",
    "constituents", "page", "results", "filter", "previous", "next",
    "program", "academy", "institute", "board", "board of", "trustees",
    "first name", "last name", "title", "location", "email", "phone",
    "teacher", "director", "counselor", "coordinator", "specialist",
    "administrator", "instructor", "coach", "educator", "paraeducator",
    "interventionist", "assistant", "supervisor", "custodian", "worker",
    "wish list", "amazon", "support", "education",
    "history", "english", "physical", "ap ", "course", "curriculum",
    "kindergarten", "grade", "principal", "superintendent", "committee"
]

SKIP_FREE_TEXT = {
    "skip to", "search", "select", "jump to", "find us",
    "phone:", "fax:", "showing", "of ", "page", "next",
    "previous", "copyright", "all rights", "powered by",
    "translate", "menu", "schools", "home", "keyword",
    "first name", "last name", "location", "all locations",
    "departments", "school district", "high school",
    "middle school", "elementary", "central school",
    "community school", "central office", "school board", "equity"
}

STAFF_URL_KEYWORDS = [
    "staff", "faculty", "directory", "staff-directory", "staff_directory",
    "staffdirectory", "people", "teacher", "teachers"
]

DEPARTMENT_URL_KEYWORDS = [
    "department", "departments"
]

NEGATIVE_URL_KEYWORDS = [
    "calendar", "lunch", "menu", "athletics", "student", "parent", "board",
    "news", "event", "events", "blog", "policy", "privacy", "employment",
    "alumni", "transportation", "resources", "resource", "technology-status",
    "status", "tech-status", "tech status", "cms", "feeds", "rss",
    "sitemap", "accessibility", "nondiscrimination", "complaint",
    "public-records", "records", "volunteer", "foundation"
]
SAFE_DIRECTORY_HINTS = ["directory", "staff", "faculty"]

SKIP_FILE_EXTENSIONS = [
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".gif", ".zip"
]

EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}")
US_STATE_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|"
    r"MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|"
    r"SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b"
)
ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")

def normalize_email(text: str) -> str:
    if not text:
        return ""
    m = re.search(EMAIL_REGEX, text)
    if m:
        return m.group().lower()
    t = text.lower()
    t = t.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
    t = t.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
    t = t.replace("{at}", "@").replace("{dot}", ".")
    t = re.sub(r"\s+", "", t)
    m = re.search(EMAIL_REGEX, t)
    if m:
        return m.group().lower()
    return ""

@dataclass
class StaffMember:
    name: str
    email: str
    role: str
    department: str
    source_url: str
    extraction_method: str

def print_logo():
    print("\033[96m")
    print("  SSS   SSS   SSS   ")
    print(" S     S     S      ")
    print("  SSS   SSS   SSS   ")
    print("     S     S     S  ")
    print("  SSS   SSS   SSS   ")
    print("\033[90m School Staff Scraper \033[0m v2.0")
    print()

def extract_domain(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.replace("www.", "")

def is_same_site(url: str, start_url: str) -> bool:
    try:
        a = urllib.parse.urlparse(url)
        b = urllib.parse.urlparse(start_url)
        if not a.netloc or not b.netloc:
            return True
        return a.netloc == b.netloc or a.netloc.endswith("." + b.netloc) or b.netloc.endswith("." + a.netloc)
    except Exception:
        return False

def looks_like_directory_page(text: str) -> bool:
    if not text:
        return False
    tl = text.lower()
    return any(k in tl for k in ["staff directory", "faculty directory", "directory", "staff", "faculty", "teacher"])

def should_skip_url(url: str) -> bool:
    if not url:
        return True
    ul = url.lower()
    if any(ext in ul for ext in SKIP_FILE_EXTENSIONS):
        return True
        if "resource" in ul and "directory" not in ul and "staff-directory" not in ul and "staff_directory" not in ul:
            return True
        if "?items_per_page" in ul and not any(k in ul for k in SAFE_DIRECTORY_HINTS):
            return True
    if any(k in ul for k in NEGATIVE_URL_KEYWORDS) and not any(k in ul for k in SAFE_DIRECTORY_HINTS) and "directory" not in ul and "staff" not in ul:
        return True
    return False

def is_stem_role(role_or_dept: str) -> bool:
    if not role_or_dept:
        return False
    text = role_or_dept.lower()
    
    for neg in NEGATIVE_ROLES:
        if re.search(r'\b' + re.escape(neg) + r'\b', text):
            return False
            
    for pos in STEM_KEYWORDS:
        if re.search(r'\b' + re.escape(pos) + r'\b', text):
            return True
    return False

def looks_like_name(text: str) -> bool:
    words = [w for w in text.strip().split() if w]
    if not (2 <= len(words) <= 5):
        return False
    if not (5 <= len(text) <= 60):
        return False
    if any(ch.isdigit() for ch in text):
        return False
    if any(frag in text.lower() for frag in BAD_FRAGMENTS):
        return False
    if not re.match(r"^[A-Za-z][A-Za-z.\-\',\s]+$", text):
        return False
    clean = [t.strip(".,") for t in words]
    alpha = [t for t in clean if re.match(r"^[A-Za-z][A-Za-z'\-]*$", t)]
    return len(alpha) >= 2 and sum(1 for t in alpha if t[0].isupper()) >= 2

def strip_page_noise(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "noscript", "iframe",
                               "svg", "path", "meta", "link", "header", "footer"]):
        tag.decompose()

    main = (
        soup.find("main") or
        soup.find("article") or
        soup.find("div", {"role": "main"}) or
        soup.find("div", {"id": re.compile(r"content|main", re.I)}) or
        soup.find("div", {"class": re.compile(
            r"content|main|staff|faculty|directory|listing", re.I)})
    )
    target = main or soup.find("body") or soup
    text = target.get_text(separator="\n", strip=True)

    mailto_lines = []
    for a in (main or soup).find_all("a", href=True):
        href = a["href"]
        if "mailto:" in href:
            email = href.replace("mailto:", "").split("?")[0].strip()
            label = a.get_text(strip=True)
            if email:
                mailto_lines.append(f"{label}: {email}" if label else email)
    if mailto_lines:
        text += "\n\nEmails in links:\n" + "\n".join(mailto_lines)

    return re.sub(r"\n{3,}", "\n\n", text)

def parse_labeled_rows(text: str, source_url: str) -> List[StaffMember]:
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    people = []
    i = 0

    while i < len(lines):
        line = lines[i]
        if not looks_like_name(line):
            i += 1
            continue

        window = " ".join(lines[i + 1: i + 9]).lower()
        if not any(k in window for k in ("titles:", "email:", "locations:", "phone:")):
            i += 1
            continue

        name = line
        role = ""
        department = ""
        email = ""
        j, consumed = i + 1, 0

        while j < len(lines) and consumed < 8:
            cur = lines[j].strip()
            cl  = cur.lower()

            if cl == "titles:":
                if j + 1 < len(lines):
                    role = lines[j+1].strip()
                    j += 1
            elif cl.startswith("titles:"):
                role = cur.split(":", 1)[1].strip()
            elif cl == "locations:" or cl == "location:":
                if j + 1 < len(lines):
                    department = lines[j+1].strip()
                    j += 1
            elif cl.startswith(("locations:", "location:")):
                department = cur.split(":", 1)[1].strip()
            elif cl == "email:":
                if j + 1 < len(lines):
                    email = normalize_email(lines[j+1])
                    j += 1
            elif cl.startswith("email:"):
                email = normalize_email(cur)
            elif cl.startswith(("phone:", "office phone:")):
                pass
            else:
                em = normalize_email(cur)
                if em and not email:
                    email = em
                    j += 1
                    break
                if consumed >= 1 and looks_like_name(cur):
                    break

            j += 1
            consumed += 1

        if email or role:
            if is_stem_role(role) or is_stem_role(department):
                people.append(StaffMember(
                    name=name,
                    email=email,
                    role=role,
                    department=department,
                    source_url=source_url,
                    extraction_method="labeled_rows"
                ))
            i = j
        else:
            i += 1

    return people

def parse_directory_table(html: str, source_url: str) -> List[StaffMember]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    for row in soup.select('.divPseudoTR.searchParent'):
        name = ""
        role = ""
        email = ""
        dept = ""

        cells = row.select('.divPseudoTD')
        for cell in cells:
            label_el = cell.select_one('.spanDataLabel')
            label = label_el.get_text(strip=True).lower() if label_el else ""
            value_el = cell.select_one('.spanDataValue')
            value_text = value_el.get_text(" ", strip=True) if value_el else cell.get_text(" ", strip=True)

            if 'name' in label:
                name = value_text
            elif 'position' in label or 'title' in label or 'role' in label:
                role = value_text
            elif 'email' in label:
                link = cell.select_one('a[href^="mailto:"]')
                if link:
                    email = normalize_email(link.get('href', '').replace('mailto:', ''))
                if not email:
                    email = normalize_email(value_text)
            elif 'department' in label or 'school' in label:
                dept = value_text

        if name and email and looks_like_name(name) and is_stem_role(role + " " + dept):
            records.append(StaffMember(
                name=name,
                email=email,
                role=role,
                department=dept,
                source_url=source_url,
                extraction_method="directory_table"
            ))

    return records

def parse_staff_profile(html: str, source_url: str) -> List[StaffMember]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    emails = re.findall(EMAIL_REGEX, html)
    if not emails:
        return []
    name = ""
    title = soup.title.get_text(strip=True) if soup.title else ""
    m = re.match(r"^([^|]+)\|", title)
    if m:
        name = m.group(1).strip()
    if not name:
        for h in soup.find_all(['h1', 'h2', 'h3']):
            if looks_like_name(h.get_text(strip=True)):
                name = h.get_text(strip=True)
                break
    role = ""
    stem_hit = False
    for line in text.split("\n"):
        if is_stem_role(line):
            role = line.strip()
            stem_hit = True
            break
    if not role:
        for line in text.split("\n"):
            if "teacher" in line.lower() or "engineering" in line.lower() or "math" in line.lower() or "science" in line.lower() or "technology" in line.lower():
                role = line.strip()
                break
    if not stem_hit:
        stem_hit = is_stem_role(text)
    if name and emails:
        email = normalize_email(emails[0])
        if email and looks_like_name(name) and (is_stem_role(role) or stem_hit):
            return [StaffMember(
                name=name,
                email=email,
                role=role,
                department="",
                source_url=source_url,
                extraction_method="staff_profile"
            )]
    return []

def parse_free_text(text: str, source_url: str) -> List[StaffMember]:
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    records = []
    i = 0

    while i < len(lines):
        line = lines[i]
        if len(line) < 3 or len(line) > 80:
            i += 1
            continue
        if any(kw in line.lower() for kw in SKIP_FREE_TEXT):
            i += 1
            continue

        words = line.split()
        is_name = (
            2 <= len(words) <= 5 and
            re.match(r"^[A-Za-z\s.\-\',]+$", line) and
            not re.search(EMAIL_REGEX, line) and
            not PHONE_RE.search(line) and
            not re.search(r"\d", line) and
            len(line) >= 5 and
            not any(p in line.lower() for p in BAD_FRAGMENTS) and
            not re.search(r"\bschool\b", line.lower()) and
            looks_like_name(line)
        )

        if not is_name:
            i += 1
            continue

        name = line.strip()
        if name == name.upper():
            name = name.title()

        email = ""
        role = ""
        department = ""
        has_contact_form = False
        
        i += 1
        consumed = 0

        while i < len(lines) and consumed < 6:
            nxt = lines[i].strip()
            if not nxt or len(nxt) < 2:
                i += 1
                consumed += 1
                continue
                
            if consumed >= 1 and looks_like_name(nxt):
                break

            em = normalize_email(nxt)
            ph = PHONE_RE.search(nxt)

            if em and not email:
                email = em
                i += 1
                consumed += 1
            elif nxt.lower().startswith(("school:", "phone:")) and ph:
                i += 1
                consumed += 1
            elif ph and not em:
                i += 1
                consumed += 1
            elif not role and len(nxt) > 3 and not re.match(r"^\d+$", nxt):
                role = nxt
                if any(x in nxt.lower() for x in ["message", "contact teacher", "contact"]):
                    has_contact_form = True
                i += 1
                consumed += 1
            elif not department and 3 < len(nxt) < 80:
                department = nxt
                i += 1
                consumed += 1
            else:
                break

        if is_stem_role(role) or is_stem_role(department):
            records.append(StaffMember(
                name=name,
                email=email,
                role=role,
                department=department,
                source_url=source_url,
                extraction_method="free_text" if email else "free_text_no_email"
            ))

    return records

async def call_ai_fallback(text: str, url: str) -> List[StaffMember]:
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    prompt = (
        "Extract structured staff data. Return JSON array matching: "
        "[{\"name\": \"\", \"email\": \"\", \"role\": \"\", \"department\": \"\"}]"
    )
    
    payload = {
        "model": "qwen/qwen3-32b",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": text[:8000]}
        ],
        "temperature": 0.0,
        "extra_body": {"chat_template_kwargs": {"thinking": False}}
    }
    
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(AI_URL, headers=headers, json=payload, timeout=25) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                
                content = re.sub(r"```json\s*", "", content)
                content = re.sub(r"```\s*", "", content)
                
                start = content.find('[')
                end = content.rfind(']') + 1
                if start != -1 and end != 0:
                    arr = json.loads(content[start:end])
                    results = []
                    for item in arr:
                        if isinstance(item, dict):
                            nm = str(item.get("name") or "").strip()
                            em = str(item.get("email") or "").strip()
                            ro = str(item.get("role") or "").strip()
                            de = str(item.get("department") or "").strip()
                            
            if nm and looks_like_name(nm) and is_stem_role(ro + " " + de):
                if em:
                    results.append(StaffMember(
                        name=nm,
                        email=em,
                        role=ro,
                        department=de,
                        source_url=url,
                        extraction_method="ai_fallback"
                    ))
                    return results
    except Exception:
        pass
    return []

def extract_from_json(json_data: dict, source_url: str) -> List[StaffMember]:
    results = []
    str_data = json.dumps(json_data).lower()
    
    has_staff_keys = any(k in str_data for k in ['title', 'role', 'department', 'firstname', 'lastname', 'position', 'jobtitle'])
    if "@" not in str_data and not has_staff_keys:
        return results
        
    def process_dict(d: dict):
        email = d.get('email') or d.get('e-mail') or d.get('Email') or ''
        if not email:
            email = d.get('emailAddress') or d.get('email_address') or d.get('mail') or d.get('mailAddress') or d.get('mail_address') or ''
        if not email and isinstance(d.get('contact'), dict):
            email = d['contact'].get('email', '') or d['contact'].get('emailAddress', '') or d['contact'].get('email_address', '') or d['contact'].get('mail', '') or d['contact'].get('mailAddress', '') or d['contact'].get('mail_address', '')
        email = normalize_email(str(email))
            
        role = str(d.get('title') or d.get('role') or d.get('position') or d.get('department') or d.get('jobTitle') or d.get('job_title') or d.get('job') or d.get('jobtitle') or '')
        name = str(d.get('name') or d.get('full_name') or d.get('fullName') or 
                  (d.get('first_name', '') + ' ' + d.get('last_name', '')).strip() or 
                  (d.get('firstName', '') + ' ' + d.get('lastName', '')).strip() or 
                  (d.get('first', '') + ' ' + d.get('last', '')).strip())
                  
        has_email = isinstance(email, str) and '@' in email
        has_valid_role_name = looks_like_name(name) and is_stem_role(role)
        
        if has_email and has_valid_role_name:
            results.append(StaffMember(
                name=name.strip()[:100],
                email=email.strip().lower()[:100],
                role=role.strip()[:100],
                department=str(d.get('department', ''))[:100],
                source_url=source_url,
                extraction_method="api_json_intercept"
            ))
                
        for v in d.values():
            if isinstance(v, dict):
                process_dict(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        process_dict(item)

    if isinstance(json_data, list):
        for item in json_data:
            if isinstance(item, dict):
                process_dict(item)
    elif isinstance(json_data, dict):
        process_dict(json_data)
        
    return results

def get_school_info(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
        
    title = soup.find('title')
    name = title.get_text(strip=True) if title else ""
    if name:
        parts = re.split(r"[|\-–—]", name)
        for p in parts:
            if re.search(r"school|academy|district|institute", p, re.I):
                name = p.strip()
                break
        else:
            name = parts[0].strip()
            
    blocks = []
    for sel in ["header", "footer", '[class*="footer"]', '[class*="address"]']:
        for el in soup.select(sel)[:2]:
            blocks.append(el.get_text(separator=" ", strip=True))
            
    full_text = " ".join(blocks)
    if not full_text:
        full_text = soup.get_text(separator=" ", strip=True)
        
    full_text = re.sub(r"\s+", " ", full_text)
    
    street = ""
    city = ""
    state = ""
    zip_code = ""
    
    street_re = re.compile(r"\b(\d{2,5}\s+[A-Za-z][A-Za-z0-9\s,\.]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Way|Place|Pl|Circle|Cir|Highway|Hwy|Route|Rt)\b)", re.IGNORECASE)
    m = street_re.search(full_text)
    if m:
        street = m.group(1).strip()
        
    sm = US_STATE_RE.search(full_text)
    if sm:
        state = sm.group(1)
        zm = ZIP_RE.search(full_text[sm.start():])
        if zm:
            zip_code = zm.group(1)
            
        prefix = full_text[:sm.start()].strip()
        city_m = re.search(r"([A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+)?)\s*,?\s*$", prefix)
        if city_m:
            city = city_m.group(1).strip()
            
    address_parts = []
    if street:
        address_parts.append(street)
        
    city_state_zip = ""
    if city:
        city_state_zip += city + ", "
    if state:
        city_state_zip += state + " "
    if zip_code:
        city_state_zip += zip_code
        
    if city_state_zip.strip():
        address_parts.append(city_state_zip.strip())
        
    return name, "\n".join(address_parts).strip()

def score_link(url: str, text: str) -> int:
    score = 0
    ul = url.lower()
    tl = text.lower()

    staffish = any(kw in ul or kw in tl for kw in STAFF_URL_KEYWORDS)
    departmentish = any(kw in ul or kw in tl for kw in DEPARTMENT_URL_KEYWORDS)
    stemish = any(kw in ul or kw in tl for kw in STEM_KEYWORDS)
    directoryish = "directory" in ul or "directory" in tl
    negativeish = any(kw in ul or kw in tl for kw in NEGATIVE_URL_KEYWORDS)
    safe_dir = any(kw in ul or kw in tl for kw in SAFE_DIRECTORY_HINTS)

    if staffish:
        score += 12
    if departmentish and stemish and not staffish:
        score += 6
    if staffish and stemish:
        score += 8
    if "index" in ul and "directory" in ul:
        score += 5
    if not staffish and not departmentish and stemish:
        score -= 5
    if negativeish and not directoryish and not safe_dir:
        score -= 40
    if not staffish and negativeish and not safe_dir:
        score -= 30
    if "mailto:" in ul:
        score -= 50
    if any(ext in ul for ext in SKIP_FILE_EXTENSIONS):
        score -= 100
    if any(kw in ul for kw in ["?page=", "&page=", "?const_page=", "&const_page=", "?start=", "&start="]):
        score -= 50
        
    return score

async def fetch_sitemap_urls(session: aiohttp.ClientSession, base_url: str) -> List[str]:
    urls = []
    for path in ['/sitemap.xml', '/sitemap_index.xml']:
        try:
            async with session.get(urllib.parse.urljoin(base_url, path), timeout=3) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'xml')
                    for loc in soup.find_all('loc'):
                        u = loc.text
                        if score_link(u, "") > 0:
                            urls.append(u)
        except Exception:
            pass
    return urls

async def fetch_thrillshare_directories(start_url: str) -> List[dict]:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            await page.goto(start_url, timeout=15000)
            await page.wait_for_timeout(2000)
            html = await page.content()
            await browser.close()
    except Exception:
        return []

    if not html:
        return []

    if "403" in html[:500] or "403 Forbidden" in html[:500]:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                staff_url = start_url.rstrip('/') + '/staff-directory'
                await page.goto(staff_url, timeout=15000)
                await page.wait_for_timeout(2000)
                html = await page.content()
                await browser.close()
        except Exception:
            return []
    
    if not any(k in html.lower() for k in ["apptegy", "thrillshare", "cmsv2"]):
        return []

    org_id = None
    m = re.search(r"/uploads/(\d+)/", html)
    if m:
        org_id = m.group(1)
    if not org_id:
        m = re.search(r"/content/(\d+)/", html)
        if m:
            org_id = m.group(1)

    if not org_id:
        return []

    base = f"https://thrillshare-cmsv2.services.thrillshare.com/api/v4/o/{org_id}/cms/directories"
    results = []
    page = 1

    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), timeout=aiohttp.ClientTimeout(total=15)) as s:
            while page <= 10:
                url = f"{base}?locale=en&page_no={page}"
                async with s.get(url) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
                    entries = data.get("directories", [])
                    if not entries:
                        break
                    results.extend(entries)
                    links = data.get("meta", {}).get("links", {})
                    next_url = links.get("next")
                    if not next_url:
                        break
                    page += 1
    except Exception:
        return results

    return results

def append_to_csv(data: List[dict], filename="school_staff.csv"):
    headers = [
        "school_name", "school_website", "school_mailing_address", "staff_name",
        "staff_email", "staff_role", "staff_department",
        "source_url", "extraction_method"
    ]
    
    existing_emails = set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_emails.add(row.get('staff_email', '').lower())
    except FileNotFoundError:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        for row in data:
            email = row.get('staff_email', '').lower()
            if email and email not in existing_emails:
                writer.writerow(row)
                existing_emails.add(email)

class Crawler:
    def __init__(self, start_url: str):
        self.start_url = start_url
        self.domain = extract_domain(start_url)
        self.visited: Set[str] = set()
        self.queue_urls: Set[str] = set()
        self.queue: List[tuple[int, str]] = [(100, start_url)]
        self.queue_urls.add(start_url)
        self.found_staff: List[StaffMember] = []
        self.seen_emails: Set[str] = set()
        self.school_name = ""
        self.school_address = ""
        self.start_time = time.time()
        self.directory_mode = False
        self.staff_pages_checked = 0
        self.url_attempts: Dict[str, int] = {}
        self.stop_crawl = False
        
    def add_url(self, url: str, score: int):
        url = url.split('#')[0].strip()
        if not url:
            return
        if should_skip_url(url):
            return
        if url in self.visited or url in self.queue_urls:
            return
        if self.directory_mode and score < 10:
            return
        if len(self.visited) + len(self.queue) >= MAX_PAGES * 2:
            return
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc == "" or is_same_site(url, self.start_url):
            if len(self.queue) >= MAX_QUEUE_LENGTH:
                return
            self.queue.append((score, url))
            self.queue_urls.add(url)
            if "/staff-directory" in url and "items_per_page" not in url and "?" not in url:
                if len(self.queue) < MAX_QUEUE_LENGTH:
                    a_url = url + "?items_per_page=50&field_last_name_from=A&field_last_name_to=Z"
                    if a_url not in self.queue_urls:
                        self.queue.append((score + 2, a_url))
                        self.queue_urls.add(a_url)
                if len(self.queue) < MAX_QUEUE_LENGTH:
                    s_url = url + "?items_per_page=50&field_last_name_from=S&field_last_name_to=Z"
                    if s_url not in self.queue_urls:
                        self.queue.append((score + 1, s_url))
                        self.queue_urls.add(s_url)
            self.queue.sort(reverse=True, key=lambda x: x[0])
                
    async def abort_unnecessary_requests(self, route: Route):
        url = route.request.url.lower()
        if any(ext in url for ext in SKIP_FILE_EXTENSIONS):
            await route.abort()
            return
        if route.request.resource_type in ["image", "stylesheet", "media", "font"]:
            await route.abort()
        else:
            await route.continue_()
            
    async def run(self):
        async with aiohttp.ClientSession() as session:
            sitemap_urls = await fetch_sitemap_urls(session, self.start_url)
            for u in sitemap_urls:
                self.add_url(u, score_link(u, ""))

            try:
                async with session.get(self.start_url, timeout=6) as r:
                    home_html = await r.text()
                    for m in re.findall(r'href="([^"]+)"', home_html):
                        if "faculty-staff" in m:
                            self.add_url(urllib.parse.urljoin(self.start_url, m), 30)
            except Exception:
                pass

        thrillshare_entries = await fetch_thrillshare_directories(self.start_url)
        if thrillshare_entries:
            for entry in thrillshare_entries:
                name = entry.get("full_name") or ""
                email = entry.get("email") or ""
                role = entry.get("title") or ""
                dept = entry.get("department") or ""
                if name and email and looks_like_name(name) and is_stem_role(role + " " + dept):
                    em = normalize_email(email)
                    if em and em not in self.seen_emails:
                        self.seen_emails.add(em)
                        self.found_staff.append(StaffMember(
                            name=name,
                            email=em,
                            role=role,
                            department=dept,
                            source_url=self.start_url,
                            extraction_method="thrillshare_api"
                        ))
            if self.found_staff:
                self.directory_mode = True
                if STAFF_FETCH_ONLY:
                    self.stop_crawl = True
                
        seed_paths = [
            "/staff", "/directory", "/faculty", "/departments", "/our-staff",
            "/staff_directory", "/staff-directory", "/directory/index",
            "/about-us/staff-directory", "/our-school/staff-directory",
            "/about/staff", "/staff-and-departments", "/faculty-staff"
        ]
        if not self.directory_mode:
            for path in seed_paths:
                self.add_url(urllib.parse.urljoin(self.start_url, path), 20)

        if not self.directory_mode and score_link(self.start_url, "") < 10:
            self.add_url(urllib.parse.urljoin(self.start_url, "/directory/index"), 25)
            self.add_url(urllib.parse.urljoin(self.start_url, "/staff-directory"), 25)
            self.add_url(urllib.parse.urljoin(self.start_url, "/staff_directory"), 25)
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            async def process_url():
                while self.queue and len(self.visited) < MAX_PAGES and not self.stop_crawl:
                    if time.time() - self.start_time > 300:
                        print("\033[93mCrawl time limit reached.\033[0m")
                        break
                        
                    _, url = self.queue.pop(0)
                    if url in self.visited:
                        continue
                    self.queue_urls.discard(url)

                    if self.directory_mode and score_link(url, "") < 10:
                        continue

                    if not self.directory_mode and score_link(url, "") >= 10:
                        if self.staff_pages_checked >= MAX_STAFF_PAGE_ATTEMPTS:
                            continue
                        self.staff_pages_checked += 1
                        
                    parsed = urllib.parse.urlparse(url)
                    if not parsed.scheme or not parsed.netloc:
                        continue

                    attempts = self.url_attempts.get(url, 0)
                    if attempts >= MAX_PAGE_ATTEMPTS:
                        continue
                    self.url_attempts[url] = attempts + 1
                        
                    self.visited.add(url)
                    self.queue_urls.add(url)
                    sys.stdout.write(f"\r\033[K\033[94mSearching:\033[0m {url[:100]}")
                    sys.stdout.flush()
                    
                    page = await context.new_page()
                    await page.route("**/*", self.abort_unnecessary_requests)
                    
                    try:
                        try:
                            await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
                        except Exception:
                            pass
                        intercepted_json = []
                        async def handle_response(response):
                            try:
                                if "json" in response.headers.get("content-type", "") or "api" in response.url.lower() or "search" in response.url.lower():
                                    data = await response.json()
                                    res = extract_from_json(data, response.url)
                                    for r in res:
                                        if r.email:
                                            if r.email not in self.seen_emails:
                                                self.seen_emails.add(r.email)
                                                intercepted_json.append(r)
                            except Exception:
                                pass
                                    
                        page.on("response", handle_response)
                        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                        await page.wait_for_timeout(PAGE_WAIT_MS)

                        page_is_404 = False
                        try:
                            text_preview = await page.evaluate("document.body.innerText")
                            low = text_preview.lower()
                            if "404" in low or "page not found" in low:
                                page_is_404 = True
                            if "just a moment" in low or "verify you are not a bot" in low:
                                await page.wait_for_timeout(4000)
                        except Exception:
                            pass
                        if page_is_404:
                            continue
                        
                        pagination_attempts = 0
                        while pagination_attempts < 40:
                            ai_results = []
                            try:
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await page.wait_for_timeout(500)
                            except Exception:
                                pass
                                
                            html = await page.content()
                            
                            for frame in page.frames[1:]:
                                try:
                                    frame_html = await frame.content()
                                    if frame_html:
                                        html += "\n<!-- IFRAME CONTENT -->\n" + frame_html
                                except Exception:
                                    pass
                                    
                            if not self.school_name:
                                self.school_name, self.school_address = get_school_info(html)
                                
                            soup = BeautifulSoup(html, 'lxml')
                            for a in soup.find_all('a', href=True):
                                href = a.get('href')
                                if not href:
                                    continue
                                full_url = urllib.parse.urljoin(url, href)
                                if should_skip_url(full_url):
                                    continue
                                if re.search(r"[\?&](page|page_no|const_page|start|offset|p)=", full_url, re.I):
                                    continue
                                text = a.get_text(strip=True)
                                score = score_link(full_url, text)
                                if score > 0:
                                    if "resource" in full_url.lower() or "resources" in full_url.lower():
                                        continue
                                    self.add_url(full_url, score)

                            if not self.directory_mode:
                                for a in soup.find_all('a', href=True):
                                    href = a.get('href')
                                    if not href:
                                        continue
                                    text = a.get_text(strip=True)
                                    full_url = urllib.parse.urljoin(url, href)
                                    if should_skip_url(full_url):
                                        continue
                                    if re.search(r"[\?&](page|page_no|const_page|start|offset|p)=", full_url, re.I):
                                        continue
                                    if "staff" in text.lower() or "directory" in text.lower():
                                        if "resource" in full_url.lower() or "resources" in full_url.lower():
                                            continue
                                        self.add_url(full_url, 20)
                                    
                            clean_text = strip_page_noise(html)
                            if pagination_attempts == 0:
                                if "directory" in clean_text.lower() and "staff" in clean_text.lower():
                                    self.directory_mode = True
                            
                            new_det = []
                            table_records = parse_directory_table(html, url)
                            profile_records = parse_staff_profile(html, url)
                            labeled = parse_labeled_rows(clean_text, url)
                            free = parse_free_text(clean_text, url)

                            for r in table_records + profile_records + labeled + free:
                                if r.email not in self.seen_emails:
                                    if r.email:
                                        self.seen_emails.add(r.email)
                                        new_det.append(r)
                                    
                            self.found_staff.extend(new_det)
                            
                            if not labeled and not free and not intercepted_json and score_link(url, "") >= 10:
                                text_lower = clean_text.lower()
                                has_stem = any(kw in text_lower for kw in STEM_KEYWORDS)
                                has_staff = any(kw in text_lower for kw in ["staff", "faculty", "directory", "teacher"])
                                
                                if has_stem and has_staff:
                                    chunks = [clean_text[i:i+8000] for i in range(0, len(clean_text), 8000)]
                                    for chunk in chunks:
                                        ai_results = await call_ai_fallback(chunk, url)
                                        for r in ai_results:
                                            if r.email and r.email not in self.seen_emails:
                                                self.seen_emails.add(r.email)
                                                self.found_staff.append(r)
                                                
                            staff_this_page = len(new_det) + len(intercepted_json) + len(ai_results)
                            if pagination_attempts == 0 and staff_this_page == 0 and score_link(url, "") <= 0:
                                if not looks_like_directory_page(clean_text):
                                    break

                            if staff_this_page > 0:
                                self.directory_mode = True
                                 
                            self.found_staff.extend(intercepted_json)
                            intercepted_json.clear()
                            
                            next_selectors = [
                                "a:has-text('Next')", "button:has-text('Next')", "a:has-text('next page')",
                                "a.next", "[aria-label='Next']", "button:has-text('Load More')", "li.next a"
                            ]
                            clicked = False
                            for sel in next_selectors:
                                try:
                                    btn = page.locator(sel).first
                                    if await btn.is_visible() and not await btn.is_disabled():
                                        old_text = await page.evaluate("document.body.innerText")
                                        await btn.click(timeout=1000)
                                        for _ in range(20):
                                            await page.wait_for_timeout(100)
                                            if old_text != await page.evaluate("document.body.innerText"):
                                                break
                                        clicked = True
                                        break
                                except Exception:
                                    pass
                            
                            if not clicked:
                                try:
                                    next_num = await page.evaluate(r'''() => {
                                        let els = document.querySelectorAll('.selected, .active, .current, [aria-current="page"]');
                                        for (let el of els) {
                                            let t = el.innerText.trim();
                                            if (t.match(/^\d+$/)) {
                                                return parseInt(t) + 1;
                                            }
                                        }
                                        return null;
                                    }''')
                                    if next_num:
                                        btn = page.locator(f"a:text-is('{next_num}'), button:text-is('{next_num}'), li:text-is('{next_num}')").first
                                        if await btn.is_visible() and not await btn.is_disabled():
                                            old_text = await page.evaluate("document.body.innerText")
                                            await btn.click(timeout=1000)
                                            for _ in range(20):
                                                await page.wait_for_timeout(100)
                                                if old_text != await page.evaluate("document.body.innerText"):
                                                    break
                                            clicked = True
                                except Exception:
                                    pass
                            
                            if not clicked:
                                break
                            pagination_attempts += 1
                            
                    except Exception as e:
                        pass
                    finally:
                        await page.close()
                        
            tasks = [process_url() for _ in range(CONCURRENT_PAGES)]
            await asyncio.gather(*tasks)
            await browser.close()
            
        sys.stdout.write("\n\033[K")
        sys.stdout.flush()
        
        csv_data = []
        for staff in self.found_staff:
            csv_data.append({
                "school_name": self.school_name,
                "school_website": self.start_url,
                "school_mailing_address": self.school_address,
                "staff_name": staff.name,
                "staff_email": staff.email,
                "staff_role": staff.role,
                "staff_department": staff.department,
                "source_url": staff.source_url,
                "extraction_method": staff.extraction_method
            })
            print(f"\033[92mFound: {staff.name} - {staff.email} - {staff.role}\033[0m")
            
        if not csv_data:
            print("\033[93mNo science/math/STEM staff found.\033[0m")
        else:
            methods = set(staff.extraction_method for staff in self.found_staff)
            print(f"\033[90mExtracted {len(csv_data)} staff via: {', '.join(methods)}\033[0m")
            append_to_csv(csv_data)

def start_loop():
    url_history = []
    if HAS_READLINE:
        readline.parse_and_bind("tab: complete")
        try:
            for h in url_history:
                readline.add_history(h)
        except:
            pass
    
    def get_input(prompt):
        if HAS_READLINE:
            try:
                line = readline.get_line_buffer()
                if line:
                    return line
            except:
                pass
            return input(prompt)
        return input(prompt)
    
    print_logo()
    print("\033[90m[Ctrl+C to quit]\033[0m")
    while True:
        try:
            url = get_input("Enter school website URL (or 'quit' to exit): ").strip()
            if url.lower() == 'quit':
                break
            if not url:
                continue
            if not url.startswith("http"):
                url = "https://" + url
            parsed = urllib.parse.urlparse(url)
            if not parsed.netloc:
                print("\033[91mInvalid URL format. Please try again.\033[0m")
                continue
            if HAS_READLINE:
                try:
                    readline.add_history(url)
                except:
                    pass
            print()
            crawler = Crawler(url)
            asyncio.run(crawler.run())
            print()
        except KeyboardInterrupt:
            print("\n\033[90mExiting...\033[0m")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    print_logo()
    
    while True:
        try:
            url = input("Enter school website URL: ").strip()
            if url.lower() in ('quit', 'exit', 'q'):
                break
            if not url:
                continue
            if not url.startswith("http"):
                url = "https://" + url
            parser = urllib.parse.urlparse(url)
            if not parser.netloc:
                print("\033[91mInvalid URL format. Please try again.\033[0m")
                continue
                
            crawler = Crawler(url)
            asyncio.run(crawler.run())
            print()
        except KeyboardInterrupt:
            print("\n\033[90mExiting...\033[0m")
            break
        except Exception as e:
            print(f"Error: {e}")
