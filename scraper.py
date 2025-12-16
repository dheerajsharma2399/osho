import json
import re
import time
from pathlib import Path
from typing import List, Dict, Optional
import argparse
from concurrent.futures import ProcessPoolExecutor
import os
import difflib

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

WAIT_DOCUMENT_READY = 0
WAIT_COOKIE_POPUP = 1
WAIT_DYNAMIC_CONTENT = 0
WAIT_CONTENT_ELEMENT = 0
WAIT_RETRY_DELAY = 1
PAGE_LOAD_TIMEOUT = 15
SCRIPT_TIMEOUT = 10
MAX_RETRIES = 2

ENABLE_IMAGES = False
ENABLE_CSS = False
ENABLE_JAVASCRIPT = True

PROGRESS_FILE = "progress.json"
WORKER_DRIVER = None  # Global driver for connection pooling


# ═══════════════════════════════════════════════════════════════════════
# PROGRESS TRACKING (NEW ROBUST SYSTEM)
# ═══════════════════════════════════════════════════════════════════════

def load_progress():
    """Load the progress file (handles both old list format and new dict format)"""
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # Handle new robust format
                if isinstance(data, dict) and "completed_discourses" in data:
                    return data
                # Handle legacy format (list of strings)
                elif isinstance(data, dict) and "completed_discourses" in data: 
                    # Wrapper for simple dict
                    return data
                # Handle raw list (very old version)
                elif isinstance(data, list):
                    return {"completed_discourses": data, "chapter_logs": []}
            except json.JSONDecodeError:
                pass
    return {"completed_discourses": [], "chapter_logs": []}


def save_progress(progress_data):
    """Save the comprehensive progress data to JSON"""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════════
# DRIVER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════

def make_driver(chromedriver_path):
    pid = os.getpid()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-web-security")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-sync")
    options.add_argument("--mute-audio")
    options.add_argument("--window-size=800,600")
    
    prefs = {
        "profile.managed_default_content_settings.images": 2 if not ENABLE_IMAGES else 1,
        "profile.managed_default_content_settings.stylesheets": 2 if not ENABLE_CSS else 1,
        "profile.managed_default_content_settings.javascript": 1 if ENABLE_JAVASCRIPT else 2,
    }
    options.add_experimental_option("prefs", prefs)
    options.page_load_strategy = 'none'
    
    try:
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.set_script_timeout(SCRIPT_TIMEOUT)
        return driver
    except Exception as e:
        print(f"[PID {pid}] ✗ Failed to create Chrome driver: {e}")
        raise

def get_worker_driver(chromedriver_path):
    global WORKER_DRIVER
    if WORKER_DRIVER is None:
        WORKER_DRIVER = make_driver(chromedriver_path)
    return WORKER_DRIVER

def close_worker_driver():
    global WORKER_DRIVER
    if WORKER_DRIVER:
        try:
            WORKER_DRIVER.quit()
        except Exception:
            pass
        finally:
            WORKER_DRIVER = None

def accept_cookies(driver):
    try:
        accept_button = WebDriverWait(driver, WAIT_COOKIE_POPUP).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]" ))
        )
        accept_button.click()
        time.sleep(0.3)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
# SCRAPING LOGIC
# ═══════════════════════════════════════════════════════════════════════

def extract_chapter(driver, url: str, discourse_name: str = "") -> Dict:
    for attempt in range(MAX_RETRIES):
        try:
            driver.get(url)
            if WAIT_DOCUMENT_READY > 0:
                WebDriverWait(driver, WAIT_DOCUMENT_READY).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            else:
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            accept_cookies(driver)
            if WAIT_CONTENT_ELEMENT > 0:
                WebDriverWait(driver, WAIT_CONTENT_ELEMENT).until(EC.presence_of_element_located((By.ID, "content")))
            html = driver.page_source
            break
        except (TimeoutException, WebDriverException) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(WAIT_RETRY_DELAY)

    # 1. Title
    title = ''
    try:
        for sel in ('h1.entry-title', 'h1.post-title', 'h1.page-title', 'h1'):
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                txt = el.text.strip()
                if txt:
                    title = txt
                    break
            except Exception: continue
    except Exception: pass
    if not title: title = (driver.title or '').strip()

    # 2. Metadata (MP3, Images, Duration, Tags)
    mp3_links = []
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, 'a[href$=".mp3"]'):
            if a.get_attribute('href'): mp3_links.append(a.get_attribute('href'))
        for s in driver.find_elements(By.CSS_SELECTOR, 'audio source[src]'):
            if s.get_attribute('src') and s.get_attribute('src').endswith('.mp3'): mp3_links.append(s.get_attribute('src'))
    except Exception: pass

    image_url = ''
    try:
        image_url = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:image"]').get_attribute('content')
    except Exception: pass

    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m: duration = m.group(0)

    tags = []
    try:
        for t in driver.find_elements(By.CSS_SELECTOR, 'a[href*="/tag/"], a[href*="/category/"], a[rel="tag"]'):
            txt = t.get_attribute("textContent").strip()
            if txt and len(txt) < 50 and txt not in tags: tags.append(txt)
    except Exception: pass

    # 3. Transcript
    transcript_lines = []
    soup = BeautifulSoup(html, 'html.parser')

    BANNED_PHRASES = {
        "Home", "OSHO", "About Osho", "Osho Biography", "Osho on Mystic", "Osho Photo Gallery", "Osho Dham", "Upcoming Events", "Meditation Programs", "Meditation", "Active Meditation", "Passive Meditation", "Discourses", "Hindi Audio Discourses", "English Audio Discourses", "Hindi E-Books", "English E-Books", "Search Archive", "Video", "News & Media", "News", "Osho Art News", "Shop", "Pearls", "Music", "Magazine", "Tarot", "FAQ", "Login", "Share", "Whatsapp", "Facebook", "Instagram", "X", "Gmail", "Pinterest", "Copied !", "Language :", "Download", "UP NEXT", "Previous", "Next", "Related", "Menu", "Search", "0", "/", "#", "english", "hindi", "Description", "ZEN AND ZEN MASTERS", "Zen and Zen Masters", "View All", "Click Here", "Read More", "Explore More", "Full Story", "osho pearls", "Store", "Oshodham Programs", "Upcoming Programs", "Audio Discourse", "Osho Magazine", "Books", "Play Audio", "Contact Us", "Osho Dham, Osho Dhyan Mandir, 44, Jhatikra Road, Pandwala Khurd,", "Near Najafgarh, New Delhi - 110043", "+91-9971992227, 9717490340", "contact@oshoworld.com", "How to reach Oshodham", "Who is Osho", "Biography", "Osho On Mystic", "Other Centres", "Documentaries", "Call of the Master", "Hindi Audio Discourse", "English Audio Discourse", "Search Book Archive", "Meditation Books", "About Oshodham", "Getting Here", "Upcoming Event", "Osho Active Meditations", "Osho Passive Meditations", "Osho Whiterobe", "Osho Mystic Rose", "Osho No-mind", "Osho Born Again", "Osho Vipassana", "Osho Zazen", "Children Meditation Camp", "Vigyan Bhairav Tantra", "Audios", "Videos", "Osho Books", "Audio", "Osho Photos", "Magazine Subscription", "Osho Arts", "Osho Gifts", "others", "Osho Centres", "Privacy Policy", "Cookie policy", "Terms and Conditions", "Shipping & Delivery Policy", "Return Policy", "Cancellation Policy", "Follow Us :"
    }

    content_element = None
    for selector in ['.entry-content', '.post-content', '.td-post-content', '.tdb-block-inner', 'article', '.main-content', '#content', '#main']:
        content_element = soup.select_one(selector)
        if content_element: break
    if not content_element: content_element = soup.body

    if content_element:
        for tag in content_element(['script', 'style', 'noscript', 'nav', 'aside', 'header', 'footer', 'iframe', 'form', 'button', 'input']):
            tag.decompose()
        for junk in content_element.select('.share, .social, .widget, .sidebar, .related-posts, .navigation, .meta, .tags, .playlist, .tracklist, .audio-playlist, .wp-playlist, .jp-playlist'):
            junk.decompose()

        raw_text = content_element.get_text(separator='\n', strip=True)
        lines = raw_text.split('\n')
        
        name_pattern = None
        if discourse_name:
            try: name_pattern = re.compile(r'^' + re.escape(discourse_name) + r'\s*\d*$', re.IGNORECASE)
            except: pass

        for line in lines:
            clean_line = line.strip()
            if not clean_line: continue
            
            if clean_line in BANNED_PHRASES: continue
            if name_pattern and name_pattern.match(clean_line): continue
            
            # Fuzzy Logic for Playlist items
            if discourse_name and len(clean_line) < 100:
                similarity = difflib.SequenceMatcher(None, clean_line.lower(), discourse_name.lower()).ratio()
                if similarity > 0.6 and re.search(r'\d+$', clean_line): continue
                if similarity > 0.9: continue

            if re.match(r'^\d{2}:\d{2}:\d{2}$', clean_line) or re.match(r'^\d{2}:\d{2}$', clean_line): continue
            if re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(st|nd|rd|th)?,?\s+\d{4}', clean_line, re.IGNORECASE): continue
            if "Copyright" in clean_line and "Osho" in clean_line: continue
            if len(clean_line) < 50 and clean_line.isupper() and not clean_line.endswith(('.', '?', '!')): continue

            transcript_lines.append(clean_line)

    seen = set()
    transcript_paragraphs = [x for x in transcript_lines if not (x in seen or seen.add(x))]

    if title.strip().lower() == "osho world": transcript_paragraphs = None
    if not transcript_paragraphs: transcript_paragraphs = None

    return {
        'title': title,
        'url': url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'image_url': image_url,
        'duration': duration,
        'tags': tags,
        'transcript': transcript_paragraphs,
    }


def generate_url_variants(url):
    variants = []
    if re.search(r'vol-0\d', url): variants.append(re.sub(r'vol-0(\d)', r'vol-\1', url))
    elif re.search(r'vol-\d-', url) or re.search(r'vol-\d$', url): variants.append(re.sub(r'vol-(\d)', r'vol-0\1', url))
    return variants


def process_chapter(chapter_task):
    """
    Returns a result dict containing status info AND data.
    """
    discourse_index, discourse_name, chapter_index, chapter_url, chromedriver_path = chapter_task
    discourse_id = f"{discourse_index + 1:03}"
    chapter_id = f"{discourse_id}{chapter_index + 1:03}"
    pid = os.getpid()
    
    start_time = time.time()
    print(f"[PID {pid}] 🚀 Starting chapter {chapter_id}")

    try:
        driver = get_worker_driver(chromedriver_path)
        
        # 1. Scrape
        chapter_details = extract_chapter(driver, chapter_url, discourse_name)
        
        # 2. Retry Logic
        if chapter_details['title'].strip().lower() == "osho world" or not chapter_details['transcript']:
            print(f"[PID {pid}] ⚠️  Possible URL mismatch. Trying variants...")
            variants = generate_url_variants(chapter_url)
            for variant_url in variants:
                print(f"[PID {pid}] 🔄 Retrying with: {variant_url}")
                retry_details = extract_chapter(driver, variant_url, discourse_name)
                if retry_details['title'].strip().lower() != "osho world" and retry_details['transcript']:
                    print(f"[PID {pid}] ✅ Variant worked!")
                    chapter_details = retry_details
                    break
        
        # 3. Add ID meta
        chapter_details["id"] = chapter_id
        
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ✅ Completed chapter {chapter_id} in {elapsed:.1f}s")
        
        # RETURN SUCCESS OBJECT
        return {
            "success": True,
            "chapter_id": chapter_id,
            "discourse_id": discourse_id,
            "url": chapter_url,
            "title": chapter_details['title'],
            "data": chapter_details,  # The full content
            "error": None
        }
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ❌ Critical error scraping {chapter_id}: {e}")
        close_worker_driver()
        
        # RETURN FAILURE OBJECT
        return {
            "success": False,
            "chapter_id": chapter_id,
            "discourse_id": discourse_id,
            "url": chapter_url,
            "title": "N/A",
            "data": None,
            "error": str(e)
        }


def save_discourse_data(discourse_index, discourse, chapters_data):
    """Save valid discourse data to output/X.json"""
    discourse_id = f"{discourse_index + 1:03}"
    
    # Only calculate stats for successful scrapes
    chapters_with_transcript = sum(1 for ch in chapters_data if ch.get('transcript'))
    chapters_without_transcript = sum(1 for ch in chapters_data if not ch.get('transcript'))
    
    discourse_data = {
        "id": discourse_id,
        "discourse_name": discourse["discourse_name"],
        "discourse_url": discourse["discourse_url"],
        "language": discourse["language"],
        "chapters": chapters_data,
        "stats": {
            "total_chapters": len(chapters_data),
            "chapters_with_transcript": chapters_with_transcript,
            "chapters_without_transcript": chapters_without_transcript
        }
    }

    safe_filename = re.sub(r'[\\/*?:":<>|]', "", discourse["discourse_name"])
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / f"{safe_filename}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(discourse_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Saved discourse {discourse_id} | Coverage: {chapters_with_transcript}/{len(chapters_data)}")
    return discourse["discourse_url"]


def main(count, workers):
    print("\n" + "="*80)
    print("🔧 SCRAPER CONFIGURATION")
    print("="*80)
    print(f"WORKERS                 : {workers}")
    print("="*80 + "\n")
    
    print("Initializing ChromeDriver...")
    try:
        chromedriver_path = ChromeDriverManager().install()
        print(f"✅ ChromeDriver ready at: {chromedriver_path}\n")
    except Exception as e:
        print(f"❌ Failed to initialize ChromeDriver: {e}")
        return
    
    try:
        with open("chapter_links.json", "r", encoding="utf-8") as f:
            all_discourses = json.load(f)
    except FileNotFoundError:
        print("❌ Error: chapter_links.json not found.")
        return

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # LOAD PROGRESS
    progress_data = load_progress()
    completed_discourses = set(progress_data.get("completed_discourses", []))
    chapter_logs = progress_data.get("chapter_logs", [])

    discourses_to_process = [
        (i, d) for i, d in enumerate(all_discourses) if d["discourse_url"] not in completed_discourses
    ]

    if count is not None:
        discourses_to_process = discourses_to_process[:count]

    if not discourses_to_process:
        print("✅ All discourses have been processed.")
        return

    chapter_tasks = []
    for discourse_index, discourse in discourses_to_process:
        for chapter_index, chapter_url in enumerate(discourse["chapter_links"]):
            chapter_tasks.append((
                discourse_index,
                discourse["discourse_name"],
                chapter_index,
                chapter_url,
                chromedriver_path
            ))

    print(f"\n📋 PROCESSING: {len(discourses_to_process)} Discourses / {len(chapter_tasks)} Chapters")
    
    raw_results = []
    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            raw_results = list(executor.map(process_chapter, chapter_tasks))
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Saving partial progress...")
    except Exception as e:
        print(f"\n\n❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
    
    # ═══════════════════════════════════════════════════════════════════
    # NEW PROCESSING LOGIC: Separate Logs from Data
    # ═══════════════════════════════════════════════════════════════════
    print("\n📦 ORGANIZING RESULTS...")
    
    # 1. Update Global Logs (Passed + Failed)
    for res in raw_results:
        if res:
            log_entry = {
                "id": res["chapter_id"],
                "discourse_id": res["discourse_id"],
                "status": "Passed" if res["success"] else "Failed",
                "title": res["title"],
                "url": res["url"],
                "error": res["error"]
            }
            chapter_logs.append(log_entry)
            
    # 2. Group Valid Data by Discourse
    discourse_chapters_data = {}
    for res in raw_results:
        if res and res["success"]:
            disc_idx = int(res["discourse_id"]) - 1  # Map back to 0-index
            if disc_idx not in discourse_chapters_data:
                discourse_chapters_data[disc_idx] = []
            
            # Attach index for sorting
            data = res["data"]
            data["_sort_id"] = res["chapter_id"]
            discourse_chapters_data[disc_idx].append(data)

    # 3. Save Discourses & Update Progress File
    for discourse_index, discourse in discourses_to_process:
        if discourse_index in discourse_chapters_data:
            # Sort by ID to ensure order
            chapters = sorted(discourse_chapters_data[discourse_index], key=lambda x: x["_sort_id"])
            
            # Remove temp sort key
            for ch in chapters:
                ch.pop("_sort_id", None)
            
            discourse_url = save_discourse_data(discourse_index, discourse, chapters)
            completed_discourses.add(discourse_url)
            
            # Save progress incrementally
            progress_data["completed_discourses"] = list(completed_discourses)
            progress_data["chapter_logs"] = chapter_logs
            save_progress(progress_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    
    main(count=args.count, workers=args.workers)