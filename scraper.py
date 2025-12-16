import json
import re
import time
from pathlib import Path
from typing import List, Dict, Optional
import argparse
from concurrent.futures import ProcessPoolExecutor
import os
import atexit

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURABLE WAIT TIMES
# ═══════════════════════════════════════════════════════════════════════

WAIT_DOCUMENT_READY = 0
WAIT_COOKIE_POPUP = 1
WAIT_DYNAMIC_CONTENT = 0
WAIT_CONTENT_ELEMENT = 0
WAIT_RETRY_DELAY = 1
PAGE_LOAD_TIMEOUT = 15
SCRIPT_TIMEOUT = 10
MAX_RETRIES = 2

# Performance optimizations
ENABLE_IMAGES = False
ENABLE_CSS = False
ENABLE_JAVASCRIPT = True

# ═══════════════════════════════════════════════════════════════════════


PROGRESS_FILE = "progress.json"
WORKER_DRIVER = None


def load_progress():
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {"completed_discourses": []}
    return {"completed_discourses": []}


def save_progress(completed_discourses):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"completed_discourses": completed_discourses}, f, indent=2)


def make_driver(chromedriver_path):
    """Create a Chrome driver instance"""
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


def extract_chapter(driver, url: str, discourse_name: str = "") -> Dict:
    """Extract chapter content from URL with smart filtering"""
    
    for attempt in range(MAX_RETRIES):
        try:
            driver.get(url)
            
            if WAIT_DOCUMENT_READY > 0:
                WebDriverWait(driver, WAIT_DOCUMENT_READY).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
            else:
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            
            accept_cookies(driver)
            
            if WAIT_CONTENT_ELEMENT > 0:
                content_selectors_for_wait = [(By.CSS_SELECTOR, '.entry-content'), (By.ID, 'content')]
                for selector_type, selector_value in content_selectors_for_wait:
                    try:
                        WebDriverWait(driver, WAIT_CONTENT_ELEMENT).until(
                            EC.presence_of_element_located((selector_type, selector_value))
                        )
                        break
                    except TimeoutException:
                        continue
            
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
            except Exception:
                continue
    except Exception:
        title = ''
    if not title:
        title = (driver.title or '').strip()

    # 2. MP3 links
    mp3_links: List[str] = []
    try:
        anchors_mp3 = driver.find_elements(By.CSS_SELECTOR, 'a[href$=".mp3"]')
        for a in anchors_mp3:
            href = a.get_attribute('href')
            if href:
                mp3_links.append(href)
    except Exception:
        pass
    try:
        sources = driver.find_elements(By.CSS_SELECTOR, 'audio source[src], audio[src]')
        for s in sources:
            src = s.get_attribute('src') or s.get_attribute('data-src')
            if src and src.endswith('.mp3'):
                mp3_links.append(src)
    except Exception:
        pass

    # 3. Image URL
    image_url = ''
    try:
        og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:image"]')
        image_url = og.get_attribute('content') or ''
    except Exception:
        try:
            imgs = driver.find_elements(By.CSS_SELECTOR, 'img[src*="/_next/image?url="]')
            for i in imgs:
                src = i.get_attribute('src')
                if src:
                    image_url = src
                    break
        except Exception:
            image_url = ''

    # 4. Duration
    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m:
        duration = m.group(0)

    # 5. Transcript
    transcript_lines = []
    soup = BeautifulSoup(html, 'html.parser')

    BANNED_PHRASES = {
        # Navigation / Menus
        "Home", "OSHO", "About Osho", "Osho Biography", "Osho on Mystic", 
        "Osho Photo Gallery", "Osho Dham", "Upcoming Events", "Meditation Programs",
        "Meditation", "Active Meditation", "Passive Meditation", "Discourses",
        "Hindi Audio Discourses", "English Audio Discourses", "Hindi E-Books",
        "English E-Books", "Search Archive", "Video", "News & Media", "News",
        "Osho Art News", "Shop", "Pearls", "Music", "Magazine", "Tarot", "FAQ",
        "Login", "Share", "Whatsapp", "Facebook", "Instagram", "X", "Gmail",
        "Pinterest", "Copied !", "Language :", "Download", "UP NEXT", "Previous",
        "Next", "Related", "Menu", "Search", "0", "/", "#", "english", "hindi",
        "Description", "ZEN AND ZEN MASTERS", "Zen and Zen Masters", "View All",
        "Click Here", "Read More", "Explore More", "Full Story", "osho pearls",
        "Store", "Oshodham Programs", "Upcoming Programs", "Audio Discourse",
        "Osho Magazine", "Books",

        # Footer / Contact Junk
        "Play Audio", "Contact Us", 
        "Osho Dham, Osho Dhyan Mandir, 44, Jhatikra Road, Pandwala Khurd,",
        "Near Najafgarh, New Delhi - 110043", "+91-9971992227, 9717490340",
        "contact@oshoworld.com", "How to reach Oshodham", "Who is Osho", 
        "Biography", "Osho On Mystic", "Other Centres", "Documentaries", 
        "Call of the Master", "Hindi Audio Discourse", "English Audio Discourse", 
        "Search Book Archive", "Meditation Books", "About Oshodham", "Getting Here",
        "Upcoming Event", "Osho Active Meditations", "Osho Passive Meditations",
        "Osho Whiterobe", "Osho Mystic Rose", "Osho No-mind", "Osho Born Again",
        "Osho Vipassana", "Osho Zazen", "Children Meditation Camp", 
        "Vigyan Bhairav Tantra", "Audios", "Videos", "Osho Books", "Audio", 
        "Osho Photos", "Magazine Subscription", "Osho Arts", "Osho Gifts", 
        "others", "Osho Centres", "Privacy Policy", "Cookie policy", 
        "Terms and Conditions", "Shipping & Delivery Policy", "Return Policy", 
        "Cancellation Policy", "Follow Us :"
    }

    content_selectors = [
        '.entry-content', '.post-content', '.td-post-content', '.tdb-block-inner',
        'article', '.main-content', '#content', '#main'
    ]

    content_element = None
    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            break
    
    if not content_element:
        content_element = soup.body

    if content_element:
        for tag in content_element(['script', 'style', 'noscript', 'nav', 'aside', 'header', 'footer', 'iframe', 'form', 'button', 'input']):
            tag.decompose()

        garbage_selectors = [
            '.share', '.social', '.widget', '.sidebar', '.related-posts', 
            '.navigation', '.meta', '.tags', '.playlist', '.tracklist', 
            '.audio-playlist', '.wp-playlist', '.jp-playlist'
        ]
        for garbage in garbage_selectors:
            for junk in content_element.select(garbage):
                junk.decompose()

        raw_text = content_element.get_text(separator='\n', strip=True)
        lines = raw_text.split('\n')
        
        name_pattern = None
        if discourse_name:
            try:
                name_pattern = re.compile(r'^' + re.escape(discourse_name) + r'\s*\d*$', re.IGNORECASE)
            except Exception:
                pass

        for line in lines:
            clean_line = line.strip()
            if not clean_line:
                continue
            
            # Check Blacklist
            if clean_line in BANNED_PHRASES:
                continue
            
            # Check Discourse Name repetition
            if name_pattern and name_pattern.match(clean_line):
                continue
            
            # Check Timestamps
            if re.match(r'^\d{2}:\d{2}:\d{2}$', clean_line) or re.match(r'^\d{2}:\d{2}$', clean_line):
                continue

            # Check for Dates (e.g. December 19th, 2025)
            # Matches Month DDth, YYYY or similar formats
            if re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(st|nd|rd|th)?,?\s+\d{4}', clean_line, re.IGNORECASE):
                continue

            # Check Copyright
            if "Copyright" in clean_line and "Osho" in clean_line:
                continue
            
            # Check ALL CAPS short headers (Sidebar junk)
            if len(clean_line) < 50 and clean_line.isupper() and not clean_line.endswith(('.', '?', '!')):
                continue

            transcript_lines.append(clean_line)

    seen = set()
    transcript_paragraphs = [x for x in transcript_lines if not (x in seen or seen.add(x))]

    # CRITICAL CHECK: If title is "Osho World", we hit the homepage (redirect error).
    # Force transcript to None so we can try a URL variant in process_chapter
    if title.strip().lower() == "osho world":
        transcript_paragraphs = None

    if not transcript_paragraphs:
        transcript_paragraphs = None
    # ═══════════════════════════════════════════════════════════════════
    # EXTRACTION PHASE 6: TAGS (Universal Method)
    # ═══════════════════════════════════════════════════════════════════
    print(f"[PID {os.getpid()}] 🏷️  Extracting tags...")
    tags = []
    
    try:
        # Method 1: The "rel=tag" standard (Works on 99% of WordPress sites)
        # This finds ANY link that the website explicitly marks as a "tag" or "category"
        tag_elements = driver.find_elements(By.CSS_SELECTOR, 'a[rel="tag"], a[rel="category tag"]')
        
        for t in tag_elements:
            txt = t.get_attribute("textContent").strip() # textContent is more reliable than .text
            if txt and txt not in tags:
                tags.append(txt)
                
    except Exception:
        pass

    # Method 2: Fallback for specific theme classes if Method 1 fails
    if not tags:
        try:
            selectors = [
                '.td-tags a',           # Newspaper Theme (Common on Osho sites)
                '.entry-tags a',        # Standard WordPress
                '.tag-links a',         # Standard WordPress
                '.post-tags a',         # Common variation
                '.taxonomy-category a'  # Categories acting as tags
            ]
            
            for sel in selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                for t in elements:
                    txt = t.get_attribute("textContent").strip()
                    if txt and txt not in tags:
                        tags.append(txt)
                if tags: break # Stop if we found tags in this selector
        except Exception:
            pass
            
    print(f"[PID {os.getpid()}]   └─ Found {len(tags)} tags")
    chapter = {
        'title': title,
        'url': url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'image_url': image_url,
        'duration': duration,
        'tags': tags,   # <--- ADD THIS LINE
        'transcript': transcript_paragraphs,
    }
    return chapter


def generate_url_variants(url):
    """Generate potential URL fixes (e.g. vol-04 -> vol-4)"""
    variants = []
    
    # CASE 1: Remove leading zero (vol-04 -> vol-4)
    # This addresses the issue where site uses 'vol-4' but we generated 'vol-04'
    if re.search(r'vol-0\d', url):
        variants.append(re.sub(r'vol-0(\d)', r'vol-\1', url))
        
    # CASE 2: Add leading zero (vol-4 -> vol-04) - just in case
    elif re.search(r'vol-\d-', url) or re.search(r'vol-\d$', url):
        variants.append(re.sub(r'vol-(\d)', r'vol-0\1', url))
        
    return variants


def process_chapter(chapter_task):
    """
    Process a single chapter. 
    INCLUDES AUTO-RETRY LOGIC FOR URL MISMATCHES.
    """
    discourse_index, discourse_name, chapter_index, chapter_url, chromedriver_path = chapter_task
    discourse_id = f"{discourse_index + 1:03}"
    chapter_id = f"{discourse_id}{chapter_index + 1:03}"
    pid = os.getpid()
    
    start_time = time.time()
    print(f"[PID {pid}] 🚀 Starting chapter {chapter_id}")

    try:
        driver = get_worker_driver(chromedriver_path)
        
        # ATTEMPT 1: Original URL
        chapter_details = extract_chapter(driver, chapter_url, discourse_name)
        
        # CHECK FOR FAILURE (Redirected to Home Page "Osho World" or Empty Transcript)
        if chapter_details['title'].strip().lower() == "osho world" or not chapter_details['transcript']:
            print(f"[PID {pid}] ⚠️  Possible URL mismatch (Redirected to Home). Trying variants...")
            
            # Generate variants (e.g., vol-04 -> vol-4)
            variants = generate_url_variants(chapter_url)
            
            for variant_url in variants:
                print(f"[PID {pid}] 🔄 Retrying with: {variant_url}")
                retry_details = extract_chapter(driver, variant_url, discourse_name)
                
                # If retry worked (Title is NOT Osho World AND we got text), use it
                if retry_details['title'].strip().lower() != "osho world" and retry_details['transcript']:
                    print(f"[PID {pid}] ✅ Variant worked!")
                    chapter_details = retry_details
                    break
        
        chapter_details["id"] = chapter_id
        chapter_details["discourse_index"] = discourse_index
        chapter_details["chapter_index"] = chapter_index
        
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ✅ Completed chapter {chapter_id} in {elapsed:.1f}s")
        return chapter_details
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ❌ Critical error scraping {chapter_id}: {e}")
        close_worker_driver()
        return None


def save_discourse_data(discourse_index, discourse, chapters_data):
    """Save completed discourse data to file"""
    discourse_id = f"{discourse_index + 1:03}"
    
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
    print("🔧 SCRAPER CONFIGURATION (AUTO-URL FIXER ENABLED)")
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

    progress = load_progress()
    completed_discourses = set(progress.get("completed_discourses", []))

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
    
    chapter_results = []
    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            chapter_results = list(executor.map(process_chapter, chapter_tasks))
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n\n❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n📦 SAVING RESULTS")
    discourse_chapters = {}
    for chapter_data in chapter_results:
        if chapter_data:
            disc_idx = chapter_data["discourse_index"]
            if disc_idx not in discourse_chapters:
                discourse_chapters[disc_idx] = []
            discourse_chapters[disc_idx].append(chapter_data)
    
    for discourse_index, discourse in discourses_to_process:
        if discourse_index in discourse_chapters:
            chapters = sorted(discourse_chapters[discourse_index], key=lambda x: x["chapter_index"])
            for ch in chapters:
                ch.pop("discourse_index", None)
                ch.pop("chapter_index", None)
            
            discourse_url = save_discourse_data(discourse_index, discourse, chapters)
            completed_discourses.add(discourse_url)
            save_progress(list(completed_discourses))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    
    main(count=args.count, workers=args.workers)