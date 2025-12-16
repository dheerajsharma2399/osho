import json
import re
import time
from pathlib import Path
from typing import List, Dict
import argparse
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURABLE WAIT TIMES (in seconds)
# Adjust these values to calibrate the scraper for your network/site speed
# ═══════════════════════════════════════════════════════════════════════

# ULTRA-FAST MODE for static websites (set to 0 to disable waits)
WAIT_DOCUMENT_READY = 0          # Time to wait for document.readyState = 'complete' (0 = skip)
WAIT_COOKIE_POPUP = 1            # Time to wait for cookie consent popup
WAIT_DYNAMIC_CONTENT = 0         # Static delay for JavaScript-rendered content (0 = skip)
WAIT_CONTENT_ELEMENT = 0         # Time to wait for main content container (0 = skip)
WAIT_RETRY_DELAY = 1             # Delay before retrying after failure
PAGE_LOAD_TIMEOUT = 15           # Maximum time for page navigation
SCRIPT_TIMEOUT = 10              # Maximum time for JavaScript execution
MAX_RETRIES = 2                  # Number of retry attempts per URL

# Performance optimizations
ENABLE_IMAGES = False            # Load images (disable for speed)
ENABLE_CSS = False               # Load CSS (disable for speed)
ENABLE_JAVASCRIPT = True         # Load JavaScript (needed for some sites)

# ═══════════════════════════════════════════════════════════════════════


PROGRESS_FILE = "progress.json"
CHROMEDRIVER_PATH = None  # Will be set once in main


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
    """Create a Chrome driver instance with the given chromedriver path"""
    pid = os.getpid()
    print(f"[PID {pid}] Creating Chrome driver instance...")
    
    options = webdriver.ChromeOptions()
    
    # ULTRA-FAST MODE: Enable headless
    options.add_argument("--headless=new")
    
    # Performance optimizations
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Aggressive performance settings
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-sync")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")
    options.add_argument("--no-proxy-server")
    options.add_argument("--dns-prefetch-disable")
    
    # Minimal window size for speed
    options.add_argument("--window-size=800,600")
    
    # Disable images/CSS/JS based on config
    prefs = {
        "profile.managed_default_content_settings.images": 2 if not ENABLE_IMAGES else 1,
        "profile.managed_default_content_settings.stylesheets": 2 if not ENABLE_CSS else 1,
        "profile.managed_default_content_settings.javascript": 1 if ENABLE_JAVASCRIPT else 2,
    }
    options.add_experimental_option("prefs", prefs)
    
    # Set page load strategy to 'none' for maximum speed
    # This doesn't wait for page load to complete
    options.page_load_strategy = 'none'
    
    try:
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.set_script_timeout(SCRIPT_TIMEOUT)
        print(f"[PID {pid}] ✓ Chrome driver created successfully (ULTRA-FAST MODE)")
        return driver
    except Exception as e:
        print(f"[PID {pid}] ✗ Failed to create Chrome driver: {e}")
        raise


def accept_cookies(driver):
    """Handle cookie popup - optimized for speed"""
    try:
        accept_button = WebDriverWait(driver, WAIT_COOKIE_POPUP).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]" ))
        )
        accept_button.click()
        time.sleep(0.3)  # Minimal wait after clicking
    except Exception:
        pass  # ignore if not found


def extract_chapter(driver, url: str) -> Dict:
    """Extract chapter content from URL - ULTRA-FAST MODE"""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"[PID {os.getpid()}] Loading URL (attempt {attempt + 1}/{MAX_RETRIES}): {url}")
            
            # WAIT POINT 1: Navigate to URL
            driver.get(url)
            
            # WAIT POINT 2: Wait for document ready state (OPTIONAL - skip if 0)
            if WAIT_DOCUMENT_READY > 0:
                print(f"[PID {os.getpid()}] ⏳ Waiting for document ready ({WAIT_DOCUMENT_READY}s)...")
                WebDriverWait(driver, WAIT_DOCUMENT_READY).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
            else:
                # For static sites, just wait for body to exist (much faster)
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            
            # WAIT POINT 3: Handle cookie consent (only on first chapter)
            if attempt == 0:  # Only try on first attempt
                accept_cookies(driver)
            
            # WAIT POINT 4: Additional static wait (OPTIONAL - skip if 0)
            if WAIT_DYNAMIC_CONTENT > 0:
                print(f"[PID {os.getpid()}] ⏳ Waiting for dynamic content ({WAIT_DYNAMIC_CONTENT}s)...")
                time.sleep(WAIT_DYNAMIC_CONTENT)

            # WAIT POINT 5: Wait for main content elements (OPTIONAL - skip if 0)
            if WAIT_CONTENT_ELEMENT > 0:
                content_selectors_for_wait = [
                    (By.CSS_SELECTOR, '.entry-content'),
                    (By.CSS_SELECTOR, '.post-content'),
                    (By.CSS_SELECTOR, 'article'),
                    (By.ID, 'content'),
                    (By.ID, 'main')
                ]
                
                waited_for_content = False
                for selector_type, selector_value in content_selectors_for_wait:
                    try:
                        WebDriverWait(driver, WAIT_CONTENT_ELEMENT).until(
                            EC.presence_of_element_located((selector_type, selector_value))
                        )
                        waited_for_content = True
                        print(f"[PID {os.getpid()}] ✓ Content element found: {selector_value}")
                        break
                    except TimeoutException:
                        continue
                        
                if not waited_for_content:
                    print(f"[PID {os.getpid()}] ⚠ Warning: No content element found for {url}")
            
            # Now safe to extract HTML - all waits completed
            html = driver.page_source
            break  # Success, exit retry loop
            
        except TimeoutException as e:
            print(f"[PID {os.getpid()}] ⏱ Timeout on attempt {attempt + 1}: {e}")
            if attempt == MAX_RETRIES - 1:
                raise
            # WAIT POINT 6: Retry delay
            print(f"[PID {os.getpid()}] ⏳ Retrying in {WAIT_RETRY_DELAY}s...")
            time.sleep(WAIT_RETRY_DELAY)
        except Exception as e:
            print(f"[PID {os.getpid()}] ❌ Error on attempt {attempt + 1}: {e}")
            if attempt == MAX_RETRIES - 1:
                raise
            # WAIT POINT 7: Retry delay
            print(f"[PID {os.getpid()}] ⏳ Retrying in {WAIT_RETRY_DELAY}s...")
            time.sleep(WAIT_RETRY_DELAY)

    # ═══════════════════════════════════════════════════════════════════
    # ALL WAITING IS COMPLETE - NOW EXTRACTING DATA FROM LOADED PAGE
    # ═══════════════════════════════════════════════════════════════════
    
    # EXTRACTION PHASE 1: Title extraction (no waiting, immediate extraction)
    print(f"[PID {os.getpid()}] 📝 Extracting title...")
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
        try:
            og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            title = (og.get_attribute('content') or '').strip()
        except Exception:
            title = (driver.title or '').strip()
    print(f"[PID {os.getpid()}]   └─ Title: {title[:50]}...")

    # EXTRACTION PHASE 2: MP3 links (no waiting, immediate extraction)
    print(f"[PID {os.getpid()}] 🎵 Extracting MP3 links...")
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
    print(f"[PID {os.getpid()}]   └─ Found {len(mp3_links)} MP3 link(s)")

    # EXTRACTION PHASE 3: Image URL (no waiting, immediate extraction)
    print(f"[PID {os.getpid()}] 🖼️  Extracting image URL...")
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
    if image_url:
        print(f"[PID {os.getpid()}]   └─ Image URL found")

    # EXTRACTION PHASE 4: Duration (no waiting, regex on already-loaded HTML)
    print(f"[PID {os.getpid()}] ⏱️  Extracting duration...")
    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m:
        duration = m.group(0)
        print(f"[PID {os.getpid()}]   └─ Duration: {duration}")

   # EXTRACTION PHASE 5: Transcript (Smart Filters)
    print(f"[PID {os.getpid()}] 📄 Extracting transcript...")
    transcript_lines = []
    soup = BeautifulSoup(html, 'html.parser')

    # 1. EXPANDED BLACKLIST (Exact matches to ignore)
    BANNED_PHRASES = {
        "Home", "OSHO", "About Osho", "Osho Biography", "Osho on Mystic", 
        "Osho Photo Gallery", "Osho Dham", "Upcoming Events", "Meditation Programs",
        "Meditation", "Active Meditation", "Passive Meditation", "Discourses",
        "Hindi Audio Discourses", "English Audio Discourses", "Hindi E-Books",
        "English E-Books", "Search Archive", "Video", "News & Media", "News",
        "Osho Art News", "Shop", "Pearls", "Music", "Magazine", "Tarot", "FAQ",
        "Login", "Share", "Whatsapp", "Facebook", "Instagram", "X", "Gmail",
        "Pinterest", "Copied !", "Language :", "Download", "UP NEXT", "Previous",
        "Next", "Related", "Menu", "Search", "0", "/", "#", "english", "hindi"
    }

    # 2. Find the Content Container
    content_selectors = [
        '.entry-content', '.post-content', '.td-post-content', '.tdb-block-inner',
        'article', '.main-content', '#content', '#main'
    ]

    content_element = None
    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            print(f"[PID {os.getpid()}]   └─ Found content with selector: {selector}")
            break
    
    if not content_element:
        print(f"[PID {os.getpid()}]   └─ Using fallback body extraction")
        content_element = soup.body

    if content_element:
        # 3. Structural Cleaning (Remove navigation, sidebars, playlists)
        for tag in content_element(['script', 'style', 'noscript', 'nav', 'aside', 'header', 'footer', 'iframe', 'form', 'button', 'input']):
            tag.decompose()

        # Remove playlist/download/widget containers specifically
        garbage_selectors = [
            '.share', '.social', '.widget', '.sidebar', '.related-posts', 
            '.navigation', '.meta', '.tags', '.playlist', '.tracklist', 
            '.audio-playlist', '.wp-playlist', '.jp-playlist'
        ]
        for garbage in garbage_selectors:
            for junk in content_element.select(garbage):
                junk.decompose()

        # 4. Extract Text
        raw_text = content_element.get_text(separator='\n', strip=True)
        
        # 5. SMART FILTERING
        lines = raw_text.split('\n')
        for line in lines:
            clean_line = line.strip()
            
            # Skip empty
            if not clean_line:
                continue
                
            # A. Check Blacklist
            if clean_line in BANNED_PHRASES:
                continue
            
            # B. Check Timestamps (e.g. 01:54:11, 00:00:00)
            # This regex matches strict time formats
            if re.match(r'^\d{2}:\d{2}:\d{2}$', clean_line) or re.match(r'^\d{2}:\d{2}$', clean_line):
                continue

            # C. Check Copyright / Generic Metadata
            if "Copyright" in clean_line and "Osho" in clean_line:
                continue
            
            # D. Check for "Playlist" style repetition
            # If the line contains the main Title (e.g. "Bodhidharma") AND a digit (e.g. "01"), it's likely a playlist link
            # But we must be careful not to delete "Question 1"
            if title and len(title) > 5 and title.lower() in clean_line.lower():
                 # It mentions the book title. Is it a chapter listing?
                 # If it ends with a number (like "Title 01", "Title 02"), ignore it.
                 if re.search(r'\d{2}$', clean_line): 
                     continue

            # If we survived all filters, accept it.
            transcript_lines.append(clean_line)

    # Remove duplicates
    seen = set()
    transcript_paragraphs = [x for x in transcript_lines if not (x in seen or seen.add(x))]

    if transcript_paragraphs:
        print(f"[PID {os.getpid()}]   └─ ✓ Extracted {len(transcript_paragraphs)} lines")
    else:
        print(f"[PID {os.getpid()}]   └─ ⚠ Warning: No transcript found for {url}")
        transcript_paragraphs = None

    chapter = {
        'title': title,
        'url': url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'image_url': image_url,
        'duration': duration,
        'transcript': transcript_paragraphs,  # Will be None if no transcript found
    }
    return chapter


def process_chapter(chapter_task):
    """Process a single chapter - this function will run in a separate process"""
    discourse_index, discourse_name, chapter_index, chapter_url, chromedriver_path = chapter_task
    discourse_id = f"{discourse_index + 1:03}"
    chapter_id = f"{discourse_id}{chapter_index + 1:03}"
    pid = os.getpid()
    
    start_time = time.time()
    print(f"\n{'='*80}")
    print(f"[PID {pid}] 🚀 Starting chapter {chapter_id} ({discourse_name})")
    print(f"{'='*80}")

    driver = None
    try:
        driver = make_driver(chromedriver_path)
        print(f"[PID {pid}] 🌐 Scraping: {chapter_url}")
        
        chapter_details = extract_chapter(driver, chapter_url)
        chapter_details["id"] = chapter_id
        chapter_details["discourse_index"] = discourse_index
        chapter_details["chapter_index"] = chapter_index
        
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ✅ Completed chapter {chapter_id} in {elapsed:.1f}s")
        return chapter_details
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[PID {pid}] ❌ Error scraping chapter {chapter_id} after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        if driver:
            try:
                driver.quit()
                print(f"[PID {pid}] 🔒 Driver closed for chapter {chapter_id}")
            except Exception as e:
                print(f"[PID {pid}] ⚠️  Error closing driver: {e}")


def save_discourse_data(discourse_index, discourse, chapters_data):
    """Save completed discourse data to file"""
    discourse_id = f"{discourse_index + 1:03}"
    
    # Count chapters with and without transcripts
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

    # Sanitize the filename
    safe_filename = re.sub(r'[\\/*?:":<>|]', "", discourse["discourse_name"])
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / f"{safe_filename}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(discourse_data, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*80}")
    print(f"✅ Successfully saved discourse {discourse_id} to {output_file}")
    print(f"   📊 Transcript Coverage: {chapters_with_transcript}/{len(chapters_data)} chapters")
    if chapters_without_transcript > 0:
        print(f"   ⚠️  Missing Transcripts: {chapters_without_transcript} chapters")
    print(f"{'='*80}\n")
    
    return discourse["discourse_url"]


def main(count, workers):
    print("\n" + "="*80)
    print("🔧 SCRAPER CONFIGURATION")
    print("="*80)
    print(f"MODE                    : {'🚀 ULTRA-FAST (Static Sites)' if WAIT_DOCUMENT_READY == 0 else 'Normal'}")
    print(f"WAIT_DOCUMENT_READY     : {WAIT_DOCUMENT_READY}s {'(SKIPPED)' if WAIT_DOCUMENT_READY == 0 else ''}")
    print(f"WAIT_COOKIE_POPUP       : {WAIT_COOKIE_POPUP}s")
    print(f"WAIT_DYNAMIC_CONTENT    : {WAIT_DYNAMIC_CONTENT}s {'(SKIPPED)' if WAIT_DYNAMIC_CONTENT == 0 else ''}")
    print(f"WAIT_CONTENT_ELEMENT    : {WAIT_CONTENT_ELEMENT}s {'(SKIPPED)' if WAIT_CONTENT_ELEMENT == 0 else ''}")
    print(f"WAIT_RETRY_DELAY        : {WAIT_RETRY_DELAY}s")
    print(f"PAGE_LOAD_TIMEOUT       : {PAGE_LOAD_TIMEOUT}s")
    print(f"SCRIPT_TIMEOUT          : {SCRIPT_TIMEOUT}s")
    print(f"MAX_RETRIES             : {MAX_RETRIES}")
    print(f"WORKERS                 : {workers}")
    print(f"ENABLE_IMAGES           : {ENABLE_IMAGES}")
    print(f"ENABLE_CSS              : {ENABLE_CSS}")
    print(f"ENABLE_JAVASCRIPT       : {ENABLE_JAVASCRIPT}")
    print("="*80 + "\n")
    
    # Initialize ChromeDriver once before spawning processes
    print("Initializing ChromeDriver...")
    try:
        chromedriver_path = ChromeDriverManager().install()
        print(f"✅ ChromeDriver ready at: {chromedriver_path}\n")
    except Exception as e:
        print(f"❌ Failed to initialize ChromeDriver: {e}")
        return
    
    with open("chapter_links.json", "r", encoding="utf-8") as f:
        all_discourses = json.load(f)

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    progress = load_progress()
    completed_discourses = set(progress.get("completed_discourses", []))

    # Filter out completed discourses
    discourses_to_process = [
        (i, d) for i, d in enumerate(all_discourses) if d["discourse_url"] not in completed_discourses
    ]

    # Apply count limit
    if count is not None:
        discourses_to_process = discourses_to_process[:count]

    if not discourses_to_process:
        print("✅ All discourses have been processed.")
        return

    # Create a list of all chapter tasks
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

    print(f"\n{'='*80}")
    print(f"📋 SCRAPING SUMMARY")
    print(f"{'='*80}")
    print(f"Discourses to process: {len(discourses_to_process)}")
    print(f"Total chapters       : {len(chapter_tasks)}")
    print(f"Parallel workers     : {workers}")
    print(f"{'='*80}\n")
    
    # Process all chapters in parallel
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
    
    # Group results by discourse and save
    print("\n\n" + "="*80)
    print("📦 GROUPING AND SAVING RESULTS")
    print("="*80 + "\n")
    
    discourse_chapters = {}
    for chapter_data in chapter_results:
        if chapter_data:
            disc_idx = chapter_data["discourse_index"]
            if disc_idx not in discourse_chapters:
                discourse_chapters[disc_idx] = []
            discourse_chapters[disc_idx].append(chapter_data)
    
    # Save each discourse
    saved_count = 0
    total_chapters_with_transcript = 0
    total_chapters_without_transcript = 0
    
    for discourse_index, discourse in discourses_to_process:
        if discourse_index in discourse_chapters:
            # Sort chapters by chapter_index
            chapters = sorted(discourse_chapters[discourse_index], key=lambda x: x["chapter_index"])
            
            # Count transcript stats
            with_transcript = sum(1 for ch in chapters if ch.get('transcript'))
            without_transcript = sum(1 for ch in chapters if not ch.get('transcript'))
            total_chapters_with_transcript += with_transcript
            total_chapters_without_transcript += without_transcript
            
            # Remove temporary fields
            for ch in chapters:
                ch.pop("discourse_index", None)
                ch.pop("chapter_index", None)
            
            discourse_url = save_discourse_data(discourse_index, discourse, chapters)
            completed_discourses.add(discourse_url)
            save_progress(list(completed_discourses))
            saved_count += 1
        else:
            print(f"⚠️  No chapters found for discourse {discourse_index + 1}: {discourse['discourse_name']}")

    successful_chapters = len([r for r in chapter_results if r is not None])
    
    print("\n" + "="*80)
    print("🎉 FINAL SUMMARY")
    print("="*80)
    print(f"Discourses saved     : {saved_count}/{len(discourses_to_process)}")
    print(f"Chapters scraped     : {successful_chapters}/{len(chapter_tasks)}")
    print(f"With transcripts     : {total_chapters_with_transcript}")
    print(f"Without transcripts  : {total_chapters_without_transcript}")
    if successful_chapters > 0:
        coverage = (total_chapters_with_transcript / successful_chapters) * 100
        print(f"Transcript coverage  : {coverage:.1f}%")
    print("="*80 + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Scrape OSHO discourses with configurable wait times.",
        epilog="Adjust wait times by modifying the constants at the top of the script."
    )
    parser.add_argument("--count", type=int, default=None, 
                       help="Number of discourses to process.")
    parser.add_argument("--workers", type=int, default=1, 
                       help="Number of concurrent workers (Chrome instances).")
    args = parser.parse_args()
    
    main(count=args.count, workers=args.workers)