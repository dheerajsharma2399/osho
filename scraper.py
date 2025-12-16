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
    # Uncomment to run headless
    # options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1200,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)
        print(f"[PID {pid}] ✓ Chrome driver created successfully")
        return driver
    except Exception as e:
        print(f"[PID {pid}] ✗ Failed to create Chrome driver: {e}")
        raise


def accept_cookies(driver):
    try:
        accept_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]" ))
        )
        accept_button.click()
        time.sleep(1)
    except Exception:
        pass # ignore if not found


def extract_chapter(driver, url: str) -> Dict:
    """Extract chapter content from URL"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"[PID {os.getpid()}] Loading URL (attempt {attempt + 1}/{max_retries}): {url}")
            driver.get(url)
            
            # Wait for page to load
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            accept_cookies(driver)
            time.sleep(1)

            # Wait for content
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
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((selector_type, selector_value))
                    )
                    waited_for_content = True
                    break
                except TimeoutException:
                    continue
                    
            if not waited_for_content:
                print(f"[PID {os.getpid()}] Warning: No content element found for {url}")
            
            html = driver.page_source
            break  # Success, exit retry loop
            
        except TimeoutException as e:
            print(f"[PID {os.getpid()}] Timeout on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
        except Exception as e:
            print(f"[PID {os.getpid()}] Error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

    # Title: try multiple fallbacks
    print(f"[PID {os.getpid()}] Extracting title...")
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
    print(f"[PID {os.getpid()}] Title: {title[:50]}...")

    # MP3 links
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

    # Image url
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

    # Duration
    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m:
        duration = m.group(0)

    # Transcript
    print(f"[PID {os.getpid()}] Extracting transcript...")
    transcript_paragraphs = []
    soup = BeautifulSoup(html, 'html.parser')

    content_selectors = [
        '.entry-content', '.post-content', '.td-post-content', '.tdb-block-inner',
        'article', '.main-content', '#content', '#main'
    ]

    content_element = None
    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            print(f"[PID {os.getpid()}] Found content with selector: {selector}")
            break

    if content_element:
        for unwanted_tag in content_element(['script', 'style', 'noscript', 'img', 'audio', 'video']):
            unwanted_tag.decompose()

        for p in content_element.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
            text = p.get_text(separator=' ', strip=True)
            if text and len(text) > 30 and "OSHO" not in text and "Copyright" not in text:
                transcript_paragraphs.append(text)
    else:
        print(f"[PID {os.getpid()}] Using fallback body text extraction")
        body_text = soup.body.get_text(separator='\n', strip=True) if soup.body else ''
        lines = body_text.split('\n')
        transcript_paragraphs = [line.strip() for line in lines if len(line.strip()) > 50 and "OSHO" not in line and "Copyright" not in line]

    transcript_paragraphs = [p for p in transcript_paragraphs if p]
    print(f"[PID {os.getpid()}] Extracted {len(transcript_paragraphs)} paragraphs")

    chapter = {
        'title': title,
        'url': url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'image_url': image_url,
        'duration': duration,
        'transcript': transcript_paragraphs,
    }
    return chapter


def process_chapter(chapter_task):
    """Process a single chapter - this function will run in a separate process"""
    discourse_index, discourse_name, chapter_index, chapter_url, chromedriver_path = chapter_task
    discourse_id = f"{discourse_index + 1:03}"
    chapter_id = f"{discourse_id}{chapter_index + 1:03}"
    pid = os.getpid()
    
    start_time = time.time()
    print(f"\n{'='*60}")
    print(f"[PID {pid}] 🚀 Starting chapter {chapter_id} ({discourse_name})")
    print(f"{'='*60}")

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
    
    discourse_data = {
        "id": discourse_id,
        "discourse_name": discourse["discourse_name"],
        "discourse_url": discourse["discourse_url"],
        "language": discourse["language"],
        "chapters": chapters_data,
    }

    # Sanitize the filename
    safe_filename = re.sub(r'[\\/*?:":<>|]', "", discourse["discourse_name"])
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / f"{safe_filename}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(discourse_data, f, ensure_ascii=False, indent=2)

    print(f"✓ Successfully saved discourse {discourse_id} to {output_file}")
    return discourse["discourse_url"]


def main(count, workers):
    # Initialize ChromeDriver once before spawning processes
    print("Initializing ChromeDriver...")
    try:
        chromedriver_path = ChromeDriverManager().install()
        print(f"✓ ChromeDriver ready at: {chromedriver_path}")
    except Exception as e:
        print(f"✗ Failed to initialize ChromeDriver: {e}")
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
        print("All discourses have been processed.")
        return

    # Create a list of all chapter tasks (now including chromedriver_path)
    chapter_tasks = []
    for discourse_index, discourse in discourses_to_process:
        for chapter_index, chapter_url in enumerate(discourse["chapter_links"]):
            chapter_tasks.append((
                discourse_index,
                discourse["discourse_name"],
                chapter_index,
                chapter_url,
                chromedriver_path  # Pass the driver path to each task
            ))

    print(f"\nStarting ProcessPoolExecutor with {workers} workers.")
    print(f"Processing {len(discourses_to_process)} discourses ({len(chapter_tasks)} chapters total)...\n")
    
    # Process all chapters in parallel
    chapter_results = []
    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            chapter_results = list(executor.map(process_chapter, chapter_tasks))
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n\n✗ Error during processing: {e}")
        import traceback
        traceback.print_exc()
    
    # Group results by discourse and save
    print("\n\nGrouping chapters and saving discourses...")
    discourse_chapters = {}
    for chapter_data in chapter_results:
        if chapter_data:
            disc_idx = chapter_data["discourse_index"]
            if disc_idx not in discourse_chapters:
                discourse_chapters[disc_idx] = []
            discourse_chapters[disc_idx].append(chapter_data)
    
    # Save each discourse
    saved_count = 0
    for discourse_index, discourse in discourses_to_process:
        if discourse_index in discourse_chapters:
            # Sort chapters by chapter_index
            chapters = sorted(discourse_chapters[discourse_index], key=lambda x: x["chapter_index"])
            
            # Remove temporary fields
            for ch in chapters:
                ch.pop("discourse_index", None)
                ch.pop("chapter_index", None)
            
            discourse_url = save_discourse_data(discourse_index, discourse, chapters)
            completed_discourses.add(discourse_url)
            save_progress(list(completed_discourses))
            saved_count += 1
        else:
            print(f"⚠ No chapters found for discourse {discourse_index + 1}: {discourse['discourse_name']}")

    successful_chapters = len([r for r in chapter_results if r is not None])
    print(f"\n✓ Completed: {saved_count}/{len(discourses_to_process)} discourses, {successful_chapters}/{len(chapter_tasks)} chapters")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape OSHO discourses.")
    parser.add_argument("--count", type=int, default=None, help="Number of discourses to process.")
    parser.add_argument("--workers", type=int, default=1, help="Number of concurrent workers (Chrome instances).")
    args = parser.parse_args()
    main(count=args.count, workers=args.workers)