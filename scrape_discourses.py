
import json
import re
import time
from pathlib import Path
from typing import List, Dict
import argparse
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


PROGRESS_FILE = "progress.json"


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


def make_driver():
    print("Creating new Chrome driver instance...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1200,900")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(30)
    print("Chrome driver instance created.")
    return driver


def process_discourse(discourse_with_index):
    discourse_index, discourse = discourse_with_index
    discourse_id = f"{discourse_index + 1:03}"
    print(f"Worker processing discourse {discourse_id}: {discourse['discourse_name']}")

    chapters_data = []

    for chapter_index, chapter_url in enumerate(discourse["chapter_links"]):
        driver = make_driver() # New driver for each chapter
        try:
            chapter_id = f"{discourse_id}{chapter_index + 1:03}"
            print(f"  Scraping chapter {chapter_id}: {chapter_url}")

            chapter_details = extract_chapter(driver, chapter_url)
            chapter_details["id"] = chapter_id
            chapters_data.append(chapter_details)
        except Exception as e:
            print(f"    Error scraping {chapter_url}: {e}")
        finally:
            driver.quit() # Quit driver after each chapter

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
    output_file = out_dir / f"{safe_filename}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(discourse_data, f, ensure_ascii=False, indent=2)

    print(f"  Successfully saved to {output_file}")
    return discourse["discourse_url"]




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
    driver.get(url)
    WebDriverWait(driver, 15).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    accept_cookies(driver)
    time.sleep(0.6) # Small sleep for good measure after cookie acceptance

    # Wait for one of the common content elements to be present
    content_selectors_for_wait = [
        (By.CSS_SELECTOR, '.article-text'), # Added this selector
        (By.CSS_SELECTOR, '.entry-content'),
        (By.CSS_SELECTOR, '.post-content'),
        (By.CSS_SELECTOR, 'article'),
        (By.ID, 'content'),
        (By.ID, 'main')
    ]
    
    # Try waiting for any of the content selectors
    waited_for_content = False
    for selector_type, selector_value in content_selectors_for_wait:
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((selector_type, selector_value)))
            waited_for_content = True
            break
        except Exception:
            continue
            
    if not waited_for_content:
        print(f"Warning: No specific content element found after waiting for {url}. Proceeding with page source.")
    html = driver.page_source

    # Title: try multiple fallbacks
    title = ''
    try:
        # common WP selectors
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
        # meta og:title or document.title
        try:
            og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            title = (og.get_attribute('content') or '').strip()
        except Exception:
            title = (driver.title or '').strip()

    # MP3 links: anchors ending with .mp3 or <audio> sources
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

    # Image url: try og:image meta or first non-empty image
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

    # Duration: regex search for hh:mm:ss
    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m:
        duration = m.group(0)

    # Transcript: try several likely containers with HTML handling (preserve <br> breaks)
    transcript_paragraphs = []
    soup = BeautifulSoup(html, 'html.parser')

    content_selectors = [
        '.article-text', # Added this selector
        '.entry-content', '.post-content', '.td-post-content', '.tdb-block-inner',
        'article', '.main-content', '#content', '#main'
    ]

    content_element = None
    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            break

    if content_element:
        # Remove script and style tags
        for unwanted_tag in content_element(['script', 'style', 'noscript', 'img', 'audio', 'video']):
            unwanted_tag.decompose()

        # Extract text, preserving paragraph structure
        for p in content_element.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
            text = p.get_text(separator=' ', strip=True)
            # Apply filtering similar to the fallback, but slightly adjusted
            if text and len(text) > 30 and "OSHO" not in text and "Copyright" not in text: # Increased min length for more robust filtering
                transcript_paragraphs.append(text)
    else:
        # Fallback to body text if no specific content element is found
        body_text = soup.body.get_text(separator='\n', strip=True) if soup.body else ''
        lines = body_text.split('\n')
        transcript_paragraphs = [line.strip() for line in lines if len(line.strip()) > 50 and "OSHO" not in line and "Copyright" not in line]

    # Post-processing: remove empty strings and join with double newlines for paragraph separation
    transcript_paragraphs = [p for p in transcript_paragraphs if p]




    # assemble
    chapter = {
        'title': title,
        'url': url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'image_url': image_url,
        'duration': duration,
        'transcript': transcript_paragraphs,
    }
    return chapter


def main(count, workers):
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

    print(f"Starting ThreadPoolExecutor with {workers} workers.")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(process_discourse, discourses_to_process)
        for discourse_url in results:
            if discourse_url:
                completed_discourses.add(discourse_url)
                save_progress(list(completed_discourses))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape OSHO discourses.")
    parser.add_argument("--count", type=int, default=None, help="Number of discourses to process.")
    parser.add_argument("--workers", type=int, default=1, help="Number of concurrent workers.")
    args = parser.parse_args()
    main(count=args.count, workers=args.workers)
