
import json
import re
import time
from pathlib import Path
from typing import List, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def make_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1200,900")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(30)
    return driver


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
    time.sleep(0.6)
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
    try:
        body_text = driver.find_element(By.TAG_NAME, 'body').text
        lines = body_text.split('\n')
        # Filter out short lines and common boilerplate
        transcript_paragraphs = [line.strip() for line in lines if len(line.strip()) > 50 and "OSHO" not in line and "Copyright" not in line]
    except Exception:
        pass

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


def main():
    with open("chapter_links.json", "r", encoding="utf-8") as f:
        discourses = json.load(f)

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    driver = make_driver(headless=True)
    try:
        # Test with the first discourse
        for discourse_index, discourse in enumerate(discourses):
            discourse_id = f"{discourse_index + 1:03}"
            print(f"Processing discourse {discourse_id}: {discourse['discourse_name']}")

            chapters_data = []
            for chapter_index, chapter_url in enumerate(discourse["chapter_links"]):
                chapter_id = f"{discourse_id}{chapter_index + 1:03}"
                print(f"  Scraping chapter {chapter_id}: {chapter_url}")

                try:
                    chapter_details = extract_chapter(driver, chapter_url)
                    chapter_details["id"] = chapter_id
                    chapters_data.append(chapter_details)
                except Exception as e:
                    print(f"    Error scraping {chapter_url}: {e}")

            discourse_data = {
                "id": discourse_id,
                "discourse_name": discourse["discourse_name"],
                "discourse_url": discourse["discourse_url"],
                "language": discourse["language"],
                "chapters": chapters_data,
            }

            # Sanitize the filename
            safe_filename = re.sub(r'[\\/*?:":<>|]', "", discourse["discourse_name"])
            output_file = out_dir / f"{safe_filename}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(discourse_data, f, ensure_ascii=False, indent=2)

            print(f"  Successfully saved to {output_file}")

    finally:
        driver.quit()


if __name__ == '__main__':
    main()
