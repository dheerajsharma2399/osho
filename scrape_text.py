import json
import logging
import signal
import sys
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --------- CONFIG ---------
INPUT_HINDI = Path("hindi-names.json")
INPUT_ENGLISH = Path("eng-names.json")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

LOG_LEVEL = logging.INFO
TIMEOUT = 30  # seconds for waits
# -------------------------


# ---- logging setup ----
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("osho_scraper")

# global flag for graceful shutdown
SHOULD_STOP = False


def signal_handler(sig, frame):
    """Handle Ctrl+C / SIGINT / SIGTERM gracefully."""
    global SHOULD_STOP
    logger.warning("Received interrupt signal, will stop after current item...")
    SHOULD_STOP = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# -------- Selenium helpers --------

def setup_driver(headless: bool = False) -> webdriver.Chrome:
    """Initialize ChromeDriver."""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    service = ChromeService()
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def safe_find_element(driver_or_element, by, value) -> Optional[Any]:
    try:
        return driver_or_element.find_element(by, value)
    except Exception:
        return None


def safe_find_elements(driver_or_element, by, value) -> List[Any]:
    try:
        return driver_or_element.find_elements(by, value)
    except Exception:
        return []


# -------- Scraping logic --------

def scrape_series_table(
    driver: webdriver.Chrome, parent_url: str
) -> List[Dict[str, Any]]:
    """Scrape the listing table on the parent discourse page."""
    logger.info(f"Loading series page: {parent_url}")
    driver.get(parent_url)
    wait = WebDriverWait(driver, TIMEOUT)

    # A more specific selector for the main table containing the discourse list.
    # This looks for a table inside the main content area.
    table = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "article table"))
    )

    # Select all table rows that have at least one table cell (td), ignoring header rows.
    rows = safe_find_elements(table, By.CSS_SELECTOR, "tbody tr")
    chapters = []

    for row in rows:
        if SHOULD_STOP:
            break

        # Get all cells in the row.
        tds = safe_find_elements(row, By.CSS_SELECTOR, "td")
        if len(tds) < 2:
            continue

        # Cell 1: Chapter Title and URL
        # We look for the first link that is not an mp3 link.
        a_title = safe_find_element(tds[0], By.CSS_SELECTOR, "a:not([href$='.mp3'])")
        if not a_title:
            continue # Skip if no valid title link is found.

        chapter_name = a_title.text.strip()
        chapter_url = urljoin(parent_url, a_title.get_attribute("href") or "").strip()

        # Cell 2: Duration
        duration_el = safe_find_element(tds[1], By.CSS_SELECTOR, "a")
        duration = duration_el.text.strip() if duration_el else None

        # Cell 3: MP3 URL
        mp3_a = safe_find_element(tds[2], By.CSS_SELECTOR, "a[href$='.mp3']")
        mp3_url = urljoin(parent_url, mp3_a.get_attribute("href") or "").strip() if mp3_a else None

        # --- Improved Chapter Number Extraction ---
        chapter_number = None
        # Try to get it from the last part of the chapter name
        name_parts = chapter_name.split()
        if name_parts:
            last_part = name_parts[-1]
            # Remove non-digit characters and convert to int
            digits = "".join(filter(str.isdigit, last_part))
            if digits:
                try:
                    chapter_number = int(digits)
                except ValueError:
                    pass # Could not convert to number

        # Fallback: Try to get it from the URL slug
        if chapter_number is None:
            try:
                slug = chapter_url.rstrip("/").split("/")[-1]
                # Extract the numeric part of the slug, e.g., "ek-omkar-satnam-01" -> "01"
                slug_parts = slug.split('-')
                if slug_parts:
                    last_slug_part = slug_parts[-1]
                    slug_digits = "".join(filter(str.isdigit, last_slug_part))
                    if slug_digits:
                        chapter_number = int(slug_digits)
            except (ValueError, IndexError):
                pass # Failed to parse from URL

        chapters.append(
            {
                "chapter_number": chapter_number,
                "chapter_name": chapter_name,
                "chapter_url": chapter_url,
                "duration": duration,
                "mp3_url": mp3_url,
            }
        )

    logger.info(f"Found {len(chapters)} chapters in the table.")
    return chapters


def scrape_chapter_page(driver, chapter: dict) -> dict:
    """Scrape chapter page for title, language, transcript, image, etc."""
    chapter_url = chapter["chapter_url"]
    logger.info(f"  Scraping chapter: {chapter_url}")
    driver.get(chapter_url)
    wait = WebDriverWait(driver, TIMEOUT)

    try:
        # Wait for the main article content to be present
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "article.post")))
    except Exception:
        logger.warning(f"  Timeout waiting for content on: {chapter_url}")
        return chapter # Return original data if page fails to load

    # --- Title from h1 ---
    # More specific selector for the main heading
    h1 = safe_find_element(driver, By.CSS_SELECTOR, "article.post h1.entry-title")
    if h1 and h1.text.strip():
        chapter["chapter_name"] = h1.text.strip()

    # --- MP3 Fallback ---
    if not chapter.get("mp3_url"):
        # Look for a download link with an MP3 extension
        mp3_a = safe_find_element(driver, By.CSS_SELECTOR, "a[href$='.mp3'][download]")
        if mp3_a:
            chapter["mp3_url"] = urljoin(chapter_url, mp3_a.get_attribute("href") or "").strip()

    # --- Duration Fallback ---
    if not chapter.get("duration"):
        # Look for the duration text, which is often in a link next to the audio player
        duration_el = safe_find_element(driver, By.CSS_SELECTOR, "a.powerpress_link_d")
        if duration_el and ":" in duration_el.text:
            chapter["duration"] = duration_el.text.strip()

    # --- Language Extraction ---
    # Find the element containing 'Language:' and get the text that follows.
    try:
        lang_el = driver.find_element(By.XPATH, "//*[contains(text(), 'Language:')]")
        # Extract the text following "Language:" and clean it up
        lang_text = driver.execute_script(
            "return arguments[0].textContent.split('Language:')[1];", lang_el
        )
        if lang_text:
            # Remove other metadata that might be on the same line
            for marker in ["Share", "Download", "Whatsapp"]:
                if marker in lang_text:
                    lang_text = lang_text.split(marker)[0]
            chapter["language"] = lang_text.strip()
    except Exception:
        logger.debug(f"  Could not find language element on {chapter_url}")
        chapter["language"] = None # Explicitly set to None if not found

    # --- Image URL ---
    # A more specific selector for the main image within the article
    image_el = safe_find_element(driver, By.CSS_SELECTOR, "article.post .wp-block-image img")
    if image_el:
        chapter["image_url"] = urljoin(chapter_url, image_el.get_attribute("src") or "").strip()
    else:
        # Fallback for other possible image locations
        image_el = safe_find_element(driver, By.CSS_SELECTOR, "article.post img")
        if image_el:
            chapter["image_url"] = urljoin(chapter_url, image_el.get_attribute("src") or "").strip()
        else:
            chapter["image_url"] = None

    # --- Transcript Extraction ---
    # Find the main content area and extract text from all paragraphs.
    content_el = safe_find_element(driver, By.CSS_SELECTOR, ".entry-content")
    if content_el:
        paragraphs = safe_find_elements(content_el, By.TAG_NAME, "p")
        # Join the text from each paragraph, separated by two newlines for readability.
        transcript_parts = [p.text.strip() for p in paragraphs if p.text.strip()]
        chapter["transcript"] = "\n\n".join(transcript_parts)
    else:
        logger.warning(f"  No content container found for transcript on {chapter_url}")
        chapter["transcript"] = ""

    return chapter


# -------- High-level orchestration --------

def load_series_list(path: Path, default_language: str) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        lst = json.load(f)
    for entry in lst:
        entry.setdefault("language", default_language)
    return lst


def output_filename_for_series(series: Dict[str, Any]) -> Path:
    title = series["title"]
    safe = "".join(
        c for c in title if c.isalnum() or c in (" ", "_", "-", "(", ")", "अ", "आ", "आ", "ं", "ु", "ि", "े", "ै", "ो", "ौ")
    )
    safe = safe.strip().replace(" ", "_")
    return OUTPUT_DIR / f"{safe}.json"


def scrape_one_series(
    driver: webdriver.Chrome, series: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Scrape a complete discourse (all chapters)."""
    if SHOULD_STOP:
        return None

    title = series["title"]
    parent_url = series["url"]
    language = series.get("language", "unknown")
    start_ep = series.get("start_episode")
    end_ep = series.get("end_episode")

    logger.info(f"Processing series: {title} ({language})")

    try:
        table_chapters = scrape_series_table(driver, parent_url)
    except Exception as e:
        logger.error(f"Failed to scrape series table for {title}: {e}")
        return None # Critical error, cannot proceed

    # If no chapters were found in the table, abort for this series.
    if not table_chapters:
        logger.warning(f"No chapters found in the table for {title}. Skipping.")
        return None

    chapters: List[Dict[str, Any]] = []
    for ch in table_chapters:
        if SHOULD_STOP:
            logger.info("Stopping chapter scraping...")
            break
        try:
            # Scrape the full details for each chapter
            enriched_chapter = scrape_chapter_page(driver, ch)
            chapters.append(enriched_chapter)
        except Exception as e:
            logger.error(f"  An error occurred while scraping chapter {ch.get('chapter_url')}: {e}")

    if SHOULD_STOP and not chapters:
        logger.info(f"Series {title} interrupted before any chapters were completed.")
        return None

    result = {
        "title": title,
        "language": language,
        "parent_url": parent_url,
        "start_episode": start_ep,
        "end_episode": end_ep,
        "total_chapters_found": len(chapters),
        "chapters": chapters,
    }
    return result


def main():
    global SHOULD_STOP

    # Load all series
    hindi_series = load_series_list(INPUT_HINDI, "hindi")
    english_series = load_series_list(INPUT_ENGLISH, "english")
    all_series = hindi_series + english_series
    logger.info(f"Loaded {len(all_series)} series to scrape.")

    driver = setup_driver()

    try:
        for idx, series in enumerate(all_series, start=1):
            if SHOULD_STOP:
                logger.info("Global stop flag set.")
                break

            out_path = output_filename_for_series(series)
            if out_path.exists():
                logger.info(f"[{idx}/{len(all_series)}] Skipping already scraped: {series['title']}")
                continue  # already scraped

            logger.info(f"[{idx}/{len(all_series)}] Scraping series: {series['title']}")
            result = scrape_one_series(driver, series)
            
            if result and result.get("chapters"):
                # Write the scraped data to a JSON file
                try:
                    with out_path.open("w", encoding="utf-8") as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    logger.info(f"✅ Successfully saved to {out_path}")
                except Exception as e:
                    logger.error(f"❌ Failed to save file {out_path}: {e}")
            elif result:
                logger.warning(f"No chapters found for {series['title']}. Not saving file.")
            else:
                logger.info(f"Skipped saving for {series['title']} due to interruption or failure.")

    finally:
        logger.info("Scraping finished. Closing driver.")
        driver.quit()