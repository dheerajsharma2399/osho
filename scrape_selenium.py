import re
import json
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


def find_chapter_links(driver, list_url: str) -> List[str]:
    driver.get(list_url)
    WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    base = '/'.join(list_url.split('/')[:3])
    slug = list_url.rstrip('/').split('/')[-1].split('#')[0]
    prefix = re.sub(r'-by-.*$', '', slug)
    links: List[str] = []

    # 1) Prefer explicit links inside any table (series/episode tables)
    try:
        table_anchors = driver.find_elements(By.CSS_SELECTOR, 'table a[href]')
        for a in table_anchors:
            href = a.get_attribute('href')
            if href and href.startswith(base) and href not in links:
                links.append(href)
    except Exception:
        pass

    # 2) Next, look for anchors that match the series prefix like '/{prefix}-<number>'
    try:
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
        pat = re.compile(rf'/{re.escape(prefix)}-\d+')
        for a in anchors:
            href = a.get_attribute('href')
            if not href or not href.startswith(base):
                continue
            if href in links:
                continue
            if pat.search(href):
                links.append(href)
    except Exception:
        pass

    # 3) Fallback: any internal links (keeps previous behavior)
    if not links:
        try:
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            for a in anchors:
                href = a.get_attribute('href')
                if href and href.startswith(base) and href not in links:
                    links.append(href)
        except Exception:
            pass

    return links


def filter_out_assets(urls: List[str]) -> List[str]:
    cleaned: List[str] = []
    for u in urls:
        if u.lower().endswith('.mp3'):
            continue
        # common upload asset path
        if '/wp-content/uploads/' in u and u.lower().endswith(('.jpg', '.png', '.mp3')):
            continue
        cleaned.append(u)
    return cleaned


def extract_chapter(driver, url: str) -> Dict:
    driver.get(url)
    WebDriverWait(driver, 15).until(lambda d: d.execute_script('return document.readyState') == 'complete')
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

    # Chapter number: from URL or from title
    chapter_number = ''
    m = re.search(r'-?(\d{1,3})$', url.rstrip('/'))
    if m:
        chapter_number = m.group(1)
    else:
        m2 = re.search(r'(?:\b|\D)(\d{1,3})\b', title)
        if m2:
            chapter_number = m2.group(1)

    # Language: robust extraction and normalization
    language = ''
    try:
        # prefer visible 'Language' label in page text nodes
        els = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'LANGUAGE', 'language'), 'language')]")
        for el in els:
            txt = el.text.strip()
            if not txt:
                continue
            m = re.search(r'[Ll]anguage\s*[:\-\u00A0]?\s*([A-Za-z\u0900-\u097F\u0400-\u04FF]+)', txt)
            if m:
                language = m.group(1).strip()
                break
        # try regex over page source for a word-like token after 'Language'
        if not language:
            m2 = re.search(r'Language\s*[:\-\u00A0]?\s*([A-Za-z\u0900-\u097F\u0400-\u04FF]+)', html, re.I)
            if m2:
                language = m2.group(1).strip()
        # fallback: search for known language names anywhere in the HTML
        if not language:
            known_map = {
                'hindi': ['हिन्दी', 'हिंदी', 'hindi'],
                'english': ['english', 'eng', 'en', 'अंग्रेज़ी', 'अंग्रेजी'],
                'sanskrit': ['sanskrit', 'संस्कृत'],
                'gujarati': ['gujarati', 'ગુજરાતી'],
                'bengali': ['bengali', 'বাংলা'],
                'tamil': ['tamil', 'தமிழ்'],
                'telugu': ['telugu', 'తెలుగు'],
                'kannada': ['kannada', 'ಕನ್ನಡ'],
                'malayalam': ['malayalam', 'മലയാളം'],
                'urdu': ['urdu', 'اردو'],
                'punjabi': ['punjabi', 'ਪੰਜਾਬੀ']
            }
            lower_html = html.lower()
            for canon, variants in known_map.items():
                for v in variants:
                    if v.lower() in lower_html:
                        language = canon
                        break
                if language:
                    break
        # final fallback: html lang attribute mapping
        if not language:
            lang_attr = (driver.find_element(By.CSS_SELECTOR, 'html').get_attribute('lang') or '').strip()
            if lang_attr:
                lang_map = {'hi': 'hindi', 'en': 'english'}
                language = lang_map.get(lang_attr.lower(), lang_attr.lower())
    except Exception:
        language = ''
    # normalize value to canonical english name where possible
    if language:
        language = language.strip().lower().strip(':;,-. ')
        native_map = {'हिन्दी': 'hindi', 'हिंदी': 'hindi', 'अंग्रेज़ी': 'english', 'अंग्रेजी': 'english', 'eng': 'english', 'en': 'english', 'hi': 'hindi'}
        language = native_map.get(language, language)

    # Duration: regex search for hh:mm:ss
    duration = ''
    m = re.search(r'\b\d{1,2}:\d{2}:\d{2}\b', html)
    if m:
        duration = m.group(0)

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
            imgs = driver.find_elements(By.CSS_SELECTOR, 'img')
            for i in imgs:
                src = i.get_attribute('src') or i.get_attribute('data-src')
                if src and 'no_image' not in src:
                    image_url = src
                    break
        except Exception:
            image_url = ''

    # Transcript: try several likely containers with HTML handling (preserve <br> breaks)
    transcript_full = ''
    tried_selectors = [
        'div.entry-content', 'div.post-content', 'article .entry-content', 'article', 'div#content', 'div.post', 'div.post-inner'
    ]
    for sel in tried_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            # Get HTML and convert <br> to newlines, then extract text to preserve breaks
            html_content = el.get_attribute('innerHTML')
            if html_content:
                # Replace <br> and <br/> and <br /> with newlines
                html_content = re.sub(r'<br\s*/?>', '\n', html_content, flags=re.IGNORECASE)
                # Remove HTML tags but keep newlines
                text_only = re.sub(r'<[^>]+>', '', html_content)
                # Clean up extra whitespace while preserving intentional breaks
                text_only = re.sub(r'\n\s*\n+', '\n\n', text_only).strip()
                if len(text_only) > 40:
                    transcript_full = text_only
                    break
        except Exception:
            pass
    
    # Fallback to rendered text if above didn't work
    if not transcript_full:
        for sel in tried_selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                text = el.text.strip()
                if len(text) > 40:
                    transcript_full = text
                    break
            except Exception:
                continue

    if not transcript_full:
        ps = driver.find_elements(By.CSS_SELECTOR, 'p')
        longps = [p.text.strip() for p in ps if len(p.text.strip()) > 80]
        if longps:
            transcript_full = '\n\n'.join(longps[:10])

    if not transcript_full:
        # final fallback: use large chunk of body text
        try:
            body = driver.find_element(By.CSS_SELECTOR, 'body').text or ''
            transcript_full = '\n'.join([line.strip() for line in body.splitlines() if len(line.strip()) > 100][:20])
        except Exception:
            transcript_full = ''

    # Split transcript into paragraphs for caption generation
    # Split on double newlines first (major breaks), then on single newlines (minor breaks)
    transcript_paragraphs = []
    if transcript_full:
        # Split on double newlines (major paragraph breaks)
        major_paras = transcript_full.split('\n\n')
        for para in major_paras:
            if para.strip():
                # Further split on single newlines if paragraph is too long
                if len(para) > 200:
                    minor_paras = para.split('\n')
                    for minor in minor_paras:
                        if minor.strip():
                            transcript_paragraphs.append(minor.strip())
                else:
                    transcript_paragraphs.append(para.strip())

    # assemble
    chapter = {
        'title': title,
        'chapter_number': chapter_number,
        'url': url,
        'language': language,
        'duration': duration,
        'image_url': image_url,
        'mp3_links': list(dict.fromkeys(mp3_links)),
        'transcript': transcript_full,  # Full text with breaks preserved
        'transcript_paragraphs': transcript_paragraphs,  # Split for caption generation
    }
    return chapter


def save_per_language(item: Dict, out_dir: Path):
    # kept for compatibility; prefer per-discourse save below
    lang = (item.get('language') or 'unknown').lower()
    fname = out_dir / f"{lang}.json"
    data = []
    if fname.exists():
        try:
            with fname.open('r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = []
    urls = {d.get('url') for d in data}
    if item['url'] not in urls:
        data.append(item)
        tmp = fname.with_suffix('.tmp')
        with tmp.open('w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(fname)


def save_discourse_json(discourse_name: str, discourse_url: str, chapters: List[Dict], out_dir: Path):
    slug = discourse_url.rstrip('/').split('/')[-1]
    fname = out_dir / f"{slug}.json"
    obj = {
        'discourse_name': discourse_name,
        'discourse_url': discourse_url,
        'chapters': chapters,
    }
    tmp = fname.with_suffix('.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(fname)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('list_url', help='Discourse/list page URL (e.g. series page)')
    parser.add_argument('--headless', action='store_true', help='Run chrome headless')
    parser.add_argument('--out', default='.', help='Output directory for json files')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of chapters (0 = all)')
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    driver = make_driver(headless=args.headless)
    try:
        # gather series title
        driver.get(args.list_url)
        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        try:
            series_title = driver.find_element(By.CSS_SELECTOR, 'h1').text.strip()
        except Exception:
            series_title = ''

        links = find_chapter_links(driver, args.list_url)
        # remove asset links like direct mp3s
        links = filter_out_assets(links)
        print(f'Found {len(links)} candidate chapter links')
        if args.limit and args.limit > 0:
            links = links[: args.limit]

        chapters: List[Dict] = []
        for i, l in enumerate(links, 1):
            print(f'[{i}/{len(links)}] Visiting {l}')
            try:
                item = extract_chapter(driver, l)
                chapters.append(item)
            except Exception as e:
                print('Error extracting', l, e)

        # save per-discourse JSON
        save_discourse_json(series_title or args.list_url, args.list_url, chapters, out_dir)
    finally:
        driver.quit()


if __name__ == '__main__':
    main()
