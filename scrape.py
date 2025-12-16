from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re

def extract_episode_range(title):
    """Extract episode range from title like '# 1-17'"""
    match = re.search(r'#?\s*(\d+)-(\d+)', title)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def generate_mp3_links_from_first(first_mp3_url, start_ep, end_ep):
    """Generate all MP3 links based on the first episode URL"""
    if not first_mp3_url:
        return []
    
    base_pattern = re.sub(r'_0*1\.mp3$', '', first_mp3_url)
    
    mp3_links = []
    for ep in range(start_ep, end_ep + 1):
        ep_str = str(ep).zfill(2)
        mp3_url = f"{base_pattern}_{ep_str}.mp3"
        mp3_links.append(mp3_url)
    
    return mp3_links

def scrape_osho_mp3_links():
    """Scrape OSHO discourse MP3 links - DEBUG VERSION"""
    
    options = webdriver.ChromeOptions()
    # Run visible to debug
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        print("="*80)
        print("OSHO DISCOURSE SCRAPER - DEBUG VERSION")
        print("="*80)
        
        base_url = "https://oshoworld.com/audio-series-home-hindi"
        driver.get(base_url)
        
        print("\n⏳ Waiting for page to load...")
        time.sleep(6)
        
        discourse_series = []
        seen_urls = set()
        total_pages = 10
        
        for page_num in range(1, total_pages + 1):
            print(f"\n{'='*80}")
            print(f"📄 PAGE {page_num}/{total_pages}")
            print(f"{'='*80}")
            
            try:
                # Wait for content
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(3)
                
                # Scroll to trigger lazy loading
                print("📜 Scrolling to load content...")
                last_height = driver.execute_script("return document.body.scrollHeight")
                for scroll_attempt in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                # DEBUG: Find ALL links and log them
                print("\n🔍 DEBUG: Finding ALL links on page...")
                all_page_links = driver.find_elements(By.TAG_NAME, "a")
                
                discourse_candidates = []
                for link in all_page_links:
                    try:
                        href = link.get_attribute('href')
                        if href and 'oshoworld.com' in href:
                            # Exclude non-discourse pages
                            if any(x in href for x in ['/audio-series-home', '/search', '/category', 
                                                        '/tag', '/page/', '/wp-', '/feed', 
                                                        'oshoworld.com/#', 'oshoworld.com/?']):
                                continue
                            
                            # Exclude homepage
                            if href.rstrip('/') == 'https://oshoworld.com':
                                continue
                            
                            discourse_candidates.append(link)
                    except:
                        pass
                
                print(f"Found {len(discourse_candidates)} potential discourse links")
                
                # DEBUG: Print first 20 URLs to see patterns
                print("\n📋 Sample URLs found:")
                for i, link in enumerate(discourse_candidates[:20]):
                    href = link.get_attribute('href')
                    text = link.text.strip()[:50]
                    print(f"  {i+1}. {href}")
                    if text:
                        print(f"      Text: {text}")
                
                # Now process each link
                print("\n🔍 Processing discourse links...")
                
                page_count = 0
                page_discourses = []
                
                for link in discourse_candidates:
                    try:
                        href = link.get_attribute('href')
                        
                        # Skip if already seen
                        if href in seen_urls:
                            continue
                        
                        # Find parent container to get full context
                        try:
                            container = link.find_element(By.XPATH, "./ancestor::div[contains(@class, 'post') or contains(@class, 'entry') or contains(@class, 'card') or position()<=3][1]")
                        except:
                            try:
                                container = link.find_element(By.XPATH, "./ancestor::div[2]")
                            except:
                                container = link
                        
                        container_text = container.text
                        
                        # Look for episode pattern
                        episode_match = re.search(r'#?\s*(\d+)\s*-\s*(\d+)', container_text)
                        if not episode_match:
                            continue
                        
                        start_ep = int(episode_match.group(1))
                        end_ep = int(episode_match.group(2))
                        
                        # Extract title
                        title = None
                        
                        # Try to find heading in container
                        for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'strong', 'b']:
                            try:
                                heading = container.find_element(By.TAG_NAME, tag)
                                heading_text = heading.text.strip()
                                if heading_text and len(heading_text) > 5:
                                    title = heading_text
                                    break
                            except:
                                pass
                        
                        # Fallback: use first line with episode pattern
                        if not title:
                            lines = container_text.split('\n')
                            for line in lines:
                                if re.search(r'#?\s*\d+\s*-\s*\d+', line) and len(line.strip()) > 5:
                                    title = line.strip()
                                    break
                        
                        # Last resort: use link text
                        if not title:
                            title = link.text.strip()
                        
                        # Clean title
                        if title:
                            title = re.sub(r'\s*View all\s*', '', title, flags=re.IGNORECASE)
                            title = re.sub(r'\s*Play\s*&?\s*Download\s*', '', title, flags=re.IGNORECASE)
                            title = re.sub(r'\s*\d+\s*Discourses.*', '', title, flags=re.IGNORECASE)
                            title = title.strip()
                        
                        if not title or len(title) < 3:
                            print(f"  ⚠️  Skipped (no title): {href[:60]}")
                            continue
                        
                        # Valid discourse found!
                        seen_urls.add(href)
                        discourse_info = {
                            'title': title,
                            'url': href,
                            'start_episode': start_ep,
                            'end_episode': end_ep
                        }
                        discourse_series.append(discourse_info)
                        page_discourses.append(discourse_info)
                        page_count += 1
                        
                        print(f"  ✅ [{page_count}] {title[:70]}")
                        print(f"      URL: {href}")
                    
                    except Exception as e:
                        continue
                
                print(f"\n📊 Page {page_num} Summary: {page_count} discourses found")
                print(f"📈 Total so far: {len(discourse_series)} discourses")
                
                # Save progress
                with open(f'progress_page_{page_num}.json', 'w', encoding='utf-8') as f:
                    json.dump(page_discourses, f, ensure_ascii=False, indent=2)
                print(f"💾 Saved progress to: progress_page_{page_num}.json")
                
                # Save ALL links for debugging
                debug_links = []
                for link in discourse_candidates:
                    try:
                        debug_links.append({
                            'href': link.get_attribute('href'),
                            'text': link.text.strip()[:100]
                        })
                    except:
                        pass
                
                with open(f'debug_all_links_page_{page_num}.json', 'w', encoding='utf-8') as f:
                    json.dump(debug_links, f, ensure_ascii=False, indent=2)
                print(f"🐛 Saved debug info to: debug_all_links_page_{page_num}.json")
                
                # Navigate to next page
                if page_num < total_pages:
                    print(f"\n➡️  Navigating to page {page_num + 1}...")
                    time.sleep(2)
                    
                    next_clicked = False
                    
                    # Method 1: Find pagination buttons
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        
                        page_buttons = driver.find_elements(By.XPATH, 
                            f"//button[text()='{page_num + 1}'] | //a[text()='{page_num + 1}']")
                        
                        if page_buttons:
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_buttons[0])
                            time.sleep(1)
                            driver.execute_script("arguments[0].click();", page_buttons[0])
                            next_clicked = True
                            print(f"  ✅ Clicked page {page_num + 1} button")
                    except Exception as e:
                        print(f"  ⚠️  Button click failed: {e}")
                    
                    # Method 2: Try Next button
                    if not next_clicked:
                        try:
                            next_buttons = driver.find_elements(By.XPATH, 
                                "//button[contains(text(), 'Next')] | //a[contains(text(), 'Next')] | "
                                "//button[contains(@aria-label, 'next')] | //a[contains(@aria-label, 'next')]")
                            
                            if next_buttons:
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_buttons[0])
                                time.sleep(1)
                                driver.execute_script("arguments[0].click();", next_buttons[0])
                                next_clicked = True
                                print("  ✅ Clicked Next button")
                        except Exception as e:
                            print(f"  ⚠️  Next button failed: {e}")
                    
                    # Method 3: URL navigation
                    if not next_clicked:
                        print("  ⚠️  Using direct URL navigation")
                        driver.get(f"{base_url}?page={page_num + 1}")
                    
                    time.sleep(4)
                    
            except Exception as e:
                print(f"\n❌ Error on page {page_num}:")
                print(f"   {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n{'='*80}")
        print(f"✅ COLLECTION PHASE COMPLETE")
        print(f"{'='*80}")
        print(f"📚 Total discourse series collected: {len(discourse_series)}")
        
        # Save all collected series
        with open('collected_series_all.json', 'w', encoding='utf-8') as f:
            json.dump(discourse_series, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved to: collected_series_all.json")
        
        # STEP 2: Extract MP3 links
        print(f"\n{'='*80}")
        print("STEP 2: EXTRACTING MP3 LINKS")
        print(f"{'='*80}\n")
        
        all_data = []
        successful = 0
        failed = 0
        failed_list = []
        
        for idx, discourse in enumerate(discourse_series, 1):
            print(f"[{idx}/{len(discourse_series)}] {discourse['title'][:65]}")
            
            try:
                driver.get(discourse['url'])
                time.sleep(2)
                
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Find MP3 links
                mp3_links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.mp3']")
                
                if mp3_links:
                    first_mp3_url = mp3_links[0].get_attribute('href')
                    
                    all_mp3_links = generate_mp3_links_from_first(
                        first_mp3_url,
                        discourse['start_episode'],
                        discourse['end_episode']
                    )
                    
                    all_data.append({
                        'discourse_name': discourse['title'],
                        'discourse_url': discourse['url'],
                        'mp3_links': all_mp3_links
                    })
                    
                    successful += 1
                    print(f"  ✅ {len(all_mp3_links)} MP3 links generated")
                else:
                    print(f"  ❌ No MP3s found")
                    failed += 1
                    failed_list.append(discourse['title'])
                
            except Exception as e:
                print(f"  ❌ Error: {str(e)[:50]}")
                failed += 1
                failed_list.append(discourse['title'])
        
        # STEP 3: Save final data
        print(f"\n{'='*80}")
        print("STEP 3: SAVING FINAL DATA")
        print(f"{'='*80}")
        
        with open('osho_mp3_links.json', 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print("✅ osho_mp3_links.json")
        
        stats = {
            'total_series_found': len(discourse_series),
            'mp3_extraction_successful': successful,
            'mp3_extraction_failed': failed,
            'failed_discourses': failed_list,
            'total_mp3_files': sum(len(d['mp3_links']) for d in all_data)
        }
        
        with open('osho_stats.json', 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        print("✅ osho_stats.json")
        
        print(f"\n{'='*80}")
        print("🎉 SCRAPING COMPLETE!")
        print(f"{'='*80}")
        print(f"📚 Series found: {len(discourse_series)}")
        print(f"✅ MP3 extraction successful: {successful}")
        print(f"❌ MP3 extraction failed: {failed}")
        print(f"🎵 Total MP3 files: {stats['total_mp3_files']}")
        print(f"{'='*80}")
        
        if failed_list:
            print(f"\n⚠️  Failed discourses ({len(failed_list)}):")
            for name in failed_list[:10]:
                print(f"  • {name}")
            if len(failed_list) > 10:
                print(f"  ... and {len(failed_list) - 10} more")
        
        return all_data
        
    finally:
        driver.quit()

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🕉️  OSHO DISCOURSE MP3 SCRAPER - DEBUG VERSION")
    print("="*80 + "\n")
    
    try:
        data = scrape_osho_mp3_links()
        print("\n✅ All done! Check the JSON files for results.")
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()