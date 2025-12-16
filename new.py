from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Force ChromeDriverManager to download fresh driver
os.environ['WDM_LOCAL'] = '1'

def extract_episode_range(title):
    """Extract episode range from title"""
    match = re.search(r'#?\s*(\d+)\s*-\s*(\d+)', title)
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)

def generate_mp3_links(first_mp3_url, start_ep, end_ep):
    """Generate all MP3 links from first URL"""
    if not first_mp3_url:
        return []
    base_pattern = re.sub(r'_0*1\.mp3$', '', first_mp3_url)
    return [f"{base_pattern}_{str(ep).zfill(2)}.mp3" for ep in range(start_ep, end_ep + 1)]

def scrape_discourse_list(driver, total_pages=10):
    """Fast scraping of discourse list"""
    print("🚀 Collecting discourse URLs...")
    
    base_url = "https://oshoworld.com/audio-series-home-hindi"
    driver.get(base_url)
    time.sleep(4)
    
    discourse_series = []
    seen_urls = set()
    
    for page_num in range(1, total_pages + 1):
        print(f"Page {page_num}/{total_pages}...", end=" ", flush=True)
        
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.5)
            
            # Quick scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.8)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.8)
            
            # Get all discourse links
            all_links = driver.find_elements(By.TAG_NAME, "a")
            page_found = 0
            
            for link in all_links:
                try:
                    href = link.get_attribute('href')
                    if not href or href in seen_urls or 'oshoworld.com' not in href:
                        continue
                    
                    # Skip non-discourse pages
                    if any(x in href for x in ['/audio-series-home', '/search', '/category', 
                                                '/tag', '/page/', '/wp-', '/feed', '/#', '/?']):
                        continue
                    
                    if href.rstrip('/') == 'https://oshoworld.com':
                        continue
                    
                    # Get container text
                    try:
                        container = link.find_element(By.XPATH, "./ancestor::div[2]")
                        container_text = container.text
                    except:
                        container_text = link.text
                    
                    # Check for episode pattern
                    episode_match = re.search(r'#?\s*(\d+)\s*-\s*(\d+)', container_text)
                    if not episode_match:
                        continue
                    
                    start_ep = int(episode_match.group(1))
                    end_ep = int(episode_match.group(2))
                    
                    # Extract title
                    title = None
                    try:
                        for tag in ['h2', 'h3', 'h4']:
                            try:
                                heading = container.find_element(By.TAG_NAME, tag)
                                title = heading.text.strip()
                                if title and len(title) > 5:
                                    break
                            except:
                                pass
                    except:
                        pass
                    
                    if not title:
                        lines = container_text.split('\n')
                        for line in lines:
                            if re.search(r'#?\s*\d+\s*-\s*\d+', line) and len(line.strip()) > 5:
                                title = line.strip()
                                break
                    
                    if not title or len(title) < 3:
                        continue
                    
                    # Clean title
                    title = re.sub(r'\s*(View all|Play.*|Download.*|\d+\s*Discourses.*)', '', title, flags=re.IGNORECASE).strip()
                    
                    seen_urls.add(href)
                    discourse_series.append({
                        'title': title,
                        'url': href,
                        'start_episode': start_ep,
                        'end_episode': end_ep
                    })
                    page_found += 1
                
                except:
                    continue
            
            print(f"{page_found} found")
            
            # Navigate to next page
            if page_num < total_pages:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.8)
                    
                    page_buttons = driver.find_elements(By.XPATH, 
                        f"//button[text()='{page_num + 1}'] | //a[text()='{page_num + 1}']")
                    
                    if page_buttons:
                        driver.execute_script("arguments[0].click();", page_buttons[0])
                    else:
                        driver.get(f"{base_url}?page={page_num + 1}")
                    
                    time.sleep(2.5)
                except:
                    break
        
        except Exception as e:
            print(f"Error: {e}")
            continue
    
    print(f"\n✅ Found {len(discourse_series)} discourses\n")
    return discourse_series

def extract_mp3_for_discourse(discourse_with_index):
    """Extract MP3 links for a single discourse"""
    idx, discourse = discourse_with_index
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--window-size=1280,720')
    options.add_argument('--log-level=3')
    
    # Use ChromeDriverManager to get correct version
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.set_page_load_timeout(10)
        driver.get(discourse['url'])
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        mp3_links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.mp3']")
        
        if mp3_links:
            first_mp3_url = mp3_links[0].get_attribute('href')
            all_mp3_links = generate_mp3_links(
                first_mp3_url,
                discourse['start_episode'],
                discourse['end_episode']
            )
            
            return (idx, {
                'discourse_name': discourse['title'],
                'discourse_url': discourse['url'],
                'mp3_links': all_mp3_links,
                'status': 'success'
            })
        else:
            return (idx, {
                'discourse_name': discourse['title'],
                'discourse_url': discourse['url'],
                'mp3_links': [],
                'status': 'no_mp3_found'
            })
    
    except Exception as e:
        return (idx, {
            'discourse_name': discourse['title'],
            'discourse_url': discourse['url'],
            'mp3_links': [],
            'status': f'error: {str(e)[:50]}'
        })
    
    finally:
        driver.quit()

def remove_none_from_list(data):
    """Remove None elements from a list"""
    return [d for d in data if d is not None]

def remove_duplicates(data):
    """Remove duplicate discourses by name"""
    seen_titles = set()
    unique_data = []
    duplicates_found = 0
    for item in data:
        title = item.get('discourse_name')
        if title and title not in seen_titles:
            seen_titles.add(title)
            unique_data.append(item)
        else:
            duplicates_found += 1
    return unique_data, duplicates_found

def main():
    print("="*70)
    print("⚡ FAST OSHO DISCOURSE SCRAPER")
    print("="*70 + "\n")
    
    print("🔧 Setting up ChromeDriver (downloading correct version)...")
    
    # Setup main driver with correct ChromeDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--log-level=3')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("✅ ChromeDriver ready\n")
    
    try:
        # Step 1: Collect all discourse URLs
        discourse_series = scrape_discourse_list(driver, total_pages=10)
        
        # Save discourse list
        with open('discourse_list.json', 'w', encoding='utf-8') as f:
            json.dump(discourse_series, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved discourse list to: discourse_list.json\n")
        
    finally:
        driver.quit()
    
    # Step 2: Extract MP3 links (parallel processing)
    print(f"🎵 Extracting MP3 links for {len(discourse_series)} discourses...")
    print("Using 8 parallel workers for maximum speed...\n")
    
    all_data = [None] * len(discourse_series)
    successful = 0
    failed = 0
    
    # Process 8 at a time for maximum speed
    max_workers = 8
    
    indexed_discourses = list(enumerate(discourse_series))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_mp3_for_discourse, item): item 
            for item in indexed_discourses
        }
        
        completed = 0
        for future in as_completed(futures):
            idx, result = future.result()
            all_data[idx] = result
            completed += 1
            
            if result['status'] == 'success':
                successful += 1
                status_icon = "✅"
            else:
                failed += 1
                status_icon = "❌"
            
            print(f"[{completed}/{len(discourse_series)}] {status_icon} {result['discourse_name'][:60]}")
    
    # Clean and process data
    print(f"\n{'='*70}")
    print("🧹 Cleaning data and removing duplicates...")
    
    # Remove None entries if any worker failed unexpectedly
    cleaned_data = remove_none_from_list(all_data)
    
    # Remove duplicates
    unique_data, duplicates_count = remove_duplicates(cleaned_data)
    print(f"🔍 Found and removed {duplicates_count} duplicate discourses.")
    
    # Save final data
    print("💾 Saving final data...")
    
    with open('osho_discourses_complete.json', 'w', encoding='utf-8') as f:
        json.dump(unique_data, f, ensure_ascii=False, indent=2)
    
    # Recalculate stats based on unique data
    final_successful = sum(1 for d in unique_data if d['status'] == 'success')
    final_failed = len(unique_data) - final_successful
    
    stats = {
        'total_discourses_scraped': len(discourse_series),
        'total_unique_discourses': len(unique_data),
        'duplicates_removed': duplicates_count,
        'successful_downloads': final_successful,
        'failed_downloads': final_failed,
        'total_mp3_files': sum(len(d.get('mp3_links', [])) for d in unique_data),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open('stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    print(f"{'='*70}")
    print("🎉 COMPLETE!")
    print(f"{'='*70}")
    print(f"📚 Total discourses scraped: {len(discourse_series)}")
    print(f"🔍 Unique discourses: {len(unique_data)} (removed {duplicates_count} duplicates)")
    print(f"✅ Successful: {final_successful}")
    print(f"❌ Failed: {final_failed}")
    print(f"🎵 Total MP3 files: {stats['total_mp3_files']}")
    print(f"\n📁 Output: osho_discourses_complete.json")
    print(f"{'='*70}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    print(f"\n⏱️  Completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")