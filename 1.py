"""
Script 1: Collect all discourse links from OSHO World
Output: discourse_links.json
"""

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

def extract_episode_range(text_content):
    """
    Extract episode range from text like '... # 1-17'
    """
    match = re.search(r'#?\s*(\d+)\s*-\s*(\d+)', text_content)
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)

def scrape_discourse_list(driver, total_pages=10):
    """Scrape discourse list from all pages"""
    print("🚀 Collecting discourse URLs...")
    
    base_url = "https://oshoworld.com/audio-series-home-english"
    driver.get(base_url)
    time.sleep(3) # Wait for initial load
    
    discourse_series = []
    seen_urls = set()
    
    for page_num in range(1, total_pages + 1):
        print(f"📄 Page {page_num}/{total_pages}...", end=" ", flush=True)
        
        try:
            # --- CHANGED: Wait for the *correct* link based on your snippet ---
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.line-clamp-2.text-sky-700"))
            )
            time.sleep(0.5) 
            
            # --- CHANGED: Use the new, specific selector ---
            links = driver.find_elements(By.CSS_SELECTOR, "a.line-clamp-2.text-sky-700")
            page_found = 0
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    # Get text directly from this link, e.g., "Agyat Ki Aur (अज्ञात की ओर) # 1-7"
                    full_text = link.text.strip() 
                    
                    if not href or not full_text:
                        continue
                        
                    # Construct full URL for consistent processing
                    if href and href.startswith('/'):
                        href = "https://oshoworld.com" + href
                        
                    if not href or href in seen_urls:
                        continue
                        
                    seen_urls.add(href)
                    
                    # --- CHANGED: Logic is now simpler ---
                    
                    # 1. Extract range from the link text
                    start_ep, end_ep = extract_episode_range(full_text)
                    
                    # 2. Clean the title by removing the episode range part
                    title = re.sub(r'#?\s*\d+\s*-\s*\d+', '', full_text).strip()
                    
                    # 3. Fallback: Try to get range from URL
                    if not start_ep:
                        url_match = re.search(r'(\d+)-(\d+)', href)
                        if url_match:
                            start_ep = int(url_match.group(1))
                            end_ep = int(url_match.group(2))
                    
                    discourse_series.append({
                        'title': title,
                        'url': href,
                        'start_episode': start_ep, # Will be None if not found
                        'end_episode': end_ep   # Will be None if not found
                    })
                    page_found += 1
                
                except Exception as e:
                    print(f" [Skipping one link due to error: {e}] ")
                    continue
            
            print(f"✅ {page_found} found")
            
            # Navigate to next page
            if page_num < total_pages:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.8)
                    
                    next_page_btn = driver.find_element(By.XPATH, 
                        f"//button[text()='{page_num + 1}'] | //a[text()='{page_num + 1}']")
                    
                    driver.execute_script("arguments[0].click();", next_page_btn)
                    time.sleep(2.5) # Wait for page transition
                
                except Exception:
                    print(f"\nCould not find 'Next Page' button. Stopping.")
                    break # Stop if pagination fails
        
        except Exception as e:
            print(f"❌ Error on page {page_num}: {e}")
            continue
    
    return discourse_series

def main():
    print("="*70)
    print("📚 STEP 1: COLLECT DISCOURSE LINKS")
    print("="*70 + "\n")
    
    print("🔧 Setting up ChromeDriver...")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-sh_usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--log-level=3') 
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("✅ ChromeDriver ready\n")
    
    try:
        # Collect discourse URLs
        discourse_series = scrape_discourse_list(driver, total_pages=10)
        
        print(f"\n{'='*70}")
        print(f"✅ COLLECTION COMPLETE")
        print(f"{'='*70}")
        print(f"📚 Total discourses found: {len(discourse_series)}")
        
        # Save to JSON
        with open('discourse_links.json', 'w', encoding='utf-8') as f:
            json.dump(discourse_series, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Saved to: discourse_links.json")
        print(f"{'='*70}\n")
        
        # Print sample
        print("Sample discourses:")
        for i, disc in enumerate(discourse_series[:3], 1):
            print(f"  {i}. {disc['title']}")
            print(f"     {disc['url']}")
            print(f"     Episodes: {disc['start_episode']}-{disc['end_episode']}\n")
        
        sample_none = next((d for d in discourse_series if d['start_episode'] is None), None)
        if sample_none:
            print("---")
            print("Sample discourse (range not found, will be handled by next script):")
            print(f"  ?. {sample_none['title']}")
            print(f"     {sample_none['url']}")
            print(f"     Episodes: {sample_none['start_episode']}-{sample_none['end_episode']}\n")

        print(f"  ... and {len(discourse_series) - 3} more\n")
        print("✅ Ready for next step: Run script 2 to extract MP3 links")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    print(f"⏱️  Completed in {elapsed:.1f} seconds")