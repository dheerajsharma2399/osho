"""
Script 2: Visit each discourse link, find the first MP3,
and generate all MP3 links for the series.

Input: discourse_links.json
Output: osho_mp3_links.json
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

def load_discourse_links():
    """Loads the links from discourse_links.json"""
    input_file = 'discourse_links.json'
    if not os.path.exists(input_file):
        print(f"❌ Error: '{input_file}' not found.")
        print("Please run Script 1 first to generate this file.")
        return None
        
    with open(input_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def scrape_first_mp3(driver, url):
    """
    Visits a discourse page and scrapes the *first* MP3 download link.
    It uses the `download` attribute to find the link.
    """
    try:
        driver.get(url)
        # Wait for the download link to be present
        # This selector targets an <a> tag with a `download` attribute that ends in .mp3
        download_link = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[download$='.mp3']"))
        )
        
        href = download_link.get_attribute('href')
        
        # Make the URL absolute
        if href and href.startswith('/'):
            href = "https://oshoworld.com" + href
            
        return href
        
    except Exception as e:
        print(f" [Error finding MP3: {str(e)[:50]}] ", end="")
        return None

def generate_mp3_links(first_mp3_url, start_ep, end_ep):
    """
    Generates all MP3 links based on the first URL and the episode range.
    
    Example:
    first_mp3_url = .../OSHO-Adhyatam_Upanishad_01.mp3
    start_ep = 1
    end_ep = 17
    
    Generates:
    .../OSHO-Adhyatam_Upanishad_01.mp3
    .../OSHO-Adhyatam_Upanishad_02.mp3
    ...
    .../OSHO-Adhyatam_Upanishad_17.mp3
    """
    if not first_mp3_url or start_ep is None or end_ep is None:
        return []

    # This regex finds the number part just before the .mp3 extension
    # It captures (base_url_part)(number)(.mp3)
    match = re.search(r'^(.*[_-])(\d+)(\.mp3)$', first_mp3_url)
    
    if not match:
        print(f" [Error: Could not parse URL pattern: {first_mp3_url}] ", end="")
        return []

    base_pattern = match.group(1) # e.g., ".../OSHO-Adhyatam_Upanishad_"
    number_str = match.group(2)   # e.g., "01"
    extension = match.group(3)    # e.g., ".mp3"
    
    # Detect padding (e.g., "01" has padding 2, "001" has padding 3)
    num_padding = len(number_str)
    
    mp3_links = []
    for ep in range(start_ep, end_ep + 1):
        # Format the episode number with the correct padding
        ep_str = str(ep).zfill(num_padding)
        mp3_url = f"{base_pattern}{ep_str}{extension}"
        mp3_links.append(mp3_url)
    
    return mp3_links

def main():
    print("="*70)
    print("🎧 STEP 2: EXTRACT MP3 LINKS")
    print("="*70 + "\n")
    
    discourse_series = load_discourse_links()
    if discourse_series is None:
        return
        
    print(f"✅ Loaded {len(discourse_series)} discourses from 'discourse_links.json'\n")
    
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
    
    all_data = []
    successful = 0
    failed = 0
    total_mp3s = 0
    
    try:
        for idx, discourse in enumerate(discourse_series, 1):
            print(f"[{idx}/{len(discourse_series)}] {discourse['title'][:50]}...", end=" ", flush=True)
            
            # Check if we have the episode range from Script 1
            start_ep = discourse.get('start_episode')
            end_ep = discourse.get('end_episode')
            
            if not start_ep or not end_ep:
                print("✗ Skipping (Missing episode range in JSON)")
                failed += 1
                continue
            
            # Scrape the first MP3 link from the page
            first_mp3_url = scrape_first_mp3(driver, discourse['url'])
            
            if not first_mp3_url:
                print("✗ Skipping (No MP3 link found on page)")
                failed += 1
                continue
                
            # Generate all links
            all_mp3_links = generate_mp3_links(first_mp3_url, start_ep, end_ep)
            
            if not all_mp3_links:
                print("✗ Skipping (Failed to generate links from pattern)")
                failed += 1
                continue
                
            all_data.append({
                'discourse_name': discourse['title'],
                'discourse_url': discourse['url'],
                'mp3_links': all_mp3_links
            })
            
            print(f"✓ {len(all_mp3_links)} MP3s")
            successful += 1
            total_mp3s += len(all_mp3_links)
            
    finally:
        driver.quit()

    # --- Save final data ---
    print(f"\n{'='*70}")
    print(f"✅ EXTRACTION COMPLETE")
    print(f"{'='*70}")
    print(f"📚 Discourses processed: {len(discourse_series)}")
    print(f"👍 Successful: {successful}")
    print(f"👎 Failed: {failed}")
    print(f"🎧 Total MP3 links generated: {total_mp3s}")
    
    # Save to JSON
    output_file = 'osho_mp3_links.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved to: {output_file}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    print(f"⏱️  Completed in {elapsed:.1f} seconds")