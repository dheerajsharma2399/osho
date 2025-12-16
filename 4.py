import json
import os
import re
import requests
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
STATE_FILE = "download_state.json"
DOWNLOAD_BASE_DIR = "Downloads"
MAX_THREADS = 16  # Max number of simultaneous downloads
# ---------------------

class DownloadManager:
    def __init__(self):
        self.state_file = STATE_FILE
        self.base_dir = DOWNLOAD_BASE_DIR
        self.downloaded_urls = self.load_state()
        self.exit_event = threading.Event()
        self.lock = threading.Lock()
        
        # Set up signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully."""
        print("\n\n🛑 Ctrl+C detected! Shutting down gracefully...")
        print("Downloads in progress will be stopped. State will be saved.")
        self.exit_event.set()

    def load_state(self):
        """Loads the set of already downloaded URLs from the state file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            except json.JSONDecodeError:
                print(f"Warning: '{self.state_file}' is corrupt. Starting fresh.")
                return set()
        return set()

    def save_state(self):
        """Saves the set of downloaded URLs to the state file."""
        print("\n💾 Saving download state...")
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.downloaded_urls), f, indent=2)
            print(f"State saved. {len(self.downloaded_urls)} total files downloaded.")
        except Exception as e:
            print(f"❌ Error saving state: {e}")

    def sanitize_filename(self, name):
        """Removes illegal characters from a path component."""
        return re.sub(r'[\\/*?:"<>|]', "", name).strip()

    def flatten_download_list(self, language_name, discourse_list):
        """
        Converts the nested JSON list into a flat list of download "jobs".
        Each job is a dict: {'url': '...', 'path': '...'}
        """
        flat_list = []
        for discourse in discourse_list:
            # Create a clean directory name for the discourse
            dir_name = self.sanitize_filename(discourse['discourse_name'])
            save_dir = os.path.join(self.base_dir, language_name, dir_name)
            
            for url in discourse['mp3_links']:
                try:
                    # Get filename from URL (e.g., OSHO-Adhyatam_Upanishad_01.mp3)
                    filename = url.split('/')[-1]
                    save_path = os.path.join(save_dir, filename)
                    flat_list.append({'url': url, 'path': save_path})
                except Exception as e:
                    print(f"\nWarning: Skipping invalid URL entry: {e}")
        
        return flat_list

    def download_file(self, job):
        """
        Downloads a single file.
        Returns (url, status_message)
        """
        url = job['url']
        path = job['path']
        
        # 1. Check if we should exit
        if self.exit_event.is_set():
            return (url, 'interrupted')

        # 2. Check if already downloaded (double-check)
        if url in self.downloaded_urls:
            return (url, 'skipped')

        # 3. Create directory
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # 4. Download to a .tmp file for atomic save
        tmp_path = path + '.tmp'
        
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(tmp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        # Check for exit *during* download
                        if self.exit_event.is_set():
                            raise KeyboardInterrupt
                        f.write(chunk)
            
            # 5. Download complete, rename .tmp to final name
            os.rename(tmp_path, path)
            
            # 6. Add to state (thread-safe)
            with self.lock:
                self.downloaded_urls.add(url)
                
            return (url, 'success')

        except KeyboardInterrupt:
            # Caused by self.exit_event.set()
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return (url, 'interrupted')
        
        except Exception as e:
            # Handle download errors (404, timeout, etc.)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return (url, f'failed: {str(e)[:50]}')

    def run(self):
        """Main interactive loop."""
        try:
            # 1. Ask for language
            print("Which discourses to download?")
            print("  1. Hindi")
            print("  2. English")
            lang_choice = input("Enter choice (1 or 2): ")
            
            if lang_choice == '1':
                json_file = 'osho_mp3_links.json' # The Hindi file
                lang_name = "Hindi"
            elif lang_choice == '2':
                json_file = 'osho_mp3_links_english.json'
                lang_name = "English"
            else:
                print("Invalid choice. Exiting.")
                return

            # 2. Load the main JSON file
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    discourses = json.load(f)
            except FileNotFoundError:
                print(f"❌ Error: '{json_file}' not found in this folder.")
                print("Please run the previous scripts first.")
                return
            
            # 3. Create the master list of all jobs
            all_jobs = self.flatten_download_list(lang_name, discourses)
            total_files = len(all_jobs)
            print(f"📚 Found {total_files} total MP3s for {lang_name}.")
            
            while not self.exit_event.is_set():
                # 4. Find pending jobs
                pending_jobs = [job for job in all_jobs if job['url'] not in self.downloaded_urls]
                
                if not pending_jobs:
                    print("\n🎉 All files have been downloaded! 🎉")
                    break
                    
                print(f"📊 Status: {len(self.downloaded_urls)} downloaded | {len(pending_jobs)} remaining.")
                
                # 5. Ask for batch size
                try:
                    batch_size_str = input("How many to download next? (e.g., 10, or 0 to exit): ")
                    batch_size = int(batch_size_str)
                except ValueError:
                    print("Invalid number. Please try again.")
                    continue
                except KeyboardInterrupt: # Handle Ctrl+C during input
                    self.signal_handler(None, None)
                    break
                    
                if batch_size == 0:
                    break
                
                # 6. Get the batch and start the threaded download
                batch_to_download = pending_jobs[:batch_size]
                num_workers = min(batch_size, MAX_THREADS)
                
                print(f"\n🚀 Starting download of {len(batch_to_download)} files with {num_workers} threads...")
                
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = [executor.submit(self.download_file, job) for job in batch_to_download]
                    
                    for future in as_completed(futures):
                        if self.exit_event.is_set():
                            break
                        
                        url, status = future.result()
                        filename = url.split('/')[-1]
                        
                        if status == 'success':
                            print(f"  ✅ Downloaded: {filename}")
                        elif status == 'failed':
                            print(f"  ❌ Failed: {filename} ({status})")
                        elif status == 'interrupted':
                            print(f"  🛑 Interrupted: {filename}")
                        # We don't print 'skipped' since they weren't in the pending list

        finally:
            # This block runs on normal exit OR Ctrl+C
            self.save_state()

# --- Main execution ---
if __name__ == "__main__":
    # Install requests if not present
    try:
        import requests
    except ImportError:
        print("Requests library not found. Installing...")
        os.system(f"{sys.executable} -m pip install requests")
        print("Install complete. Please run the script again.")
        sys.exit()

    manager = DownloadManager()
    manager.run()