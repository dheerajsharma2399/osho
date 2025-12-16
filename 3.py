"""
Script 3: Create final formatted JSON with discourse names and MP3 links
Input: discourse_with_mp3.json
Output: osho_discourses_final.json
"""

import json
import time

def create_final_json():
    print("="*70)
    print("📦 STEP 3: CREATE FINAL JSON")
    print("="*70 + "\n")
    
    # Load data from script 2
    try:
        with open('discourse_with_mp3.json', 'r', encoding='utf-8') as f:
            discourse_data = json.load(f)
        print(f"✅ Loaded {len(discourse_data)} discourses from discourse_with_mp3.json\n")
    except FileNotFoundError:
        print("❌ Error: discourse_with_mp3.json not found!")
        print("   Please run script 2 first to extract MP3 links.\n")
        return
    
    # Create final formatted structure
    print("🔨 Creating final JSON structure...")
    
    final_data = []
    successful_count = 0
    
    for discourse in discourse_data:
        if discourse['status'] == 'success' and discourse['mp3_links']:
            final_data.append({
                'discourse_name': discourse['title'],
                'discourse_url': discourse['url'],
                'episodes': f"{discourse['start_episode']}-{discourse['end_episode']}",
                'total_episodes': discourse['end_episode'] - discourse['start_episode'] + 1,
                'mp3_links': discourse['mp3_links']
            })
            successful_count += 1
    
    # Save final JSON
    with open('osho_discourses_final.json', 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    # Calculate statistics
    total_mp3_files = sum(len(d['mp3_links']) for d in final_data)
    
    final_stats = {
        'total_discourses_with_mp3': successful_count,
        'total_mp3_files': total_mp3_files,
        'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'data_structure': {
            'discourse_name': 'Name of the discourse series',
            'discourse_url': 'URL to the discourse page',
            'episodes': 'Episode range (e.g., 1-17)',
            'total_episodes': 'Total number of episodes',
            'mp3_links': 'Array of all MP3 download links'
        }
    }
    
    with open('final_stats.json', 'w', encoding='utf-8') as f:
        json.dump(final_stats, f, indent=2)
    
    print(f"{'='*70}")
    print("🎉 FINAL JSON CREATED!")
    print(f"{'='*70}")
    print(f"📚 Total discourses with MP3: {successful_count}")
    print(f"🎵 Total MP3 files: {total_mp3_files}")
    print(f"\n📁 Output: osho_discourses_final.json")
    print(f"📊 Stats: final_stats.json")
    print(f"{'='*70}\n")
    
    # Show sample data
    print("Sample data structure:")
    if final_data:
        sample = final_data[0]
        print(json.dumps(sample, ensure_ascii=False, indent=2))
        print(f"\n... and {len(final_data) - 1} more discourses\n")
    
    print("✅ All done! You can now use osho_discourses_final.json")
    print(f"{'='*70}")

def main():
    start_time = time.time()
    create_final_json()
    elapsed = time.time() - start_time
    print(f"\n⏱️  Completed in {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()