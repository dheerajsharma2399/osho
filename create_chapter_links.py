
import json
from pathlib import Path
import re

def generate_chapter_links():
    """
    Reads discourse information from *-names.json files, generates chapter links,
    and saves them to a new JSON file.
    """
    all_series_data = []
    for lang in ["eng", "hindi"]:
        names_file = f"{lang}-names.json"
        with open(names_file, "r", encoding="utf-8") as f:
            series_list = json.load(f)
            for series in series_list:
                series['language'] = lang
                all_series_data.append(series)

    output_data = []
    for series in all_series_data:
        title = series["title"]
        series_url = series["url"]
        start_ep = series.get("start_episode")
        end_ep = series.get("end_episode")

        if not start_ep or not end_ep:
            print(f"Skipping {title} because of missing start/end episode.")
            continue

        slug = series_url.rstrip('/').split('/')[-1]
        prefix = re.sub(r'-by-.*$', '', slug)
        prefix = re.sub(r'-\d+-\d+$', '', prefix) # remove chapter range
        base_url = '/'.join(series_url.split('/')[:-1])

        chapter_links = [f"{base_url}/{prefix}-{i:02}" for i in range(start_ep, end_ep + 1)]

        output_data.append({
            "discourse_name": title,
            "discourse_url": series_url,
            "language": series.get("language"),
            "chapter_links": chapter_links
        })

    output_file = Path("chapter_links.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"Successfully generated chapter links and saved to {output_file}")

if __name__ == '__main__':
    generate_chapter_links()
