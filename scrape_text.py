
import json
from bs4 import BeautifulSoup
import requests

def scrape_text():
    with open('eng-names.json', 'r') as f:
        discourses = json.load(f)

    all_texts = {}

    for discourse in discourses:
        title = discourse['title']
        base_url = discourse['url']
        start_episode = discourse['start_episode']
        end_episode = discourse['end_episode']

        discourse_texts = []

        # Generate chapter URLs
        for i in range(start_episode, end_episode + 1):
            # Handle the case where the URL already ends with a number
            if base_url.endswith(f"{start_episode:02d}"):
                url = base_url[:-2] + f"{i:02d}"
            else:
                url = f"{base_url}-{i:02d}"


            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for bad status codes
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find the chapter title
                chapter_title_tag = soup.find('a', class_='text-base font-500 leading-tight text-sky-800 lg:font-semibold')
                chapter_title = chapter_title_tag.get_text(strip=True) if chapter_title_tag else ''


                # Find the chapter text
                text_div = soup.find('div', class_='leading-6 tracking-wide text-neutral-500 xs:text-base xs:leading-6 sm:text-lg sm:leading-7 lg:leading-8')
                chapter_text = text_div.get_text(separator='\n', strip=True) if text_div else ''

                if chapter_text:
                    discourse_texts.append({
                        'chapter_title': chapter_title,
                        'text': chapter_text
                    })

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url}: {e}")
                continue

        all_texts[title] = discourse_texts

    with open('eng-text.json', 'w') as f:
        json.dump(all_texts, f, indent=2)

if __name__ == '__main__':
    scrape_text()
