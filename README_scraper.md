Scraper using Selenium + ChromeDriver

Quick start

1. Create and activate a Python virtual environment (Windows):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Run the scraper against a discourse/series page:

```powershell
python scrape_selenium.py "https://oshoworld.com/ek-omkar-satnam-by-osho-01-20" --out output --headless
```

Outputs
- Writes per-language JSON files into the `output` directory (e.g. `hindi.json`, `english.json`).

Notes and next steps
- If pages rely heavily on JS, run without `--headless` to watch the browser during troubleshooting.
- Selector heuristics are conservative; update CSS/XPath selectors in `scrape_selenium.py` after inspecting a rendered page (open in browser + DevTools) to improve accuracy.
