# SSS Scraper ;3

PLEASE NOTE: Some schools omit emails from their staff listings and use contact forms instead. In these cases, this scraper will NOT work.

Additionally, my Hack Club AI API Key is hardcoded, it's set to work for this project only and is not a paid key, so I'm not worried about key sharing, and should be perfectly fine for up to ~2000 requests, but if you want to use this project personally or at a large scale, please create and plug in a key at https://ai.hackclub.com/ or reconfigure the code to use Claude.

sss is a highly concurrent, intelligent web scraper designed to extract STEM, Math, and Science staff information from arbitrary school websites.

## Features

- **Asynchronous Engine**: Built on `asyncio` and `playwright.async_api` to crawl multiple pages and render JavaScript simultaneously.
- **Smart Link Scoring**: Automatically prioritizes high-value directories and staff pages over irrelevant content.
- **API Interception**: Hooks into Playwright response streams to capture XHR/JSON data directly.
- **AI Fallback**: When deterministic parsing fails, seamlessly queries an AI model to detect staff and normalize results.
- **Role Validation**: Filters out non-STEM staff via a rigorous positive and negative keyword system (e.g., skips nurses, registrars, or history teachers).
- **Graceful Error Handling**: Manages sites without staff emails by printing warnings and omitting them from the final CSV.

## Installation

### Prerequisites

- Python 3.10+
- Node.js

### macOS / Linux

open up yer terminal!

1. clone or download the repository. (`git clone https://github.com/newtontriumphant/sss_scraper/`)
2. run the included setup script:
   ```bash
   ./install.sh
   ```
3. the setup script will create a virtual environment, install all dependencies, and set up a global CLI alias.

### Windows

you're sunk...

...jk!

1. Create a virtual environment and install requirements:
   ```cmd
   python -m venv venv
   call venv\Scripts\activate
   pip install -r requirements.txt
   playwright install chromium
   ```
2. Run the script manually via Python:
   ```cmd
   python scraper.py
   ```

## Usage

You can launch the scraper in two ways:

1. **Interactive CLI loop** (if installed via `install.sh`):
   ```bash
   sss
   ```
   You will be prompted to enter school URLs continuously!!!

2. **Direct execution**:
   ```bash
   sss "https://sbhs.sbschools.net"
   ```

All successfully matched staff members will be aggregated and saved into `school_staff.csv`. (all members will be saved into ONE csv file, to create a new csv file, move the created csv file to another directory!)
