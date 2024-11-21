import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from csv import reader
import re
import logging
import time
import random
import validators

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
INPUT_FILE = 'bookslinks.csv'
OUTPUT_FILE = 'collectedMails.csv'
REJECTED_FILE = 'rejected_urls.csv'

# Data containers
urls = []
all_data = []
rejected_urls = []

# Load URLs from the CSV file
with open(INPUT_FILE, 'r') as f:
    csv_reader = reader(f)
    for row in csv_reader:
        if validators.url(row[0]):  # Validate URLs
            urls.append(row[0])
        else:
            logging.warning(f"Invalid URL skipped: {row[0]}")

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

# Function to scrape data from a URL
def transform(url):
    for attempt in range(3):  # Retry logic
        try:
            # Add random delay to mimic human browsing
            time.sleep(random.uniform(1, 5))

            # Send GET request
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, 'html.parser')

            # Extract data
            data = {'url': url}
            email_matches = []
            for text in soup.find_all(string=True):
                email_match = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
                email_matches.extend(email_match)
            data['Company Email'] = email_matches if email_matches else None

            all_data.append(data)
            logging.info(f"Successfully processed {url}")
            return  # Exit loop on success

        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt == 2:  # After 3 attempts, log as rejected
                rejected_urls.append({'url': url, 'reason': 'Request Error'})
        except Exception as e:
            logging.exception(f"Error processing {url}: {e}")
            rejected_urls.append({'url': url, 'reason': str(e)})
            return

# Use ThreadPoolExecutor for concurrent processing
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(transform, urls)

# Save results to files
if all_data:
    pd.DataFrame(all_data).to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Data saved to {OUTPUT_FILE}")
else:
    logging.info("No data collected.")

if rejected_urls:
    pd.DataFrame(rejected_urls).to_csv(REJECTED_FILE, index=False)
    logging.info(f"Rejected URLs saved to {REJECTED_FILE}")
else:
    logging.info("No URLs were rejected.")

logging.info("Complete.")
