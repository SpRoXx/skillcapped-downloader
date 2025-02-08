import os
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor

# Constants (these may be fragile if the site changes its HTML)
VIDEO_TITLES_DIV_CLASS_NAME = 'css-1mkvlph'
VIDEO_IDS_DIV_ID_PREFIX = 'BrVidRow-'

########################################
# 1. Concurrent segment downloads
########################################

def download_segment(session, video_id, segment_number):
    piece_number = str(segment_number).zfill(5)
    url = f'https://d13z5uuzt1wkbz.cloudfront.net/{video_id}/HIDDEN4500-{piece_number}.ts'
    try:
        r = session.get(url, stream=True, timeout=10)
    except Exception as e:
        print(f"Error downloading segment {segment_number}: {e}")
        return None

    if r.status_code != 200:
        print(f"Segment {segment_number} not found (status code {r.status_code}).")
        return None

    file_name = f"HIDDEN4500-{piece_number}.ts"
    # If a file by that name already exists (perhaps from a previous run), remove it.
    if os.path.exists(file_name):
        try:
            os.remove(file_name)
        except Exception as e:
            print(f"Error removing existing file {file_name}: {e}")
            return None

    try:
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    except Exception as e:
        print(f"Error writing file {file_name}: {e}")
        return None

    file_size = os.path.getsize(file_name) / (1024 * 1024)
    print(f"Downloaded segment {segment_number} as {file_name} ({file_size:.2f} Mb)")
    return file_name

def download_segments(video_id, session, max_workers=8, batch_size=20):
    segment_files = []
    current_segment = 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            # Schedule a batch of segment downloads.
            futures = {}
            for seg in range(current_segment, current_segment + batch_size):
                futures[seg] = executor.submit(download_segment, session, video_id, seg)
            stop_batch = False
            # Process the batch sequentially (to keep the order correct).
            for seg in range(current_segment, current_segment + batch_size):
                file_name = futures[seg].result()
                if file_name is None:
                    stop_batch = True
                    break
                else:
                    segment_files.append(file_name)
                    current_segment += 1
            if stop_batch:
                break
    return segment_files

########################################
# 2. Video assembly (concatenation) in Python
########################################

def get_video(video_id, video_title, folder_name):
    print(f"\nDownloading video '{video_title}' with ID: {video_id}")
    session = requests.Session()
    segment_files = download_segments(video_id, session)
    if not segment_files:
        print("No segments downloaded. Aborting video creation.")
        return

    safe_title = video_title.replace(":", " - ")
    output_file = os.path.join(folder_name, f"{safe_title}.ts")
    print(f"Concatenating {len(segment_files)} segments into {output_file} ...")

    try:
        with open(output_file, 'wb') as outfile:
            for fname in segment_files:
                with open(fname, 'rb') as infile:
                    outfile.write(infile.read())
    except Exception as e:
        print(f"Error concatenating files: {e}")
        return

    print(f"Video file '{output_file}' created.")

    # Cleanup: remove temporary segment files.
    for fname in segment_files:
        try:
            os.remove(fname)
        except Exception as e:
            print(f"Error deleting temporary file {fname}: {e}")
    print("Temporary segment files deleted.")

########################################
# 3. Dynamic page loading using headless Selenium
########################################

def fetch_dynamic_url(dynamic_url):
    from selenium.webdriver.firefox.options import Options
    options = Options()
    options.headless = True  # run without GUI
    browser = webdriver.Firefox(options=options)
    browser.get(dynamic_url)
    try:
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, VIDEO_TITLES_DIV_CLASS_NAME))
        )
    except Exception as e:
        print("Error waiting for dynamic content:", e)
        browser.quit()
        return None
    soup = BeautifulSoup(browser.page_source, "html.parser")
    browser.quit()
    return soup

def extract_ids(soup):
    """
    Extracts video IDs from div elements whose id starts with VIDEO_IDS_DIV_ID_PREFIX.
    """
    video_ids = []
    for row in soup.find_all('div', id=re.compile(VIDEO_IDS_DIV_ID_PREFIX)):
        video_ids.append(row.get('id').split('-')[-1])
    return video_ids

def extract_titles(soup):
    """
    Extracts video titles from div elements with class VIDEO_TITLES_DIV_CLASS_NAME.
    """
    video_titles = []
    num = 1
    for row in soup.find_all('div', attrs={'class': VIDEO_TITLES_DIV_CLASS_NAME}):
        video_titles.append(f"{num}. {row.get_text()}")
        num += 1
    return video_titles

########################################
# 4. Main: Process input & start downloads
########################################

if __name__ == "__main__":
    input_file = "inputs.txt"
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        exit(1)
    with open(input_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # If the line contains a comma, assume the format is "folder,url"
        if ',' in line:
            parts = line.split(',')
            folder_name = parts[0].strip()
            url = parts[1].strip()
        else:
            # Otherwise, treat the entire line as the URL and derive a default folder name.
            url = line
            folder_name = url.split("/")[-1]
            print(f"No folder name specified. Using default folder name: {folder_name}")

        # Create the folder if it doesn't exist.
        os.makedirs(folder_name, exist_ok=True)

        if "commentaries" not in url:
            soup = fetch_dynamic_url(url)
            if soup is None:
                continue  # Skip if dynamic page loading failed.
            video_ids = extract_ids(soup)
            video_titles = extract_titles(soup)
            print("\nVideo IDs:", video_ids)
            print("Video Titles:", video_titles)
            for vid, title in zip(video_ids, video_titles):
                get_video(vid, title, folder_name)
        else:
            # For commentaries pages, assume the video ID is the last segment of the URL.
            video_id = url.split("/")[-1]
            get_video(video_id, folder_name, folder_name)
