import os
import re
import subprocess
import asyncio
import aiohttp
import zipfile
import shutil
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
import requests  # Used in the ffmpeg installer

# --- Constants used for scraping ---
VIDEO_TITLES_DIV_CLASS_NAME = 'css-1mkvlph'
VIDEO_IDS_DIV_ID_PREFIX = 'BrVidRow-'

########################################
# A. ffmpeg Automatic Installer & Verifier
########################################

def is_ffmpeg_installed():
    try:
        subprocess.run(["ffmpeg", "-version"],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       check=True)
        return True
    except Exception:
        return False

def install_ffmpeg():
    print("ffmpeg not found. Downloading ffmpeg...")
    # URL for a prebuilt Windows ffmpeg (essentials build)
    url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
    local_zip = "ffmpeg.zip"
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print("Error downloading ffmpeg:", e)
        return False

    print("Downloaded ffmpeg zip. Extracting...")
    try:
        with zipfile.ZipFile(local_zip, "r") as zip_ref:
            zip_ref.extractall("ffmpeg_temp")
    except Exception as e:
        print("Error extracting ffmpeg zip:", e)
        return False

    # Look for the extracted directory starting with "ffmpeg" (case-insensitive)
    extracted_dirs = os.listdir("ffmpeg_temp")
    ffmpeg_dir = None
    for d in extracted_dirs:
        if d.lower().startswith("ffmpeg"):
            ffmpeg_dir = os.path.join("ffmpeg_temp", d)
            break
    if ffmpeg_dir is None:
        print("Could not locate the extracted ffmpeg folder.")
        return False

    # The bin folder inside ffmpeg_dir should contain ffmpeg.exe
    bin_path = os.path.join(ffmpeg_dir, "bin")
    ffmpeg_exe = os.path.join(bin_path, "ffmpeg.exe")
    if not os.path.exists(ffmpeg_exe):
        print("ffmpeg.exe not found in the bin folder.")
        return False

    # Move the bin folder to a permanent location (e.g. "./ffmpeg")
    if os.path.exists("ffmpeg"):
        shutil.rmtree("ffmpeg")
    shutil.move(bin_path, "ffmpeg")

    # Clean up temporary files
    os.remove(local_zip)
    shutil.rmtree("ffmpeg_temp")

    # Add the new ffmpeg folder to the PATH for this process
    ffmpeg_abs_path = os.path.abspath("ffmpeg")
    os.environ["PATH"] += os.pathsep + ffmpeg_abs_path
    print("ffmpeg installed successfully and added to PATH.")
    return True

def ensure_ffmpeg():
    if not is_ffmpeg_installed():
        print("ffmpeg is not installed.")
        if not install_ffmpeg():
            print("Failed to install ffmpeg automatically.")
            return False
    else:
        print("ffmpeg is already installed.")
    # Verify installation
    try:
        output = subprocess.run(["ffmpeg", "-version"],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                check=True)
        first_line = output.stdout.decode().splitlines()[0]
        print("Verified ffmpeg installation:", first_line)
        return True
    except Exception as e:
        print("Error verifying ffmpeg installation:", e)
        return False

########################################
# B. Asynchronous segment downloads using aiohttp
########################################

async def download_segment_async(session, video_id, segment_number, semaphore):
    piece_number = str(segment_number).zfill(5)
    url = f'https://d13z5uuzt1wkbz.cloudfront.net/{video_id}/HIDDEN4500-{piece_number}.ts'
    
    async with semaphore:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    print(f"Segment {segment_number} not found (status {resp.status}).")
                    return None
                data = await resp.read()
        except Exception as e:
            print(f"Error downloading segment {segment_number}: {e}")
            return None

    file_name = f"HIDDEN4500-{piece_number}.ts"
    # Write the downloaded bytes to a file
    try:
        with open(file_name, 'wb') as f:
            f.write(data)
    except Exception as e:
        print(f"Error writing {file_name}: {e}")
        return None

    size_mb = os.path.getsize(file_name) / (1024 * 1024)
    print(f"Downloaded segment {segment_number} as {file_name} ({size_mb:.2f} Mb)")
    return file_name

async def download_all_segments(video_id, batch_size=20, max_concurrent=50):
    semaphore = asyncio.Semaphore(max_concurrent)
    segment_files = []
    segment = 1
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [
                download_segment_async(session, video_id, seg, semaphore)
                for seg in range(segment, segment + batch_size)
            ]
            results = await asyncio.gather(*tasks)
            # If any segment in this batch is missing, assume we've reached the end.
            stop = False
            for res in results:
                if res is None:
                    stop = True
                    break
                segment_files.append(res)
                segment += 1
            if stop:
                break
    return segment_files

########################################
# C. Assembling the final video using ffmpeg
########################################

async def download_video_async(video_id, video_title, folder_name):
    print(f"\nDownloading video '{video_title}' with ID: {video_id}")
    segment_files = await download_all_segments(video_id)
    if not segment_files:
        print("No segments downloaded. Aborting video creation.")
        return

    safe_title = video_title.replace(":", " - ")
    output_file = os.path.join(folder_name, f"{safe_title}.ts")
    print(f"Concatenating {len(segment_files)} segments into {output_file} using ffmpeg...")

    # Create a temporary file list for ffmpeg’s concat demuxer
    list_file = "segments.txt"
    with open(list_file, "w") as f:
        for seg in segment_files:
            f.write(f"file '{os.path.abspath(seg)}'\n")

    # Call ffmpeg to concatenate segments (no re-encoding)
    cmd = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file, "-c", "copy", output_file]
    try:
        subprocess.run(cmd, check=True)
        print(f"Video file '{output_file}' created.")
    except subprocess.CalledProcessError as e:
        print("ffmpeg failed:", e)
        return
    finally:
        os.remove(list_file)

    # Remove temporary segment files
    for seg in segment_files:
        try:
            os.remove(seg)
        except Exception as e:
            print(f"Error deleting temporary file {seg}: {e}")
    print("Temporary segment files deleted.")

def sync_download_video(video_id, video_title, folder_name):
    asyncio.run(download_video_async(video_id, video_title, folder_name))

########################################
# D. Dynamic page loading with headless Selenium
########################################

def fetch_dynamic_url(dynamic_url):
    options = Options()
    options.headless = True  # run Firefox in headless mode
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
    video_ids = []
    for row in soup.find_all('div', id=re.compile(VIDEO_IDS_DIV_ID_PREFIX)):
        video_ids.append(row.get('id').split('-')[-1])
    return video_ids

def extract_titles(soup):
    video_titles = []
    num = 1
    for row in soup.find_all('div', class_=VIDEO_TITLES_DIV_CLASS_NAME):
        video_titles.append(f"{num}. {row.get_text()}")
        num += 1
    return video_titles

########################################
# E. Main: Process input and initiate downloads
########################################

def main():
    # Ensure ffmpeg is available before proceeding
    if not ensure_ffmpeg():
        print("Cannot continue without ffmpeg. Exiting.")
        return

    input_file = "inputs.txt"
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        return

    with open(input_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # If a comma is present, assume the format is "folder,url"
        if ',' in line:
            parts = line.split(',')
            folder_name = parts[0].strip()
            url = parts[1].strip()
        else:
            url = line
            folder_name = url.split("/")[-1]
            print(f"No folder name specified. Using default folder name: {folder_name}")

        os.makedirs(folder_name, exist_ok=True)

        if "commentaries" not in url:
            soup = fetch_dynamic_url(url)
            if soup is None:
                continue  # Skip if dynamic loading failed.
            video_ids = extract_ids(soup)
            video_titles = extract_titles(soup)
            print("\nVideo IDs:", video_ids)
            print("Video Titles:", video_titles)
            for vid, title in zip(video_ids, video_titles):
                sync_download_video(vid, title, folder_name)
        else:
            # For commentary pages, assume the video ID is the last segment of the URL.
            video_id = url.split("/")[-1]
            sync_download_video(video_id, folder_name, folder_name)

if __name__ == "__main__":
    main()
