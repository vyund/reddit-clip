import praw
import requests
from requests.exceptions import RequestException
import os
import pandas as pd
import uuid
import time
from datetime import datetime
import json

# Function to load existing submission titles and sample_IDs from the CSV to avoid re-downloading or naming sample with same UID
def load_existing(csv_filename):
    try:
        df = pd.read_csv(csv_filename)
        return list(set(df['Title'].values)), list(set(df['Sample_ID']))
    except FileNotFoundError:
        return set()

# Function to save data to CSV (appending if file exists)
def save_data_to_csv(data, csv_filename):    
    df = pd.DataFrame(data)
    
    # Append to the CSV if it already exists, otherwise write a new one
    if os.path.exists(csv_filename):
        df.to_csv(csv_filename, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_filename, index=False)

# Fetch image posts with rate limit handling
def fetch_image_posts(scrape_csv, subreddit_name, max_file_size, limit=None, reddit=None, time_filter='month', existing_titles=None, existing_uids=None):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.top(time_filter=time_filter, limit=limit):
        try:
            # Skip submissions with a duplicate title
            if submission.title in existing_titles:
                print(f'Skipping submission with title: {submission.title} (already downloaded).')
                continue

            # Check if the URL is an image and post is not stickied
            if submission.url.endswith(('.jpg', '.png', '.jpeg')) and not submission.stickied:
                sample_id = str(uuid.uuid4())
                while sample_id in existing_uids: # Make sure no duplicate UID
                    sample_id = str(uuid.uuid4())

                image_filename = download_image(submission.url, sample_id, max_file_size)

                # Save the data to CSV after each successful download
                if image_filename:
                    # TODO: Make dictionary of other possible info to scrape and make as argument --> dictionary of all possible attr?
                    save_data_to_csv([{
                            'Sample_ID': sample_id,
                            'Title': submission.title,
                            'Image_Filename': image_filename,
                            'Url': submission.permalink,
                            'Author': submission.author.name,
                            'NSFW': submission.over_18
                        }], scrape_csv)
            
            # Rate limit handling
            time.sleep(1)

        except RequestException as e:
            if e.response and e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 60))  # Default to 60 seconds
                print(f'Rate limit exceeded, sleeping for {retry_after} seconds...')
                time.sleep(retry_after)

# Download the image from the URL
def download_image(url, sample_id, max_file_size):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Get file size
        file_size = int(response.headers.get('Content-Length', 0))
        max_file_size_bytes = max_file_size * 1024 * 1024  # Convert MB to bytes

        if file_size <= max_file_size_bytes:
            filename = os.path.join(export_dir, f'{sample_id}.jpg')
            filename = os.path.normpath(filename)  # Normalize the path to be OS-independent

            with open(filename, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            print(f'Downloaded: {filename}')

            # Convert the path to use forward slashes
            return filename.replace(os.sep, '/')
        else:
            print(f'Skipped {url}: File size {file_size / (1024 * 1024):.2f} MB exceeds limit.')
            return None
    except Exception as e:
        print(f'Failed to download {url}: {e}')
        return None

# Fetch all comments by the submission author
def fetch_author_comments(author):
    if not author:
        return None
    try:
        comments = [comment.body for comment in author.comments.new(limit=None)]
        return ' | '.join(comments)
    except Exception as e:
        print(f'Failed to fetch comments for {author}: {e}')
        return None

if __name__ == '__main__':
    # Load authentication details
    with open('secret.json', 'r') as f:
        auth = json.load(f)

    try:
        # Configure PRAW with authentication
        reddit = praw.Reddit(
            client_id = auth['client_id'],
            client_secret = auth['client_secret'],
            user_agent = auth['user_agent']
        )

        # Time of script execution to name download directory
        scrape_id = datetime.now().strftime('%Y-%m-%d_%H-%M')

        subreddit_name = 'analog'  # Replace with the name of the subreddit
        time_filter = 'month'
        limit = None

        data_dir = './data'
        export_dir = os.path.join(data_dir, scrape_id)

        scrape_csv = '{}.csv'.format(scrape_id)
        scrape_csv_path = os.path.join(data_dir, scrape_csv)

        os.makedirs(export_dir, exist_ok=True)

        existing_titles = []
        existing_uids = []

        # Load existing titles and sample IDs from the CSV to avoid duplicates
        for file in os.listdir(data_dir):
            if file.endswith('.csv'):
                scraped_csv_path = os.path.join(data_dir, file)
                scraped_titles, scraped_uids = load_existing(scraped_csv_path)
                
                existing_titles.extend(scraped_titles)
                existing_uids.extend(scraped_uids)

        # Run the image scraping and download process
        max_file_size_mb = 25  # Set the maximum file size in MB
        fetch_image_posts(scrape_csv_path, subreddit_name, max_file_size_mb, limit, reddit, time_filter, existing_titles, existing_uids)

    except KeyboardInterrupt:
        print('\nScript interrupted...')
    except Exception as e:
        print(f'An error occurred: {e}')
