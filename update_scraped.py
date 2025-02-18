import praw
import os
import pandas as pd
import numpy as np
import json

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

        data_dir = './data'
        scrape_to_fix = '2025-01-16_12-17.csv'

        scrape_path = os.path.join(data_dir, scrape_to_fix)

        scrape_df = pd.read_csv(scrape_path)

        # Submission info you want to add to existing scrape CSV
        columns_to_add = ['Author', 'NSFW']
        for col in columns_to_add:
            if col not in scrape_df.columns:
                scrape_df[col] = np.nan
            else:
                print(f'Column {col} already exists...')

        for i, row in scrape_df.iterrows():
            if pd.isna(row[columns_to_add[0]]):
                sample_url = f'https://www.reddit.com{row['Url']}'
                try:
                    submission = reddit.submission(url=sample_url)
                    # TODO: Handle other types of information to scrape
                    if submission.author is not None:
                        scrape_df.at[i, 'Author'] = submission.author.name
                    else:
                        scrape_df.at[i, 'Author'] = 'Deleted_Account'
                    scrape_df.at[i, 'NSFW'] = submission.over_18
                except Exception as e:
                    print(f'An error occurred: {e}')

                # Save progress every 50 rows
                if (i + 1) % 50 == 0: 
                    scrape_df.to_csv(scrape_path, index=False)
                    print(f'Saved progress at row {i + 1}')
            else:
                continue
        scrape_df.to_csv(scrape_path, index=False)

    except Exception as e:
        print(f'An error occurred: {e}')     
    