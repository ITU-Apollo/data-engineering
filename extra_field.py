# Import dependencies
import json
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

# Loads variables in the .env file as environment variables
load_dotenv()
# Get github token from env vars
token = os.environ.get('GITHUB_TOKEN')

# Import data
df = pd.read_csv('/home/ubuntu/data/python.csv', low_memory=True)

# Save to-be-added fields to a list
extra_fields = ['commit_date', 'user_name', 'commit_message', 'avatar_url', 'follower_count']
# Add fields as None to fill them later using indexing
for field in extra_fields:
    df[field] = None

# Set header for authorization
header = {"Authorization": f"token {token}"}

# Define a counter to count requests
count = 0

for i in range(len(df)):
    # Split github_repo_url
    protocol, _, domain, user, repo = str(df['github_repo_url'][i]).split('/')
    # Get commit hash from dataframe
    commit_hash = str(df['commit_hash'][i]).replace('\n', '')

    # Merge url to send request github api
    url = protocol + '//' + 'api.' + domain + '/repos/' + user + '/' + repo + '/' + 'commits/' + commit_hash
    # Send request to api
    r = requests.get(url, headers=header)
    # Dump json
    data = r.json()
    ## GET FIELDS AND ADD THEM TO DATAFRAME ##
    # Get date of the commit and add to dataframe
    commit_date = data['commit']['author']['date']
    df['commit_date'][i] = commit_date
    
    # Get user's real name
    user_name = data['commit']['author']['name']
    df['user_name'][i] = user_name
    
    # Get commit message
    commit_message = data['commit']['message']
    df['commit_message'][i] = commit_message
    
    # Get user's avatar url
    avatar_url = data['author']['avatar_url']
    df['avatar_url'][i] = avatar_url

    # Get user's follower count at commit time
    p = requests.get(data['author']['followers_url'])
    follower_count = len(p.json())
    df['follower_count'][i] = follower_count
    
    # Increment the counter
    count += 1
    # Only 5000 requests per hour is allowed. So wait for an hour when reached to 5000
    if (count % 5000) == 0:
        time.sleep(3600 + 5)

# Export data
df.to_csv('python_extra.csv', index=False)
        