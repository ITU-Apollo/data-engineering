# Import dependencies
import json, traceback
import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

logFile = "log-" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+".txt"
def writeLog(text):
        with open(logFile, 'a') as log:
                log.write("{0},{1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),str(text)))
                
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
resetEpoch = 0
remainingLimit = 0
currentEpoch = 0
#
exceptionCount = 100

hashDataDict = dict()

def writeToDataframe(data, i):
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

for i in range(len(df)):
    # Split github_repo_url
    protocol, _, domain, user, repo = str(df['github_repo_url'][i]).split('/')
    # Get commit hash from dataframe
    commit_hash = str(df['commit_hash'][i]).replace('\n', '')
    
    if (commit_hash in hashDataDict):
        data = hashDataDict[commit_hash]
        writeToDataframe(data, i)
    else:        
        # Merge url to send request github api
        url = protocol + '//' + 'api.' + domain + '/repos/' + user + '/' + repo + '/' + 'commits/' + commit_hash
        # Send request to api
        try:
            r = requests.get(url, headers=header)
            recievedHeaders = dict(r.headers)
            remainingLimit = int(recievedHeaders['X-RateLimit-Remaining'])
            resetEpoch = int(recievedHeaders['X-RateLimit-Reset'])
            # Dump json
            data = r.json()
            writeToDataframe(data, i)
        except:
            writeLog("Exception happened! Current remainingLimit : {0}, resetEpoch : {1}, currentEpoch : {2}".format(remainingLimit, resetEpoch, currentEpoch))
            writeLog(traceback.format_exc())
            i -= 1
            exceptionCount -= 1
            if (exceptionCount < 0):
                i = len(df)
        if(remainingLimit == 0):
            # sleep till you get the time limit but check every 5 mins
            while (currentEpoch < resetEpoch):
                time.sleep(300)
                currentEpoch = int(time.time())
# Export data
df.to_csv('python_extra.csv', index=False)
