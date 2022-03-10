import pandas as pd
import requests

# Import data
df = pd.read_csv('/home/ubuntu/data/python.csv', low_memory=True)

for i in range(len(df)):
    dicty = dict()
    dicty['commit_hash']=str(df['commit_hash'][i])
    dicty['id']=str(df['id'][i])
    dicty['language']=str(df['language'][i])
    dicty['snippet']=str(df['snippet'][i])
    dicty['repo_file_name']=str(df['repo_file_name'][i])
    dicty['github_repo_url']=str(df['github_repo_url'][i])
    dicty['license']=str(df['license'][i])
    dicty['starting_line_number']=str(df['starting_line_number'][i])
    dicty['chunk_size']=str(df['chunk_size'][i])
    
    url = 'http://3.122.101.195:9661/api/apollo/create'
    
    requests.post(url, data=dicty)
    
    print("sended :#", i)
