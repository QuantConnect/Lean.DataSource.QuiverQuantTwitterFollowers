# 1. Download the data
# 2. process (if required)
# 3. Output data by date tp /temp-output-directory/alternative/quiver/twitter/universe/{date}.csv
# 4. Output data for each ticker to /temp-output-directory/alternative/quiver/twitter/{symbol}.csv

import clr_loader
import os
from pythonnet import set_runtime
set_runtime(clr_loader.get_coreclr(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.json")))

from AlgorithmImports import *
AddReference("Fasterflect")

from datetime import datetime
import pathlib
import requests
import shutil
import time

token = os.environ["QUIVER_API_KEY"]

file_path = '/temp-output-directory/alternative/quiver/twitter/'
universe_file_path = file_path + 'universe/'
if os.path.exists(universe_file_path):
    shutil.rmtree(universe_file_path)
    
pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)
pathlib.Path(universe_file_path).mkdir(parents=True, exist_ok=True)

headers = {
    'accept': 'application/json',
    'Authorization': 'Token ' + token
}

base_url = 'https://api.quiverquant.com/beta'
companies_url = f'{base_url}/companies'
twitter_url = f'{base_url}/historical/twitter'

companies = requests.get(companies_url, headers=headers).json()
companies = sorted(companies, key=lambda x: x['Ticker'])

for c in companies:
    ticker = c['Ticker']
    print('Processing ticker: ' + ticker)
    i = 5

    while i != 0:
        try:
            ticker_twitter = requests.get(f'{twitter_url}/{ticker}', headers=headers).json()
            time.sleep(0.03)

            if len(ticker_twitter) == 0:
                print('No data for: ' + ticker)
                break

            lines = []

            for row in sorted(ticker_twitter, key=lambda x: x['Date']):
                dt = datetime.strptime(row['Date'], '%Y-%m-%d')
                date = dt.strftime('%Y%m%d') # 2020-05-08
                followers = str(row['Followers'])
                pct_change_day = str(row['pct_change_day'])
                pct_change_week = str(row['pct_change_week'])
                pct_change_month = str(row['pct_change_month'])

                line = [date, followers, pct_change_day, pct_change_week, pct_change_month]
                lines.append(','.join(line))
                
                sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, dt)
                
                universe_line = [str(sid), ticker, followers, pct_change_day, pct_change_week, pct_change_month]
                
                with open(universe_file_path + date.replace("-", "") + '.csv', 'a') as ticker_file:
                    ticker_file.write(','.join(universe_line) + "\n")
            
            csv_lines = '\n'.join(lines)
            with open(file_path + ticker.lower() + '.csv', 'w') as ticker_file:
                ticker_file.write(csv_lines)

            print('Finished processing ' + ticker)
            break

        except Exception as e:
            print(str(e) + ' - Failed to parse data for ' + ticker + ' - Retrying')
            time.sleep(1)
            i = i - 1