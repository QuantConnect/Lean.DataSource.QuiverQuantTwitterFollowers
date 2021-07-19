

# 1. Download the data
# 2. process (if required)
# 3. Output data for each ticker to /temp-output-directory/alternative/quiver/twitter/{symbol}.csv

import os
import requests
import json
import time
import pathlib
from datetime import datetime
token = os.environ["QUIVER_API_KEY"]

pathlib.Path('/temp-output-directory/alternative/quiver/twitter/').mkdir(parents=True, exist_ok=True)

headers = {
    'accept': 'application/json',
    'Authorization': 'Token ' + token
}

base_url = 'https://api.quiverquant.com/beta'
companies_url = f'{base_url}/companies'
twitter_url = f'{base_url}/historical/twitter'

companies = requests.get(companies_url, headers=headers).json()

for c in companies:
    ticker = c['Ticker']
    print('Processing ticker: ' + ticker)
    i = 5

    while i != 0:
        try:
            ticker_twitter = requests.get(f'{twitter_url}/{ticker}', headers=headers).json()
            time.sleep(0.03)

            lines = []

            for row in sorted(ticker_twitter, key=lambda x: x['Date']):
                date = datetime.strptime(row['Date'], '%Y-%m-%d').strftime('%Y%m%d') # 2020-05-08
                followers = str(row['Followers'])
                pct_change_day = str(row['pct_change_day'])
                pct_change_week = str(row['pct_change_week'])
                pct_change_month = str(row['pct_change_month'])

                line = [date, followers, pct_change_day, pct_change_week, pct_change_month]
                lines.append(','.join(line))

            if len(lines) == 0:
                print('No data for: ' + ticker)
                break
            
            csv_lines = '\n'.join(lines)
            with open('/temp-output-directory/alternative/quiver/twitter/' + ticker.lower() + '.csv', 'w') as ticker_file:
                ticker_file.write(csv_lines)

            print('Finished processing ' + ticker)
            break

        except Exception as e:
            print(str(e) + ' - Failed to parse data for ' + ticker + ' - Retrying')
            time.sleep(1)
            i = i - 1