# 1. Download the data
# 2. process (if required)
# 3. Output data by date tp /temp-output-directory/alternative/quiver/twitter/universe/{date}.csv
# 4. Output data for each ticker to /temp-output-directory/alternative/quiver/twitter/{symbol}.csv

import os
import pathlib
import requests
import shutil
from time import sleep

# CLRImports is required to handle Lean C# objects
from CLRImports import *

VendorName = "quiver";
VendorDataName = "twitter";

class QuiverTwitterFollowersDataDownloader:
    def __init__(self, destinationFolder, canCreateUniverseFiles, apiKey = None):
        self.destinationFolder = os.path.join(destinationFolder, VendorDataName)
        self.universeFolder = os.path.join(self.destinationFolder, "universe")
        self.canCreateUniverseFiles = canCreateUniverseFiles
        self.clientKey = apiKey if apiKey else os.environ["QUIVER_API_KEY"]

        if os.path.exists(self.universeFolder):
            shutil.rmtree(self.universeFolder)
    
        pathlib.Path(self.destinationFolder).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.universeFolder).mkdir(parents=True, exist_ok=True)

    def Run(self):

        mapFileProvider = LocalZipMapFileProvider()
        mapFileProvider.Initialize(DefaultDataProvider());

        companies = sorted(self.HttpRequester("companies"), key=lambda x: x['Ticker'])
    
        for company in companies:
            ticker = company['Ticker']
            filename = os.path.join(self.destinationFolder, f'{ticker.lower()}.csv')
            print(f'Processing ticker: {ticker}')
            trial = 5

            while trial != 0:
                try:
                    ticker_twitter = self.HttpRequester(f"historical/{VendorDataName}/{ticker}")
                    if isinstance(ticker_twitter, Exception):
                        raise ticker_twitter

                    sleep(0.2)

                    if len(ticker_twitter) == 0:
                        print(f'No data for: {ticker}')
                        break

                    lines = []

                    for row in sorted(ticker_twitter, key=lambda x: x['Date']):
                        date_time = datetime.strptime(row['Date'], '%Y-%m-%d')
                        date = date_time.strftime('%Y%m%d') # 2020-05-08
                        info = f"{row['Followers']},{row['pct_change_day']},{row['pct_change_week']},{row['pct_change_month']}"

                        lines.append(','.join([date, info]))
                
                        if self.canCreateUniverseFiles:
                            with open( os.path.join(self.universeFolder, f'{date.replace("-", "")}.csv'), 'a') as universe_file:
                                sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, True, mapFileProvider, date_time)                
                                universe_file.write(f'{sid},{ticker},{info}\n')
            
                    with open(filename, 'w') as ticker_file:
                        ticker_file.write('\n'.join(lines))

                    print(f'Finished processing {ticker}')
                    break                  

                except Exception as e:
                    print(f'{e} - Failed to parse data for {ticker} - Retrying')
                    sleep(30)
                    trial -= 1

    def HttpRequester(self, url):       
        base_url = 'https://api.quiverquant.com/beta'
        headers = { 'accept': 'application/json', 'Authorization': 'Token ' + self.clientKey , 'Connection': 'close'}
        with requests.Session() as session:
            json = session.get(f'{base_url}/{url}', headers=headers).json()
        
        if all([isinstance(x, dict) for x in json]):
            return json
        else:
            return Exception('HTTP Request returned corrupt output.')


if __name__ == "__main__":
    
    dataFolder = Config.Get("data-folder", Globals.DataFolder)
    canCreateUniverseFiles = os.path.isdir(os.path.join(dataFolder, 'equity','usa','map_files'))

    destinationDirectory = os.path.join(Config.Get("temp-output-directory", "/temp-output-directory"), "alternative", f"{VendorName}")
    instance = QuiverTwitterFollowersDataDownloader(destinationDirectory, canCreateUniverseFiles);
    instance.Run()
