# Webscraper script to get liquidity data from CMC
# cd into the folder using terminal/anaconda and run >python CMC_Webscraper.py

print("firing up scraper")

#%% Imports and Initializations

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import datetime
import time


# url = 'https://coinmarketcap.com/rankings/exchanges/liquidity/'
url = 'https://coinmarketcap.com/rankings/exchanges/'

#%% Get into soup with outer for loop that runs every hour

r = requests.get(url)
c = r.content
soup = BeautifulSoup(c,'html.parser')
length = len(soup.find_all("tr", {"class":"cmc-table-row"}))

# Build initial name list to ini pandas df
ini_names = []
for i in range(length):
    ini_names.append(soup.find_all("tr", {"class":"cmc-table-row"})[i].find("a").string)

df = pd.DataFrame(ini_names, columns = ['Names'])


#%% Loop through and get names and liquidity 

week_time = 604800  # seconds in one week
day_time = week_time/7
timeout = time.time() + 1

while True:
    # Set a break statement after 1 week
    if time.time() > timeout:
        break
    else:

        # initialize lists
        names = []
        liquidity = []
        web_traffic = []
        
        r = requests.get(url)
        c = r.content
        soup = BeautifulSoup(c,'html.parser')
        length = len(soup.find_all("tr", {"class":"cmc-table-row"}))

        # Get date and hour data
        date = str(datetime.datetime.now().date())
        hour = str(datetime.datetime.now().hour)
        minute = str(datetime.datetime.now().minute)
        print('Getting and writing Liq data at {}_{}_{}'.format(date,hour,minute))

        for i in range(length):
            
            # Get names, liq, and web_traffic here
            names.append(soup.find_all("tr", {"class":"cmc-table-row"})[i].find("a").string)
            liquidity.append(int(soup.find_all("td", {"cmc-table__cell cmc-table__cell--sortable cmc-table__cell--right cmc-table__cell--sort-by__effective-liquidity-24-h"})[i].find("a").text))
            web_traffic.append(int(soup.find_all("td", {"cmc-table__cell cmc-table__cell--sortable cmc-table__cell--right cmc-table__cell--sort-by__traffic-score"})[i].text))            

        # Get liq dict
        liq_dict = dict(zip(names, liquidity))
        web_dict = dict(zip(names, web_traffic))

        # Make a mergable df
        merger = pd.DataFrame.from_dict(liq_dict, orient = 'index') 
        merger = merger.reset_index()
        merger.columns = ['Names', 'Liq_{}_{}_{}'.format(date,hour,minute)]

        merger2 = pd.DataFrame.from_dict(web_dict, orient = 'index') 
        merger2 = merger2.reset_index()
        merger2.columns = ['Names', 'Web_{}_{}_{}'.format(date,hour,minute)]

        df = df.merge(merger, on = 'Names', how = 'outer')
        df = df.merge(merger2, on = 'Names', how = 'outer')

        # Print to make sure that we are getting updated values (website hasn't changed)
        print(df.iloc[30,0], '\n', df.iloc[30,-2:])

        # CLose connections 
        r.close()
        print('\n','Sleeping...ZZZzzz....')
        time.sleep(598)

#%% Writing to a csv

# Save as a csv
df.to_csv('CMC_Liq_Dict.csv', index = False)
print ("Saved liquidity data at {}".format(str(datetime.datetime.now().time())))


