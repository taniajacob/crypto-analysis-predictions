import os
import glob
from bs4 import BeautifulSoup
import re
import pandas as pd
import datetime
import requests

class Downloader:
    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_CACHE_PATH = 'cache'
    DEFAULT_REFETCH = False
    TEMP_DIR = 'temp'

    def __init__(self, *args, **kwargs):
        self.refetch = kwargs.pop('refetch') if 'refetch' in kwargs else self.DEFAULT_REFETCH
        self.cache_path = kwargs.pop('cache_path') if 'cache_path' in kwargs else self.DEFAULT_CACHE_PATH
        self.encoding = kwargs.pop('encoding') if 'encoding' in kwargs else self.DEFAULT_ENCODING
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.temp_path = os.path.join(self.cache_path, self.TEMP_DIR)
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)
 
    def download(self, url):
        url_hash = str(hash(url))
        path = os.path.join(self.temp_path, url_hash)
 
        if os.path.exists(path) and not self.refetch:
            with open(path, 'rb') as input_file:
                data = input_file.read()
            return data
 
        data = requests.get(url).text.encode(self.encoding)
        with open(path, 'wb') as output_file:
            output_file.write(data)
 
        return data
 
class HistoricalDataDownloader(Downloader):
    DEFAULT_START_DATE = datetime.datetime(2013, 4, 28)
    DEFAULT_END_DATE = datetime.date.today() - datetime.timedelta(1) # yesterday
 
    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start') if 'start' in kwargs else self.DEFAULT_START_DATE
        self.end_date = kwargs.pop('end') if 'end' in kwargs else self.DEFAULT_END_DATE

        super().__init__(*args, **kwargs)

class CoinMarketCapScraper(HistoricalDataDownloader):
    CMC_ALL_CURRENCIES_URL = 'https://coinmarketcap.com/coins/views/all/'
    CMC_SINGLE_CURRENCY_URL = 'https://coinmarketcap.com/currencies/%s/'
    CMC_HIST_DATA_SUFFIX = 'historical-data/?start=%s&end=%s'
    CMC_ALL_CURRENCIES_COLS = ['Name', 'Symbol', 'Market Cap', 'Price', 'Circulating Supply', 'Volume (24h)', 'URL']
    CURRENCIES_DIR = 'currencies'
    CURRENCIES_FILENAME = 'coinmarketcap_index.csv'

    index = None
    symbols = {}
 
    def get_symbols(self):
        self.fetch_currencies_overview()
        return self.index['Symbol'].values.tolist()
 
    def fetch_all(self, print_progress=True):
        symbols = self.get_symbols()
        for index, symbol in enumerate(symbols):
            if print_progress:
                print('Fetching %s... (%d / %d)' % (symbol, index + 1, len(symbols)))
            self.fetch_by_symbol(symbol)
  
    def fetch_by_symbol(self, symbol):
        self.fetch_currencies_overview()
        self.df_symbol = self.index[self.index['Symbol'] == symbol]
        return self._fetch_historical_data(self.df_symbol)
 
    def _fetch_historical_data(self, df_symbol):
        format_date = lambda x: x.strftime('%Y%m%d')
        start = format_date(self.start_date)
        end = format_date(self.end_date)
        symbol = df_symbol['Symbol'].values[0]
        url = df_symbol['URL'].values[0] + self.CMC_HIST_DATA_SUFFIX % (start, end)
 
        key = '%s-%s-%s' % (symbol, start, end)
        currency_cache_file = os.path.join(self.cache_path, self.CURRENCIES_DIR, '%s.csv' % key)
 
        currencies_path = os.path.join(self.cache_path, self.CURRENCIES_DIR)
        if not os.path.exists(currencies_path):
            os.makedirs(currencies_path)
 
        if key in self.symbols and not self.refetch:
            return self.symbols[key]
 
        if os.path.exists(currency_cache_file) and not self.refetch:
            df_symbol = pd.read_csv(currency_cache_file)
            df_symbol = df_symbol.set_index('Date')
            self.symbols[key] = df_symbol
            return df_symbol
 
        data = self.download(url)
 
        soup = BeautifulSoup(data, 'lxml')
        table = soup.find('table', {'class': 'table'})
 
        # Handle cryptos without history
        if table is None:
             return
 
        # Retrieve a list of column names
        columns = ['Symbol']
        for thead in table.findAll('thead'):
            for row in thead.findAll('tr'):
                for cell in row.findAll('th'):
                    columns.append(cell.text.strip())
						
        # Retrieve the data
        data = []
        for tbody in table.findAll('tbody'):
            for row in tbody.findAll('tr'):
                row_values = [symbol]
                for cell in row.findAll('td'):
                    cell_value = cell.text.strip()
                    cell_value = re.sub('\s+', ' ', cell_value)
                    row_values.append(cell_value)
 
                if len(row_values) == len(columns):
                    keyvalues = {key: value for key, value in zip(columns, row_values)}
                    data.append(keyvalues)
                else:
                    print('Mismatching data, skipping: %s' % row_values)
 
        # Clean data
        df = pd.DataFrame(data)
        #df = df[[self.CMC_SINGLE_CURRENCY_COLS]]
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']]
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
        df = df.sort_index()
        for column in df.columns[1:7]:
            df[column] = pd.to_numeric(df[column].apply(self._format_number), errors='coerce')
 
        self.symbols[key] = df
        df.to_csv(currency_cache_file)
 
        return df
 
    # Fetch a list of all currencies
    def fetch_currencies_overview(self):
        # Return data if it already exists (in mem?) and refetch is not required
        if self.index is not None and not self.refetch:
            return self.index
 
        # Return cached data if it exists and refetch is not required
        df_path = os.path.join(self.cache_path, self.CURRENCIES_FILENAME)
        if os.path.exists(df_path) and not self.refetch:
            df = pd.read_csv(df_path)
            self.index = df
            return df
 
        # Download Coinmarketcap data
        data = self.download(self.CMC_ALL_CURRENCIES_URL)
 
        # Use BeautifulSoup to retrieve the requried data
        soup = BeautifulSoup(data, 'lxml')
        table = soup.find('table', {'class': 'table'})
 
        # Retrieve a list of column names
        columns = []
        for thead in table.findAll('thead'):
            for row in thead.findAll('tr'):
                for cell in row.findAll('th'):
                    columns.append(cell.text.strip())
  
        # Retrieve the data
        data = []
        for tbody in table.findAll('tbody'):
            for row in tbody.findAll('tr'):
                row_values = []
                url_suffix = ''
                for cell in row.findAll('td'):
                    for link in cell.find_all('a', href=True):
                        href = link['href']
                        matches = re.findall(r'[/]{1}currencies[/]{1}(.*)[/]{1}', href)
                        if len(matches) > 0:
                            url_suffix = matches[0]
                    cell_value = cell.text.strip()
                    cell_value = re.sub('\s+', ' ', cell_value)
                    row_values.append(cell_value)
 
                if len(row_values) == len(columns):
                    keyvalues = {key: value for key, value in zip(columns, row_values)}
                    # Add Url for currency
                    keyvalues['URL'] = self.CMC_SINGLE_CURRENCY_URL % url_suffix
                    data.append(keyvalues)
                else:
                    print('Mismatching data, skipping: %s' % keyvalues)
 
        # Build the dataframe and clean the data
        df = pd.DataFrame(data)
        df = df[self.CMC_ALL_CURRENCIES_COLS]
        df['Name'] = df['Name'].apply(lambda x: x.split(' ')[1]) # Why?
        df['Circulating Supply'] = pd.to_numeric(df['Circulating Supply'].apply(self._format_number), errors='coerce')
        df['Market Cap'] = pd.to_numeric(df['Market Cap'].apply(self._format_number), errors='coerce')
        df['Price'] = pd.to_numeric(df['Price'].apply(self._format_number), errors='coerce')
        df['Volume (24h)'] = pd.to_numeric(df['Volume (24h)'].apply(self._format_number), errors='coerce')
 
        # save the data to csv file and index
        df.to_csv(df_path)
        self.index = df
 
        return df
 
    # Removes unwanted characters from number
    def _format_number(self, x):
        return x.replace(',', '').replace('$', '').replace('*', '')
 
    def export_all_currencies(self):
        cache_path = os.path.join(self.cache_path, self.CURRENCIES_DIR)
        df = pd.concat(map(pd.read_csv, glob.glob(os.path.join(cache_path, "*.csv"))))
        df.to_csv('all_currencies.csv')
 
if __name__ == '__main__':
    #scraper = CoinMarketCapScraper(end=datetime.datetime(2018, 3, 24))
    scraper = CoinMarketCapScraper(end=datetime.datetime(2018, 4, 5))
    df = scraper.fetch_all()
    scraper.export_all_currencies()
	
