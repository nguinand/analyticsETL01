import pandas 
import requests 
import json

class Extract:
    def __init__(self):
        # loading our json file here to usse it across different class methods
        self.data_sources = json.load(open('data_config.json'))
        self.api = self.data_sources['data_sources']['api']
        #self.csv_path = self.data_sources['data_sources']['csv']

    def getAPISData(self, api_name):
        # since we have multiple API's (Pollution and Economy Data), 
        # so we can get apt API link by passing in its name in function argument.
        api_url = self.api[api_name]
        response = requests.get(api_url)
        # response.json() will convert json data into Python dictionary.
        return response.json()
