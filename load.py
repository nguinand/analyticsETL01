from pymongo import MongoClient
import pandas as pd

class MongoDB:

    # Initialize the common usable variables in below function:
    def __init__(self, user, password, host, db_name, port='27017', authSource='admin'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.authSource = authSource
        # self.uri = 'mongodb://' + self.user + ':' + self.password + '@'+ self.host + ':' + self.port + '/' + self.db_name + '?authSource=' + self.authSource
        # This is my local mongodb
        self.uri = 'mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb'
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print('MongoDB Connection Successful!')
        except Exception as e:
            print('Connection Unsuccessful!')
            print(e)

    # Function to insert data in DB, could handle Python dictionary and Pandas dataframes
    def insert_into_db(self, data, collection):
        print("------------------")
        print(pd.DataFrame)
        print(data)
        print("------------------")
        if isinstance(data, pd.DataFrame):
            try:
                self.db[collection].insert_many(data.to_dict('records'))
                print('Data Inserted Successfully')
            except Exception as e:
                print('ERROR Occurred1')
                print(e)
        else:
            try:
                print("******************")
                print(self.db)
                print(collection)
                print(self.db[collection])
                print(data)
                print(type(data))
                print("******************")
                self.db[collection].insert(data)
                print('Data Inserted Successfully')
            except Exception as e:
                print('ERROR Occurred2')
                print(e)

    def read_from_db(self, collection):
        try:
            data = pd.DataFrame(list(self.db[collection].find()))
            print('Data Fetched Successfully')
            return data
        except Exception as e:
            print('ERROR Occurred3')
            print(e)