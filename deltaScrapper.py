from pymongo import MongoClient
import sqlite3
from utils import *
from utils import ts_print as _log
import _thread
import pandas as pd
from connector import connector

class deltaScrapper:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        mClient = self.initMongo()
        self.scalper = mClient.scalper
        self.db = sqlite3.connect("scalper.db", check_same_thread=False)

    def initMongo(self)-> MongoClient:
        mclient = MongoClient("uri")["collection"]
        _log(f"mongoDB connection succesfull","info")
        return mclient
    
    def initConnector(self, scalper):
        client = connector(self.api_key, self.secret_key, scalper['symbol'])
        return client 

    def createTable(self, scalper):
        query = '''CREATE TABLE IF NOT EXISTS {} (TIMESTAMP PRIMARY KEY NOT NULL, DELTA FLOAT NOT NULL)'''.format(scalper['name'])
        self.db.execute(query)
        return
    
    def updateTable(self, **kwargs):
        scalper = kwargs['scalper']
        timestamp = datetime.utcfromtimestamp(utc_timestamp())
        delta = kwargs['delta'] if kwargs['delta'] is not None else 0
        if kwargs['key'] == "delta":
            try:
                query = '''INSERT OR REPLACE INTO {}(TIMESTAMP, DELTA)VALUES(?,?)'''.format(scalper['name'])
                self.db.execute(query,(timestamp, delta))
                self.db.commit()
            except:
                pass
        return 

    def get_delta(self,scalper, lock):
        _log(f"scraping delta for : {scalper['name']}")
        self.createTable(scalper)
        client = self.initConnector(scalper)
        if scalper['init_calc']==0:  
            initial = (float(client.getBBO()['asks'][0][0]) + float(client.getBBO()['bids'][0][0]))*0.5
            self.scalper.find_one_and_update({'name': scalper['name']},{"$set": {'init_calc':initial}})
        elif int(time.time()) >= scalper['next_calc'] and scalper['next_calc']!=0:
            final = (float(client.getBBO()['asks'][0][0]) + float(client.getBBO()['bids'][0][0]))*0.5
            delta = final-scalper['init_calc']
            self.updateTable(delta=delta, key="delta", scalper=scalper)
            self.scalper.find_one_and_update({'name': scalper['name']}, {'$set':{'next_calc':int(int(time.time())+scalper['time_interval']), 'init_calc': 0}})
        elif scalper['next_calc']==0:
            self.scalper.find_one_and_update({'name': scalper['name']}, {'$set':{'next_calc':int(int(time.time())+scalper['time_interval'])}})
        lock.release()
        return

    def run(self):
        scalper_coll = self.initMongo().scalper
        df = pd.DataFrame(list(scalper_coll.find({})))
        locks = []
        n = range(len(df))
        for i in n:
            lock = _thread.allocate_lock()
            a = lock.acquire()
            locks.append(lock) 

        for i in n:
            _thread.start_new_thread(self.get_delta,(df.iloc[i], locks[i]))
        
        for i in n:
            while locks[i].locked(): pass

        
        return

if __name__ == "__main__":
    api_key = ""
    secret_key = ""
    deltaScrapper = deltaScrapper(api_key=api_key, secret_key=secret_key)
    while True:
        deltaScrapper.run()
