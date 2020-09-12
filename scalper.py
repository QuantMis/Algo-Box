from connector import connector
from datetime import datetime
from utils import *
from utils import ts_print as _log
import _thread
import time
import sqlite3
import pandas as pd
from pymongo import MongoClient
import sys 


class scalper:
    def __init__(self):
        mClient = self.initMongo()
        self.scalper = mClient.scalper
        self.demoId = mClient.demoID
        self.liveId = mClient.liveID
        self.trades = mClient.trades
        self.users = mClient.user
        self.db = sqlite3.connect("scalper.db", check_same_thread=False)

        # class parameters
        self.fee = 0.01

    def initConnector(self, scalper):
        user = self.users.find_one({"user":scalper['user']})
        client = connector(user['api'], user['secret'], scalper['symbol'])
        return client 

    def initMongo(self)-> MongoClient:
        mclient = MongoClient("uri")["collection"]
        _log(f"mongoDB connection succesfull","info")
        return mclient

    def updateLeverage(self, scalper):
        client = self.initConnector(scalper)
        leverage = ((scalper['leverage'])/100)*scalper['max_leverage']
        response = client.update_leverage(scalper['symbol'], leverage)
        return leverage
    
    def updateTrades(self, scalper, pnl, time):
        docs = {
            "name": scalper['name'],
            "result": pnl,
            "timestamp": time
        }
        self.trades.insert_one(dict(docs))
        return 
    
    def updateOrder(self, **kwargs):
        scalper = kwargs['scalper']
        if scalper['status']=="LIVE":
            docs = {
                "name": scalper['name'],
                "orderId": kwargs['response']['clientOrderId'],
                "timestamp": utc_timestamp(),
                "side": kwargs['response']['side'],
                "symbol": kwargs['response']['symbol'],
                "entryPrice": kwargs['entryPrice'],
                "exit": kwargs['exit'],
                "qty": kwargs['response']['origQty'],
                "cost": kwargs['cost'],
                "pnl": kwargs['pnl'],
                "paper_pnl": kwargs['paper_pnl']
            }
            updateOrder = self.liveId.insert_one(dict(docs))
            _log(f"newOrder: {docs['orderId']} inserted into liveId collecton","info")

        elif scalper['status']=="DEMO":
            docs = {
                "name": scalper['name'],
                "orderId": kwargs['orderId'],
                "timestamp": utc_timestamp(),
                "side": scalper['side'],
                "symbol": scalper['symbol'],
                "entryPrice": kwargs['entryPrice'],
                "exit": kwargs['exit'],
                "qty": kwargs['size'],
                "cost": kwargs['cost'],
                "pnl": kwargs['pnl'],
                "paper_pnl": kwargs['paper_pnl']
            }
            updateOrder = self.demoId.insert_one(dict(docs))
            _log(f"newOrder: {docs['orderId']} inserted into demoId collecton","info")
        
        return

    def calc_zscore(self, scalper):
        df = pd.read_sql_query(f"SELECT * FROM {scalper['name']}", self.db)[:-1]
        if len(df) >= scalper['zs_period']:
            ma = df['DELTA'].rolling(window=scalper['zs_period']).mean()
            std = df['DELTA'].rolling(window=scalper['zs_period']).std()
            z_score = (df['DELTA'] - ma)/std
            z_mean = z_score.rolling(window=scalper['zs_period']).mean()
            z_dev = z_score.rolling(window=scalper['zs_period']).std()
            ub = (z_mean+z_dev).iloc[-2]
            lb = (z_mean-z_dev).iloc[-2]
            zs = z_score.iloc[-1]
    
        else:
            ub = 0
            lb = 0
            zs = 0
            
        return ub, lb, zs

    def calc_size(self, scalper):
        leverage=self.updateLeverage(scalper)
        cost=(scalper['marginBatch']/100)*scalper['margin']*leverage
        client = self.initConnector(scalper)
        if scalper['side']=="BUY":
            size = round(cost/float(client.getBBO()['asks'][0][0]),3)
        elif scalper['side']=="SELL":
            size = round(cost/float(client.getBBO()['bids'][0][0]),3)
        
        # unit test
        _log(f"size: {size},leverage: {leverage}, marginBatch: {scalper['marginBatch']}, cost: {cost}, scalper: {scalper['name']}")
        return size, cost
    
    def entry_trigger(self, scalper, lock):
        client = self.initConnector(scalper)
        client.hedge_mode()
        positionSide = scalper['positionSide']
        orderType = "MARKET"
        ub, lb, zs = self.calc_zscore(scalper)
        _log(f"Status {scalper['name']} => lb: {lb} | zs: {zs} | ub: {ub}")
        if (scalper['side']=="BUY") and (scalper['marginUsed'] < scalper['margin']):
            if zs > ub :
                self.scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"signal":1}})
                _log(f"Buy Triggered for {scalper['name']}")
                orderId = gen_uuid()
                size, cost = self.calc_size(scalper)
                marginBatch = (scalper['marginBatch']/100)*scalper['margin']
                if scalper['status']=="LIVE":
                    response = client.post_order(scalper['symbol'], size, scalper['side'], positionSide, orderType)         
                    try:
                        if response['status']=="NEW":
                            _log(f"Live order executed for Id: {orderId}",'info')
                            entryPrice = client.getBBO()['asks'][0][0]
                            self.updateOrder(response=response, entryPrice=entryPrice,  cost=cost, exit=False, pnl=0, paper_pnl=0, scalper=scalper)
                            self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"marginUsed":marginBatch}})
                    except KeyError:
                        _log(response, 'error')
                elif scalper['status']=="DEMO":
                    entryPrice = client.getBBO()['asks'][0][0]
                    _log(f"Demo order executed for Id: {orderId}",'info')
                    self.updateOrder(orderId=orderId, cost=cost, size=size, entryPrice=entryPrice, exit=False, pnl=0, paper_pnl=0, scalper=scalper)
                    self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"marginUsed":marginBatch}})
                
        if (scalper['side']=="SELL") and (scalper['marginUsed'] < scalper['margin']):
            if zs < lb :
                self.scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"signal":1}})
                _log(f"Sell Triggered for {scalper['name']}")
                orderId = gen_uuid()
                size, cost = self.calc_size(scalper)
                marginBatch = (scalper['marginBatch']/100)*scalper['margin']
                if scalper['status']=="LIVE":
                    response = client.post_order(scalper['symbol'], size, scalper['side'], positionSide, orderType)  
                    try:            
                        if response['status']=="NEW":
                            _log(f"Live order executed for Id: {orderId}",'info')
                            entryPrice = client.getBBO()['bids'][0][0]
                            self.updateOrder(response=response, entryPrice=entryPrice,  cost=cost, exit=False, pnl=0, paper_pnl=0, scalper=scalper)
                            self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"marginUsed":marginBatch}})
                    except KeyError:
                            _log(response, "error")
                elif scalper['status']=="DEMO":
                    entryPrice = client.getBBO()['bids'][0][0]
                    _log(f"Demo order executed for Id: {orderId}",'info')
                    self.updateOrder(orderId=orderId, cost=cost, size=size, entryPrice=entryPrice, exit=False, pnl=0, paper_pnl=0, scalper=scalper)
                    self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"marginUsed":marginBatch}})

        lock.release()
        return

    def exit_trigger(self, scalper, lock):
        client = self.initConnector(scalper)
        if scalper['status']=="LIVE":
            orders = list(self.liveId.find({"$and": [{'name':scalper['name']},{'exit':False}]}))
        if scalper['status']=="DEMO":
            orders = list(self.demoId.find({"$and": [{'name':scalper['name']},{'exit':False}]}))
        positionSide = scalper['positionSide']
        orderType = "MARKET"
        cumulative_margin = round(len(orders)*(scalper['marginBatch']/100)*scalper['margin'],3)
        cumulative_price = 0
        cumulative_size = 0 
        cumulative_pnl = 0
        if len(orders) > 0:
            for order in orders:
                cumulative_price += float(order['entryPrice'])
                cumulative_size += float(order['qty'])
        
            average_price = cumulative_price/len(orders)

            if scalper['side']=="BUY":
                cumulative_pnl = round(cumulative_size*(float(client.getBBO()['bids'][0][0]) - average_price) - cumulative_size*(self.fee/100),2)

            if scalper['side']=="SELL":
                cumulative_pnl = round(cumulative_size*(float(client.getBBO()['asks'][0][0]) - average_price) - cumulative_size*(self.fee/100),2)*-1
        
        _log(f"totalSize: {cumulative_size}, marginUsed: {cumulative_margin}, totalPnl: {cumulative_pnl} scalper: {scalper['name']}")

        cumulative_size = round(cumulative_size,3)
        if (cumulative_pnl >= scalper['margin']*(scalper['take_profit']/100)) or (cumulative_pnl <= -1*(scalper['margin']*(scalper['stop_loss']/100))):
            if scalper['side'] == "BUY":
                side = "SELL"
                if scalper['status'] == "LIVE":
                    response = client.post_order(scalper['symbol'], cumulative_size, side, positionSide, orderType)
                    try:
                        if response['status'] == "NEW":
                            for order in orders:
                                self.liveId.find_one_and_update({"orderId":order['orderId']}, {"$set": {'exit':True}})
                            self.updateTrades(scalper, cumulative_pnl, time.ctime())
                    except:
                        _log(response, "error")
                if scalper['status'] == "DEMO":                   
                    for order in orders:
                        self.demoId.find_one_and_update({"orderId":order['orderId']}, {"$set": {'exit':True}})
                self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$set": {"marginUsed":0}})
            
            if scalper['side'] == "SELL":
                side = "BUY"
                if scalper['status'] == "LIVE":
                    response = client.post_order(scalper['symbol'], cumulative_size, side, positionSide, orderType)
                    try:
                        if response['status'] == "NEW":
                            for order in orders:
                                self.liveId.find_one_and_update({"orderId":order['orderId']}, {"$set": {'exit':True}})
                            self.updateTrades(scalper, cumulative_pnl, time.ctime())
                    except:
                        _log(response, "error")
                if scalper['status'] == "DEMO":                   
                    for order in orders:
                        self.demoId.find_one_and_update({"orderId":order['orderId']}, {"$set": {'exit':True}})
                self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$set": {"marginUsed":0}})

            self.initMongo().scalper.find_one_and_update({"name":scalper['name']},{"$inc": {"performance":cumulative_pnl}})
            return

    def run(self):
        df = pd.DataFrame(list(self.scalper.find({"$or": [{'status':"LIVE"},{'status':"DEMO"}]})))
        locks = []
        n = range(len(df))
        for i in n:
            lock = _thread.allocate_lock()
            a = lock.acquire()
            locks.append(lock) 

        for i in n:
            _thread.start_new_thread(self.entry_trigger,(df.iloc[i], locks[i]))
            _thread.start_new_thread(self.exit_trigger,(df.iloc[i], locks[i]))
        
        for i in n:
            while locks[i].locked(): pass
    
        return

if __name__ == "__main__":
    while True:
        scalper().run()
    
