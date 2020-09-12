import requests
import json
import asyncio
import hmac
import hashlib
import binascii


class connector:
    def __init__(self, api_key, secret_key, symbol):
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbol = symbol
        self.limit = 5
        self.interval = 1

        self.header = {
            'content-type': 'application/json',
            'X-MBX-APIKEY': f'{self.api_key}'
        }  

        self.host = f"https://fapi.binance.com"
        self.params = {
        }

        #enable hedge by default
        self.hedge_mode()

    def createsignature(self,params):
        query_string = []
        for key in params.keys():
            vals = params[key]
            query_string.append(str(key + '=' + str(vals)))
        query_string = "&".join(query_string)
        query_string = query_string.encode('utf-8')
        return hmac.new(self.secret_key.encode('utf-8'), query_string, hashlib.sha256).hexdigest().upper()

    def get_servertime(self):
        url = self.host + "/fapi/v1/time"
        response = requests.get(url=url, headers=self.header)
        return response.json()['serverTime']
    
    def getBBO(self):
        url = self.host + "/fapi/v1/depth"
        params = {
            "symbol":self.symbol,
            "limit":self.limit
        }
        response = requests.get(url=url, headers=self.header, params=params)
        return response.json()
    
    def get_accountinfo(self):
        url = self.host + "/fapi/v1/account"
        params ={
            "recvWindow": 5000,
            "timestamp": self.get_servertime()
        }
        params['signature'] = self.createsignature(params)
        response = requests.get(url=url, headers=self.header, params=params)
        return response.json()
    
    def get_balance(self):
        url = self.host + "/fapi/v1/balance"
        params ={
            "recvWindow": 5000,
            "timestamp": self.get_servertime()
        }
        params['signature'] = self.createsignature(params)
        response = requests.get(url=url, headers=self.header, params=params)
        return response.json()
    
    def post_order(self, symbol, qty, side, positionSide, orderType):
        url = self.host + "/fapi/v1/order"
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": positionSide,
            "type": orderType,
            "quantity": qty,
            "timestamp": self.get_servertime(),
            "recvWindow": 10000
        }
        params['signature'] = self. createsignature(params)
        response = requests.post(url=url, headers=self.header, params=params)
        return response.json()
    
    def get_trades(self, orderId, symbol):
        url = self.host + "/fapi/v1/userTrades"
        params = {
            "symbol": symbol,
            "fromId": orderId,
            "timestamp": self.get_servertime(),
        }
        params['signature'] = self.createsignature(params)
        response = requests.get(url=url, headers=self.header, params=params)
        return response.json()
    
    def update_leverage(self, symbol, leverage):
        url = self.host + "/fapi/v1/leverage"
        params = {
            "symbol": symbol,
            "leverage": leverage,
            "timestamp": self.get_servertime()
        }
        params['signature'] = self.createsignature(params)
        response = requests.post(url=url, headers=self.header, params=params)
        return response.json()
    
    def hedge_mode(self):
        url = self.host + "/fapi/v1/positionSide/dual"
        params = {
            "dualSidePosition": "true",
            "timestamp": self.get_servertime()
        }
        params['signature'] = self.createsignature(params)
        response = requests.post(url=url, headers=self.header, params=params)
        return response.json()
    
    def query_order(self, symbol, orderId):
        url = self.host + "/fapi/v1/openOrder"
        params = {
            "symbol": symbol,
            "origClientOrderId": orderId,
            "timestamp": self.get_servertime()
        }
        params['signature'] = self.createsignature(params)
        response = requests.get(url=url, headers=self.header, params=params)
        return response.json()

        
if __name__ == "__main__":
    pass
    

        
