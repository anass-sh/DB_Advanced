from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import pymongo as mongo
import redis

r = redis.Redis(host='redis', port=6379, db=0, charset="utf-8" ,decode_responses=True)

myclient = mongo.MongoClient("mongo:27017")
mydb = myclient["Bitcoin"]
mycol = mydb["Values"]

def parser(): 
  
    hash = r.lrange("hash", 0, 1000)
    time = r.lrange("time", 0, 1000)
    btc = r.lrange("btc", 0, 1000)
    usd = r.lrange("usd", 0, 1000)    

    data = {
        'Hash' : hash,
        'Time' : time,
        'Amount BTC' : btc,
        'Amount USD' : usd
    }
    df = pd.DataFrame(data)
    df = df.sort_values(by=['Amount BTC'], ascending = False)
    df = df.head(1)

    arr = df.columns
    mydict = {}

    lista = df.stack().tolist()
    for x in range(len(lista)):
        mydict[arr[x]] = lista[x]

    x = mycol.insert_one(mydict)  
    x = mycol.insert_one

def scraper():
    response = requests.get('https://www.blockchain.com/btc/unconfirmed-transactions')

    content = response.content
    parser = BeautifulSoup(content, 'html.parser')


    hashs = parser.find_all('div', class_='sc-1au2w4e-0 bTgHwk')
    Time = parser.find_all('div', class_='sc-1au2w4e-0 emaUuf')
    amount = parser.find_all('div', class_='sc-1au2w4e-0 fTyXWG')

    
    amountusd= []
    amountbtc = []
    hash = []
    time = []

    for i in hashs:
        hash.append(i.text[4:])
    for i in Time:
        time.append(i.text[4:])
    for i in amount:
        if 'BTC' in i.text:
            amountbtc.append(i.text[12:])
        else:
            amountusd.append(i.text[12:])

    data = {
        'Hash' : hash,
        'Time' : time,
        'Amount BTC' : amountbtc,
        'Amount USD' : amountusd
    }

    df = pd.DataFrame(data)
    df = df.sort_values(by=['Amount BTC'], ascending = False)
    df = df.head(10)
    df
  
     for x in hash:
        r.lpush('hash', x)
    for x in time:
        r.lpush('time', x)
    for x in amountbtc:
        r.lpush('btc', x)
    for x in amountusd:
        r.lpush('usd', x)
    
while True:
    r.flushall()
    scraper()
    parser()
    time.sleep(60)
