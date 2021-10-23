from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import pymongo as mongo

def scraper():
    response = requests.get('https://www.blockchain.com/btc/unconfirmed-transactions')

    content = response.content
    parser = BeautifulSoup(content, 'html.parser')


    hashs = parser.find_all('div', class_='sc-1au2w4e-0 bTgHwk')
    Time = parser.find_all('div', class_='sc-1au2w4e-0 emaUuf')
    amount = parser.find_all('div', class_='sc-1au2w4e-0 fTyXWG')

    myclient = mongo.MongoClient("mongodb ://127.0.0.1:27017")
    mydb = myclient["Bitcoin"]
    mycol = mydb["Values"]

    amountusd= []
    amountbtc = []
    hash = []
    tijd = []

    for i in hashs:
        hash.append(i.text[4:])
    for i in Time:
        tijd.append(i.text[4:])
    for i in amount:
        if 'BTC' in i.text:
            amountbtc.append(i.text[12:])
        else:
            amountusd.append(i.text[12:])

    data = {
        'Hash' : hash,
        'Time' : tijd,
        'Amount BTC' : amountbtc,
        'Amount USD' : amountusd
    }

    df = pd.DataFrame(data)
    df = df.sort_values(by=['Amount BTC'], ascending = False)
    df = df.head(10)

    arr = df.columns
    mydict = {}

    lijst = df.stack().tolist()
    for x in range(len(lijst)):
        mydict[arr[x]] = lijst[x]
    
    x = mycol.insert_one(mydict)  
    x = mycol.insert_one
    

while True:
    scraper()
    time.sleep(60)