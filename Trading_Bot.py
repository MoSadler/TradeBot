import requests, json
import pandas as pd
from datetime import date
from datetime import datetime
import time

BARS_URL = 'https://data.alpaca.markets/v1/bars'
BASE_URL = "https://paper-api.alpaca.markets"
ACCOUNT_URL = "{}/v2/account".format(BASE_URL)
ORDERS_URL = "{}/v2/orders".format(BASE_URL)

API_KEY = 'PK8OCYR51MX0TNU8WHKU'
SECRET_KEY = '1WMYJKhu9570QZFiTLw699k4sMoJA9vX25ZrS05o'

HEADERS = {'APCA-API-KEY-ID': API_KEY,'APCA-API-SECRET-KEY':SECRET_KEY}

investment = 10000

def get_bar(today,yesterday):
    bar = ''
    #1
    if today['h']<=yesterday['h'] and today['l']>=yesterday['l']:
        bar = '1'
    elif today['h']>yesterday['h'] and today['l']>yesterday['l']:
        bar = '2u'
    elif today['h']<yesterday['h'] and today['l']<yesterday['l']:
        bar = '2d'
    else:
        bar = '3'
    
    #print(bar)
    return bar

def get_target_diff(today,yesterday,direction):
    if direction == 'up':
        diff = yesterday['h']-today['h']
    elif direction == 'down':
        diff = today['l']-yesterday['l']
    else:
        diff = 0
    
    #print(diff)
    return diff

def get_continuity(month,week):
    if month*week > 1:
        continuity =1
    else:
        continuity = 0
    
    #print(continuity)
    return continuity

def get_direction(month,week):
    if month>0 and week>0:
        direction = 'up'
    elif month<0 and week<0:
        direction = 'down'
    else: 
        direction = 'N/A'
    #print(direction)
    return direction

def get_target_percent(today,diff):
    percent = diff/today['c']
    #print(percent)
    return percent

def get_time():
    now = datetime.now().time()
    #print(now.hour)
    if now.hour<8 or now.hour <9 and now.minute < 30:
        timeAdj = 0
    elif now.hour>= 15:
        timeAdj = 0
    else:
        timeAdj= 1
    #print(timeAdj)
    return timeAdj
    
def get_account():
    
    r = requests.get(ACCOUNT_URL, headers=HEADERS)
    
    print(r.content)

def create_order(symbol, qty, side, type, time_in_force,stop_price,limit_price):
    
    data = {
        "symbol":symbol,
        "qty":qty,
        "side":side,
        "type":type,
        "time_in_force":time_in_force,
        "order_class":"bracket",
        "take_profit": {
            "limit_price":limit_price
            },
        "stop_loss":{
            "stop_price":stop_price
            }
        }
    
    r = requests.post(ORDERS_URL,json= data, headers=HEADERS)
    
    return json.loads(r.content)

def get_data(stocks):
    limit=30
    timeAdj = get_time()
    
    datetime = date.today()
    stocks = stocks
    
    day_bars_url = '{}/day?symbols={}&limit={}'.format(BARS_URL, stocks,limit)
    
    r = requests.get(day_bars_url,headers=HEADERS)
    
    data = json.loads(r.content)
    
    data = pd.DataFrame.from_dict(data)
        
    column_names = ["Stock","Date", "Prev Day Low", "Prev Day High","Open","Close","Day Low","Day High","Volume","Bar","Prev Day Bar","Target Diff","Target %","Month Performance","Week Performance","Continuity","Direction","Traded","9 EMA", "20 EMA"]
    
    df = pd.DataFrame(columns = column_names)
    
    
    for ticker in data:
        column = ["Stock","Open", "High", "Low","Close"]
        ema_df = pd.DataFrame(columns = column)
        test = data[ticker]
        for thing in test:
            row = {"Stock":ticker,"Open":thing['o'],"High":thing['h'],"Low":thing['l'],"Close":thing['c']}
            ema_df = ema_df.append(row, ignore_index=True)
        ema_df['9dayEWM'] = ema_df['Close'].ewm(span=9, adjust=False).mean()
        ema_df['20dayEWM'] = ema_df['Close'].ewm(span=20, adjust=False).mean()

        ema_9 = ema_df['9dayEWM'][29]
        ema_20 = ema_df['20dayEWM'][29]
        

        today = data[ticker][limit-timeAdj-1]
        yesterday = data[ticker][limit-timeAdj-2]
        three_days_ago = data[ticker][limit-timeAdj-3]
        week = data[ticker][limit-timeAdj-6]
        month = data[ticker][0]
        prevLow = yesterday['l']
        volume = today['v']
        prevHigh = yesterday['h']
        high = today['h']
        open = today['o']
        close = today['c']
        low = today['l']
        monthPerf = high - month['o']
        weekPerf = high - week['o']
        bar = get_bar(today,yesterday)
        previous_bar = get_bar(yesterday,three_days_ago)
        continuity = get_continuity(monthPerf, weekPerf)
        direction = get_direction(monthPerf,weekPerf)
        targetDiff = get_target_diff(today,yesterday,direction)
        targetPercent = get_target_percent(today,targetDiff)
        new_row = {"Stock":ticker,"Date":datetime, "Prev Day Low":prevLow, "Prev Day High":prevHigh,"Open":open,"Close":close,"Day Low":low,"Day High":high,"Volume":volume,"Bar":bar,"Prev Day Bar":previous_bar,"Target Diff":targetDiff,"Target %":targetPercent,"Month Performance":monthPerf,"Week Performance":weekPerf,"Continuity":continuity,"Direction":direction,"Traded":'No',"9 EMA":ema_9, "20 EMA": ema_20}
        df = df.append(new_row, ignore_index=True)
    #time.sleep(60)    
    #print(df)
    return df

def to_string(df):
    string = ''
    length = len(df)
    for index, row in df.iterrows():
        temp = df['Stock'][index]
        string = string + temp
        if index<length-1:
            string = string +','
        
    return string


def stream_data(stocks,df):
        
    limit = 1
    trade = True
    run = 1

    while trade == True:
        min_bars_url = '{}/1Min?symbols={}&limit={}'.format(BARS_URL, stocks,limit)
            
        r = requests.get(min_bars_url,headers=HEADERS)
        
        data = json.loads(r.content)
        
        data = pd.DataFrame.from_dict(data)
        
        for ticker in data:
            #print(ticker)
            now = data[ticker][0]
            open = now['o']
            close = now['c']
            high = now ['h']
            low = now['l']
            #print("open",open)
            #print("close",close)
            temp_df = df.loc[df['Stock']==ticker]
            temp_df=temp_df.reset_index(drop=True)
            direction = temp_df['Direction'][0]
            traded = temp_df['Traded'][0]
            if direction == 'up'and traded =='No':
                #print(ticker, direction)
                trigger = temp_df['Day High'][0]
                profit = temp_df['Prev Day High'][0]
                stop = trigger - temp_df['Target Diff'][0]
                shares = round(investment/trigger,0)
                #print(trigger)
                if open>trigger and run == 1:
                    print(ticker, "GAP UP")
                    index = df.index
                    condition = df["Stock"] == ticker
                    index = index[condition]
                    index = index.tolist()
                    #print(index)
                    df.at[index,'Traded']='GAP UP'
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)

                
                elif close > trigger:
                    print("BUY TRIGGERED")
                    index = df.index
                    condition = df["Stock"] == ticker
                    index = index[condition]
                    index = index.tolist()
                    #print(index)
                    df.at[index,'Traded']='Yes'
                    response = create_order(ticker, shares, "buy","market","gtc",stop,profit)
                    print(response)
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)

                else: 
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)

                
            elif direction =='down' and traded =='No':
                #print(ticker, direction)
                trigger = temp_df['Day Low'][0]
                profit = temp_df['Prev Day Low'][0]
                stop = trigger + temp_df['Target Diff'][0]
                shares = round(investment/trigger,0)

                #print(trigger)
                if open<trigger and run ==1:
                    print(ticker, "GAP DOWN")
                    index = df.index
                    condition = df["Stock"] == ticker
                    index = index[condition]
                    index = index.tolist()
                    df.at[index,'Traded']='GAP DOWN'
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)

                
                elif close < trigger:
                    print("SELL TRIGGERED")
                    index = df.index
                    condition = df["Stock"] == ticker
                    index = index[condition]
                    index = index.tolist()
                    df.at[index,'Traded']='Yes'
                    response = create_order(ticker, shares, "sell","market","gtc",stop,profit)
                    print(response)
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)

                else: 
                    print("Ticker", ticker, "Open",open, "Close",close,"High",high, "Low", low, "Trigger",trigger)
                
        print (df)
        print("Run", run)
        
        run = run + 1

                    
        time.sleep(15)

watchlist = 'SOFI,NOW,MNST,BAC,AMZN,SFIX,TMDX,JPM,MRK,NKE,PM,NEM,NCLH,MAA,LVS,ISRG,CCL,DLR,DHR,F,GNRC,GPN,GWW,ISRG,AAL,ABT,ACN,AEE,ANET,ARE,AMD,ILMN,GILD,CCIV,PFE,ABNB,FUBO,COIN,SPCE,DMTK,AAPL,MSFT,NVDA,PYPL,ADBE,INTC,CSCO,AVGO,TXN,QCOM,INTU,AMAT,AMD,LRCX,MU,ZM,ADP,FISV,ADSK,ADI,NXPI,ASML,DOCU,KLAC,MRVL,WDAY,SNPS,MCHP,PAYX,CDNS,CTSH,TEAM,XLNX,OKTA,ANSS,SWKS,MXIM,VRSN,CDW,SPLK,CHKP,GOOG,FB,GOOGL,CMCSA,NFLX,TMUS,CHTR,ATVI,BIDU,MTCH,EA,NTES,SIRI,FOXA,FOX,AMZN,TSLA,SBUX,BKNG,MELI,JD,PDD,MAR,EBAY,LULU,ROST,ORLY,PTON,DLTR,TCOM,AMGN,ISRG,MRNA,GILD,ILMN,REGN,BIIB,IDXX,VRTX,ALGN,DXCM,ALXN,SGEN,CERN,INCY,PEP,COST,MDLZ,KHC,KDP,CELH,MNST,WBA,CSX,CTAS,CPRT,PCAR,FAST,VRSK,EXC,AEP,XEL,XRAY'

df = get_data(watchlist)

inside_day_df = df.loc[df['Bar']=='1']

inside_day_df = inside_day_df.loc[inside_day_df['Continuity']==1]

inside_day_df = inside_day_df.loc[inside_day_df['Target %'] > .003]

inside_day_df=inside_day_df.reset_index(drop=True)

inside_day_df['Strategy'] = 'Inside Day'



rev_strat_long_df = df.loc[df['Bar']=='2d']

rev_strat_long_df = rev_strat_long_df.loc[rev_strat_long_df['Continuity']==1]

rev_strat_long_df = rev_strat_long_df.loc[rev_strat_long_df['Direction'] =='up']

rev_strat_long_df = rev_strat_long_df.loc[rev_strat_long_df['Prev Day Bar'] =='1']

rev_strat_long_df=rev_strat_long_df.reset_index(drop=True)

rev_strat_long_df['Strategy'] = 'Rev Strat Up'




rev_strat_short_df = df.loc[df['Bar']=='2u']

rev_strat_short_df = rev_strat_short_df.loc[rev_strat_short_df['Continuity']==1]

rev_strat_short_df = rev_strat_short_df.loc[rev_strat_short_df['Direction'] =='down']

rev_strat_short_df = rev_strat_short_df.loc[rev_strat_short_df['Prev Day Bar'] =='1']

rev_strat_short_df=rev_strat_short_df.reset_index(drop=True)

rev_strat_short_df['Strategy'] = 'Rev Strat Down'



day_df = inside_day_df

day_df = day_df.append(rev_strat_long_df)

day_df = day_df.append(rev_strat_short_df)

day_df=day_df.reset_index(drop=True)

symbols = to_string(day_df)

#%%

r = stream_data(symbols, day_df)

### Next step is to update the STREAM_DATA def. I need to be able to trade any of the 3 current strategies where right now I can only trade an inside day.




#%%



