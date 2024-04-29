import ccxt, sqlite3, argparse, time, os
import pandas as pd
import requests 
import json 

class Extract_binance_data: 
    def __init__(self,db_path,symbol,market,export_dir):
        self.db_path = db_path
        self.symbols = symbol
        self.market = market
        self.export_dir = export_dir

    def download_binance_futures_data(self):
        # DB 초기화5
        db = sqlite3.connect(self.db_path)
        url2 = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        binance = ccxt.binance(
            {
                "options" : {
                    "defaultType" : self.market
                },
                "enableRateLimit" : True
            }
        )
        response2 = requests.get(url2)
        data2 = json.loads(response2.text)

        symbols2 = data2['symbols']
        futures = []
        for symbol in symbols2:
            if symbol['quoteAsset'] == 'USDT':
                futures.append(symbol['symbol'])
        len_symbols = len(futures)

        for symbol in futures:
            # 테이블 없다면 DB 만들기
            db.execute(f"""
            CREATE TABLE IF NOT EXISTS _{symbol.replace("/", "")} (
                datetime date, 
                open float, 
                high float, 
                low float, 
                close float, 
                volume float
            )""")
            # 시간 로깅 용
            t = time.time()
    
            # 테이블 만들고 커밋
            db.commit()
    
            #로깅용 
            downloaded = 0
            # 바이낸스에서 1일봉 받아오기
             
            tohlcv = binance.fetch_ohlcv(
                symbol=symbol, 
                timeframe="1d",  
                limit=100
            )
            tohlcv = pd.DataFrame(tohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
            tohlcv['datetime'] = pd.to_datetime(tohlcv['datetime'], unit='ms')
            
            
            # db에 저장
            for index, row in tohlcv.iterrows():
                # Extract values from the current row
                datetime = row['datetime']
                open = row['open']
                high = row['high']
                low = row['low']
                close = row['close']
                volume = row['volume']
    
                db.execute(f"""
                INSERT INTO _{symbol.replace('/', '')} VALUES (
                    '{datetime}', {open}, {high}, {low}, {close}, {volume})""")
            # db에 commit
            db.commit()
            # 다운로드된 양
            downloaded += len(tohlcv)
    
            # 현재 지난 시간
            delta_t = time.time() - t
            len_symbols -=1
            # Symbol 개수
    
            # 로깅
            print(f""" Symbols left : {len_symbols} downloaded {downloaded} rows for {symbol} in {round(delta_t)} seconds, download speed is {round(downloaded / delta_t)} row per second""", end="\r")
        print("\n")
        print("="*80)
        print("Data Collection Complete.")
        db.commit()
    
    
    
    def read_binance_futures_data(db_path, symbol, timeframe):
        # symbol이 BTC/USDT와 같은 형태로 들어오면 DB와 맞지 않으므로 BTCUSDT 처럼 바꿈
        symbol = symbol.replace("/", "")
    
        # DB에 연결
        db = sqlite3.connect(db_path)
    
        # fetchall() 메서드 사용해서 DB에서 데이터 받아오기
        data = db.execute(f"SELECT * FROM _{symbol}").fetchall()
        
        # DB에서 받아온 데이터로 pd.DataFrame 만들기
        data = pd.DataFrame(
            data, columns=["datetime", "open", "high", "low", "close", "volume"])
        data['datetime'] = pd.to_datetime(data['datetime'], unit='ms')
    
        # 타임프레임이 기본 (1 minute) 가 아니라면, resample
        if timeframe != "1T":
            data = data.resample(timeframe).agg(
                {
                    "open" : "first",
                    "high" : "max",
                    "low" : "min",
                    "close" : "last",
                    "volume" : "sum"
                }
            )
            data = data.ffill() # missing data 제거 (binance 서버 터짐 등)
        
        return data
    
    
    def export_data(self):
        timeframes = timeframes.split(",")
        
        # 심볼 정하기: 다운로드 코드와 같음
        if self.symbols == "all":
            binance = ccxt.binance(
                {
                    "options" : {
                        "defaultType" : "future"
                    },
                    "enableRateLimit" : True
                }
            )
            self.symbols = [mkt["symbol"] for mkt in binance.fetch_markets()]
    
        else:
            self.symbols = self.symbols.split(",")
    
        # 익스포팅 루프
        for symbol in self.symbols:
            for timeframe in timeframes:    
                # 데이터 가져오기
                df = self.read_binance_futures_data(self.db_path)
                
                # export path: export_dir/symbol_timeframe.csv
                export_path = os.path.join(self.export_dir, f'{symbol.replace("/", "")}_{timeframe}.csv')
                
                # csv로 내보내기
                df.to_csv(export_path) 
    
                print(f"exported data to {export_path}")
    
