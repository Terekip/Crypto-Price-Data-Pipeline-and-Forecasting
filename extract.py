from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from pycoingecko import CoinGeckoAPI
import psycopg2

user ='avnadmin'
password='AVNS_5fLPUVkBBuUzmOuroVq'
host='pg-3700d966-gilbert-c4d7.c.aivencloud.com'
port='26765'
database='defaultdb'
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def fetch_crypto_data():
    cg = CoinGeckoAPI()
    coins = ['bitcoin', 'ethereum','beam-bridged-usdt-beam',
            'bifrost-bridged-bnb-bifrost', 'solana',
             'dogecoin', 'tron', 'cardano', 'polkadot',
            'chainlink','avalanche-2', 'litecoin',
            'uniswap', 'stellar', 'aptos',
            'render-token', 'polygon-ecosystem-token'               ]

    rename_dict = {
            'aptos': 'Aptos(APT)',
            'avalanche-2': 'Avalanche(AVAX)',
            'beam-bridged-usdt-beam': 'USDT',
            'bifrost-bridged-bnb-bifrost': 'BNB',
            'bitcoin': 'BTC', 'cardano': 'Cardano(ADA)',
            'chainlink': 'Chainlink(LINK)',
            'dogecoin': 'DOGE', 'ethereum': 'ETH',
            'litecoin': 'Litecoin(LTC)',
            'polygon-ecosystem-token': 'Polygon',
            'polkadot': 'Polkadot(DOT)',
            'render-token': 'Render(RNDR)', 'solana': 'SOL',
            'stellar': 'Stellar(XLM)',
            'tron': 'TRX', 'uniswap': 'Uniswap(UNI)'
        }

    now = datetime.utcnow()
    from_ts = int((now - timedelta(hours=1)).timestamp())
    to_ts = int(now.timestamp())
    for coin in coins:
        try:
           chart_data =cg.get_coin_market_chart_range_by_id(id=coin,
                                                            vs_currency='usd',
                                                            from_timestamp=from_ts,
                                                            to_timestamp=to_ts)
                                        
           coin_data = [{"timestamp": datetime.utcfromtimestamp(ts / 1000), 
                        "coin": rename_dict.get(coin, coin),  
                         "price": round(price, 2)
                        }
           for ts, price in chart_data.get("prices", [])
                       ]
           if coin_data:
                df = pd.DataFrame(coin_data)
                with engine.connect() as connection:
                    df.to_sql('crypto_prices_raw', con=connection,if_exists='append', index=False)
        except Exception as e:
            print(f"Error fetching {coin}: {e}")  
            #time.sleep(1.5)
    return 'Data successfully fetched'


