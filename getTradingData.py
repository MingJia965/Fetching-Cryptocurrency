# -*- coding:utf-8 -*-
# Author: Mingjia Xie
# time: 2021/7/15
import ccxt
import time
import pandas as pd
from datetime import datetime


class getTradingData:
    """
    This class is used to load trading data
    """

    def __init__(self, exchange, type):
        """
        :param exchange: choose the exchange to load trading data
        :param type: spot, future
        """
        self.type = type
        self.exchange = exchange

    @property
    def set_exchange(self):
        """
        It is used to set exchange
        :return: exchange
        """
        params = {
            'timeout': 30000,
            'enableRateLimit': True,
            'rateLimit': 250,
            'options': {'defaultType': self.type, 'hedgeMode': True
                        }
        }

        if self.exchange == 'binance':
            return ccxt.binance(params)

        elif self.exchange == 'huobi':
            return ccxt.huobipro(params)
        else:
            print("Exchange Error: the supported exchanges are ['binance', 'huobi']")
            return None

    def get_kline_data(self, symbol, frequency='1m', since=None, limit=None):
        """
        get kline data
        :param symbol: spot, future
        :param frequency: minute: 1m, daily:1d
        :param since: the start time
        :param limit: get limit number of recent bars
        :return: OHCLV
        """

        self._exchange = self.set_exchange
        if self._exchange.has['fetchOHLCV']:
            OHLCV = self._exchange.fetchOHLCV(symbol=symbol, timeframe=frequency, since=since,limit=limit)
            OHLCV = pd.DataFrame(OHLCV)
            OHLCV.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            OHLCV['datetime'] = OHLCV['datetime'].apply(
                lambda x: datetime.utcfromtimestamp(x / 1000).strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            print('Exchange Error: No fetchOHLCV in %s' % self.exchange)

        OHLCV['exchange'] = self.exchange
        OHLCV['symbol'] = symbol.replace('/','')
        OHLCV['type'] = self.type

        return OHLCV[['datetime','exchange','symbol','type','open', 'high', 'low', 'close', 'volume']]

    def load_data(self,symbol,start_time, end_time=None):

        timeArray = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = int(time.mktime(timeArray) * 1000)

        if end_time is None:
            end_time = datetime.utcnow().timestamp() * 1000
        else:
            timeArray = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(time.mktime(timeArray) * 1000)

        OHLCV = self.get_kline_data(symbol=symbol,since=start_time)

        while start_time < end_time:
            try:
                start_time += 60000 * 499
                new = self.get_kline_data(symbol=symbol, since=start_time)
                OHLCV = OHLCV.append(new)
                time_local = time.localtime(start_time / 1000)
                print("Continuing with %s for %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time_local), symbol))
                time.sleep(0.05)
            except Exception as e:
                print(e)
                print('error happened at: %s' % start_time)
                time.sleep(3)

        OHLCV.drop_duplicates('datetime', inplace=True)
        OHLCV['datetime'] = pd.to_datetime(OHLCV['datetime'])
        OHLCV.set_index('datetime', inplace=True)
        OHLCV.to_parquet(f'cryptocurrency/{self.type}/{symbol.replace("/","")}.parquet')

        return OHLCV

