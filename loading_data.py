from getTradingData import getTradingData
import sys
symbol = sys.argv[1]
start_time = sys.argv[2]
type = sys.argv[3]
future_data = getTradingData(exchange = 'binance', type = type)
try:
    OHLCV = future_data.load_data(symbol,start_time = start_time)
except Exception as e:
    print('Error: %s' % e)
    
    
