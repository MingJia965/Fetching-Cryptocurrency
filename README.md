# Fetching-Cryptocurrency
This repository is used to obtain the newest Kline trading data for cryptocurrency at a frequency of 1 minute.
We can get cryptocurrency's spot or future data.



## example
To get the future of BTC/USDT from 2021-08-01 00:00:00, by typing this command in terminal.

```
python loading_data.py 'BTC/USDT' '2021-08-01 00:00:00' 'future'
```
## Note
You need to install ccxt and pandas.
```
pip install ccxt

pip install pandas
```
