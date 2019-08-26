import json
from random import randint
from noise import pnoise3
from time import time, sleep

from multiprocessing import Process, Manager

symbol_list_cache = None
seed = (time() * 1000 % 1000000)


def calc_balance(time, prices, funds, shares):
    result = funds
    for symbol, s in shares.items():
        value = get_price(time, prices, symbol) * s
        result += value
    return result


def fetch_symbol_list():
    global symbol_list_cache

    if symbol_list_cache:
        return symbol_list_cache
    with open("symbols", "r") as f:
        result = [l[:-1] for l in f.readlines()]
    symbol_list_cache = result
    return result


def fetch_symbol_price_change(time, prices, volumes, s):
    symbols = fetch_symbol_list()
    old_price = get_price(time, prices, s)
    new_price = gen_price(time, symbols.index(s), old_price)

    price_change = new_price - old_price

    prices[time][s] = new_price

    # Adding entropy to stock volume

    volume = get_volume(time, volumes, s)

    if volume > 0:
        op_type = (-1 if price_change > 0 else 1)
        change_perc = abs(price_change / old_price)
        multiplier = (randint(0, int(round(volume * change_perc))))
        volumes[time][s] += op_type * multiplier

    # Clamp volume to zero
    if get_volume(time, volumes, s) <= 0:
        volumes[time][s] = 0

    return old_price, new_price, price_change


init_prices = None
init_volumes = None


def assure_init_data():
    global init_prices, init_volumes

    if init_prices and init_volumes:
        print("Returning memory cached data")
        return init_prices, init_volumes

    init_prices = list()
    init_prices.append(dict())
    init_volumes = list()
    init_volumes.append(dict())

    # Attempt to fetch cached data from disk
    try:
        with open("symbol_data", "r") as f:
            print("Loading cached price data")
            packed_data = json.load(f)
            init_prices[0] = packed_data["price_data"]
            init_volumes[0] = packed_data["volume_data"]

            symbols = fetch_symbol_list()

            flag = True

            for s in symbols:
                if s not in init_prices[0].keys() or s not in init_volumes[0].keys():
                    print("Symbol Data Incomplete or Corrupted")
                    flag = False
                    break
            if flag:
                print("Returning disk cached data")
                return init_prices, init_volumes
    except FileNotFoundError as e:
        print("No Cached Price Data Found")
    except KeyError as e:
        print("Symbol Data Corrupted!")

    return init_prices, init_volumes


# Overwrite Mode: Each period overwrites the previous periods data
# Append Mode: Each period appends data to file. Previous Runs are stored. Not Supported Format.
def save_data(user_id, strategy_id, backtest_id, data, mode="overwrite"):
    with open("./backtest_results/{user_id}-{strategy_id}-{backtest_id}".format(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id), "w" if mode == "overwrite" else ("a" if mode == "append" else "w")) as f:
        json.dump(data, f)


def load_data(user_id, strategy_id, backtest_id):
    with open("./backtest_results/{user_id}-{strategy_id}-{backtest_id}".format(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id)) as f:
        return json.load(f)


def gen_price(time, index, old_price):
    global seed
    min_price = 0.001
    scale = 50
    new_price = old_price + pnoise3(time / scale, index, seed) * randint(1, max(1, int(round(0.10 * old_price * 1000)))) / 1000
    return min_price if new_price < min_price else new_price
    # return old_price + sin(self.time / scale) * randint(1, max(1, int(round(0.10 * old_price * 1000)))) / 1000
    # return old_price + sin(self.time / scale)


def gen_volume():
    min = 10
    max = 10000
    return randint(min, max)


def get_price(time_index, prices, symbol):
    return prices[time_index][symbol]


def get_volume(time_index, volumes, symbol):
    return volumes[time_index][symbol]


def gen_sell_signal(symbol, quantity):
    return TradeSignal(signal_type=-1, symbol=symbol, quantity=quantity)


def gen_buy_signal(symbol, quantity):
    return TradeSignal(signal_type=1, symbol=symbol, quantity=quantity)


class TradeSignal:

    def __init__(self, signal_type, symbol, quantity):
        self.signal_type = signal_type
        self.symbol = symbol
        self.quantity = quantity

    def __str__(self):
        return "{signal_type}, {symbol}, {quantity}".format(signal_type=("Buy" if self.signal_type == 1 else "Sell"), symbol=self.symbol, quantity=self.quantity)

    def __repr__(self):
        return "<{signal_type}, {symbol}, {quantity}>".format(signal_type=("Buy" if self.signal_type == 1 else "Sell"), symbol=self.symbol, quantity=self.quantity)
