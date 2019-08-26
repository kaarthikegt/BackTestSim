from multiprocessing import Manager
from collections import defaultdict
from copy import deepcopy

from src.backtest_utils import *
from src.timer import Timer


class BackTest(Process):

    def __init__(self, user_id, strategy_id, backtest_id, period=365):
        self.user_id = user_id
        self.strategy_id = strategy_id
        self.backtest_id = backtest_id
        self.period = period
        self._manager = Manager()

        super().__init__(target=self.run_backtest, args=())

    def run_backtest(self):
        print("Execution Mode: Multiprocessing")
        t = Timer()
        t.start()
        try:
            self._run_backtest()
        except KeyboardInterrupt as e:
            quit(0)
        t.end()
        print("Total Time Taken:", str(t.get()) + "s")

    def _run_backtest(self):
        ts = [Timer(mode=1) for _ in range(7)]

        print("Executing Backtest")

        initial_funds = 10000

        # Fetches and caches historical price data
        data = assure_init_data(self._manager)

        # Must Convert Prices and volumes Lists to Managed Lists to Ensure Appends Are Persistent
        prices = self._manager.list()
        volumes = self._manager.list()

        # Must occur before prices and volumes declaration in manager to allow these to be static and synced from host process to all child processes
        for i in range(self.period):

            new_prices = self._manager.dict()
            new_volumes = self._manager.dict()

            if i == 0:
                for k, v in data[0][i].items():
                    new_prices[k] = v

                for k, v in data[1][i].items():
                    new_volumes[k] = v
            else:
                for k, v in prices[i - 1].items():
                    new_prices[k] = v

                for k, v in volumes[i - 1].items():
                    new_volumes[k] = v

            prices.append(new_prices)
            volumes.append(new_volumes)

        # Must use manager to ensure all values are shallow-synced
        kwargs = self._manager.dict()

        kwargs["user_id"] = self.user_id                    # Static
        kwargs["strategy_id"] = self.strategy_id            # Static
        kwargs["backtest_id"] = self.backtest_id            # Static
        kwargs["period"] = self.period                      # Static
        kwargs["stage_a_time"] = 0                          # Dynamic
        kwargs["stage_b_time"] = 0                          # Dynamic
        kwargs["stage_c_time"] = 0                          # Dynamic
        kwargs["universe"] = self._manager.list()           # Dynamic
        kwargs["shares"] = self._manager.dict()             # Dynamic
        kwargs["prices"] = prices                           # Static
        kwargs["volumes"] = volumes                         # Static
        kwargs["trade_signals"] = self._manager.Queue()     # Dynamic
        kwargs["statistics"] = self._manager.list()         # Dynamic
        kwargs["buy_count"] = 0                             # Dynamic
        kwargs["sell_count"] = 0                            # Dynamic
        kwargs["initial_funds"] = initial_funds             # Static
        kwargs["funds"] = initial_funds                     # Dynamic
        kwargs["transaction_cost"] = 6                      # Static
        kwargs["max_stock_percentage"] = 0.20               # Static

        print("Running Period:", self.period)

        ts[6].start()

        stage_a_proc = Process(target=self.stage_a, args=(kwargs,))
        stage_b_proc = Process(target=self.stage_b, args=(kwargs,))
        stage_c_proc = Process(target=self.stage_c, args=(kwargs,))

        stage_a_proc.start()
        stage_b_proc.start()
        stage_c_proc.start()

        stage_a_proc.join()
        stage_b_proc.join()
        stage_c_proc.join()

        ts[6].end()

        print("-----Finished Backtest:", "|", self.user_id, self.strategy_id, self.backtest_id, "|-----")
        print("Module Time Taken:\n\tcalc_universe: {calc_universe}s\n\texec_strategy: {exec_strategy}s\n\trebal_portpolio: {rebal_portfolio}s\n\tgen_order: {gen_order}s\n\tcalc_stats: {calc_stats}s\n\tpush_data: {push_data}s\n\tTotal: {total}s".format(calc_universe=ts[0].get(), exec_strategy=ts[1].get(), rebal_portfolio=ts[2].get(), gen_order=ts[3].get(), calc_stats=ts[4].get(), push_data=ts[5].get(), total=ts[6].get()))

        final_funds = kwargs["funds"]
        final_shares = kwargs["shares"]
        final_balance = calc_balance(kwargs["period"] - 1, kwargs["prices"], final_funds, final_shares)
        net = final_balance - initial_funds

        print("Initial Balance:", initial_funds)
        print("Remaining Funds:", final_funds)
        print("Final Balance:", final_balance)
        print("Net:", net)
        print("Final Shares:", final_shares)

    def stage_a(self, kwargs):
        try:
            period = kwargs["period"]
            stage_a_time = kwargs["stage_a_time"]

            while stage_a_time < period:
                # Start Universe Calculation for Each Period
                self.calc_universe(kwargs)

                stage_a_time += 1
                kwargs["stage_a_time"] = stage_a_time
        except KeyboardInterrupt:
            quit(0)

        print("Completed Stage A")

    def stage_b(self, kwargs):
        try:
            period = kwargs["period"]
            stage_a_time = kwargs["stage_a_time"]
            stage_b_time = kwargs["stage_b_time"]
            stage_c_time = kwargs["stage_c_time"]

            while stage_b_time < period:
                # Check to see if there are available universes on which to execute the strategy
                # Check to see that previous stage c handling has been completed for the next period
                if stage_a_time > stage_b_time and stage_b_time <= stage_c_time:
                    # Start Strategy Execution
                    self.exec_strategy(kwargs)
                    stage_b_time += 1
                    kwargs["stage_b_time"] = stage_b_time

                stage_a_time = kwargs["stage_a_time"]
                stage_c_time = kwargs["stage_c_time"]
        except KeyboardInterrupt:
            quit(0)

        print("Completed Stage B")

    def stage_c(self, kwargs):
        try:
            period = kwargs["period"]
            stage_b_time = kwargs["stage_b_time"]
            stage_c_time = kwargs["stage_c_time"]

            while stage_c_time < period:
                if stage_c_time < stage_b_time:
                    # Start Portfolio Rebalancing
                    self.rebal_portfolio(kwargs)

                    # Start Generating Orders
                    self.gen_order(kwargs)

                    # Start Statistics Calculation
                    self.calc_stats(kwargs)

                    # Start Pushing Data
                    self.push_data(kwargs)

                    stage_c_time += 1
                    kwargs["stage_c_time"] = stage_c_time

                stage_b_time = kwargs["stage_b_time"]
        except KeyboardInterrupt:
            quit(0)

        print("Completed Stage C")

    @staticmethod
    def calc_universe(kwargs={}):
        result = [s for s in fetch_symbol_list() if len(s) == 1]
        kwargs["universe"].append(result)

    @staticmethod
    def exec_strategy(kwargs={}):
        time = kwargs["stage_b_time"]
        universe = kwargs["universe"][time]
        prices = kwargs["prices"]
        volumes = kwargs["volumes"]
        funds = kwargs["funds"]
        shares = kwargs["shares"]
        transaction_cost = kwargs["transaction_cost"]
        trade_signals = kwargs["trade_signals"]

        balance_buffer = funds

        for s in universe:
            old_price, new_price, price_diff = fetch_symbol_price_change(time, prices, volumes, s)
            stock_shares = shares[s] if s in shares else 0
            volume = get_volume(time, volumes, s)

            # if abs(price_diff/old_price) < 0.05:
            #     continue
            old_bal = balance_buffer

            if new_price < old_price:
                # Sell
                if stock_shares > 0:
                    max_sell_out_percentage = 0.10
                    sell_out_capacity = int(max_sell_out_percentage * stock_shares)
                    sell_target = randint(0, sell_out_capacity)

                    if sell_target > 0 and sell_target * new_price >= transaction_cost:
                        trade_signals.append(gen_sell_signal(s, sell_target))
                        balance_buffer += (sell_target * new_price - transaction_cost)
                        kwargs["sell_count"] += 1

                # TODO: Remove test code
                if balance_buffer < old_bal:
                    print("Sell Error")
            elif new_price > old_price:
                # Buy
                max_buy_out_percentage = 0.10
                max_buy_out_capcity = int(max_buy_out_percentage * volume)
                buy_capcity = int((balance_buffer - transaction_cost) / new_price)
                buy_target = min(randint(0, max_buy_out_capcity), buy_capcity)

                if buy_target > 0:
                    trade_signals.append(gen_buy_signal(s, buy_target))
                    balance_buffer -= (buy_target * new_price + transaction_cost)
                    kwargs["buy_count"] += 1

                # TODO: Remove test code
                if balance_buffer > old_bal:
                    print("Buy Error")

    @staticmethod
    def _get_corresponding_signal(trade_signals, symbol):
        for ts in trade_signals:
            if ts.symbol == symbol:
                return ts

    @staticmethod
    def rebal_portfolio(kwargs={}):
        time = kwargs["stage_c_time"]
        prices = kwargs["prices"]
        funds = kwargs["funds"]
        shares = kwargs["shares"]
        trade_signals = kwargs["trade_signals"]
        transaction_cost = kwargs["transaction_cost"]
        max_stock_percentage = kwargs["max_stock_percentage"]
        balance = calc_balance(time, prices, funds, shares)

        signals_to_remove = []

        for s, c in shares.items():
            price = get_price(time, prices, s)
            trade_signal = BackTest._get_corresponding_signal(trade_signals, s)
            stock_percentage = (price * c) / balance
            post_trade_stock_percentage = stock_percentage if not trade_signal else (price * (c + trade_signal.signal_type * trade_signal.quantity))

            if stock_percentage > max_stock_percentage:
                # Reduce Signal Quantity And Stock Quantity If Needed
                post_trade_quantity = c

                if trade_signal:
                    if trade_signal.signal_type < 0:
                        post_trade_quantity -= trade_signal.quantity
                    elif trade_signal.signal_type > 0 and post_trade_stock_percentage > max_stock_percentage:
                        # Remove Order
                        signals_to_remove.append(trade_signal)

                post_trade_stock_percentage = (price * post_trade_quantity) / balance

                if post_trade_stock_percentage > max_stock_percentage:
                    sell_target = int((post_trade_stock_percentage - max_stock_percentage) * balance / price)

                    # print("Reduce Target", sell_target, "|", (price * (post_trade_quantity - sell_target)) / balance)
                    trade_signals.append(gen_sell_signal(s, sell_target))
                    funds += (sell_target * price - transaction_cost)
            elif stock_percentage < max_stock_percentage:
                # Reduce Signal Quantity If Needed

                post_trade_quantity = c

                if trade_signal and trade_signal.signal_type > 0:
                    post_trade_quantity += trade_signal.quantity

                    post_trade_stock_percentage = (price * post_trade_quantity) / balance

                    if post_trade_stock_percentage > max_stock_percentage:
                        # Reduce Buy Quantity

                        reduce_target = int((post_trade_stock_percentage - max_stock_percentage) * balance / price)

                        # print("Reduce Target", reduce_target, "|", (price * (post_trade_quantity - reduce_target)) / balance)
                        trade_signal.quantity -= reduce_target

        # for s in signals_to_remove:
        #     for i in range(len(trade_signals) - 1, -1, -1):
        #         ts = trade_signals[i]
        #
        #         if ts.symbol == s.symbol:
        #             trade_signals.remove(i)

    @staticmethod
    def gen_order(kwargs={}):
        time = kwargs["stage_c_time"]
        prices = kwargs["prices"]
        funds = kwargs["funds"]
        shares = kwargs["shares"]
        trade_signals = kwargs["trade_signals"]
        transaction_cost = 6

        for s in trade_signals:
            # Lazy add symbols to shares list if they don't already exist
            if s.symbol not in shares:
                shares[s.symbol] = 0

            shares[s.symbol] += (s.signal_type * s.quantity)
            funds += (-s.signal_type * s.quantity * get_price(time, prices, s.symbol))

        kwargs["funds"] = funds - transaction_cost * len(trade_signals)

    @staticmethod
    def calc_stats(kwargs={}):
        time = kwargs["stage_c_time"]
        initial_funds = kwargs["initial_funds"]
        funds = kwargs["funds"]
        shares = kwargs["shares"]
        buy_count = kwargs["buy_count"]
        sell_count = kwargs["sell_count"]
        balance = calc_balance(time, kwargs["prices"], funds, shares)
        net = balance - initial_funds

        kwargs["statistics"].append({"time": time, "initial_funds": initial_funds, "funds": funds, "shares": shares, "balance": balance, "net": net, "buy_count": buy_count, "sell_count": sell_count})

    @staticmethod
    def push_data(kwargs={}):
        user_id = kwargs["user_id"]
        strategy_id = kwargs["strategy_id"]
        backtest_id = kwargs["backtest_id"]
        statistics = kwargs["statistics"]

        # data = []
        #
        # for s in statistics:
        #     d = dict()
        #
        #     for k, v in s.items():
        #
        #
        #         d[k] = v
        #
        #     data.append(d)
        #
        # save_data(user_id, strategy_id, backtest_id, [])
