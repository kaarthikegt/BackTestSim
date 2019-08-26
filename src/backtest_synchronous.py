from src.backtest_modules import *
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
        print("Execution Mode: Synchronous")
        t = Timer()
        t.start()
        try:
            self._run_backtest()
        except KeyboardInterrupt as e:
            quit(0)
        t.end()
        print("Total Process Execution Taken:", str(t.get()) + "s")

    def _run_backtest(self):
        ts = [Timer(mode=2) for _ in range(7)]

        print("Executing Backtest")

        initial_funds = 10000

        # Fetches and caches historical price and volume data
        data = assure_init_data()

        args = dict()

        prices = []
        volumes = []

        args["user_id"] = self.user_id
        args["strategy_id"] = self.strategy_id
        args["backtest_id"] = self.backtest_id
        args["period"] = self.period
        args["time"] = 0
        args["universe"] = list()
        args["shares"] = list()
        args["prices"] = prices
        args["volumes"] = volumes
        args["trade_signals"] = list()
        args["statistics"] = list()
        args["buy_count"] = list()
        args["sell_count"] = list()
        args["initial_funds"] = initial_funds
        args["funds"] = list()
        args["trade_signals"] = list()
        args["transaction_cost"] = 6
        args["max_stock_percentage"] = 0.20

        print("Running Period:", self.period)

        ts[6].start()

        p = args["time"]

        while p < self.period:
            if p % 100 == 0:
                print(p)

            new_prices = dict()
            new_volumes = dict()
            new_shares = dict()

            if p == 0:
                for k, v in data[0][p].items():
                    new_prices[k] = v

                for k, v in data[1][p].items():
                    new_volumes[k] = v

                args["funds"].append(initial_funds)

            else:
                for k, v in args["prices"][p - 1].items():
                    new_prices[k] = v

                for k, v in args["volumes"][p - 1].items():
                    new_volumes[k] = v

                for k, v in args["shares"][p - 1].items():
                    new_shares[k] = v

                args["funds"].append(args["funds"][p - 1])

            args["prices"].append(new_prices)
            args["volumes"].append(new_volumes)
            args["shares"].append(new_shares)

            args["buy_count"].append(0)
            args["sell_count"].append(0)
            args["trade_signals"].append(list())

            ts[0].start()
            calc_universe(args)
            ts[0].end()
            ts[1].start()
            exec_strategy(args)
            ts[1].end()
            ts[2].start()
            rebal_portfolio(args)
            ts[2].end()
            ts[3].start()
            gen_order(args)
            ts[3].end()
            ts[4].start()
            calc_stats(args)
            ts[4].end()
            ts[5].start()
            push_data(args)
            ts[5].end()

            p += 1

            args["time"] = p

        ts[6].end()

        print("-----Finished Backtest:", "|", self.user_id, self.strategy_id, self.backtest_id, "|-----")
        print("Module Time Taken:\n\tcalc_universe: {calc_universe}s\n\texec_strategy: {exec_strategy}s\n\trebal_portpolio: {rebal_portfolio}s\n\tgen_order: {gen_order}s\n\tcalc_stats: {calc_stats}s\n\tpush_data: {push_data}s\n\tTotal: {total}s\n\tAvg Time per Period: {avg_time_per_period}s".format(calc_universe=ts[0].get(), exec_strategy=ts[1].get(), rebal_portfolio=ts[2].get(), gen_order=ts[3].get(), calc_stats=ts[4].get(), push_data=ts[5].get(), total=ts[6].get(), avg_time_per_period=ts[6].get() / args["period"]))

        # Print Backtest Results Stored in args dict
        total_period = args["period"]
        final_funds = args["funds"][args["time"] - 1]
        final_shares = args["shares"][args["time"] - 1]
        final_balance = calc_balance(args["period"] - 1, args["prices"], final_funds, final_shares)
        net = final_balance - initial_funds

        print()
        print("Total Period:", total_period)
        print("Initial Balance:", initial_funds)
        print("Remaining Funds:", final_funds)
        print("Final Balance:", final_balance)
        print("Net:", net)
        print("Final Shares:", final_shares)

        for s, _ in final_shares.items():
            print(s, ":", args["prices"][args["period"] - 1][s])
