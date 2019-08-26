from src.backtest_modules import *
from src.backtest_utils import load_data
from src.timer import Timer

from threading import Thread

import json
import pika


class BackTest(Process):

    def __init__(self, user_id, strategy_id, backtest_id, period=365):
        self.user_id = user_id
        self.strategy_id = strategy_id
        self.backtest_id = backtest_id
        self.period = period

        super().__init__(target=self.run_backtest, args=())

    def run_backtest(self):
        print("Execution Mode: Rabbit MQ")
        t = Timer()
        t.start()
        try:
            self._run_backtest()
        except KeyboardInterrupt as e:
            quit(0)
        t.end()
        print("Total Process Execution Taken:", str(t.get()) + "s")

    def _run_backtest(self):
        ts = [Timer(mode=1) for _ in range(4)]

        print("Executing Backtest")

        initial_funds = 10000

        # Fetches and caches historical price data
        data = assure_init_data()

        # Must Convert Prices and volumes Lists to Managed Lists to Ensure Appends Are Persistent
        prices = list()
        volumes = list()

        # Must occur before prices and volumes declaration in manager to allow these to be static and synced from host process to all child processes
        for i in range(self.period):

            new_prices = dict()
            new_volumes = dict()

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
        args = dict()

        args["user_id"] = self.user_id                    # Static
        args["strategy_id"] = self.strategy_id            # Static
        args["backtest_id"] = self.backtest_id            # Static
        args["period"] = self.period                      # Static
        args["initial_funds"] = initial_funds             # Static
        args["transaction_cost"] = 6                      # Static
        args["max_stock_percentage"] = 0.20               # Static

        print("Running Period:", self.period)

        ts[3].start()

        stage_a_proc = StageA(args)
        stage_b_proc = StageB(args)
        stage_c_proc = StageC(args)

        ts[0].start()
        stage_a_proc.start()

        ts[1].start()
        stage_b_proc.start()

        ts[2].start()
        stage_c_proc.start()

        stage_a_proc.join()
        ts[0].end()

        stage_b_proc.join()
        ts[1].end()

        stage_c_proc.join()
        ts[2].end()

        ts[3].end()

        print("-----Finished Backtest:", "|", self.user_id, self.strategy_id, self.backtest_id, "|-----")
        print("Module Time Taken:\n\tStage A:\n\t\tcalc_universe: {stage_a}s\n\tStage B:\n\t\texec_strategy: {stage_b}s\n\tStage C:\n\t\trebal_portpolio + gen_order + calc_stats + push_data: {stage_c}s\n\tTotal: {total}s\n\tAvg Time per Period: {avg_time_per_period}s".format(stage_a=ts[0].get(), stage_b=ts[1].get(), stage_c=ts[2].get(), total=ts[3].get(), avg_time_per_period=ts[3].get() / args["period"]))

        # Print Backtest Results Stored in file
        data = load_data(user_id=self.user_id, strategy_id=self.strategy_id, backtest_id=self.backtest_id)[-1]

        total_period = args["period"]
        final_funds = data["funds"]
        final_shares = data["shares"]
        final_balance = calc_balance(args["period"] - 1, data["prices"], final_funds, final_shares)
        net = final_balance - initial_funds

        print()
        print("Total Period:", total_period)
        print("Initial Balance:", initial_funds)
        print("Remaining Funds:", final_funds)
        print("Final Balance:", final_balance)
        print("Net:", net)
        print("Final Shares:", final_shares)

        for s, _ in final_shares.items():
            print(s, ":", data["prices"][args["period"] - 1][s])


STAGE_A_NAME = "STAGEA"
STAGE_B_NAME = "STAGEB"
STAGE_C_NAME = "STAGEC"

RABBIT_MQ_SERVER_IP = "localhost"


class Stage(Process):

    def __init__(self, args):
        self.args = args
        self.queue_prefix = "<{user_id}><{strategy_id}><{backtest_id}>".format(user_id=self.args["user_id"], strategy_id=self.args["strategy_id"], backtest_id=self.args["backtest_id"])
        super().__init__(target=self.setup, args=())

    def setup(self):
        self.exec_stage()

    def exec_stage(self):
        pass


class StageA(Stage):

    def __init__(self, args):
        super().__init__(args=args)
        self.connection = None
        self.channel = None

    def setup(self):
        print("Starting Stage A")
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
            self.channel = self.connection.channel()

            # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
            self.channel.queue_declare(queue=self.queue_prefix + STAGE_A_NAME, durable=True)

            super().setup()
            self.connection.close()
        except KeyboardInterrupt:
            self.connection.close()
            quit(0)
        finally:
            print("Exiting Stage A")

    def exec_stage(self):
        # Create local stage a variables
        self.args["universe"] = list()

        period = self.args["period"]

        for p in range(period):
            data = dict()

            # Start Universe Calculation for Each Period
            calc_universe(self.args)

            # Publish Update to Subscribers
            data["universe"] = self.args["universe"][p]
            data["time"] = p
            self.channel.basic_publish(exchange="", routing_key=self.queue_prefix + STAGE_A_NAME, body=json.dumps(data), properties=pika.BasicProperties(delivery_mode=2,))

        print("Completed Stage A")


class StageB(Stage):

    def __init__(self, args):
        super().__init__(args=args)
        self.connection = None
        self.channel = None
        self.connectionA = None
        self.connectionC = None
        self.channelA = None
        self.channelC = None

    def setup(self):
        print("Starting Stage B")
        try:
            # Init Arguments Used
            self.args["universe"] = list()
            self.args["prices"] = list()
            self.args["volumes"] = list()

            self.args["funds"] = list()
            self.args["shares"] = list()

            self.args["buy_count"] = list()
            self.args["sell_count"] = list()

            self.args["stage_a_time"] = -1
            self.args["stage_c_time"] = -1

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
            self.channel = self.connection.channel()

            # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
            self.channel.queue_declare(queue=self.queue_prefix + STAGE_B_NAME, durable=True)

            stage_a_coms = Thread(target=self.handle_stage_a_data, daemon=True)
            stage_c_coms = Thread(target=self.handle_stage_c_data, daemon=True)

            stage_a_coms.start()
            stage_c_coms.start()

            super().setup()

            stage_a_coms.join()
            stage_c_coms.join()
        except KeyboardInterrupt:
            self.connection.close()
            quit(0)
        finally:
            print("Exiting Stage B")

    def handle_stage_a_data(self):
        self.connectionA = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
        self.channelA = self.connectionA.channel()

        # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
        self.channelA.queue_declare(queue=self.queue_prefix + STAGE_A_NAME, durable=True)

        # Handle A Messages
        self.channelA.basic_consume(self.proc_stage_a_data, queue=self.queue_prefix + STAGE_A_NAME)
        self.channelA.start_consuming()

        self.connectionA.close()

    def proc_stage_a_data(self, ch, method, properties, body):
        # Store data
        data = json.loads(body)

        self.args["universe"].append(data["universe"])

        # Acknowledge request once processing is complete to ensure remaining request, currently allocated to this worker, will be processed by another worker
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # End listening when all data is received
        if data["time"] == self.args["period"] - 1:
            ch.stop_consuming()

        self.args["stage_a_time"] = data["time"]

    def handle_stage_c_data(self):
        self.connectionC = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
        self.channelC = self.connectionC.channel()

        # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
        self.channelC.queue_declare(queue=self.queue_prefix + STAGE_C_NAME, durable=True)

        # Handle C Messages
        self.channelC.basic_consume(self.proc_stage_c_data, queue=self.queue_prefix + STAGE_C_NAME)
        self.channelC.start_consuming()

        self.connectionC.close()

    def proc_stage_c_data(self, ch, method, properties, body):
        # Store data

        data = json.loads(body)

        # Repack TradeSignals
        trade_signals = []

        for st, s, q in data["trade_signals"]:
            trade_signals.append(TradeSignal(signal_type=st, symbol=s, quantity=q))

        self.args["trade_signals"][data["time"]] = data["trade_signals"]

        self.args["prices"][data["time"]] = data["prices"]
        self.args["volumes"][data["time"]] = data["volumes"]
        self.args["shares"][data["time"]] = data["shares"]
        self.args["funds"][data["time"]] = data["funds"]
        self.args["buy_count"][data["time"]] = data["buy_count"]
        self.args["sell_count"][data["time"]] = data["sell_count"]

        # Acknowledge request once processing is complete to ensure remaining request, currently allocated to this worker, will be processed by another worker
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # End listening when all data is received
        if data["time"] == self.args["period"] - 1:
            ch.stop_consuming()

        self.args["stage_c_time"] = data["time"]

    def exec_stage(self):
        # Create local stage a variables
        self.args["trade_signals"] = list()

        period = self.args["period"]

        p = 0

        # Price and Volume Data "Gathering" is done by stage b to increase computation time of stage b and for no other reason
        # Price and Volume Data is randomly generated using perlin noise for semi-predictable random data
        # Price and Volume Data will be passed to stage c for synchronization purposes

        # Fetches and caches historical price and volume data
        historical_data = assure_init_data()
        # tm = Timer(mode=2)
        while p < period:
            # Check to see if there are available universes on which to execute the strategy
            # Check to see that previous stage c handling has been completed for the next period

            self.args["time"] = p

            new_prices = dict()
            new_volumes = dict()
            new_shares = dict()

            if self.args["stage_c_time"] == p - 1 and p <= self.args["stage_a_time"]:
                # print(self.args["stage_a_time"], p, self.args["stage_c_time"])
                # Copying previous period's Price and Volume Data

                if p % 100 == 0:
                    print(p)

                if p == 0:
                    for k, v in historical_data[0][p].items():
                        new_prices[k] = v

                    for k, v in historical_data[1][p].items():
                        new_volumes[k] = v

                    self.args["funds"].append(self.args["initial_funds"])
                else:
                    # for k, v in self.args["prices"][p - 1].items():
                    #     new_prices[k] = v
                    #
                    # for k, v in self.args["volumes"][p - 1].items():
                    #     new_volumes[k] = v
                    # for k, v in self.args["shares"][p - 1].items():
                    #     new_shares[k] = v

                    new_prices = self.args["prices"][p - 1].copy()
                    new_volumes = self.args["volumes"][p - 1].copy()
                    new_shares = self.args["shares"][p - 1].copy()

                    self.args["funds"].append(self.args["funds"][p - 1])

                self.args["prices"].append(new_prices)
                self.args["volumes"].append(new_volumes)
                self.args["shares"].append(new_shares)

                self.args["buy_count"].append(0)
                self.args["sell_count"].append(0)
                self.args["trade_signals"].append(list())

                data = dict()

                # print(self.args["shares"][p])

                # Start Execution of Strategy for Period
                exec_strategy(self.args)

                # Publish Update to Subscribers

                # Unpack Trade Signals
                trade_signals = []

                for ts in self.args["trade_signals"][p]:
                    trade_signals.append((ts.signal_type, ts.symbol, ts.quantity))
                data["trade_signals"] = trade_signals

                # Sending Price and Volume Data
                data["prices"] = self.args["prices"][p]
                data["volumes"] = self.args["volumes"][p]

                data["funds"] = self.args["funds"][p]
                data["shares"] = self.args["shares"][p]
                data["buy_count"] = self.args["buy_count"][p]
                data["sell_count"] = self.args["sell_count"][p]

                data["time"] = p

                # tm.start()
                self.channel.basic_publish(exchange="", routing_key=self.queue_prefix + STAGE_B_NAME, body=json.dumps(data), properties=pika.BasicProperties(delivery_mode=2, ))
                # print(tm.end())

                p += 1
        print("Completed Stage B")


class StageC(Stage):

    def __init__(self, args):
        super().__init__(args=args)
        self.connection = None
        self.channel = None

        self.connectionB = None
        self.channelB = None

    def setup(self):
        print("Starting Stage C")

        try:
            # Init Arguments Used
            self.args["prices"] = list()
            self.args["volumes"] = list()

            self.args["trade_signals"] = list()

            self.args["funds"] = list()
            self.args["shares"] = list()

            self.args["buy_count"] = list()
            self.args["sell_count"] = list()

            self.args["statistics"] = list()

            self.args["stage_b_time"] = -1

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
            self.channel = self.connection.channel()

            # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
            self.channel.queue_declare(queue=self.queue_prefix + STAGE_C_NAME, durable=True)

            stage_b_coms = Thread(target=self.handle_stage_b_data, daemon=True)

            stage_b_coms.start()

            super().setup()

            stage_b_coms.join()
        except KeyboardInterrupt:
            self.connection.close()
            quit(0)
        finally:
            print("Exiting Stage C")

    def handle_stage_b_data(self):
        self.connectionB = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_MQ_SERVER_IP))
        self.channelB = self.connectionB.channel()

        # Naming Queue and setting Durable tag to ensure currently queued messages are guaranteed to not be lost even in the case of a RabbitMQ error or shutdown
        self.channelB.queue_declare(queue=self.queue_prefix + STAGE_B_NAME, durable=True)

        # Handle A Messages
        self.channelB.basic_consume(self.proc_stage_b_data, queue=self.queue_prefix + STAGE_B_NAME)
        self.channelB.start_consuming()

        self.connectionB.close()

    def proc_stage_b_data(self, ch, method, properties, body):
        # Store data
        data = json.loads(body)

        # Repack TradeSignals
        trade_signals = []

        for st, s, q in data["trade_signals"]:
            trade_signals.append(TradeSignal(signal_type=st, symbol=s, quantity=q))

        self.args["trade_signals"].append(trade_signals)

        self.args["prices"].append(data["prices"])
        self.args["volumes"].append(data["volumes"])
        self.args["shares"].append(data["shares"])
        self.args["funds"].append(data["funds"])
        self.args["buy_count"].append(data["buy_count"])
        self.args["sell_count"].append(data["sell_count"])

        # Acknowledge request once processing is complete to ensure remaining request, currently allocated to this worker, will be processed by another worker
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # End listening when all data is received
        if data["time"] == self.args["period"] - 1:
            ch.stop_consuming()

        self.args["stage_b_time"] = data["time"]

    def exec_stage(self):
        period = self.args["period"]

        p = 0

        while p < period:
            self.args["time"] = p

            if self.args["stage_b_time"] == p:
                data = dict()

                # print(self.args["trade_signals"][p])

                # Start Portfolio Rebalancing
                rebal_portfolio(self.args)

                # Start Generating Orders
                gen_order(self.args)

                # Start Statistics Calculation
                calc_stats(self.args)

                # Start Pushing Data
                push_data(self.args)

                # Publish Update to Subscribers

                # Unpack Trade Signals
                trade_signals = []

                for ts in self.args["trade_signals"][p]:
                    trade_signals.append((ts.signal_type, ts.symbol, ts.quantity))
                data["trade_signals"] = trade_signals

                # Sending Price and Volume Data
                data["prices"] = self.args["prices"][p]
                data["volumes"] = self.args["volumes"][p]

                data["funds"] = self.args["funds"][p]
                data["shares"] = self.args["shares"][p]
                data["buy_count"] = self.args["buy_count"][p]
                data["sell_count"] = self.args["sell_count"][p]

                data["time"] = p

                self.channel.basic_publish(exchange="", routing_key=self.queue_prefix + STAGE_C_NAME, body=json.dumps(data), properties=pika.BasicProperties(delivery_mode=2, ))

                p += 1

        print("Completed Stage C")
