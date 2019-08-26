from src.backtest_synchronous import BackTest as BTS
from src.backtest_multiprocessing import BackTest as BTM
from src.backtest_rabbitmq import BackTest as BTR

scheduled_backtests = dict()
EM_SYNCHRONOUS = 0
EM_MULTIPROCESSING = 1
EM_RABBITMQ = 2


def signal_backtest(user_id, strategy_id, execution_mode=0, kwargs={"period": 30}):
    print("Scheduling Backtest")

    global scheduled_backtests
    backtest_id = len(scheduled_backtests.items())
    period = kwargs["period"]
    backtest = BTR(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id, period=period) if execution_mode == EM_RABBITMQ else (BTM(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id, period=period) if execution_mode == EM_MULTIPROCESSING else (BTS(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id, period=period) if execution_mode == EM_SYNCHRONOUS else None))

    if not backtest:
        print("Invalid Execution Mode Specified:", execution_mode)
        quit(0)

    scheduled_backtests[user_id, strategy_id, backtest_id] = backtest
    backtest.start()

    print("Finished Scheduling Backtest")


def terminate():
    global scheduled_backtests

    print("Cleaning Up")

    for _, value in scheduled_backtests.items():
        value.join()
