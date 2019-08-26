from src.backtest_runner import *
from time import sleep


def main():
    print("-----START-----")
    user_id = 0
    strategy_id = 0
    backtest_count = 1

    is_finished = False

    # TODO: Sync run for 5000 takes 500 seconds and RabbitMQ run for 5000 takes 700.
    # TODO: Locate time discrepancy and determine cause for non linear time consumption for linearly scaled loads

    for i in range(backtest_count):
        signal_backtest(user_id=user_id, strategy_id=strategy_id, execution_mode=EM_SYNCHRONOUS, kwargs={"period": 30})

    try:
        while not is_finished:
            # print("Waiting for additional Backtests")
            sleep(3)
    except KeyboardInterrupt as e:
        terminate()

    print("-----END-----")


if __name__ == "__main__":
    main()
