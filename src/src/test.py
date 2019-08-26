import backtest_utils


def main():
    orig = 0.01
    flag = True
    for i in range(10**5):
        p = backtest_utils.gen_price(i, 0, orig)

        if p <= 0:
            print(p)
            flag = False

    if flag:
        print("No Negatives Found")
    else:
        print("Negatives Found")


if __name__ == "__main__":
    main()
