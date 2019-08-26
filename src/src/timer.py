import time


class Timer:
    __slots__ = ['start_time', 'end_time', 'time_diff', 'mode']

    def __init__(self, mode=0):
        self.start_time = 0
        self.end_time = 0
        self.time_diff = 0
        self.mode = mode  # Use -1 for min time, 0 for last time, +1 for max time, +2 for total time

    def reset(self):
        self.start_time = 0
        self.end_time = 0
        self.time_diff = 0

    def start(self):
        self.start_time = time.time()

    def end(self):
        self.end_time = time.time()
        time_diff = self.end_time - self.start_time

        if self.mode == -1:
            self.time_diff = min(self.time_diff, time_diff)
        elif self.mode == 0:
            self.time_diff = time_diff
        elif self.mode == 1:
            self.time_diff = max(self.time_diff, time_diff)
        elif self.mode == 2:
            self.time_diff += time_diff
        else:
            self.time_diff = 0
            print("Invalid Timer Mode")
        return self.time_diff

    def get(self):
        return self.time_diff
