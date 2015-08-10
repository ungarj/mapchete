from multiprocessing import Pool
import time
import random


def func(i):
    t = random.random() * 3
    time.sleep(t)
    return i, t

pool = Pool(3)

for i, t in pool.imap_unordered(func, range(10)):
    print i, t
