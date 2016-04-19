import sys
import time
from datetime import datetime, timedelta
import math
from log.logger import setup_logging
import importlib
import os
from application.routes import check
from multiprocessing import Process

cfg = 'Config'
c = getattr(importlib.import_module('config'), cfg)
config = {}

for key in dir(c):
    if key.isupper():
        config[key] = getattr(c, key)

setup_logging(config)


if len(sys.argv) < 3:
    print('Insuffcient parameters specified')
    exit()

s_year = int(sys.argv[1])
e_year = int(sys.argv[2])


# sd = datetime.fromtimestamp(time.mktime(time.strptime(s, '%Y-%m-%d')))
# ed = datetime.fromtimestamp(time.mktime(time.strptime(e, '%Y-%m-%d')))
# days = (ed - sd).days

# Slightly buggy: this will slice to a minimum of 1 day, so if there are too many slices it'll increase the width
# of the range.
slices = int(os.getenv("MIGRATOR_WORKERS", '16'))
years_at_a_go = int(os.getenv("YEAR_CHUNKS", '20'))

c_year = s_year
while c_year <= e_year:
    range_end = c_year + (years_at_a_go - 1)
    if range_end > e_year:
        range_end = e_year
    range_start = c_year

    print("{} --> {}".format(c_year, range_end))
    c_year += years_at_a_go

    start_date = "{}-01-01".format(range_start)
    end_date = "{}-12-31".format(range_end)
    sd = datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d')))
    ed = datetime.fromtimestamp(time.mktime(time.strptime(end_date, '%Y-%m-%d')))
    days = (ed - sd).days
    day_slice = math.floor(days / slices)
    ranges = []  # {start: x, end: y}
    for x in range(0, slices):
        if x == 0:
            start = sd
        else:
            start = ranges[x - 1]['end'] + timedelta(days=1)

        if start > ed:
            break

        if x == slices - 1:
            end = ed
        else:
            end = start + timedelta(days=day_slice)

        if end > ed:
            end = ed

        ranges.append({
            'start': start,
            'end': end
        })

    print("{} ranges".format(len(ranges)))
    for r in ranges:
        name = "Check {} -> {}".format(r['start'].strftime('%Y-%m-%d'), r['end'].strftime('%Y-%m-%d'))
        print("  " + name)
        p = Process(target=check,
                    args=(config, r['start'].strftime('%Y-%m-%d'), r['end'].strftime('%Y-%m-%d')),
                    name=name)
        p.start()



#
#
# day_slice = math.floor(days / slices)
# #print(days / slices)
# #print(day_slice)
#



