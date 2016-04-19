import sys
import time
from datetime import datetime, timedelta
import math
from log.logger import setup_logging
import importlib
import os
from application.routes import migrate
from multiprocessing import Process

exit(1)

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
    
s = sys.argv[1]
e = sys.argv[2]


sd = datetime.fromtimestamp(time.mktime(time.strptime(s, '%Y-%m-%d')))
ed = datetime.fromtimestamp(time.mktime(time.strptime(e, '%Y-%m-%d')))
days = (ed - sd).days

# Slightly buggy: this will slice to a minimum of 1 day, so if there are too many slices it'll increase the width
# of the range.
slices = int(os.getenv("MIGRATOR_WORKERS", '8'))
day_slice = math.floor(days / slices)

ranges = []  # {start: x, end: y}
for x in range(0, slices):
    if x == 0:
        start = sd
    else:
        start = ranges[x - 1]['end'] + timedelta(days=1)

    if x == slices - 1:
        end = ed
    else:
        end = start + timedelta(days=day_slice)

    ranges.append({
        'start': start,
        'end': end
    })

for r in ranges:
    name = "Migrate {} -> {}".format(r['start'].strftime('%Y-%m-%d'), r['end'].strftime('%Y-%m-%d'))
    print(name)
    p = Process(target=migrate,
                args=(config, r['start'].strftime('%Y-%m-%d'), r['end'].strftime('%Y-%m-%d')),
                name=name)
    p.start()
