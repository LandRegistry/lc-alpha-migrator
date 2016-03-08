import sys
import time
from datetime import datetime, timedelta
import math
from log.logger import setup_logging
import importlib
from application.routes import migrate
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
    
s = sys.argv[1]
e = sys.argv[2]


sd = datetime.fromtimestamp(time.mktime(time.strptime(s, '%Y-%m-%d')))
ed = datetime.fromtimestamp(time.mktime(time.strptime(e, '%Y-%m-%d')))
days = (ed - sd).days

day_slice = math.floor(days / 4)

r1s = sd
r1e = (sd + timedelta(days=day_slice))

r2s = r1e + timedelta(days=1)
r2e = r2s + timedelta(days=day_slice)

r3s = r2e + timedelta(days=1)
r3e = r3s + timedelta(days=day_slice)

r4s = r3e + timedelta(days=1)
r4e = ed

print("{} -> {}".format(r1s.strftime('%Y-%m-%d'), r1e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r2s.strftime('%Y-%m-%d'), r2e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r3s.strftime('%Y-%m-%d'), r3e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r4s.strftime('%Y-%m-%d'), r4e.strftime('%Y-%m-%d')))

p1 = Process(target=migrate, args=(config, r1s.strftime('%Y-%m-%d'), r1e.strftime('%Y-%m-%d')))
p2 = Process(target=migrate, args=(config, r2s.strftime('%Y-%m-%d'), r2e.strftime('%Y-%m-%d')))
p3 = Process(target=migrate, args=(config, r3s.strftime('%Y-%m-%d'), r3e.strftime('%Y-%m-%d')))
p4 = Process(target=migrate, args=(config, r4s.strftime('%Y-%m-%d'), r4e.strftime('%Y-%m-%d')))
p1.start()
p2.start()
p3.start()
p4.start()