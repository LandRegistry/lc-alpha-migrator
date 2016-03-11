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

day_slice = math.floor(days / 8)

r1s = sd
r1e = (sd + timedelta(days=day_slice))

r2s = r1e + timedelta(days=1)
r2e = r2s + timedelta(days=day_slice)

r3s = r2e + timedelta(days=1)
r3e = r3s + timedelta(days=day_slice)

r4s = r3e + timedelta(days=1)
r4e = r4s + timedelta(days=day_slice)

r5s = r4e + timedelta(days=1)
r5e = r5s + timedelta(days=day_slice)

r6s = r5e + timedelta(days=1)
r6e = r6s + timedelta(days=day_slice)

r7s = r6e + timedelta(days=1)
r7e = r7s + timedelta(days=day_slice)

r8s = r7e + timedelta(days=1)
r8e = ed

print("{} -> {}".format(r1s.strftime('%Y-%m-%d'), r1e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r2s.strftime('%Y-%m-%d'), r2e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r3s.strftime('%Y-%m-%d'), r3e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r4s.strftime('%Y-%m-%d'), r4e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r5s.strftime('%Y-%m-%d'), r5e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r6s.strftime('%Y-%m-%d'), r6e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r7s.strftime('%Y-%m-%d'), r7e.strftime('%Y-%m-%d')))
print("{} -> {}".format(r8s.strftime('%Y-%m-%d'), r8e.strftime('%Y-%m-%d')))


p1 = Process(target=migrate, args=(config, r1s.strftime('%Y-%m-%d'), r1e.strftime('%Y-%m-%d')))
p2 = Process(target=migrate, args=(config, r2s.strftime('%Y-%m-%d'), r2e.strftime('%Y-%m-%d')))
p3 = Process(target=migrate, args=(config, r3s.strftime('%Y-%m-%d'), r3e.strftime('%Y-%m-%d')))
p4 = Process(target=migrate, args=(config, r4s.strftime('%Y-%m-%d'), r4e.strftime('%Y-%m-%d')))
p5 = Process(target=migrate, args=(config, r5s.strftime('%Y-%m-%d'), r5e.strftime('%Y-%m-%d')))
p6 = Process(target=migrate, args=(config, r6s.strftime('%Y-%m-%d'), r6e.strftime('%Y-%m-%d')))
p7 = Process(target=migrate, args=(config, r7s.strftime('%Y-%m-%d'), r7e.strftime('%Y-%m-%d')))
p8 = Process(target=migrate, args=(config, r8s.strftime('%Y-%m-%d'), r8e.strftime('%Y-%m-%d')))
p1.start()
p2.start()
p3.start()
p4.start()
p5.start()
p6.start()
p7.start()
p8.start()