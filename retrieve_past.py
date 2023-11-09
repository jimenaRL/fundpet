import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


first_monday = datetime.strptime("2023-05-01", "%Y-%m-%d")

mi = 0
mf = 26

starts = [(first_monday + relativedelta(weekday=MO(i))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(mi, mf)]
ends = [(first_monday + relativedelta(weekday=MO(i+1))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(mi, mf)]

client_id = os.environ['client_id']
client_secret = os.environ['client_secret']

for s, e in zip(starts, ends):
    os.system(f"./pipeline.sh  {client_id} {client_secret} {s} {e}")