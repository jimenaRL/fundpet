import os
from argparse import ArgumentParser
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


# parse arguments
ap = ArgumentParser()
ap.add_argument('--config', type=str, default="config.yaml")
ap.add_argument('--query', type=str)
ap.add_argument('--client_id', type=str)
ap.add_argument('--client_secret', type=str)
args = ap.parse_args()
config = args.config
query = args.query
client_id = args.client_id
client_secret = args.client_secret


# first_monday
m1 = datetime.strptime("2023-05-15", "%Y-%m-%d")

# nb_weeks = 59

nb_weeks = 2

form = "%Y-%m-%d"
rng = range(1, nb_weeks+1)
starts = [(m1 + relativedelta(weekday=MO(i))).strftime(form) for i in rng]
ends = [(m1 + relativedelta(weekday=MO(i+1))).strftime(form) for i in rng]


for start, end in zip(starts, ends):

    command_pipe = [
        "python",
        "pipeline.py",
        f"--client_id={client_id}",
        f"--client_secret={client_secret}",
        f"--start={start}",
        f"--end={end}",
        f"--query={query}"
    ]
    command = ' '.join(command_pipe)
    print(command)
    os.system(command)
