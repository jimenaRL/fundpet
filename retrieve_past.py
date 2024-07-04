import os
from argparse import ArgumentParser
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


# parse arguments
ap = ArgumentParser()
ap.add_argument('--config', type=str, default="config.yaml")
ap.add_argument('--query', type=str)
ap.add_argument('--start', type=str)
ap.add_argument('--nb_weeks', type=int)
args = ap.parse_args()
config = args.config
query = args.query
start = args.start
nb_weeks = args.nb_weeks

# start = "2023-05-15"
# nb_weeks = 59

# first_monday
m1 = datetime.strptime(start, "%Y-%m-%d")

form = "%Y-%m-%d"
rng = range(1, nb_weeks+1)
starts = [(m1 + relativedelta(weekday=MO(i))).strftime(form) for i in rng]
ends = [(m1 + relativedelta(weekday=MO(i+1))).strftime(form) for i in rng]


for start, end in zip(starts, ends):

    command_pipe = [
        "python",
        "pipeline.py",
        f"--start={start}",
        f"--end={end}",
        f"--query={query}"
    ]
    command = ' '.join(command_pipe)
    print(command)
    os.system(command)
