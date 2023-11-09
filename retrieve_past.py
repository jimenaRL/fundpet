import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


client_id = os.environ['client_id']
client_secret = os.environ['client_secret']

first_monday = datetime.strptime("2023-7-17", "%Y-%m-%d")

nb_weeks = 2

starts = [(first_monday + relativedelta(weekday=MO(i))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(1, nb_weeks+1)]
ends = [(first_monday + relativedelta(weekday=MO(i+1))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(1, nb_weeks+1)]

query = 'SoMe4DemItalian'

for start, end in zip(starts, ends):
        command_pipe = [
            "./pipeline.sh",
            client_id,
            client_secret,
            start,
            end,
            query
        ]
        # subprocess.run(command_pipe, shell=True)
        os.system(' '.join(command_pipe))
        # print(' '.join(command_pipe))

