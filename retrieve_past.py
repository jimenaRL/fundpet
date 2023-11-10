import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


client_id = os.environ['client_id']
client_secret = os.environ['client_secret']

first_monday = datetime.strptime("2023-05-08", "%Y-%m-%d")

nb_weeks = 27

starts = [(first_monday + relativedelta(weekday=MO(i))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(1, nb_weeks+1)]
ends = [(first_monday + relativedelta(weekday=MO(i+1))).strftime("%Y-%m-%dT00:00:00-00:00") for i in range(1, nb_weeks+1)]

query = [
    'SoMe4DemItalian',
    'SoMe4DemSpanish',
    "SoMe4DemDutch",
    "SoMe4DemFrench",
    "SoMe4DemGerman",
    "SoMe4DemPolish",
    # "SoMe4DemRomanian",
]

for query in QUERIES:
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
