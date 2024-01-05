import os
from glob import glob
import pandas as pd
from sqlite import updateDumpStatsTable, showAggDumpStatsTable

LFFOLDER = os.environ['LFFOLDER']
dumpsWildCard = os.path.join(LFFOLDER, 'dumps', '*')
files = glob(dumpsWildCard)

df = pd.DataFrame(
    data=[[f, len(pd.read_csv(f))] for f in files],
    columns=['path', 'nb_entries'])


df = df.assign(
    file=df.path.apply(lambda s: os.path.split(s)[-1]))
df = df.assign(
    lang=df.file.apply(lambda s: s.split('SoMe4Dem')[1].split('_')[0]))
df = df.assign(
    platform=df.file.apply(lambda s: s[6:].split('_SoMe4Dem')[0]))
df = df.assign(
    start=df.file.apply(lambda s: s.split('start_')[1].split('_end')[0][:10]))
df = df.assign(
    end=df.file.apply(lambda s: s.split('end_')[1][:-4][:10]))

df.drop(columns=['path'], inplace=True)

updateDumpStatsTable(df)

print(showAggDumpStatsTable())
