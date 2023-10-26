import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


VALIDFOLDERS = [
    'dumps',
    'fetched',
    'preprocessed',
    'scrapped'
]

if 'LFFOLDER' not in os.environ:
    raise ValueError('`LFFOLDER` environment variable must be set.')


def get_folder(folder):
    if folder not in VALIDFOLDERS:
        raise ValueError(
            f'Wrong folder kind `{kind}` must be one of {VALIDFOLDERS}.')

    return os.path.join(os.environ['LFFOLDER'], folder)


def get_path(start, end, folder):
    filename = '%s__start_%s_end_%s.csv' % (folder, start, end)
    outpath = os.path.join(get_folder(folder), filename)
    return outpath


def get_date(start, end):
    """
    If no start or end date, download last week from last monday
    """
    if not (start and end):
        today = date.today()
        last_monday = today + relativedelta(weekday=MO(-1))
        last_last_monday = today + relativedelta(weekday=MO(-2))
        end_timestamp = last_monday.strftime("%Y-%m-%dT00:00:00-00:00")
        start_timestamp = last_last_monday.strftime("%Y-%m-%dT00:00:00-00:00")
    else:
        end_timestamp = end
        start_timestamp = start

    return start_timestamp, end_timestamp
