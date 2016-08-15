import csv
import os
from datetime import datetime

data_store = {}  # {report_id: [Data objects]}
metadata = {}  # {report_id: {key: value}}


class Data:
    def __init__(self):
        pass


def retrieve_meta_data(path, keys=("POINT_1_STREET", "POINT_2_STREET"), key="REPORT_ID"):
    """ Retrieves the keys for each report ID mentioned in meta_data file(path)
        and populates metadata.
        :param key: Value to act as key in meta-data.
        :param keys: Keys to extract
        :param path: Path to meta-data csv file
    """
    with open(path, 'rb') as metadata_file:
        reader = csv.DictReader(metadata_file)
        for row in reader:
            metadata[row[key]] = {_key: row[_key] for _key in keys}


def retreiveData(path, keys=["TIMESTAMP", "vehicleCount"]):
    ''' Retrieves data and populates data_store'''

    files = os.listdir(path)
    for file in files:
        f = open(file)
        csv.DictReader(f)
        #       ....
        for row in f:
            d = Data()
            for key in keys:
                d.__dict__[key] = row[key]
    pass


def get_time(date_time):
    """
    :param date_time: example 2014-08-01T07:50:00
    :return: example [2014, 8, 1, 7, 50, 0]
    """
    date_time = date_time.split('T')
    return map(int, date_time[0].split('-') + date_time[1].split(':'))


max_report_id = None
max_count = None
start_time = get_time('2014-08-01T07:50:00')
stop_time = get_time('2014-09-30T23:55:00')

for report_id in data_store:
    p = filter(lambda x: start_time <= get_time(x.TIMESTAMP) <= stop_time, data_store[report_id])
    sum([i.vehileCount for i in p])
