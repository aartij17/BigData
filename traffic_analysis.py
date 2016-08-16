<<<<<<< HEAD
import csv,os
import time,pickle
data_store={}#{report_id: [Data objects]}
metadata={}#{report_id: {key: value}}

class Data:
    __slots__=['status','avgMeasuredTime','avgSpeed','extID','medianMeasuredTime','TIMESTAMP','vehicleCount','_id','REPORT_ID']
    pass


def retreiveMetaData(path,keys=["POINT_1_STREET","POINT_2_STREET"],key="REPORT_ID"):

    """ Retrieves the keys for each report ID mentioned in meta_data file(path)
    and populates metadata.
    :param key: Value to act as key in meta-data.
    :param keys: Keys to extract
    :param path: Path to meta-data csv file
=======
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
>>>>>>> bb99c169b3ee12744011e10ad648747435758a39
    """
    with open(path, 'rb') as metadata_file:
        reader = csv.DictReader(metadata_file)
        for row in reader:
            metadata[row[key]] = {_key: row[_key] for _key in keys}
<<<<<<< HEAD
    pass


def retreiveData(path=None,searchCondition=None,stopCondition=lambda x:False,keys=["TIMESTAMP", "vehicleCount"],test=False,op="datastore.pkl",load=True):
=======


def retreiveData(path, keys=["TIMESTAMP", "vehicleCount"]):
>>>>>>> bb99c169b3ee12744011e10ad648747435758a39
    ''' Retrieves data and populates data_store'''
    global data_store
    if load:
        f = open(op,"rb")
        data_store = pickle.load(f)
        return
    files = os.listdir(path)
    _t = time.clock()
    count = 0
    data_store={}
    for file in files:
<<<<<<< HEAD
        f = open(os.path.join(path,file))
        f = csv.DictReader(f)
        _list = data_store[file.strip('trafficData').strip('.csv')]=[]
=======
        f = open(file)
        csv.DictReader(f)
        #       ....
>>>>>>> bb99c169b3ee12744011e10ad648747435758a39
        for row in f:
            d = Data()
            for key in keys:
<<<<<<< HEAD
                d.__dict__[key]=row[key]
            
            if stopCondition(d):
                break
            
            if searchCondition(d):
                _list.append(d)
        count+=1
        if count%50==0:
            print count
    with open(op,"wb") as f:
        pickle.dump(data_store,f)
    if test:
        print time.clock()-_t
        print str(data_store)[:100]
    
=======
                d.__dict__[key] = row[key]
    pass

>>>>>>> bb99c169b3ee12744011e10ad648747435758a39

def get_time(date_time):
    """
    :param date_time: example 2014-08-01T07:50:00
    :return: example [2014, 8, 1, 7, 50, 0]
    """
    date_time = date_time.split('T')
    return map(int, date_time[0].split('-') + date_time[1].split(':'))


<<<<<<< HEAD
start_time = get_time('2014-08-01T07:50:00')
stop_time = get_time('2014-08-31T23:55:00')
generate = False

# retreive meta data
retreiveMetaData(path='./trafficMetaData.csv')
print "Meta Data retreived"
# retreive data store
if generate:
    retreiveData(path = "./citypulse_traffic_raw_data_surrey_june_sep_2014/traffic_june_sep",
             searchCondition = lambda x: start_time <= get_time(x.TIMESTAMP) <= stop_time,
             stopCondition = lambda x: get_time(x.TIMESTAMP) > stop_time ,
             test=True,
             load=False)
else:
    retreiveData(load=True)



max_count = -1
max_report_id = None

for report_id in data_store:
    p = data_store[report_id]
    count=sum([int(i.vehicleCount) for i in p])
    if count>max_count:
        max_count=count
        max_report_id=report_id

print "Report ID",max_report_id
print "Start and End point",metadata[max_report_id]['POINT_1_STREET'],metadata[max_report_id]['POINT_2_STREET']
print max_count
print "Pollution levels", 4620119
=======
max_report_id = None
max_count = None
start_time = get_time('2014-08-01T07:50:00')
stop_time = get_time('2014-09-30T23:55:00')
>>>>>>> bb99c169b3ee12744011e10ad648747435758a39

for report_id in data_store:
    p = filter(lambda x: start_time <= get_time(x.TIMESTAMP) <= stop_time, data_store[report_id])
    count=sum([i.vehileCount for i in p])
    if count>max_count:
        max_count=count
        max_report_id=report_id
print max_report_id, max_count
