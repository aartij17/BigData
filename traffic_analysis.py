import csv,os

data_store={}#{report_id: [Data objects]}
metadata={}#{report_id: {key: value}}

class Data(Object):
    def __init__(self):
        pass


def retreiveMetaData(path,keys=["POINT_1_STREET","POINT_2_STREET"],key="REPORT_ID"):
    ''' Retrieves the keys for each report ID mentioned in meta_data file(path)
        and populates metadata
    '''
    
    pass


def retreiveData(path,keys=["TIMESTAMP","vehicleCount"]):
    ''' Retrieves data and populates data_store'''

    files = os.listdir(path)
    for file in files:
        f=open(file)
        csv.DictReader(f)
 #       ....
        for row in f:
            d=Data()
            for key in keys:
                d.__dict__[key]=row[key]
    pass
max_report_id=None
max_count = None
for report_id in data_store:
    p=filter(data_store[report_id],lambda x:<=Date(x.TIMESTAMP)<=)
    sum([i.vehileCount for i in p])

