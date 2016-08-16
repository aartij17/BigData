import csv,os
from threading import Thread
data_store={}#{report_id: [Data objects]}
metadata={}#{report_id: {key: value}}

class Data:
    def __init__(self):
        pass
    def __str__(self):
        return ",".join([key+":"+self.__dict__[key] for key in self.__dict__])


def retreiveMetaData(path,keys=["POINT_1_STREET","POINT_2_STREET"],key="REPORT_ID"):
    ''' Retrieves the keys for each report ID mentioned in meta_data file(path)
        and populates metadata
    '''
    
    pass

def processFiles(path,_list, keys):
    for file in _list:
        f = open(os.path.join(path,file))
        f = csv.DictReader(f)
        _list = data_store[file.strip('trafficData').strip('.csv')]=[]
        for row in f:
            d=Data()
            for key in keys:
                d.__dict__[key]=row[key]
            _list.append(d)
    print "done"

def retreiveData(path,keys=["TIMESTAMP","vehicleCount"],chunks=8,test=False):
    ''' Retrieves data and populates data_store'''
    files = os.listdir(path)
    chunkSize = len(files)/chunks
    chunks = [files[i:i+chunkSize] for i in range(0,len(files),chunkSize)]
    threads = []
    for l in chunks:
        t = Thread(target = processFiles, args = (path,l,keys))
        threads.append(t)
        t.start()
    print len(threads)
    for t in threads:
        t.join()
        
    if test:
        print str(data_store)[:100]
    pass
retreiveData("./citypulse_traffic_raw_data_surrey_june_sep_2014/traffic_june_sep",test=True)
'''
max_report_id=None
max_count = None
for report_id in data_store:
    p=filter(data_store[report_id],lambda x:<=Date(x.TIMESTAMP)<=)
    sum([i.vehileCount for i in p])

'''
