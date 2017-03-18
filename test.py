from hdfs import Config, InsecureClient
import cPickle as pickle
from tuple import Tuple

client = InsecureClient('http://localhost:50070', user='juanli')
# client.delete('p')
# client.write('p', '')
#
# with client.write('p', append=True) as f:
#     i = 1
#     while True:
#         pickle.dump(Tuple(i, i), f, protocol=-1)
#         i += 1


with client.read('p') as f:
    while True:
        try:
            t = pickle.load(f)
            print t.tuple_id
        except Exception, e:
            print e
            break