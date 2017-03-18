from hdfs import Config, InsecureClient
import cPickle as pickle
from tuple import Tuple

client = InsecureClient('http://127.0.0.1:50070', user='juanli')
# client = Config(
# ).get_client('dev')
client.makedirs('/user/juanli/')
# print client.list('')

with client.write('s') as writer:
    writer.write('abs')

# with open('node.py') as reader, client.write('n') as writer:
#     for l in reader:
#         writer.write(l)
    # dump([1, 2, 3], writer)

# with c.write('ba/p') as f:
#     # while True:
#     i = 1
#     pickle.dump(Tuple(i), f, protocol=-1)

