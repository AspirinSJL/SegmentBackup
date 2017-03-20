from hdfs import Config, InsecureClient
import cPickle as pickle
from tuple import Tuple

client = Config().get_client('dev')
client.write('a/p', 'aaa', overwrite=True)
print client.status('a')