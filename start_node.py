from node import *

import cPickle as pickle
from optparse import OptionParser


parser = OptionParser()
parser.add_option('-f', '--file', dest='pickled_file')

(options, args) = parser.parse_args()
if len(args) != 1:
    parser.error('incorrect number of arguments')

node = pickle.load(open(args.file, 'rb'))
node.run()