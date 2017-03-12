#!/usr/bin/env python

import CONSTANTS
from node import *

import cPickle as pickle
from optparse import OptionParser


parser = OptionParser()
parser.add_option('-f', '--file', dest='pickled_file')
parser.add_option('-r', '--replay', dest='replay', action='store_true', default=False)

(options, args) = parser.parse_args()
# if len(args) != 1:
#     parser.error('incorrect number of arguments')

node = pickle.load(open(options.pickled_file, 'rb'))
if options.replay:
    node.run(replay=True)
else:
    node.run()