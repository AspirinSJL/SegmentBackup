#!/usr/bin/env python

from node import *

import cPickle as pickle
from optparse import OptionParser


parser = OptionParser()
parser.add_option('-f', '--file', dest='pickled_file')
parser.add_option('-r', '--replay', dest='replay', action='store_true', default=False)

(options, args) = parser.parse_args()
if len(args) != 1:
    parser.error('incorrect number of arguments')

node = pickle.load(open(args.pickled_file, 'rb'))
if args.replay:
    node.pending_window.replay()
node.run()