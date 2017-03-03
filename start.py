import CONSTANTS
from node import *
from tuple import *

import os
import time
import yaml
import cPickle as pickle
from subprocess import Popen
from optparse import OptionParser

class AppStarter(object):
    def __init__(self, conf_file):
        with open(conf_file) as f:
            self.conf = yaml.load(f)

        self.pickle_dir = os.path.join(CONSTANTS.ROOT_DIR, 'pickled_nodes')

        self.nodes = {}

    def init_nodes(self):
        # turn the conf_file into node instances

        for n in self.conf['nodes']:
            if n['type'] == 'normal':
                node = Node(n, n['operator'], n['from'], n['to'])
            else:
                node = ConnectingNode(n, n['operator'], n['from'], n['to'],
                                      self.conf['cuts'][n['from_cut_no']] if 'from_cut_no' in n else None,
                                      self.conf['cuts'][n['to_cut_no']] if 'to_cut_no' in n else None)

                if n['type'] == 'source':
                    def gen_tuple(delay=3, barrier_interval=50):
                        i = 1
                        while True:
                            if i % barrier_interval:
                                yield BarrierTuple(i)
                            else:
                                yield Tuple(i)
                            time.sleep(delay)
                            i += 1

                    node.operator = gen_tuple
                elif n['type'] == 'sink':
                    def collect(tuple_):
                        if tuple_.tuple_id % 80:
                            print tuple_.tuple_id

                    node.operator = collect

            self.nodes[n] = node
            pickle.dump(node, open(os.path.join(self.pickle_dir, '%d.pkl' % n), 'wb'))

    def recover_nodes(self, back):
        # adjust state (should be BFS or DFS)
        for ck, cv in self.conf['cuts'].iteritems():
            # because a cut may have multiple downstream cuts,
            # so we make every cut responsible for its upstream coverage
            if ck == 0:
                continue

            connecting_nodes = cv['cnodes']
            segment_nodes = cv['snodes']

            # the version that cut truncated to
            downstream_cut_versions =
            truncate_version_to = min()

    def start_nodes(self):
        for n in reversed(self.conf):
            Popen([os.path.join(CONSTANTS.ROOT_DIR, 'strat_node.py'),
                   '-f', os.path.join(CONSTANTS.ROOT_DIR, '%d.pkl' % n)])

    def start_app(self):
        self.init_nodes()
        self.start_nodes()

    def restart_app(self):
        self.recover_nodes()
        self.start_nodes()



if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-m', '--mode', dest='start_mode')
    parser.add_option('-f', '--file', dest='conf_file', default=os.path.join(CONSTANTS.ROOT_DIR, 'conf.yaml'))

    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.error('incorrect number of arguments')

    starter = AppStarter(args.conf_file)