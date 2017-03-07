import CONSTANTS
from node import *
from tuple import *

import os
import shutil
import time
import yaml
import cPickle as pickle
from subprocess import Popen
from optparse import OptionParser


class AppStarter(object):
    """Instantiated separately for start and restart
    """
    def __init__(self, conf_file, start_mode):
        with open(conf_file) as f:
            self.conf = yaml.load(f)

        self.pickle_dir = os.path.join(CONSTANTS.ROOT_DIR, 'pickled_nodes')
        self.backup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'backup_dir')

        if start_mode == 'new':
            # create/overwrite these directories
            for d in (self.pickle_dir, self.backup_dir):
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.makedirs(d)

        # self.nodes = {}

    def init_nodes(self):
        """Turn the conf_file into node instances, and pickle them for reuse
        """

        for n_id, n_info in self.conf['nodes'].iteritems():
            if not n_info['is_connecting']:
                node = Node(n_id, n_info['type'], n_info['rule'], n_info['from'], n_info['to'])
            else:
                node = ConnectingNode(n_id, n_info['type'], n_info.get('rule', None), n_info['from'], n_info['to'],
                            self.conf['cuts'][n_info['from_cut_no']]['cnodes'] if 'from_cut_no' in n_info else None,
                            self.conf['cuts'][n_info['to_cut_no']]['cnodes'] if 'to_cut_no' in n_info else None)

                # special cases for source and sink nodes
                if n_info['type'] == 'source':

                    node.operator = gen_tuple
                elif n['type'] == 'sink':
                    def collect(tuple_):
                        if tuple_.tuple_id % 80:
                            print tuple_.tuple_id

                    node.operator = collect

            # self.nodes[n] = node
            pickle.dump(node, open(os.path.join(self.pickle_dir, '%d.pkl' % n), 'wb'))

    def recover_nodes(self, back):
        # adjust state (should be BFS or DFS)
        for ck, cv in self.conf['cuts'].iteritems():
            # because a cut may have multiple downstream cuts,
            # so we make every cut responsible for its upstream coverage
            if ck == 0:
                continue

            # the segment-ending nodes
            connecting_nodes = cv['cnodes']
            # the interior nodes
            segment_nodes = cv['snodes']

            # the version that cut truncated to = min(node versions of the seg-end cut)
            # TODO: no valid backup version
            safe_version = min(
                max(filter(lambda i: i.isdigits(), os.listdir(os.path.join(self.backup_dir, n, 'node'))))
                for n in connecting_nodes)
            for n in segment_nodes:
                self.nodes[n].latest_checked_version = safe_version

            for n in connecting_nodes:
                n.pending_window.rewind(safe_version)

    def start_nodes(self):
        for n in reversed(self.conf):
            Popen([os.path.join(CONSTANTS.ROOT_DIR, 'strat_node.py'),
                   '-f', os.path.join(CONSTANTS.ROOT_DIR, '%d.pkl' % n)])

    def start_app(self):
        self.init_nodes()
        self.start_nodes()

    def restart_app(self)
        self.recover_nodes()
        self.start_nodes()

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-m', '--mode', dest='start_mode', help='new or restart')
    parser.add_option('-f', '--file', dest='conf_file', default=os.path.join(CONSTANTS.ROOT_DIR, 'conf.yaml'))

    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.error('incorrect number of arguments')

    starter = AppStarter(args.conf_file)