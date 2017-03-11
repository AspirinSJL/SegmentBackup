#!/usr/bin/env python

import CONSTANTS
from node import *
from tuple import *

import os
import shutil
import time
import yaml
import pickle
from subprocess import Popen
from optparse import OptionParser


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Separate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)

LOGGER = logging.getLogger('Starter')


class AppStarter(object):
    """Instantiated separately for start and restart
    """
    def __init__(self, conf_file, start_mode):
        with open(conf_file) as f:
            self.conf = yaml.load(f)
        self.start_mode = start_mode

        # Topo information
        # In production, it should be ready in any standby data-center for quick handing over
        self.pickle_dir = os.path.join(CONSTANTS.ROOT_DIR, 'pickled_nodes')

        self.backup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'backup')

        if start_mode == 'new':
            # create/overwrite these directories
            for d in (self.pickle_dir, self.backup_dir):
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.makedirs(d)

    def configure_nodes(self):
        """Turn the config into node instances, and pickle them for reuse
        """

        for n_id, n_info in self.conf.iteritems():
            if n_info['type'] == 'spout':
                node = Spout(n_id, n_info['type'], n_info['downstream_nodes'], n_info['downstream_connectors'],
                             n_info['delay'], n_info['barrier_interval'])
            else:
                if n_info['type'] == 'sink' or n_info['is_connecting']:
                    node = Connector(n_id, n_info['type'], n_info['rule'],
                                     n_info['upstream_nodes'], n_info['upstream_connectors'],
                                     n_info.get('downstream_nodes', None), n_info.get('downstream_connectors', None))
                else:
                    node = Bolt(n_id, n_info['type'], n_info['rule'],
                                n_info['upstream_nodes'], n_info['downstream_nodes'])

            with open(os.path.join(self.pickle_dir, '%d.pkl' % n_id), 'wb') as f:
                pickle.dump(node, f, protocol=-1)
                LOGGER.info('node %d pickled' % n_id)

    def recover_nodes(self):
        """Adjust the backup data (both pending window and node state) to the latest consistent state,
            and update the pickled nodes to that state
        """
        nodes = {}
        for n in self.conf:
            with open(os.path.join(self.pickle_dir, '%d.pkl' % n), 'rb') as f:
                nodes[n] = pickle.load(f)
        print nodes
        # adjust state (should be BFS or DFS?)
        for c_id, c_info in self.conf.iteritems():
            # each connector should be responsible for make its downstream segment consistent
            # if multiple connectors converge to a single downstream connector, they should have agreement naturally
            # agreement should be achieved in lower level by some classical distributed algorithm
            if c_info['is_connecting'] and c_info['type'] != 'sink':
                # TODO: no valid backup version
                with open(os.path.join(self.backup_dir, str(c_id), 'pending_window', 'safe_version')) as f:
                    # the tuples before (inclusively) safe version have been handled by downstream connectors
                    safe_version = f.read()

                # 1. adjust node state
                for n in c_info['cover']:
                    nodes[n].computing_state = safe_version

                # 2. adjust pending window state
                for n in c_info['downstream_connectors']:
                    nodes[n].pending_window.rewind(safe_version)

        for n in nodes:
            with open(os.path.join(self.pickle_dir, '%d.pkl' % n), 'wb') as f:
                pickle.dump(nodes[n], f, protocol=-1)

    def start_nodes(self):
        """Load each pic_idled node and run it in a new process
        """

        for n in reversed(self.conf.keys()):
            LOGGER.info('starting node %d' % n)

            Popen(['python', os.path.join(CONSTANTS.ROOT_DIR, 'start_node.py'),
                   '-f', os.path.join(CONSTANTS.ROOT_DIR, 'pickled_nodes', '%d.pkl' % n),
                   '-r' if self.start_mode == 'restart' and self.conf[n]['is_connecting'] and self.conf[n]['type'] != 'sink' else ''])

    def start_app(self):
        self.configure_nodes()
        self.start_nodes()

    def restart_app(self):
        self.recover_nodes()
        self.start_nodes()

    def run(self):
        if self.start_mode == 'new':
            self.start_app()
        elif self.start_mode == 'restart':
            self.restart_app()
        else:
            LOGGER.error('unknown start mode %s' % self.start_mode)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-m', '--mode', action='store', dest='start_mode', default='new',
                      help='new or restart')
    parser.add_option('-f', '--file', action='store', dest='conf_file',
                      default=os.path.join(CONSTANTS.ROOT_DIR, 'conf.yaml'))

    (options, args) = parser.parse_args()
    # if len(options) < 1:
    #     parser.error('%d args is too few' % len(options))

    starter = AppStarter(options.conf_file, options.start_mode)
    starter.run()