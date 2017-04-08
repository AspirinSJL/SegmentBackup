#!/usr/bin/env python

import CONSTANTS
from node import *
from tuple import *

import os
import shutil
import time
import yaml
import pickle
import hdfs
from subprocess import Popen
from optparse import OptionParser


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Separate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)

LOGGER = logging.getLogger('Starter')
logging.getLogger('hdfs.client').setLevel(logging.WARNING)

class AppStarter(object):
    """Instantiated separately for start and restart
    """
    def __init__(self, conf_file, start_mode):
        with open(conf_file) as f:
            self.conf = yaml.load(f)
        self.start_mode = start_mode

        self.hdfs_client = hdfs.Config().get_client('dev')

        # Topo information
        # In production, it should be ready in any standby data-center for quick handing over
        self.pickle_dir = 'pickled_nodes'
        self.pickle_dir_local = os.path.join(CONSTANTS.ROOT_DIR, 'pickled_nodes')

        # used for recovery
        self.backup_dir = 'backup'

        # used for testing
        self.computing_state_dir = 'computing_state'

        if start_mode == 'new':
            # create/overwrite these directories
            for d in (self.pickle_dir, self.backup_dir, self.computing_state_dir):
                self.hdfs_client.delete(d, recursive=True)
                self.hdfs_client.makedirs(d)

    def configure_nodes(self):
        """Turn the config into node instances, and pickle them for reuse
        """

        for n_id, n_info in self.conf.iteritems():
            if n_info['type'] == 'spout':

                node = Spout(n_id, n_info['type'], n_info['downstream_nodes'], n_info['downstream_connectors'])
            else:
                if n_info['type'] == 'sink' or n_info['is_connecting']:
                    node = Connector(n_id, n_info['type'], n_info['rule'],
                                     n_info['upstream_nodes'], n_info['upstream_connectors'],
                                     n_info.get('downstream_nodes', None), n_info.get('downstream_connectors', None))
                else:
                    node = Bolt(n_id, n_info['type'], n_info['rule'],
                                n_info['upstream_nodes'], n_info['downstream_nodes'])

            self.hdfs_client.write(os.path.join(self.pickle_dir, '%d.pkl' % n_id), pickle.dumps(node, protocol=-1))
            LOGGER.info('node %d pickled' % n_id)

    def recover_nodes(self):
        """Adjust the backup data (both pending window and node state) to the latest consistent state,
            and update the pickled nodes to that state
        """
        nodes = {}
        for n in self.conf:
            with self.hdfs_client.read(os.path.join(self.pickle_dir, '%d.pkl' % n)) as f:
                nodes[n] = pickle.load(f)

        # adjust state (each connector is independent)
        for c_id, c_info in self.conf.iteritems():
            if c_info['is_connecting']:
                latest_version = nodes[c_id].get_latest_version()

                nodes[c_id].restore(latest_version)
                LOGGER.info('node %d restored to version %d' % (c_id, latest_version))
                nodes[c_id].pending_window.rewind(latest_version)
                LOGGER.info('pwnd %d rewound to version %d' % (c_id, latest_version))

                # each connector is responsible for upper bolts' states
                if c_info['type'] != 'spout':
                    for n in c_info['cover']:
                        nodes[n].restore(latest_version)
                        LOGGER.info('node %d restored to version %d' % (n, latest_version))

                    # ensure latest acks are received a
                    # if latest_version > 0:
                    for n in c_info['upstream_connectors']:
                        nodes[n].pending_window.handle_version_ack(VersionAck(c_id, latest_version))

        for n in nodes:
            self.hdfs_client.write(
                os.path.join(self.pickle_dir, '%d.pkl' % n),
                data=pickle.dumps(nodes[n], protocol=-1),
                overwrite=True)


    def start_nodes(self):
        """Load each pic_idled node and run it in a new process
        """

        self.hdfs_client.download(self.pickle_dir, CONSTANTS.ROOT_DIR, overwrite=True)

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
                      default=os.path.join(CONSTANTS.ROOT_DIR, 'linear_5_3_vanilla.yaml'))

    (options, args) = parser.parse_args()
    # if len(options) < 1:
    #     parser.error('%d args is too few' % len(options))

    starter = AppStarter(options.conf_file, options.start_mode)
    starter.run()