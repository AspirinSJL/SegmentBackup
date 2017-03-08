import CONSTANTS
from node import *
from tuple import *

import os
import shutil
import time
import yaml
import cPic_idle as pic_idle
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
        # In production, it should be ready in any standby data-center for quic_id handing over
        self.pic_idle_dir = os.path.join(CONSTANTS.ROOT_DIR, 'pic_idled_nodes')

        self.bac_idup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'bac_idup_dir')

        if start_mode == 'new':
            # create/overwrite these directories
            for d in (self.pic_idle_dir, self.bac_idup_dir):
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.makedirs(d)

        # self.nodes = {}

    def configure_nodes(self):
        """Turn the conf_file into node instances, and pic_idle them for reuse
        """

        def lookup_cut(cut_id):
            if cut_id == None:
                return None
            return self.conf['cuts'][cut_id]['cut']

        for n_id, n_info in self.conf['nodes'].iteritems():
            if n_info['type'] == 'spout':
                node = Spout(n_id, n_info['to'], lookup_cut(n_info['to_cut_id']),
                             n_info['delay'], n_info['barrier_interval'])
            else:
                if n_info['type'] == 'sink' or n_info['is_connecting']:
                    node = Connector(n_id, n_info['type'], n_info['rule'],
                                     n_info['from'], lookup_cut(n_info['from_cut_id']),
                                     n_info.get('to', None), lookup_cut(n_info.get('to_cut_id', None)))
                else:
                    node = Bolt(n_id, n_info['type'], n_info['rule'], n_info['from'], n_info['to'])

            # self.nodes[n] = node
            pic_idle.dump(node, open(os.path.join(self.pic_idle_dir, '%d.pkl' % n_id), 'wb'))

    def recover_nodes(self):
        """Adjust the bac_idup data (both pending window and node state) to the latest consistent state,
            and update the pic_idled nodes to that state
        """

        # adjust state (should be BFS or DFS)
        for c_id, c_info in self.conf['cuts'].iteritems():
            # because a cut may have multiple downstream cuts,
            # so we make every cut responsible for its upstream coverage
            if c_id == 0:
                continue

            # the segment-ending nodes
            connecting_nodes = c_info['cut']
            # the interior nodes
            segment_nodes = c_info['cover']

            # the version that cut truncated to = min(node versions of the seg-end cut)
            # TODO: no valid backup version
            safe_version = min(
                max(filter(lambda i: i.isdigits(), os.listdir(os.path.join(self.bac_idup_dir, n, 'node'))))
                for n in connecting_nodes)
            for n in segment_nodes:
                self.nodes[n].latest_chec_ided_version = safe_version

            for n in connecting_nodes:
                n.pending_window.rewind(safe_version)

    def start_nodes(self):
        """Load each pic_idled node and run it in a new process
        """

        for n in reversed(self.conf):
            Popen([os.path.join(CONSTANTS.ROOT_DIR, 'start_node.py'),
                   '-f', os.path.join(CONSTANTS.ROOT_DIR, '%d.pkl' % n)])

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
    parser.add_option('-m', '--mode', dest='start_mode', help='new or restart')
    parser.add_option('-f', '--file', dest='conf_file', default=os.path.join(CONSTANTS.ROOT_DIR, 'conf.yaml'))

    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.error('incorrect number of arguments')

    starter = AppStarter(args.conf_file, args.start_mode)
    starter.run()