from tuple import *

import os
import logging
import cPickle as pickle
from collections import deque
from hdfs import Config


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Separate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)

LOGGER = logging.getLogger('Pending Window')


class PendingWindow(object):
    """docstring for PendingWindow"""
    def __init__(self, backup_dir, node):
        self.hdfs_client = Config().get_client('dev')

        # TODO: not cut
        # each pending window (or node) only has a single downstream cut,
        # otherwise inconsistency occurs during truncating

        self.backup_dir = backup_dir
        self.hdfs_client.makedirs(self.backup_dir)

        self.node = node

        # # each backup file is named by the ending version, so the current writing one is named temporarily
        # self.current_file = open(os.path.join(self.backup_dir, 'current'), 'wb')

        # the version that last truncation conducted against
        with open(os.path.join(self.backup_dir, 'safe_version'), 'w') as f:
            f.write(str(0))

        if self.node.type != 'sink':
            self.version_acks = dict()
            for n in self.node.downstream_connectors:
                self.version_acks[n] = deque()

    def append(self, tuple_):
        """Make an output tuple persistent, and complete a version if necessary
        """

        # for atomicity
        with open(os.path.join(self.backup_dir, 'temp'), 'wb') as f:
            pickle.dump(tuple_, f)

        os.rename(os.path.join(self.backup_dir, 'temp'),
                  os.path.join(self.backup_dir, str(tuple_.tuple_id)))

        # if isinstance(tuple_, BarrierTuple):
        #     self.current_file.close()
        #     os.rename(os.path.join(self.backup_dir, 'current'),
        #         os.path.join(self.backup_dir, str(tuple_.version)))
        #
        #     self.current_file = open(os.path.join(self.backup_dir, 'current'), 'wb')

    def extend(self, tuples):
        # TODO: can be improved
        for t in tuples:
            self.append(t)

    def truncate(self, version):
        """Delete files with filename <= version
        """

        with open(os.path.join(self.backup_dir, 'safe_version'), 'w') as f:
            # f.seek(0)
            f.write(str(version))
            # # note that this 'truncate()' means differently in Python from our definition
            # self.safe_version_file.truncate()

        for f in os.listdir(self.backup_dir):
            if f.isdigit() and int(f) <= version:
                os.remove(os.path.join(self.backup_dir, f))

    def handle_version_ack(self, version_ack):
        self.version_acks[version_ack.sent_from].append(version_ack.version)

        if all(self.version_acks.values()) and len(set(map(lambda q: q[0], self.version_acks.values()))) == 1:
            self.truncate(version_ack.version)

            for q in self.version_acks.values():
                q.popleft()

    def rewind(self, version):
        """Delete files with filename > version
        """

        for f in os.listdir(self.backup_dir):
            if f != 'safe_version' and (f == 'temp' or int(f) > version):
                os.remove(os.path.join(self.backup_dir, f))

        # self.current_file = open(os.path.join(self.backup_dir, 'current'), 'w')

    def replay(self):
        """When both the node and pending window state are ready, replay the pending window before resuming
        """

        tuples = []
        for t_id in sorted(filter(str.isdigit, os.listdir(self.backup_dir))):
            with open(os.path.join(self.backup_dir, t_id), 'rb') as f:
                # TODO: incomplete writing when compacted
                try:
                    t = pickle.load(f)
                    tuples.append(t)
                except pickle.UnpicklingError:
                    print 'unpickle end'
                    break

        self.node.multicast(self.node.downstream_nodes, tuples)
