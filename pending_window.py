from tuple import *

import os
import cPickle as pickle
from collections import deque
from hdfs import Config

class PendingWindow(object):
    """docstring for PendingWindow"""
    def __init__(self, backup_dir, node):
        # TODO: not cut
        # each pending window (or node) only has a single downstream cut,
        # otherwise inconsistency occurs during truncating
        self.backup_dir = backup_dir
        self.node = node

        self.hdfs_client = Config().get_client('dev')

        self.hdfs_client.makedirs(self.backup_dir)

        # each backup file is named by the ending version, so the current writing one is named temporarily
        self.current_backup_path = os.path.join(self.backup_dir, 'current')
        # touch the file for later appending
        self.hdfs_client.write(self.current_backup_path, data='')

        # the version that last truncation conducted against
        self.safe_version_path = os.path.join(self.backup_dir, 'safe_version')
        # special case for initial version
        self.hdfs_client.write(self.safe_version_path, data=str(0))

        if self.node.type != 'sink':
            self.version_acks = dict()
            for n in self.node.downstream_connectors:
                self.version_acks[n] = deque()

    def append(self, tuple_):
        """Make an output tuple persistent, and complete a version if necessary
        """

        self.hdfs_client.write(self.current_backup_path, data=pickle.dumps(tuple_), append=True)

        if isinstance(tuple_, BarrierTuple):
            self.hdfs_client.rename(self.current_backup_path, os.path.join(self.backup_dir, str(tuple_.version)))
            self.hdfs_client.write(self.current_backup_path, data='')

    def extend(self, tuples):
        # TODO: can be improved
        with self.hdfs_client.write(self.current_backup_path, append=True) as f:
            for t in tuples:
                pickle.dump(t, f)

        if isinstance(tuples[-1], BarrierTuple):
            self.hdfs_client.rename(self.current_backup_path, os.path.join(self.backup_dir, str(tuples[-1].version)))
            self.hdfs_client.write(self.current_backup_path, data='')

    def truncate(self, version):
        """Delete files with filename <= version
        """

        self.hdfs_client.write(self.safe_version_path, data=str(version), overwrite=True)

        for f in self.hdfs_client.list(self.backup_dir):
            if f.isdigit() and int(f) <= version:
                self.hdfs_client.delete(os.path.join(self.backup_dir, f))

        # self.node.LOGGER.info('truncated version %d' % version)

    def handle_version_ack(self, version_ack):
        self.version_acks[version_ack.sent_from].append(version_ack.version)

        if all(self.version_acks.values()) and len(set(map(lambda q: q[0], self.version_acks.values()))) == 1:
            self.truncate(version_ack.version)

            for q in self.version_acks.values():
                q.popleft()

    def rewind(self, version):
        """Delete files with filename > version (including current file)
        """

        for f in self.hdfs_client.list(self.backup_dir):
            if f.isdigit() and int(f) > version:
                self.hdfs_client.delete(os.path.join(self.backup_dir, f))

        self.hdfs_client.write(self.current_backup_path, data='', overwrite=True)

    def replay(self):
        """When both the node and pending window state are ready, replay the pending window before resuming
        """

        for v in sorted(map(str, filter(unicode.isdigit, self.hdfs_client.list(self.backup_dir)))):
            tuples = []
            with self.hdfs_client.read(os.path.join(self.backup_dir, str(v))) as f:
                while True:
                    try:
                        t = pickle.load(f)
                        tuples.append(t)
                    except EOFError:
                        self.node.LOGGER.debug('reached EOF, send this version')
                        break
                    except pickle.UnpickleableError:
                        self.node.LOGGER.debug('spout reached partial dump location, send this incomplete version')
                        break
                self.node.multicast(self.node.downstream_nodes, tuples)
