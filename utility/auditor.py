import CONSTANTS

import os
import hdfs
import time


class TimeAuditor(object):
    def __init__(self, node):
        self.node = node

        self.start = time.time()

        self.start_new = None

        self.pending_window_write = 0
        self.pending_window_handle_ack = 0

        self.handle_barrier = 0
        self.handle_normal = 0

    def read(self):
        self.node.LOGGER.info('''%s
        total:            %f
        delay before new: %f
        pwnd write:       %f
        pwnd handle ack:  %f
        handle barrier:   %f
        handle normal:    %f
        ''' % ('-' * 10 + str(self.node.node_id),
               time.time() - self.start, self.start_new - self.start,
               self.pending_window_write, self.pending_window_handle_ack,
               self.handle_barrier, self.handle_normal))

    def run(self):
        while True:
            time.sleep(CONSTANTS.TIME_AUDIT_INTERVAL)
            self.read()


class SpaceAuditor(object):
    def __init__(self, node):
        self.node = node

        self.hdfs_client = hdfs.Config().get_client('dev')

        # only measure the backup part, which is most important
        self.storage_max = 0
        self.storage_avg = None

        # in bytes, only audit sender
        self.network_normal = 0
        self.network_other = 0

    def read(self):
        def read_backup_space(node_id):
            total = 0

            for k in self.hdfs_client.list(node_id):
                for f in self.hdfs_client.list(os.path.join(node_id, k)):
                    s = self.hdfs_client.status(os.path.join(node_id, k, f))
                    total += s['length']

            return total

        current_storage = read_backup_space(os.path.join('backup', str(self.node.node_id)))
        self.storage_max = max(current_storage, self.storage_max)
        if self.storage_avg == None:
            self.storage_avg = current_storage
        else:
            # moving average
            self.storage_avg = self.storage_avg * (1.0 - CONSTANTS.FADING_FACTOR) + \
                               current_storage * CONSTANTS.FADING_FACTOR

        self.node.LOGGER.info('''%s
        storage max:      %f
        storage avg:      %f
        network normal:   %f
        network other:    %f
        ''' % ('-' * 10 + str(self.node.node_id),
               self.storage_max, self.storage_avg,
               self.network_normal, self.network_other))

    def run(self):
        while True:
            time.sleep(CONSTANTS.SPACE_AUDIT_INTERVAL)
            self.read()