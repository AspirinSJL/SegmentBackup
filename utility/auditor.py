import CONSTANTS

import os
import hdfs
import time


class TimeAuditor(object):
    def __init__(self, node, restart=False, test_mode=True):
        self.node = node
        self.read_interval = CONSTANTS.TIME_AUDIT_INTERVAL_RESTART if restart else CONSTANTS.TIME_AUDIT_INTERVAL
        self.test_mode = test_mode

        self.log_path = os.path.join(CONSTANTS.ROOT_DIR, 'results', '_'.join([str(self.node.node_id), 'time']))

        self.start = None

        self.start_new = None

        self.pending_window_write = 0
        self.pending_window_handle_ack = 0

        self.handle_barrier = 0
        self.handle_normal = 0

    def read(self):
        if self.test_mode:
            with open(self.log_path, 'w') as f:
                # f.write('\n'.join(map(str, [
                #     time.time() - self.start,
                #     self.start_new - self.start if self.start_new else -1,
                #     self.pending_window_write,
                #     self.pending_window_handle_ack,
                #     self.handle_barrier,
                #     self.handle_normal,
                # ])))
                f.write('''
                    total:            %f
                    delay before new: %f
                    pwnd write:       %f
                    pwnd handle ack:  %f
                    handle barrier:   %f
                    handle normal:    %f
                    ''' % (time.time() - self.start, self.start_new - self.start if self.start_new != None else -1,
                           self.pending_window_write, self.pending_window_handle_ack,
                           self.handle_barrier, self.handle_normal))
        else:
            self.node.LOGGER.info('''%s
            total:            %f
            delay before new: %f
            pwnd write:       %f
            pwnd handle ack:  %f
            handle barrier:   %f
            handle normal:    %f
            ''' % ('-' * 10 + str(self.node.node_id),
                   time.time() - self.start, self.start_new - self.start if self.start_new else -1,
                   self.pending_window_write, self.pending_window_handle_ack,
                   self.handle_barrier, self.handle_normal))

    def run(self):
        while True:
            time.sleep(self.read_interval)
            self.read()


class SpaceAuditor(object):
    def __init__(self, node, restart=False, test_mode=True):
        self.node = node
        self.read_interval = CONSTANTS.SPACE_AUDIT_INTERVAL_RESTART if restart else CONSTANTS.SPACE_AUDIT_INTERVAL
        self.test_mode = test_mode

        self.log_path = os.path.join(CONSTANTS.ROOT_DIR, 'results', '_'.join([str(self.node.node_id), 'space']))

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
                    try:
                        s = self.hdfs_client.status(os.path.join(node_id, k, f))
                        total += s['length']
                    except hdfs.util.HdfsError:
                        # TODO: miss a file
                        pass

            return total

        current_storage = read_backup_space(os.path.join('backup', str(self.node.node_id)))
        self.storage_max = max(current_storage, self.storage_max)
        if self.storage_avg == None:
            self.storage_avg = current_storage
        else:
            # moving average
            self.storage_avg = self.storage_avg * (1.0 - CONSTANTS.FADING_FACTOR) + \
                               current_storage * CONSTANTS.FADING_FACTOR

        if self.test_mode:
            with open(self.log_path, 'w') as f:
                # f.write('\n'.join(map(str, [
                #     self.storage_max,
                #     self.storage_avg,
                #     self.network_normal,
                #     self.network_other,
                # ])))
                f.write('''
                    storage max:      %f
                    storage avg:      %f
                    network normal:   %f
                    network other:    %f
                    ''' % (self.storage_max, self.storage_avg,
                           self.network_normal, self.network_other))
        else:
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
            time.sleep(self.read_interval)
            self.read()