import time
import os
import pickle

from subprocess import Popen
from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt

import CONSTANTS

# number of runs
NUM_RUN = 10
NUM_NODES = 7

result_path = os.path.join(CONSTANTS.ROOT_DIR, 'results')

normal = {str(i): defaultdict(list) for i in xrange(1, NUM_NODES + 1)}
restart = {str(i): defaultdict(list) for i in xrange(1, NUM_NODES + 1)}

# t_total = []
# t_delay_before_new = []
# t_pwnd_write = []
# t_pwnd_handle_ack = []
# t_handle_barrier = []
# t_handle_normal = []
#
# s_storage_max = []
# s_storage_avg = []
# s_network_normal = []
# s_network_other = []

for topo in ('linear_7_4_vanilla.yaml', ):
    for i in xrange(NUM_RUN):
        Popen(['python', os.path.join(CONSTANTS.ROOT_DIR, 'start.py'),
               '-f', os.path.join(CONSTANTS.ROOT_DIR, 'topologies', topo),
               '-m', 'new'])
        time.sleep(CONSTANTS.TIME_AUDIT_INTERVAL + 10)
        Popen(['/bin/bash', os.path.join(CONSTANTS.ROOT_DIR, 'utility', 'kill_by_port.sh')])
        time.sleep(5)
        Popen(['/bin/bash', os.path.join(CONSTANTS.ROOT_DIR, 'utility', 'kill_by_port.sh')])

        for fn in os.listdir(result_path):
            n_id, _ = fn.split('_')
            with open(os.path.join(result_path, fn)) as f:
                for l in f:
                    if not l.split():
                        continue
                    metric, value = map(str.strip, l.split(':'))
                    normal[n_id][metric].append(float(value))

        time.sleep(15)

        Popen(['python', os.path.join(CONSTANTS.ROOT_DIR, 'start.py'),
               '-f', os.path.join(CONSTANTS.ROOT_DIR, 'topologies', topo),
               '-m', 'restart'])
        time.sleep(CONSTANTS.TIME_AUDIT_INTERVAL_RESTART + 10)
        Popen(['/bin/bash', os.path.join(CONSTANTS.ROOT_DIR, 'utility', 'kill_by_port.sh')])
        time.sleep(5)
        Popen(['/bin/bash', os.path.join(CONSTANTS.ROOT_DIR, 'utility', 'kill_by_port.sh')])

        for fn in os.listdir(result_path):
            n_id, _ = fn.split('_')
            with open(os.path.join(result_path, fn)) as f:
                for l in f:
                    if not l.split():
                        continue
                    metric, value = map(str.strip, l.split(':'))
                    restart[n_id][metric].append(float(value))

        time.sleep(15)

    with open(topo.split('.')[0] + '_data.pkl', 'wb') as f:
        pickle.dump([normal, restart], f)

    lines = []
    for i in map(str, range(1, NUM_NODES + 1)):
        line, = plt.plot(range(NUM_RUN), restart[i]['delay before new'])
        lines.append(line)

    plt.ylim((0, 15))
    plt.legend(lines, [str(i) + '_' + str(np.average(restart[str(i)]['delay before new'])) for i in xrange(1, NUM_NODES + 1)])
    plt.savefig('restart_delay_' + topo.split('.')[0])

    plt.clf()

    lines = []
    for i in map(str, range(1, NUM_NODES + 1)):
        line, = plt.plot(range(NUM_RUN), normal[i]['storage avg'])
        lines.append(line)

    # plt.ylim((0, 15))
    plt.legend(lines, [str(i) + '_' + str(np.average(normal[str(i)]['delay before new'])) for i in xrange(1, NUM_NODES + 1)])
    plt.savefig('storage_avg_' + topo.split('.')[0])