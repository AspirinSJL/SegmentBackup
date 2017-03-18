import os

#=== system / project

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

PORT_BASE = 2330

TCP_BUFFER_SIZE = 2048

#=== experiment config

# inter-node communication delay
LINK_DELAY = 0.5
# processing delay for each tuple
NODE_DELAY = 0.1
# number interval between two consecutive barriers
BARRIER_INTERVAL = 25
TIMER_INTERVAL = 100