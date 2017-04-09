
import numpy as np
import matplotlib.pyplot as plt

test = {'1':{'a': [2, 3], 'b':[3, 5]}, '2':{'a': [3, 7], 'b':[3, 5]}}

lines = []
for i in '12':
    line, = plt.plot(range(2), test[i]['a'], label=i)
    lines.append(line)

plt.legend(lines, ['1', '2'])
plt.show()