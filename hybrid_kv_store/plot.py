#!/usr/local/bin/python3
import sys
import numpy as np
import matplotlib.pyplot as plt


AXIS_FONT_SIZE = 20


def smoothed(data, amt=100):
	result = []
	for i in range(len(data) - amt):
		end = min(i + amt, len(data))
		result.append(np.mean(data[i:end]))
	return result

# filename = sys.argv[1]
# data = smoothed(np.loadtxt(filename))
# plt.plot(data)
filenames = {"btree_latencies" : "B-Tree",
	"lsm_latencies": "LSM-Tree",
	"transition_latencies": "Transition"}
# ideal_data = np.ones(3900) * 100.
for f in filenames:
	fname = "{}.txt".format(f)
	data = smoothed(np.loadtxt(fname))
	plt.plot(data, label=filenames[f], linewidth=2.0)

# ideal_data = [np.mean(ideal_data[:2000])] * 2000 + [np.mean(ideal_data[2000:])] * 1900

# plt.plot(ideal_data, color='black', linestyle="--", linewidth=3.2, label="Ideal Data Structure")

# plt.xlabel("Query")
plt.ylabel("Latency (normalized)", fontsize=AXIS_FONT_SIZE)
plt.tick_params(axis='y', labelsize=16)
plt.xticks([])
# plt.title("")

plt.legend(frameon=False, fontsize=16)
plt.show()

