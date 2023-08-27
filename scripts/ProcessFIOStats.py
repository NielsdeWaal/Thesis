import json
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr  
import numpy as np

def sizeof_fmt(x, pos):
    print(x)
    if x<0:
        return ""
    for x_unit in ['bytes/s', 'kB/s', 'MB/s', 'GB/s', 'TB/s']:
        if x < 1000.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1000.0

labels = ["4KB", "2MB", "4MB"]
means = []
stddev = []
for size in ["4kb", "2mb", "4mb"]:
    with open(f"{size}.json", 'r') as jfile:
        data = json.load(jfile)
        # print(data['jobs'][1]['write']['bw_mean'])
        # print(data['jobs'][1]['write']['bw_dev'])
        mean = int(data['jobs'][0]['write']['bw_mean']) * 1024
        dev = int(data['jobs'][0]['write']['bw_dev']) * 1024
        means.append(mean)
        stddev.append(dev)


fig, ax = plt.subplots()
fig.subplots_adjust(left=0.2)

bars = ax.bar(labels, means, yerr=stddev)
ax.yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
ax.set_ylabel("Bandwidth (Byte per second)")
plt.savefig("fio_bandwidth_stats.png")
plt.savefig("fio_bandwidth_stats.pdf", format="pdf")
