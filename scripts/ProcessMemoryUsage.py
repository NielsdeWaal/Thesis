import msparser
import pathlib
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr  
import numpy as np

# massifeFiles = sorted(pathlib.Path("./").glob("massif.out.*"))
# assert(len(massifeFiles) > 0)
# data = msparser.parse_file(massifeFiles[0])

def sizeof_fmt(x, pos):
    if x<0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1000.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1000.0

for scale in [32, 320, 3200]:
    data = msparser.parse_file(f"massif-{scale}")

    print(f"{data['cmd']}, unit {data['time_unit']}")

    for snapshot in data['snapshots']:
        print(f"{snapshot['time']} {snapshot['mem_heap']} {snapshot['mem_heap_extra']} {snapshot['mem_heap'] + snapshot['mem_heap_extra']} {snapshot['mem_stack']}")

    totalHeap = [snapshot['mem_heap'] + snapshot['mem_heap_extra'] for snapshot in data['snapshots']]
    timestamps = [snapshot['time'] for snapshot in data['snapshots']]

    fig, ax = plt.subplots()
    ax.set_ylabel("Memory usage (bytes)")
    ax.set_xlabel("Elapsed Time (seconds)")
    # ax.set_xlabel("Time since start of application")
    ax.yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
    ax.xaxis.set_major_formatter(tkr.FuncFormatter(lambda x, pos: str(x / 1e9)))
    ax.plot(timestamps, totalHeap)

    plt.savefig(f"{scale}_memory_usage.png")
    plt.savefig(f"{scale}_memory_usage.pdf", format="pdf")
# plt.show()
