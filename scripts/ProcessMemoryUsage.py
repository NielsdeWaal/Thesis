import msparser
import pathlib
import matplotlib.pyplot as plt

massifeFiles = sorted(pathlib.Path("./").glob("massif.out.*"))
assert(len(massifeFiles) > 0)
data = msparser.parse_file(massifeFiles[0])

print(f"{data['cmd']}, unit {data['time_unit']}")

for snapshot in data['snapshots']:
    print(f"{snapshot['time']} {snapshot['mem_heap']} {snapshot['mem_heap_extra']} {snapshot['mem_heap'] + snapshot['mem_heap_extra']} {snapshot['mem_stack']}")

totalHeap = [snapshot['mem_heap'] + snapshot['mem_heap_extra'] for snapshot in data['snapshots']]
timestamps = [snapshot['time'] for snapshot in data['snapshots']]

fig, ax = plt.subplots()
ax.set_ylabel("Memory usage (bytes)")
ax.set_xlabel("Time since start of application")
ax.plot(timestamps, totalHeap)

plt.savefig("memory_usage.png")
# plt.show()
