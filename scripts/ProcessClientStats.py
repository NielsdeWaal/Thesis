import csv
import matplotlib.pyplot as plt
from typing import List

def ff_memtable_plots():
    psBox = []
    files = ["./ff_4kb.csv", "./ff_2mb.csv", "./ff_4mb.csv"]
    labels = ['4KB', '2MB', '4MB']
    for file in files:
        psBox.append([])
        with open(file) as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            for row in reader:
                psBox[-1].append(float(row[5]))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(psBox, labels=labels)
    plt.savefig("memtable_box.png", dpi=400)

bandwidth = []
latency = []
ps = []
xaxis = []

psBox: List[List[float]] = [[]]

with open("./ff_2mb.csv") as csvfile:
    ffReader = csv.reader(csvfile)
    headers = next(ffReader)
    for row in ffReader:
        latency.append(float(row[3]))
        bandwidth.append(float(row[4]))
        ps.append(float(row[5]))

        # xaxis = [i for i in range(len(bandwidth))]
        xaxis.append(int(row[0]))
        psBox[0].append(float(row[5]))

psBox.append([])
influxPs = []
influxAxis = []
with open("./influx_smp1.csv") as influxFile:
    influxReader = csv.reader(influxFile)
    headers = next(influxReader)
    for row in influxReader:
        influxPs.append(float(row[1]))
        influxAxis.append(int(row[0]))
        psBox[-1].append(float(row[1]))

psBox.append([])
with open("./quest_smp1.csv") as questFile:
    questReader = csv.reader(questFile)
    qheaders = next(questReader)
    for qrow in questReader:
        val = float(qrow[1])
        psBox[-1].append(val)

psBox.append([])
with open("./quest_smp1_influx_protocol.csv") as questFile:
    questReader = csv.reader(questFile)
    qheaders = next(questReader)
    for qrow in questReader:
        val = float(qrow[1])
        psBox[-1].append(val)

psBox.append([])
with open("./clickhouse_smp1.csv") as questFile:
    questReader = csv.reader(questFile)
    qheaders = next(questReader)
    for qrow in questReader:
        val = float(qrow[1])
        psBox[-1].append(val)

fig, (ax0, ax1, ax2) = plt.subplots(nrows=3, ncols=1, sharex=True)
ax0.set_ylabel("Latency (seconds)")
ax0.set_xlabel("Time")
ax0.set_title("Ingestion latency")
ax0.plot(xaxis, latency)

ax1.set_ylabel("Bandwith (MB/s)")
ax1.set_xlabel("Time")
ax1.set_title("Ingestion bandwidth (MB/s)")
ax1.plot(xaxis, bandwidth)

ax2.set_ylabel("Points per second (million)")
ax2.set_xlabel("Time")
ax2.set_title("Ingestion bandwidth (Points per second)")
ax2.plot(xaxis, ps)

plt.savefig("ff_bandwidth_overview.png", dpi=200)

labels = ["FrogFishDB", "InfluxDB", "QuestDB", "QuestDB (influx)", "Clickhouse"]
fig, ax = plt.subplots()
ax.set_ylabel("Points per second (million)")
ax.boxplot(psBox, labels=labels)

# plt.show()
plt.savefig("tsdb_boxplot.png", dpi=200)

ff_memtable_plots()