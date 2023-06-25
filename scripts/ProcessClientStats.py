import csv
import matplotlib.pyplot as plt
from typing import List

with open("./res.csv") as csvfile:
    reader = csv.reader(csvfile)
    headers = next(reader)
    # for row in reader:
    #     print(f"{row[3]}")
    bandwidth = []
    latency = []
    ps = []
    xaxis = []

    psBox: List[List[float]] = [[]]
    
    for row in reader:
        latency.append(float(row[3]))
        bandwidth.append(float(row[4]))
        ps.append(float(row[5]))

        # xaxis = [i for i in range(len(bandwidth))]
        xaxis.append(int(row[0]))
        psBox[0].append(float(row[5]))

    psBox.append([])
    influxPs = []
    influxAxis = []
    with open("./influx.csv") as influxFile:
        influxReader = csv.reader(influxFile)
        headers = next(influxReader)
        for row in influxReader:
            influxPs.append(float(row[1]))
            influxAxis.append(int(row[0]))
            psBox[1].append(float(row[1]))

    psBox.append([])
    with open("./quest.csv") as questFile:
        questReader = csv.reader(questFile)
        qheaders = next(questReader)
        for qrow in questReader:
            val = float(qrow[1])
            psBox[2].append(val)

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

    labels = ["FrogFishDB", "InfluxDB", "QuestDB"]
    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(psBox, labels=labels)

    # plt.show()
    plt.savefig("boxplot.png")
