import csv
import matplotlib.pyplot as plt
from typing import List

def ff_2_clients():
    psBox = []
    files = ["./ff_client_1.csv", "./ff_client_2.csv"]
    # labels = ["client 1", "client 2"]
    labels = ["2 clients"]

    for file in files:
        # psBox.append([])
        with open(file) as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            counter = 0
            for row in reader:
                # psBox[-1].append(float(row[2]))
                psBox.append(float(row[2]))
                counter += 1
            print(counter)

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(psBox, labels=labels)
    plt.savefig("multi_client_2.png")

def collect_stats(files: List[str]):
    psBox: List[List[float]] = []
    for file in files:
        psBox.append([])
        with open(file) as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            for row in reader:
                psBox[-1].append(float(row[2]))

    res: List[float] = []
    for points in zip(*psBox):
        res.append(sum([item for item in points]))

    return res
    
def ff_4_clients_600_series():
    labels = ["2 clients", "4 clients"]
    res = []
    files = ["./ff_2_client_1_600_series.csv", "./ff_2_client_2_600_series.csv"]
    res.append(collect_stats(files))
    
    files = ["./ff_4_client_1_600_series.csv", "./ff_4_client_2_600_series.csv", "./ff_4_client_3_600_series.csv", "./ff_4_client_4_600_series.csv"]
    res.append(collect_stats(files))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    plt.savefig("multi_client_4.png")

def ff_4_clients_600_vs_20000():
    labels = ["600 series", "200000 series"]
    res = []
    files = ["./ff_4_client_1_600_series.csv", "./ff_4_client_2_600_series.csv", "./ff_4_client_3_600_series.csv", "./ff_4_client_4_600_series.csv"]
    res.append(collect_stats(files))

    files = ["./ff_4_client_1_20000_series.csv", "./ff_4_client_2_20000_series.csv", "./ff_4_client_3_20000_series.csv", "./ff_4_client_4_20000_series.csv"]
    res.append(collect_stats(files))
    
    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    plt.savefig("cardinality_effect.png")

# ff_2_clients()
ff_4_clients_600_series()
ff_4_clients_600_vs_20000()