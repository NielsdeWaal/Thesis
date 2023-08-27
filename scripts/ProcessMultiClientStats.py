import csv
import numpy as np
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

def collect_stats_ff(files: List[str]):
    psBox: List[List[float]] = []
    for file in files:
        psBox.append([])
        with open(file) as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            for row in reader:
                psBox[-1].append(float(row[2]) / 1000000)

    res: List[float] = []
    for points in zip(*psBox):
        res.append(sum([item for item in points]))

    return res

def collect_stats_tsbs(file: str):
    res = []
    with open(file) as csvFile:
        reader = csv.reader(csvFile)
        headers = next(reader)
        for row in reader:
            res.append(float(row[1]) / 1000000)

    return res
    
def ff_4_clients_600_series():
    labels = ["2 clients", "4 clients"]
    res = []
    files = ["./ff_2_client_1_600_series.csv", "./ff_2_client_2_600_series.csv"]
    res.append(collect_stats_ff(files))
    
    files = ["./ff_4_client_1_600_series.csv", "./ff_4_client_2_600_series.csv", "./ff_4_client_3_600_series.csv", "./ff_4_client_4_600_series.csv"]
    res.append(collect_stats_ff(files))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    plt.savefig("multi_client_4.png")

def ff_4_clients():
    # labels = ["200 series", "600 series", "2000 series", "200000 series"]
    # res = []

    # files = ["./ff_4_client_1_200_series.csv", "./ff_4_client_2_200_series.csv", "./ff_4_client_3_200_series.csv", "./ff_4_client_4_200_series.csv"]
    # res.append(collect_stats(files))

    # files = ["./ff_4_client_1_600_series.csv", "./ff_4_client_2_600_series.csv", "./ff_4_client_3_600_series.csv", "./ff_4_client_4_600_series.csv"]
    # res.append(collect_stats(files))

    # files = ["./ff_4_client_1_2000_series.csv", "./ff_4_client_2_2000_series.csv", "./ff_4_client_3_2000_series.csv", "./ff_4_client_4_2000_series.csv"]
    # res.append(collect_stats(files))

    # files = ["./ff_4_client_1_20000_series.csv", "./ff_4_client_2_20000_series.csv", "./ff_4_client_3_20000_series.csv", "./ff_4_client_4_20000_series.csv"]
    # res.append(collect_stats(files))
    
    # fig, ax = plt.subplots()
    # ax.set_ylabel("Points per second (million)")
    # ax.boxplot(res, labels=labels)
    # plt.savefig("cardinality_effect.png")
    # plt.savefig("cardinality_effect.pdf", format="pdf")
    labels = ["320 series", "3200 series", "32000 series"]
    res = []

    # files = [f"./ff_8_client_{client}_800_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    # files = [f"./ff_8_client_{client}_2000_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    # files = [f"./ff_8_client_{client}_20000_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    files = [f"./ff_4_client_{client}_32_series.csv" for client in range(1, 5)]
    res.append(collect_stats_ff(files))

    files = [f"./ff_4_client_{client}_320_series.csv" for client in range(1, 5)]
    res.append(collect_stats_ff(files))

    files = [f"./ff_4_client_{client}_3200_series.csv" for client in range(1, 5)]
    res.append(collect_stats_ff(files))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    plt.savefig("4_clients.png")
    plt.savefig("4_clients.pdf", format="pdf")

def ff_8_clients():
    # labels = ["800 series", "2000 series", "200000 series"]
    labels = ["320 series", "3200 series", "32000 series"]
    res = []

    # files = [f"./ff_8_client_{client}_800_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    # files = [f"./ff_8_client_{client}_2000_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    # files = [f"./ff_8_client_{client}_20000_series.csv" for client in range(1, 9)]
    # res.append(collect_stats(files))

    files = [f"./ff_8_client_{client}_32_series.csv" for client in range(1, 9)]
    res.append(collect_stats_ff(files))

    files = [f"./ff_8_client_{client}_320_series.csv" for client in range(1, 9)]
    res.append(collect_stats_ff(files))

    files = [f"./ff_8_client_{client}_3200_series.csv" for client in range(1, 9)]
    res.append(collect_stats_ff(files))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    plt.savefig("8_clients.png")
    plt.savefig("8_clients.pdf", format="pdf")

def ff_all():
    for nr_client in [1, 2, 4, 8, 16]:
        labels = ["32 series", "320 series", "3200 series"]
        res = []

        files = [f"./2mb/ff_{nr_client}_client_{client}_32_series.csv" for client in range(1, nr_client + 1)]
        res.append(collect_stats_ff(files))

        files = [f"./2mb/ff_{nr_client}_client_{client}_320_series.csv" for client in range(1, nr_client + 1)]
        res.append(collect_stats_ff(files))

        files = [f"./2mb/ff_{nr_client}_client_{client}_3200_series.csv" for client in range(1, nr_client + 1)]
        res.append(collect_stats_ff(files))

        fig, ax = plt.subplots()
        ax.set_ylabel("Points per second (million)")
        ax.boxplot(res, labels=labels)
        plt.savefig(f"{nr_client}_clients.png", dpi=400)
        plt.savefig(f"{nr_client}_clients.pdf", format="pdf")
    plt.close()

def global_comparison():
    for memtable in ["4kb", "2mb", "4mb"]:
        for nr_client in [1, 2, 4, 8, 16]:
            labels = ["FrogFishDB", "InfluxDB", "QuestDB", "Clickhouse"]
            scales = [32, 320, 3200]
            res = [[]]

            fig, (ax1, ax2, ax3, ax4) = plt.subplots(1, 4, sharey=True)
            # plt.set_title(f"{nr_client} clients")
            ax1.set_ylabel("Points per second (million)")

            # FIXME this only check single client, client 1??
            for scale in scales:
                files = [f"./{memtable}/ff_{nr_client}_client_{client}_{scale}_series.csv" for client in range(1, nr_client + 1)]
                res[-1].append(collect_stats_ff(files))

            res.append([])
            for scale in scales:
                res[-1].append(collect_stats_tsbs(f"influx_{nr_client}_{scale}_series_fixed.csv"))

            res.append([])
            for scale in scales:
                res[-1].append(collect_stats_tsbs(f"quest_{nr_client}_{scale}_series_fixed.csv"))

            res.append([])
            for scale in scales:
                res[-1].append(collect_stats_tsbs(f"clickhouse_{nr_client}_{scale}_series_fixed.csv"))

            ax1.boxplot(res[0], labels=scales)
            ax1.label_outer()
            ax1.set_title("FrogFishDB")
            ax1.set_xlabel("scale")
            ax2.boxplot(res[1], labels=scales)
            ax2.label_outer()
            ax2.set_title("InfluxDB")
            ax2.set_xlabel("scale")
            ax3.boxplot(res[2], labels=scales)
            ax3.label_outer()
            ax3.set_title("QuestDB")
            ax3.set_xlabel("scale")
            ax4.boxplot(res[3], labels=scales)
            ax4.label_outer()
            ax4.set_title("Clickhouse")
            ax4.set_xlabel("scale")
            # ax.boxplot(res[0], positions=np.array(range(len(res[0])))*2.0-0.4)
            # ax.boxplot(res[1], positions=np.array(range(len(res[1])))*2.0+0.4)

            # plt.xticks(range(0, len(labels) * 2, 3), labels)
            # plt.xticks(labels)
            plt.savefig(f"{memtable}_{nr_client}_clients_multi.png", dpi=400)
            plt.savefig(f"{memtable}_{nr_client}_clients_multi.pdf", format="pdf")

    plt.close()

def memtable_comp():
    labels = ["4KB", "2MB", "4MB"]
    res = []

    for memtable in ["4kb", "2mb", "4mb"]:
        res.append(collect_stats_ff([f"./{memtable}/ff_1_client_1_32_series.csv"]))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    ax.set_ylim(bottom=0)
    plt.savefig(f"memtables.png", dpi=400)
    plt.savefig(f"memtables.pdf", format="pdf")
    plt.close()
    
def noop_comp():
    labels = [1, 2, 4, 8, 16]
    res = []

    for nr_client in labels:
        res.append(collect_stats_ff([f"./noop/ff_{nr_client}_client_{client}_32_series.csv" for client in range(1, nr_client + 1)]))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.set_xlabel("Number of clients")
    ax.boxplot(res, labels=labels)
    plt.savefig(f"noop_multi_client.png", dpi=400)
    plt.savefig(f"noop_multi_client.pdf", format="pdf")
    plt.close()

    labels = ["NO-OP", "Single client"]
    res = []
    res.append(collect_stats_ff([f"./noop/ff_1_client_1_32_series.csv"]))
    res.append(collect_stats_ff([f"./2mb/ff_1_client_1_32_series.csv"]))

    fig, ax = plt.subplots()
    ax.set_ylabel("Points per second (million)")
    ax.boxplot(res, labels=labels)
    ax.set_ylim(bottom=0)
    plt.savefig(f"noop_single_client.png", dpi=400)
    plt.savefig(f"noop_single_client.pdf", format="pdf")
    plt.close()

    for nr_client in [1, 2 ,4, 8, 16]:
        labels = [32, 320, 3200]
        res = []

        for label in labels:
            res.append(collect_stats_ff([f"./noop/ff_{nr_client}_client_{client}_{label}_series.csv" for client in range(1, nr_client + 1)]))
        
        fig, ax = plt.subplots()
        ax.set_ylabel("Points per second (million)")
        ax.set_xlabel("Scale")
        ax.boxplot(res, labels=labels)
        ax.set_ylim(bottom=0)
        plt.savefig(f"noop_{nr_client}_series.png", dpi=400)
        plt.savefig(f"noop_{nr_client}_series.pdf", format="pdf")
        plt.close()

def latency_figures():
    def collect_latencies(files):
        res = []
        for file in files:
            with open(file) as csvFile:
                reader = csv.reader(csvFile)
                try:
                    headers = next(reader)
                except StopIteration:
                    continue
                for row in reader:
                    res.append(float(row[0]) / 1000000000)

        return res
        
    for nr_client in [1, 2, 4, 8, 16]:
        labels = ["32", "320", "3200"]
        res = []

        files = [f"./latencies/ff_{nr_client}_client_{client}_32_series_latency.csv" for client in range(1, nr_client + 1)]
        res.append(collect_latencies(files))

        files = [f"./latencies/ff_{nr_client}_client_{client}_320_series_latency.csv" for client in range(1, nr_client + 1)]
        res.append(collect_latencies(files))

        files = [f"./latencies/ff_{nr_client}_client_{client}_3200_series_latency.csv" for client in range(1, nr_client + 1)]
        res.append(collect_latencies(files))

        from matplotlib import dates
        import matplotlib.ticker as tkr  
        def to_milliseconds(x, pos):
            val = x * 1000000
            for unit in ['μs', 'ms', 's']:
                if val < 1000.0:
                    return "%3.1f %s" % (val, unit)
                val /= 1000.0

        fig, ax = plt.subplots()
        ax.set_ylabel("Latency")
        ax.set_xlabel("Scale")
        ax.set_yscale('log')
        ax.boxplot(res, labels=labels)
        ax.yaxis.set_major_formatter(tkr.FuncFormatter(to_milliseconds))
        plt.tight_layout()
        plt.savefig(f"{nr_client}_insertion_latency.png", dpi=400)
        plt.savefig(f"{nr_client}_insertion_latency.pdf", format="pdf")
        plt.close()

        # for iter, arr in enumerate(res):
            # fig, ax = plt.subplots(figsize=(8, 4))
        fig, ax = plt.subplots()

        for r, name in zip(res, ["32 series", "320 series", "3200 series"]):
            x = np.sort(r)
            y = 1. * np.arange(len(r)) / (len(r) - 1)
            ax.plot(x, y, label=name)

        # n, bins, patches = ax.hist(arr, 1000, density=True, histtype="step", cumulative=True, label=labels[iter])
        # n, bins, patches = ax.hist(res[0], 1000, density=True, histtype="step", cumulative=True, label="32 series")
        # n, bins, patches = ax.hist(res[1], 1000, density=True, histtype="step", cumulative=True, label="320 series")
        # n, bins, patches = ax.hist(res[2], 1000, density=True, histtype="step", cumulative=True, label="3200 series")


        ax.grid(True)
        ax.legend(loc="lower right")
        ax.set_ylabel("Probability")
        ax.set_xlabel("Latency (seconds)")
        ax.set_xscale("log")
        plt.savefig(f"{nr_client}_insertion_latency_cdf.png", dpi=400)
        plt.savefig(f"{nr_client}_insertion_latency_cdf.pdf", format="pdf")
        plt.close()

# ff_all()
# global_comparison()
# memtable_comp()
# noop_comp()
latency_figures()