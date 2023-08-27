import csv
import numpy as np
import matplotlib.pyplot as plt
from itertools import islice

def batched(iterable, chunk_size):
    iterator = iter(iterable)
    while chunk := tuple(islice(iterator, chunk_size)):
        yield chunk

bench_queries = [
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")))",
    "(->> (index 39747) (range 1452120960000000000 1452123510000000000))",
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (where (> #V 50)))",
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (groupby 1h avg))",
    "(->> (index 39747) (range 1452120960000000000 1452123510000000000) (groupby 15m max))",
]
res = []
labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]

with open("query_latencies.csv") as csvFile:
    reader = csv.reader(csvFile)
    headers = next(reader)
    for row in reader:
        # psBox[-1].append(float(row[2]))
        res.append(float(row[0]))


# TODO create subplots for each of the queries
fig, ax = plt.subplots(1, 5)
fig.set_size_inches(10.5, 7.5)
# ax[0].set_ylabel("Latency (seconds)")
# ax.set_yscale('log')

batches = []
for label, axis, batch in zip(labels, ax, batched(res, 10)):
    # batches.append(list(batch))
    axis.boxplot(list(batch), labels=[label])
    axis.set_ylabel("Latency (seconds)")

fig.tight_layout()
# ax.bar(["Q1", "Q2", "Q3", "Q4", "Q5"], batches)
# ax.boxplot(batches, labels=labels)
plt.savefig(f"query_latencies.png", dpi=400)
plt.savefig(f"query_latencies.pdf", format="pdf")
# plt.show()
plt.close()

