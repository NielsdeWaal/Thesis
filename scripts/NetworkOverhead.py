import csv
import matplotlib.pyplot as plt
from typing import List

psBox: List[List[float]] = []

with open('./ff_noop.csv') as csvfile:
    psBox.append([])
    noop_reader = csv.reader(csvfile)
    headers = next(noop_reader)

    for row in noop_reader:
        psBox[-1].append(float(row[5]))

with open('./ff_2mb.csv') as csvfile:
    psBox.append([])
    reader = csv.reader(csvfile)
    headers = next(reader)

    for row in reader:
        psBox[-1].append(float(row[5]))

from statistics import mean, quantiles
print(f"min: {min(psBox[0])} max: {max(psBox[0])} avg: {mean(psBox[0])} quantiles: {quantiles(psBox[0])}")

labels = ["NOOP", "Normal"]
fig, ax = plt.subplots()
ax.set_ylabel("Points per second")
ax.boxplot(psBox, labels=labels)

plt.savefig('nop_vs_normal_box.png')
