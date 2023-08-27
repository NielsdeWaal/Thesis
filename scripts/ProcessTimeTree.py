import matplotlib.pyplot as plt
import json
import pathlib
import numpy as np
from typing import Dict, List

def process_insert_bench():
    with open("InsertBench") as jfile:
        from matplotlib.ticker import NullFormatter

        def formatter(x, pos):
            return str(round(x / 1e6, 1))

        data = json.load(jfile)
        points = [int(x['name'].split()[1]) for x in data['results']]
        res = [1 / x['median(elapsed)'] for x in data['results']]
        combined = [(1 / x['median(elapsed)'], int(x['name'].split()[1])) for x in data['results']]

        errors = [x["medianAbsolutePercentError(elapsed)"] for x in data['results']]

        maxVal = max(combined, key=lambda x: x[0])

        fig, ax = plt.subplots()
        ax.set_ylabel("Million insertions per second")
        ax.set_xlabel("Fanout")
        ax.set_title("Insertion performance for different fanout configurations")
        # ax.yaxis.get_major_formatter().set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
        # ax.yaxis.get_major_formatter().set_useOffset(False)        
        ax.bar(points, res, yerr=map(lambda x,y: x*y, errors, res), width=18)

        bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
        arrowprops = dict(arrowstyle="->",connectionstyle="angle,angleA=0,angleB=60")
        kw = dict(xycoords='data',textcoords="axes fraction",
                  arrowprops=arrowprops, bbox=bbox_props, ha="right", va="top")

        ax.annotate(f"fanout={maxVal[1]}", xy=(maxVal[1], maxVal[0]), xytext=(0.97, 0.96), **kw)
    
        plt.savefig('TimeTree-Insertions.png', dpi=400)
        plt.savefig("TimeTree-Insertions.pdf", format="pdf")
        plt.close()

        fig, ax = plt.subplots()
        ax.set_ylabel("Nr. page faults (median)")
        ax.set_xlabel("Fanout")

        res = [x['median(pagefaults)'] for x in data['results']]
        ax.bar(points, res, width=18)

        plt.savefig("timetree_page_faults.png")
        plt.savefig("timetree_page_faults.pdf", format="pdf")

        fig, ax = plt.subplots()
        ax.set_ylabel("IPC (median)")
        ax.set_xlabel("Fanout")

        res = [x['median(branchinstructions)'] for x in data['results']]
        ax.bar(points, res, width=18)

        plt.savefig("timetree_ipc.png")
        plt.savefig("timetree_ipc.pdf", format="pdf")

def process_query_performance():
    arities = [8, 16, 32, 64, 128, 256, 512, 1024]
    sizes = ["10", "1K", "10K", "100K", "1M"]
    width = 0.15
    multiplier = 0
    x = np.arange(len(sizes))

    # fig, axs = plt.subplots(4, 4, squeeze=True)
    
    # for arity, ax in zip(arities, axs.flatten()):
    for arity in arities:
        fig, ax = plt.subplots()

        multiplier = 0
        with open(f"QueryArity{arity}", 'r') as jfile:
            data = json.load(jfile)
            for name, res in zip(sizes, data['results']):
                print(f"median: {1 / res['median(elapsed)']}")
                offset = width * multiplier
                rects = ax.bar(x + offset, (1 / res['median(elapsed)']), width, label=name)
                ax.bar_label(rects, padding=3)
                ax.set_yscale('log')
                ax.set_xticks(x + width, sizes)
                multiplier += 1
    
        plt.savefig(f"overview_query_arity_{arity}.png")
        plt.savefig(f"overview_query_arity_{arity}.pdf", format="pdf")

def process_query_sizes():
    arities = [8, 16, 32, 64, 128, 256, 512, 1024]
    sizes = ["10", "1K", "10K", "100K", "1M"]
    width = 0.15
    multiplier = 0
    x = np.arange(len(arities))

    # fig, axs = plt.subplots(4, 4, squeeze=True)
    
    # for arity, ax in zip(arities, axs.flatten()):
    for iter, size in enumerate(sizes):
        fig, ax = plt.subplots()
        rate = []
        print(f"{iter} - {size}")
        for arity in arities:
            with open(f"QueryArity{arity}", 'r') as jfile:
                data = json.load(jfile)
                # print(f"{1 / data['results'][iter]['median(elapsed)']}")
                print(f"{1 / data['results'][iter]['median(branchmisses)']}")
                rate.append(1 / data['results'][iter]['median(elapsed)'])
                # offset = width * multiplier
                # rects = ax.bar(x + offset, (1 / data['results'][iter]['median(elapsed)']), width, label=size)
                # p = ax.bar()
                # ax.bar_label(rects, padding=3)
                # ax.set_yscale('log')
                # ax.set_xticks(x + width, sizes)
                # multiplier += 1
        ax.bar(list(map(lambda x: f"{x}", arities)), rate)
        ax.set(ylabel="Queries per second", title=f"Queries per second for size: {size}")
        ax.set_xlabel("Fanout degree")
                    
        # multiplier = 0
        #     for name, res in zip(sizes, data['results']):
        #         print(f"median: {1 / res['median(elapsed)']}")
        #         offset = width * multiplier
        #         rects = ax.bar(x + offset, (1 / res['median(elapsed)']), width, label=name)
        #         ax.bar_label(rects, padding=3)
        #         ax.set_yscale('log')
        #         ax.set_xticks(x + width, sizes)
        #         multiplier += 1
    
        plt.savefig(f"overview_query_size_{size}.png")
        plt.savefig(f"overview_query_size_{size}.pdf", format="pdf")

process_insert_bench()
process_query_performance()
process_query_sizes()

# queryFiles = sorted(pathlib.Path("./").glob("QueryArity*"))
# means : Dict[str, List[float]] = {}
# names: List[str] = []

# for path in queryFiles:
#     print(f"Processing: {path}")
#     with open(path) as jfile:
#         data = json.load(jfile)
#         for res in data['results']:
#             if not res['title'].split()[-1] in names:
#                 names.append(res['title'].split()[-1])

#             print(f"median: {res['median(elapsed)']}")

#             name = res['name'].split()[-1]
#             if not name in means:
#                 means[name] = []
#             means[name].append(1 / res['median(elapsed)'])

# fig, ax = plt.subplots(layout='constrained')

# width = 0.15
# multiplier = 0
# x = np.arange(len(names))

# for attribute, measurement in means.items():
#     offset = width * multiplier
#     rects = ax.bar(x + offset, measurement, width, label=attribute)
#     ax.bar_label(rects, padding=3)
#     multiplier += 1

# # Add some text for labels, title and custom x-axis tick labels, etc.
# ax.set_ylabel('Queries per second')
# ax.set_title('Fanout configuration')
# ax.set_xticks(x + width, names)
# ax.set_yscale('log')
# ax.legend(loc='upper left', ncols=3)

# plt.savefig('TimeTree-QueryPerformance.png')
# # plt.show()