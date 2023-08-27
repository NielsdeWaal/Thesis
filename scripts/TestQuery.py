import capnp
import socket
from typing import List, Dict
import time
import numpy as np
import matplotlib.pyplot as plt

BATCH_CAPNP = capnp.load("../DB/source/IngestionProtocol/proto-python.capnp")
HOST = "127.0.0.1"
MANAGEMENT_PORT = 8080

def recvall(sock):
    BUFF_SIZE = 4096 # 4 KiB
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            # either 0 or end of data
            break
    return data

def execute_query(query):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, MANAGEMENT_PORT))
        msg = BATCH_CAPNP.ManagementMessage.new_message()

        q = BATCH_CAPNP.QueryMessage.new_message()
        q.query = query

        msg.type.query = q

        start = time.time_ns()
        s.sendall(msg.to_bytes())
        # time.sleep(0.5)
        # with BATCH_CAPNP.QueryResult.from_bytes(recvall(s)) as resp:
        #     # print(resp)
        #     # print(f"Query took: {(time.time_ns() - start) / (10 ** 9)}s")    
        #     return (time.time_ns() - start) / (10 ** 9)
        return 0

# with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#     s.connect((HOST, MANAGEMENT_PORT))
#     msg = BATCH_CAPNP.ManagementMessage.new_message()

#     q = BATCH_CAPNP.QueryMessage.new_message()

    # q.query = "(->> (index 39747) (range 1452120960000000000 1452123510000000000) (groupby 15m max))"
    # q.query = "(->> (index 39747) (range 1452120960000000000 1452123510000000000) )"
    # q.query = "(->> (index 32283) (range 1452113280000000000 1452118390000000000) )"
    # q.query = "(->> (index 32283))"
    # q.query = "(->> (index 32283) (groupby 15m max))"

    # q.query = "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")))"
    # q.query = "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (groupby 1h max))"
    # q.query = "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (groupby 1h avg))"


    # msg.type.query = q

    # start = time.time_ns()
    # s.sendall(msg.to_bytes())
    # with BATCH_CAPNP.QueryResult.from_bytes(recvall(s)) as resp:
    #     # print(resp)
    #     print(f"Query took: {(time.time_ns() - start) / (10 ** 9)}s")    

bench_queries = [
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")))",
    "(->> (index 39747) (range 1452120960000000000 1452123510000000000))",
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (where (> #V 50)))",
    "(->> (metric \"usage_user\") (tag \"hostname\" '(\"host_0\")) (groupby 1h avg))",
    "(->> (index 39747) (range 1452120960000000000 1452123510000000000) (groupby 15m max))",
]

res = []

for query in bench_queries:
    res.append([execute_query(query) for _ in range(10)])

labels = ["Select all", "Group by 1h", "Basic filter", "Select time range"]

# fig, ax = plt.subplots()
# ax.set_ylabel("Latency (second)")
# # ax.set_xlabel("Scale")
# ax.set_yscale('log')
# ax.boxplot(res, labels=labels)
# plt.savefig(f"query_benchmark.png")
# plt.savefig(f"query_benchmark.pdf", format="pdf")
# plt.close()
