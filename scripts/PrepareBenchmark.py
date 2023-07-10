import capnp
import argparse
import socket
from collections import defaultdict
from typing import List, Dict

BATCH_CAPNP = capnp.load("../DB/source/IngestionProtocol/proto-python.capnp")
HOST = "127.0.0.1"
MANAGEMENT_PORT = 8080

parser = argparse.ArgumentParser(description="Preprocess influx files for FrogFishDB")
parser.add_argument('filename')
parser.add_argument('clients')
parser.add_argument('scale')
parser.add_argument('chunksize')
args = parser.parse_args()

assert(int(args.scale) % int(args.clients) == 0)

# output_file = open(args.filename + ".capfile", 'w+b')
files = {client: open(f"{args.filename}-client-{client}.capfile", 'w+b') for client in range(int(args.clients))}
per_client = int(args.scale) / int(args.clients)

print(f"{per_client} lines per client")

# Split influx file into multiple files split over `clients`
# script assumes cpu-only workload, which contains 10 values per line
# - read scale number of lines to determine timeseries set
# - read from start in batches of `scale`
# - divide `scale` across `clients`

tagsets = set()
mapping = {}

def write_batch(client: int, data: List[str]):
    msg = BATCH_CAPNP.InsertionBatch.new_message()
    batches = defaultdict(list)
    rec_count = 0

    # for k, v in mapping.items():
    #     print(f"{k} -> {v}")
    
    for line in data:
        if len(line) == 0:
            break;
        # print(line)
        sections = line.split(" ")
        # print(sections)
        measure_string = sections[1].split(',')
        tags_string = ','.join(sections[0].split(',')[1:])
        # print(tags_string)
        ts = sections[2]
        for meas in measure_string:
            k, v = meas.split('=')
            batches[tags_string + "," + k].append((int(ts),int(v[:-1])))
            rec_count += 1

    # print(len(batches))
    # print(len(data))

    # NOTE this is rather fragile
    # if the data list is not devisible by the number of timeseries
    # if data does not contain all tagsets registered
    recordings = msg.init('recordings', min(rec_count, per_client))

    for (tag, values), record in zip(batches.items(), recordings):
        record.tag = mapping[tag]
        measurements = record.init('measurements', len(values))
        for (ts, val), entry in zip(values, measurements):
            entry.timestamp = ts
            entry.value = val

    msg.write(files[client])

with open(args.filename, 'r') as f:
    # read first `scale` number of lines to check for tagset
    head = [next(f).strip() for _ in range(int(args.scale))]
    for line in head:
        sections = line.split(" ")

        # tags
        tags_string = sections[0]
        tags_string = ",".join(tags_string.split(',')[1:])
        for measurement in sections[1].split(','):
            k, _ = measurement.split('=')
            tagsets.add(tags_string + "," + k)
        # tagset = [tag.split('=') for tag in tags_string]

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, MANAGEMENT_PORT))

    # print(tagsets)
    # print(len(tagsets))

    for test in tagsets:
        items = test.split(',')
        req = BATCH_CAPNP.IdRequest.new_message()
        req.identifier = 1337
        req.metric = items[-1]

        tags_collection = items[:-1]        
        tags = req.init('tagSet', len(tags_collection))
        for tag, target in zip(tags_collection, tags):
            k, v = tag.split('=')
            target.name = k
            target.value = v

        s.sendall(req.to_bytes())
        with BATCH_CAPNP.IdResponse.from_bytes(s.recv(1024)) as resp:
            mapping[test] = resp.setId

print(len(mapping))
with open(args.filename, 'r') as f:
    nr_batches = 0
    # lines = []
    client = 0
    batches = defaultdict(list)
    end = False
    # for line in f:
    while True:
        for client in range(int(args.clients)):
            lines = [f.readline().strip() for _ in range(int(per_client))]
            print(f"{len(lines)} ({len(lines) * 10} points) lines for client {client}")
            # print(f"{lines} of len: {len(lines)}")
            if not lines[-1]:
                end = True
            batches[client].extend(lines)

        if len(batches[0]) == int(args.chunksize):
            print("Creating batch")
            [write_batch(client, batches[client]) for client in range(int(args.clients))]
            nr_batches += 1
            batches.clear()

        if end:
            break

    if len(batches[0]):
        print("Creating remaining batch")
        [write_batch(client, batches[client]) for client in range(int(args.clients))]
        nr_batches += 1

    # for k, v in batches.items():
    #     print(k)
    #     print(len(v))
    # for line in f:
    #     lines.append(line.strip())
    #     if len(lines) == int(args.chunksize):
    #         write_batch(lines)
    #         nr_batches += 1
    #         lines = []

    # if lines:
    #     write_batch(lines)
    #     nr_batches += 1

    print(f"Generated {nr_batches} batches for ingestion")