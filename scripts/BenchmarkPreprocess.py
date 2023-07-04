import capnp
import argparse
import socket
from collections import defaultdict

BATCH_CAPNP = capnp.load("../DB/source/IngestionProtocol/proto-python.capnp")
HOST = "127.0.0.1"
MANAGEMENT_PORT = 8080

parser = argparse.ArgumentParser(description="Preprocess influx files for FrogFishDB")
parser.add_argument('filename')
parser.add_argument('chunksize')

args = parser.parse_args()

tagsets = set()
mapping = {}
output_file = open(args.filename + ".capfile", 'w+b')

def write_batch(data):
    msg = BATCH_CAPNP.InsertionBatch.new_message()
    batches = defaultdict(list)

    # NOTE this is rather fragile
    # if the data list is not devisible by the number of timeseries
    # if data does not contain all tagsets registered
    recordings = msg.init('recordings', len(mapping))
    
    for line in data:
        sections = line.split(" ")
        measure_string = sections[1].split(',')
        tags_string = ','.join(sections[0].split(',')[1:])
        ts = sections[2]
        for meas in measure_string:
            k, v = meas.split('=')
            batches[tags_string + "," + k].append((int(ts),int(v[:-1])))

    for (tag, values), record in zip(batches.items(), recordings):
        record.tag = mapping[tag]
        measurements = record.init('measurements', len(values))
        for (ts, val), entry in zip(values, measurements):
            entry.timestamp = ts
            entry.value = val

    msg.write(output_file)


with open(args.filename, 'r') as f:
    # read first 1000 lines to check for tagset
    head = [next(f).strip() for _ in range(int(args.chunksize))]
    for line in head:
        sections = line.split(" ")

        # tags
        tags_string = sections[0]
        for measurement in sections[1].split(','):
            k, _ = measurement.split('=')
            tagsets.add(tags_string + "," + k)
        # tagset = [tag.split('=') for tag in tags_string]
        # print(tagset)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, MANAGEMENT_PORT))

    for test in tagsets:
        print(test)
        metric = test.split(',')[0]

        req = BATCH_CAPNP.IdRequest.new_message()
        req.identifier = 1337
        req.metric = test.split(',')[-1]

        tags_string = test.split(',')[1:-1]        
        tags = req.init('tagSet', len(tags_string))
        for tag, target in zip(tags_string, tags):
            k, v = tag.split('=')
            target.name = k
            target.value = v

        s.sendall(req.to_bytes())
        with BATCH_CAPNP.IdResponse.from_bytes(s.recv(1024)) as resp:
            tagset_string = ",".join(test.split(',')[1:-1]) + "," + test.split(',')[-1]
            mapping[tagset_string] = resp.setId

with open(args.filename, 'r') as f:
    strings = []
    for line in f:
        strings.append(line.strip())
        if len(strings) == int(args.chunksize):
            write_batch(strings)
            strings = []

    if strings:
        write_batch(strings)