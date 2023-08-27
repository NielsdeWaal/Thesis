import argparse

parser = argparse.ArgumentParser(description="Generate config files for FrogFish clients")
parser.add_argument('nr_client')
parser.add_argument('scale')
args = parser.parse_args()

filename_base = "/home/deploy/data/timeseries_data/preprocessed/{client_nr}_clients/large-{scale}-client-{client}.capfile"
# filename_base = "/tmp/small-{scale}-client-{client}.capfile"

for client in range(1, int(args.nr_client) + 1):
    filename = filename_base.format(client_nr = args.nr_client, scale = args.scale, client = (client - 1))
    with open(f'FrogFishClient-{client}.toml', 'w') as f:
        f.write(f"""[EventLoop]
RunHot = true
StatInterval = 10

[Client]
TestingFile = \"{filename}\"
ManagementPort = 8080
Sync = true
IngestMode = \"preprocessed\"""")
