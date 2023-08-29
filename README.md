# FrogFishDB
This repository contains the database and demo client for the FrogFish timeseries database.
The database is built for my thesis in an attempt to close the gap between the bandwidth of timeseries databases
and that of modern NVMe storage devices.

# Build the code
Building the database can be done usingthe following commands:
```bash
cd DB
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. 
```
The client is build similar to the database:
```bash
cd client
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. 
```

# Benchmark the database
First use the [TSBS](https://github.com/timescale/tsbs) tool to generate timeseries data.
In the scripts folder we provide a tool for converting influx line protocol data to a format which can be used by the demo client.
