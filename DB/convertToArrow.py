import pyarrow as pa

input_file = "./small-5"
schema = pa.schema({'strings': pa.string()})
chunk_size = 1000
writer = pa.RecordBatchFileWriter("./small-5.arrow", schema)
schema = pa.schema({'line': pa.string()})
writer = pa.RecordBatchFileWriter("./small-5.arrow", schema)

with open(input_file, 'r') as f:
    strings = []
    for line in f:
        strings.append(line.strip())
        if len(strings) == chunk_size:
            record_batch = pa.RecordBatch.from_arrays([pa.array(strings)], schema=schema)
            writer.write_batch(record_batch)
            strings = []
    if strings:
        record_batch = pa.RecordBatch.from_arrays([pa.array(strings)], schema=schema)
        writer.write_batch(record_batch)

writer.close()