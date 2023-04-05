import pyarrow as pa

input_file = "./small-1"

chunk_size = 1000
schema = pa.schema({'line': pa.string()})
writer = pa.RecordBatchFileWriter(input_file + ".arrow", schema)

with open(input_file, 'r') as f:
    strings = []
    for line in f:
        strings.append(line.strip() + '\n')
        if len(strings) == chunk_size:
            record_batch = pa.RecordBatch.from_arrays([pa.array(strings)], schema=schema)
            writer.write_batch(record_batch)
            strings = []
    if strings:
        record_batch = pa.RecordBatch.from_arrays([pa.array(strings)], schema=schema)
        writer.write_batch(record_batch)

writer.close()