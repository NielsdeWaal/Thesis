import capnp

batch_capnp = capnp.load("./source/IngestionProtocol/proto-python.capnp")

input_file = "./large"
chunk_size = 1000

output_file = open(input_file + ".capfile", 'w+b')

def write_batch(data): 
    batch = batch_capnp.Batch.new_message()
    messages = batch.init('recordings', len(data))
    for line, message in zip(data, messages):
        sections = line.split(" ")
        # recording = batch_capnp.Batch.Message.new_message()
        recording = message
        recording.metric = sections[0].split(',')[0]
        recording.timestamp = int(sections[-1])
        if int(sections[-1]) == 0: 
            exit(1)
        
        tags_string = sections[0].split(',')[1:]        
        tags = message.init('tags', len(tags_string))
        for tag, target in zip(tags_string, tags):
            k, v = tag.split('=')
            target.name = k
            target.value = v

        values_string = sections[1].split(',')
        measurements = message.init('measurements', len(values_string))
        for kv, target in zip(values_string, measurements):
            k, v = kv.split('=')
            target.name = k
            target.value = int(v[:-1])

    batch.write(output_file)

with open(input_file, 'r') as f:
    strings = []
    for line in f:
        strings.append(line.strip())
        if len(strings) == chunk_size:
            write_batch(strings)
            strings = []

    if strings:
        write_batch(strings)