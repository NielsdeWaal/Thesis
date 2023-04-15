import capnp
import socket
from functools import reduce
import time

batch_capnp = capnp.load("./source/IngestionProtocol/proto-python.capnp")

port = 8080

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", port))

# msg = batch_capnp.IdRequest.new_message()
# tagset = msg.init('tagSet', 2)
# tagset[0].name = "hostname"
# tagset[0].value = "host_0"
# tagset[1].name = "rack"
# tagset[1].value = "6"
# msg.metric = "usage_user"

# client.send(msg.to_bytes())

# resp = batch_capnp.IdResponse.new_message()
# resp.setId = 11
# bytesMsg = batch_capnp.IdResponse.from_bytes(resp.to_bytes())
# print(bytesMsg.setId)

# response = client.recv(4096)

# print(f"{response}")

f = open('small-1.capfile', 'r+b')
nameSet = set()
nameMap = {}
for batch in batch_capnp.Batch.read_multiple(f):
    sendBatch = {}
    for msg in batch.recordings:
        name = f"{msg.metric},"
        # name += reduce(lambda acc, tag: acc + (tag.name + "=" + tag.value + ","), recording.tags)
        for tag in msg.tags:
            name += f"{tag.name}={tag.value},"
        for measurement in msg.measurements:
            name += f"{measurement.name}"
            if not name in sendBatch:
                sendBatch[name] = {"data": [], "request": batch_capnp.IdRequest.new_message()}
                if not name in nameMap:
                    tagSet = sendBatch[name]["request"].init('tagSet', len(msg.tags))

                    sendBatch[name]["request"].metric = measurement.name
                    for entry, tag in zip(tagSet, msg.tags):
                        entry.name = tag.name
                        entry.value = tag.value

                    client.send(sendBatch[name]["request"].to_bytes())
                    response = client.recv(4096)
                    with batch_capnp.IdResponse.from_bytes(response) as respMsg:
                        print(f"Received {respMsg.setId} as id, inserting into nameMap")
                        nameMap[name] = respMsg.setId
                    

            sendBatch[name]["data"].append({"ts": msg.timestamp,"val": measurement.value})
            name.removesuffix(measurement.name)


    # print(sendBatch)
    for k, v in sendBatch.items():
        insertMsg = batch_capnp.InsertionBatch.new_message()
        recording = insertMsg.init('recordings', 1) # TODO move out of loop as multple groups can be send in one request
        recording[0].tag = nameMap[k]
        values = recording[0].init('measurements', len(v["data"]))
        for val, measurement in zip(values, v["data"]):
            val.timestamp = measurement["ts"]
            val.value = measurement["val"]

        # print(len(insertMsg.to_bytes()))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        sock.sendto(insertMsg.to_bytes(), ("127.0.0.1", 1337))
        # time.sleep(1)

        # print(insertMsg)

        # for batch in v["data"]:
        #     id = nameMap[k]
    #     if k not in nameMap:
            # msg = batch_capnp.IdRequest.new_message()
            
            
    # exit(0)
                # nameSet.add(name)


print(nameSet)

        
    # for recording in batch.recordings:
    #     for measurement in recording.measurements:
    #         msg = batch_capnp.IdRequest.new_message()
    #         tagSet = msg.init('tagSet', len(recording.tags))
    #     # name = f"{recording.metric},"
    #     # name += reduce(lambda acc, tag: acc + (tag.name + "=" + tag.value + ","), recording.tags)
    #         for entry, tag in zip(tagSet, recording.tags):
    #             entry.name = tag.name
    #             entry.value = tag.value
    #         # name += f"{tag.name}={tag.value},"

    #         msg.metric = measurement.name
    #         client.send(msg.to_bytes())

    #         response = client.recv(4096)
    #         # print(response)
    #         with batch_capnp.IdResponse.from_bytes(response) as respMsg:
    #             print(f"Received {respMsg.setId} as id, creating batch")
    #             insertMsg = batch_capnp.InsertionBatch.new_message()
    #             msgs = insertMsg.init('recordings', 1)
    #             msgs[0].tag = respMsg.setId
    #             vals = msgs[0].init('measurements', 1)
    #             vals[0].timestamp = recording.timestamp
    #             vals[0].value = measurement.value

    #             print(insertMsg)
    #             # client.send(insertMsg.to_bytes())
    #             sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    #             sock.sendto(insertMsg.to_bytes(), ("127.0.0.1", 1337))
            # print(f"{name}{measurement.name}")
            
    # exit(0)

# resp = batch_capnp.IdResponse.from_bytes(response)
# with batch_capnp.IdResponse.from_bytes(response) as msg:
#     print(f"Received {msg.id} as id, creating batch")
# print(resp.setId)
# for k,v in temp_dict:
#     print(f"{k} -> {v}")
# print(f"Got response ID: {resp.id}")