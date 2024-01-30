import zmq
context = zmq.Context()
sock = context.socket(zmq.PULL)

workerIP = "127.0.0.1"           # 127.0.0.1  3.69.231.248
connectResult = sock.connect("tcp://" + workerIP + ":7777")
print("Worker-Testing without key listen  producer")
while True:
    message = sock.recv()
    print("Received work:", message.decode())
