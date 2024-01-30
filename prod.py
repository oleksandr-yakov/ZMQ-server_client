import time
import os
import logging
import sys

import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

SECURE = True
message_count = 0
max_messages = 200


def pre_run():
    base_dir = os.path.dirname(__file__)
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')

    if not (
            os.path.exists(public_keys_dir)
            and os.path.exists(secret_keys_dir)):
        logging.critical("Certificates are missing: run generate_certificates.py script first")
        sys.exit(1)

    context = zmq.Context()
    sock = context.socket(zmq.PUSH)

    ctx = zmq.Context()
    auth = ThreadAuthenticator(ctx)
    auth.start()
    auth.allow('127.0.0.1')         # white list ip
    auth.configure_curve(domain='*', location=public_keys_dir)
    zmq_socket = sock

    server_secret_file = os.path.join(secret_keys_dir, "server.key_secret")
    server_public_file = os.path.join(public_keys_dir, "server.key")
    server_public, server_secret = zmq.auth.load_certificate(server_secret_file)

    if SECURE:
        print("Secure transport")
        zmq_socket.curve_secretkey = server_secret
        zmq_socket.curve_publickey = server_public
        zmq_socket.curve_server = True

    ip = "127.0.0.1"                       # 127.0.0.1    172.31.6.31
    sock.bind("tcp://" + ip + ":7777")
    print("Producer connected to worker at " + ip + ":7777")
    return sock


def send_work(sock):
    global message_count
    if message_count >= max_messages:       # message_count >= max_messages:
        print("Finished sending 2 messages. Exiting.")
        sock.close()
        exit()
    print("Sending work")
    sock.send(b"some work")
    message_count += 1


if __name__ == "__main__":
    if zmq.zmq_version_info() < (4, 0):
        raise RuntimeError(
            "Security is not supported in libzmq version < 4.0. libzmq version {}".format(
                zmq.zmq_version()
            )
        )

    if '-v' in sys.argv:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")
    sock = pre_run()
    while True:
        send_work(sock)
        time.sleep(1)




