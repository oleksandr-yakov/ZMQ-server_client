import os, logging, sys
import zmq.auth

SECURE = True
TIME_WAIT = 1500
TIME_WAIT_2 = 10000

def pre_run():
    base_dir = os.path.dirname(__file__)
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')
    if not (
        os.path.exists(public_keys_dir)
        and os.path.exists(secret_keys_dir)
    ):
        logging.critical(
            "Certificates are missing: run generate_certificates.py script first"
        )
        sys.exit(1)
    context = zmq.Context()
    sock = context.socket(zmq.PULL)
    producer_ip = "127.0.0.1"            # 127.0.0.1  3.69.231.248

    if SECURE:
        zmq_socket = sock
        print("Secure transport")
        client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        zmq_socket.curve_secretkey = client_secret
        zmq_socket.curve_publickey = client_public

        server_public_file = os.path.join(public_keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        # The client must know the server's public key to make a CURVE connection.
        zmq_socket.curve_serverkey = server_public

    connect_result = sock.connect("tcp://" + producer_ip + ":7777")
    return sock, producer_ip


def foo_dynamic(sock, producer_ip):
    if sock.poll(TIME_WAIT):        # 1
        while True:
            message = sock.recv()
            if message.decode() == "some work":
                logging.info("Connection test OK")
                print("Received work:", message.decode())
    else:
        logging.error("Connection test FAIL")
        print("Worker-Frankfurt NOT connected to producer at {}:7777 \nConnection time out of {} sec".format(producer_ip, TIME_WAIT))


def foo_static(sock, producer_ip ):
    while sock.poll(TIME_WAIT):
        message = sock.recv()
        if message.decode() == "some work":
            logging.info("Connection test OK")
            print("Received work:", message.decode())
    else:   # (2)
        logging.error("Connection test FAIL")
        print("Worker-Frankfurt NOT connected to producer at {}:7777 \nConnection time out of {} sec".format(producer_ip, TIME_WAIT))


def foo_combine(sock, producer_ip ):
    logging.info("Trying to connect to producer at {}:7777 ".format(producer_ip))
    while sock.poll(TIME_WAIT):
        message = sock.recv()
        if message.decode() == "some work":
            print("Received work:", message.decode())
    else:   # (2)
        logging.error(
            "Connection test FAIL \nWorker-Frankfurt  cant connect to producer at {}:7777 \nConnection time out of {} ms".format(
                producer_ip, TIME_WAIT))
        if sock.poll(TIME_WAIT_2):
            logging.info("Connection test OK 2nd time")# 1
            while True:
                message = sock.recv()
                if message.decode() == "some work":
                    print("Received work:", message.decode())
        else:
            logging.error("Connection test FAIL \nWorker-Frankfurt SECOND TIME cant connect to producer at {}:7777 \nConnection time out of {} ms".format(
                producer_ip, TIME_WAIT_2))


if __name__ == "__main__":
    sock, producer_ip = pre_run()
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
    #foo_static(sock, producer_ip)
    #foo_dynamic(sock, producer_ip)
    foo_combine(sock, producer_ip)


# є варіант вкинуть (1) в else (2) щоб мати додаткову перевірку з'єднання коли змінилась
# кількість воркерів 1 перевірка йде через while - міняється кількість - падає статична умова  - while sock.poll(5000):
# заходив в if sock.poll чекаєм додатково вказаний час і використовуємо динамічно але без можливості обірвати зв'язок коли
# наприклад впаде producer - ми будемо все одно висіти й слухати його сокет


