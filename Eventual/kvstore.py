import subprocess
import sys
import argparse
from socket import *
import logging
import pickle
import heapq
import time
import threading
import os

MESSAGE_LEN = 512


class server:
    def __init__(self):
        self.lock = threading.Lock()
        self.Q = []
        heapq.heapify(self.Q)
        self.lamport_clock = 0
        self.message_ACKs = {}
        self.final = []
        self.keep_alive = True

        self.dict_key_val = {}

    def tcp_send(self, message, port):
        client_socket = socket(AF_INET, SOCK_STREAM)

        try:
            client_socket.connect(("localhost", port))
            client_socket.sendall(pickle.dumps(message))
            client_socket.close()
        except error as e:
            client_socket.close()

    def tcp_thread_work(self, connect_socket, address, my_port, ports, id):
        host = "localhost"
        message = None

        try:
            message = connect_socket.recv(MESSAGE_LEN)
            message = pickle.loads(message)

            # For the purpose of testing the application
            if message['type'] == 'get_result':
                logging.info("Store key length, data: {} {}".format(len(self.dict_key_val), self.dict_key_val))
                logging.info("FINAL ORDER: {}".format(self.final))
                return

            logging.info("Message: {} from: {}".format(message, address))

            # Client request a key
            # {"type": "GET", 'key': "m1", "delay": 0}
            if message['type'] == 'GET':
                key = message['key']
                time.sleep(message['delay'])
                if self.dict_key_val.get(key):
                    logging.info("Key found")
                    connect_socket.sendall(str.encode(self.dict_key_val[key]))
                else:
                    logging.info("Key not found")
                    connect_socket.sendall(str.encode("Key not found"))

            # client requests to store a key
            # {"type": "STORE", 'key': "m1", "value": 0}
            elif message['type'] == "STORE":
                tom_message = {}

                publish_LC = None
                time.sleep(message['delay'])
                self.lock.acquire()
                self.lamport_clock += 1
                publish_LC = self.lamport_clock
                logging.info("LAMPORT: {}".format(self.lamport_clock))
                self.message_ACKs[message['key'] + "_" + message['value']] = []
                self.lock.release()

                tom_message['lamport'] = (publish_LC, id)
                tom_message['type'] = 'TOM'
                tom_message['key'] = message['key']
                tom_message['value'] = message['value']
                # Broadcast this message
                for port in ports:
                    if port != my_port:
                        logging.info("TOM Message to be issued: {} to: {}".format(tom_message, (host, port)))
                        self.tcp_send(tom_message, port)

                self.lock.acquire()
                heapq.heappush(self.Q, (
                tom_message['lamport'][0], tom_message['lamport'][1], tom_message['key'], tom_message['value']))
                self.lock.release()


            connect_socket.close()
        except error:
            connect_socket.close()

    def main(self, my_port, ports, id):
        # citing: https://docs.python.org/2/howto/logging-cookbook.html#multiple-handlers-and-formatters
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # create file handler which logs even debug messages
        fh = logging.FileHandler('./logs/Process_' + str(id))
        fh.setLevel(logging.INFO)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s' + " Process_" + str(id) + ": %(message)s",
                                      datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        # add the handlers to logger
        logger.addHandler(ch)
        logger.addHandler(fh)

        host = 'localhost'
        server_socket = socket(AF_INET, SOCK_STREAM)
        server_socket.bind((host, my_port))
        server_socket.listen(10)

        while True:
            try:
                connect_socket, address = server_socket.accept()

                sock_thread = threading.Thread(target=self.tcp_thread_work,
                                               args=(connect_socket, address, my_port, ports, id), daemon=True)
                sock_thread.start()
            except error:
                server_socket.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=None,
                        help='Enter port number for the key-value server')
    parser.add_argument('-a', '--all', nargs='+', type=int,
                        help='Enter port number of the primary server')
    parser.add_argument('-i', '--id', type=int, help='Server id')
    request_list = parser.parse_args()

    obj = server()
    obj.main(request_list.port, request_list.all, request_list.id)
