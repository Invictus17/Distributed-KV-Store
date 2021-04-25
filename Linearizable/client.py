import argparse
import json
import logging
import pickle
from socket import *

MESSAGE_LEN = 512

def tcp_connect(message, port):
    host = 'localhost'
    client_socket = socket(AF_INET, SOCK_STREAM)
    try:
        client_socket.connect((host, port))
        client_socket.sendall(pickle.dumps(message))
        logging.info("Request: {}".format(message))
        response = client_socket.recv(MESSAGE_LEN).decode()
        logging.info("Response: {}".format(response))
        client_socket.close()
    except error as e:
        print("error:", e)
        client_socket.close()


def main(testcase, id):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('./logs/Client_' + str(id))
    fh.setLevel(logging.INFO)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s' + " Client_" + str(id) + ": %(message)s",
                                  datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)


    with open("./test_case.json") as f:
        test_json = json.load(f)

    for i in test_json[str(testcase)][str(id)]:
        tcp_connect(i['message'], i['port'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--testcase', type=int, default=None,
                        help='Enter test case')
    parser.add_argument('-i', '--id', type=int,
                        help='Enter client id')
    request_list = parser.parse_args()

    main(request_list.testcase, request_list.id)