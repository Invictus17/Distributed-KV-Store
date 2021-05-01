import subprocess
import sys
import time
import os
import logging
from socket import *
import pickle
import shutil

logging.basicConfig(level=logging.INFO, format="DRIVER" + ": %(message)s")
MESSAGE_LEN = 512


def main(case, num_kv_processes, num_clients, ports, output_sleep):
    process = 'python kvstore.py'

    client = 'python client.py'

    list_kv = [""] * num_kv_processes

    all_ports_str = ""
    for port in ports:
        all_ports_str += " " + str(port)

    for index, port in enumerate(ports):
        list_kv[index] = process + " -p " + str(port) + " -a " + all_ports_str + " -i " + str(index)

    client_processes = []
    for i in range(1, num_clients + 1):
        client_processes.append(client + " -t " + str(case) + " -i " + str(i))

    processes = [subprocess.Popen(process, shell=True) for process in list_kv]

    # Give the processes some time to spawn
    time.sleep(2)

    # spawn clients & send messages
    host = "localhost"
    client_socket = None

    cl_processes = [subprocess.Popen(process, shell=True) for process in client_processes]

    time.sleep(output_sleep)

    try:
        for port in ports:
            client_socket = socket(AF_INET, SOCK_STREAM)
            client_socket.connect((host, port))
            client_socket.sendall(pickle.dumps({"type": "get_result"}))
            client_socket.close()

    except error as e:
        print("error:", e)
        client_socket.close()

    time.sleep(2)
    logging.info("You may terminate the program")

    for process in processes:
        process.wait()


if __name__ == "__main__":
    # PLEASE DEFINE YOUR CASE (refer the report or the comments below to choose one)
    case = 6

    dirs = ['logs']

    for dir in dirs:
        if os.path.exists("./logs"):
            for filename in os.listdir(dir):
                file_path = os.path.join(dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    continue

    ################################################################################################################
    # TOM
    messages, num_kv_processes, num_clients, ports, output_sleep = None, None, None, None, None
    if case == 1:
        num_kv_processes = 2
        num_clients = 2
        ports = [8081, 8082]
        output_sleep = 5

    elif case == 2:
        num_kv_processes = 2
        num_clients = 2
        ports = [8081, 8082]
        output_sleep = 7

    elif case == 3:
        num_kv_processes = 3
        num_clients = 2
        ports = [8081, 8082, 8083]
        output_sleep = 15
    elif case == 4:
        num_kv_processes = 3
        num_clients = 5
        ports = [8081, 8082, 8083]
        output_sleep = 15
    elif case == 5:
        num_kv_processes = 5
        num_clients = 11
        ports = [8081, 8082, 8083, 8084, 8085]
        output_sleep = 20
    elif case == 6:
        num_kv_processes = 10
        num_clients = 21
        ports = [8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089, 8090]
        output_sleep = 30

    main(case, num_kv_processes, num_clients, ports, output_sleep)
