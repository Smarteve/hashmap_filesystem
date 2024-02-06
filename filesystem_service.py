import socket
from queue import Queue
import threading

from keyvaluestore import DB

HASHMAP_DB = DB("filesystem.db", 2, overwrite=True)
host = "localhost"
port = 9000
client_list = []


def handle_client(conn):
    try:
        while True:
            data = conn.recv(1024).decode().strip()
            print(data)
            parts = data.split()
            print(parts)
            command = parts[0].upper()
            if command == "SET" and len(parts) == 3:
                HASHMAP_DB[parts[1]] = parts[2]
                conn.send(b"set key value succesfully\n")
            elif command == "GET" and len(parts) == 2:
                value = HASHMAP_DB[parts[1]]
                print(value)
                if value:
                    conn.send(f"{value}\n".encode())
                else:
                    conn.send(b"key not found\n")
            else:
                conn.send(b"invald command\n")
    except Exception as e:
        print("An Error occured")
        remove_conn(conn)


def remove_conn(conn):
    if conn in client_list:
        client_list.remove(conn)


thread_queue = Queue()
if __name__ == "__main__":

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen()
    print("server is listening on port", port)

    while True:
        try:
            conn, address = s.accept()
            client_list.append(conn)
            print(address[0], "connected")
            conn.send(b"connected to server\n")
            t = threading.Thread(target=handle_client, args=(conn,))
            t.start()
            thread_queue.put(t)
        except KeyboardInterrupt:
            pass
        finally:
            while not thread_queue.empty():
                thread = thread_queue.get()
                thread.join()
