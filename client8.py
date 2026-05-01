"""
CECS 327 - Assignment 8 Client
"""

import socket

QUERIES = {
    "1": "What is the average moisture inside our kitchen fridges in the past hour, week and month?",
    "2": "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?",
    "3": "Which house consumed more electricity in the past 24 hours, and by how much?",
}

def print_menu():
    print("\n" + "=" * 67)
    print("  Smart House IoT Query System")
    print("=" * 67)
    for num, query in QUERIES.items():
        print("  [" + num + "] " + query)
    print("  [q]  Quit")
    print("=" * 67)

def main():
    server_ip   = input("Enter IP address of server: ").strip()
    server_port = int(input("Enter port number: ").strip()) #enter 5044

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Try to connect to socket, if server cant be connected to, return error
    try:
        sock.connect((server_ip, server_port))
        print("\n  Connected to server at " + server_ip + ":" + str(server_port))
    except ConnectionRefusedError:
        print("  Could not connect to " + server_ip + ":" + str(server_port) + ". Is the server running?")
        return

    while True:
        print_menu()
        choice = input("\n  Your choice: ").strip()

        if choice == "q":
            print("\n  Ending connection. Goodbye!")
            break

        if choice in QUERIES:
            query = QUERIES[choice]
            print("\n  Sending: " + query)
            print("  Please wait...\n")
            try:
                sock.send(bytearray(query, encoding="utf-8"))
                response = b""
                while True:
                    chunk = sock.recv(4096)
                    response += chunk
                    if len(chunk) < 4096:
                        break
                print("=" * 67)
                print(response.decode("utf-8"))
                print("=" * 67)
            except Exception as e:
                print("  Error: " + str(e))
                break
        else:
            print("\n  Sorry, this query cannot be processed.")
            print("  Try a supported query.")

    sock.close()

if __name__ == "__main__":
    main()