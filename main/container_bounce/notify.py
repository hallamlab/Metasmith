import socket

def notify_background_process(port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.sendto(message.encode(), ('localhost', port))

if __name__ == "__main__":
    port = 12345
    message = "Hello from the main script!"
    notify_background_process(port, message)
