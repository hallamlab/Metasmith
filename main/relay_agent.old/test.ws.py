import socketio
import time
import socket
# Create a Socket.IO client instance
sio = socketio.Client()

# Define event handlers using decorators
@sio.event
def connect():
    print('Connection established')
    # Emit a message to the server upon connection
    sio.emit('my response', {'response': 'my response'})

@sio.event
def echo(data):
    print(f"echo {data}", end="\r")

@sio.event
def disconnect():
    print()
    print('Disconnected from server')

# Connect to the Socket.IO server
# Replace 'http://localhost:5000' with your server's address
sio.connect(f'ws://localhost:12001', transports=['websocket'])
# sio.connect(f'ws://localhost:12001')

# sio.emit("echo", dict(test="asdf"))
for i in range(100):
    sio.emit("echo", dict(test=f"a{i}"))
    sio.sleep(0)
    time.sleep(0.1)
sio.sleep(1)
sio.disconnect()