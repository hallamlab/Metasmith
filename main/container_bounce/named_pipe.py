import os

# Create a named pipe
os.mkfifo("mypipe")

# Read from and write to the named pipe
with open("mypipe", "r") as pipe:
    data = pipe.read()
    print("Received:", data)
    
os.remove("mypipe")
