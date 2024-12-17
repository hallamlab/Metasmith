# Read from and write to the named pipe
with open("mypipe", "w") as pipe:
    pipe.write("Hello, World!")
    