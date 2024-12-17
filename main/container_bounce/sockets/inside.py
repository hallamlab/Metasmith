import asyncio
from datetime import datetime as dt

def Timestamp(timestamp: dt|None = None):
    ts = dt.now() if timestamp is None else timestamp
    FORMAT = '%Y-%m-%d_%H-%M-%S'
    return f"{ts.strftime(FORMAT)}"

hist = []
async def handle(reader, writer):
    data = await reader.read(255)
    message = data.decode()

    hist.append((f"{Timestamp()}:{message}")) 
    for m in hist+["END"]:
        writer.write(m.encode())
        await writer.drain()

    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(
        handle, '0.0.0.0', 56101
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
