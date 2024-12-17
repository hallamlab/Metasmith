import asyncio
import sys

m = sys.argv[-1]

async def tcp_echo_client(message):
    reader, writer = await asyncio.open_connection(
        'localhost', 56100
    )

    writer.write(message.encode())
    await writer.drain()

    for _ in range(10):
        data = await reader.read(255)
        res = data.decode()
        print(f'> {res}')
        if res == "END": break

    writer.close()
    await writer.wait_closed()

asyncio.run(tcp_echo_client(m))
