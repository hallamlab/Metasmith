import asyncio

async def handle_notification(reader, writer):
    data = await reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print(f"Received notification from {addr}: {message}")
    writer.close()

async def main():
    server = await asyncio.start_server(handle_notification, 'localhost', 12345)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
    