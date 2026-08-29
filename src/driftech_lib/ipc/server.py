import asyncio, logging
logger = logging.getLogger("ipc:server")
__all__ = ["start"]

open_connections = 1

async def start(HOST = "127.0.0.1", PORT = 8000):
    server = await asyncio.start_server(interface, HOST, PORT)
    logger.info(f"[LISTENING] Server is running on {HOST}:{PORT}")
    async with server:
        await server.serve_forever()

async def interface(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global open_connections
    addr = writer.get_extra_info("peername")
    logger.info(f"[CONNECTION] Connection opened for {addr} #{open_connections}")
    open_connections += 1
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            logger.info(f"[{addr}] Received: {data.decode()}")
            writer.write(b"Message processed")
            await writer.drain()  # Ensure data is flushed to the network buffer
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        open_connections -= 1
        logger.info(f"[DISCONNECTED] Connection closed for {addr} #{open_connections-1}")
        writer.close()
        await writer.wait_closed()
    finally:
        open_connections -= 1
        logger.info(f"[DISCONNECTED] Connection closed for {addr} #{open_connections-1}")
        writer.close()
        await writer.wait_closed()
