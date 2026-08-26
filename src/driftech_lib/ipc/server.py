import asyncio, logging
__all__ = ["start"]

async def start(HOST, PORT):
    server = await asyncio.start_server(interface, HOST, PORT)
    logging.info(f"[LISTENING] Server is running on {HOST}:{PORT}")
    async with server:
        await server.serve_forever()

async def interface(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    logging.info(f"[CONNECTION]: Connection opened for {addr}")
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            logging.info(f"[{addr}] Received: {data.decode()}")
            writer.write(b"Message processed")
            await writer.drain()  # Ensure data is flushed to the network buffer
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        logging.info(f"[DISCONNECTED] Connection closed for {addr}")
        writer.close()
        await writer.wait_closed()
    finally:
        logging.info(f"[DISCONNECTED] Connection closed for {addr}")
        writer.close()
        await writer.wait_closed()
