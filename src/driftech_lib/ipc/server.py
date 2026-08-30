import asyncio, logging, sys
logger = logging.getLogger("ipc:server")
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
__all__ = ["Server", "connections"]
Port = int

conn_count = 1
connections: dict[Port, Server] = {}

class Server:
    """
    Inherit this class to overwrite the on_message() function.

    Run `await Server().start()` to initialize a server.
    Do not apply class attributes when initializing,
    they are only used for the connections,
    which can be accessed within the `connections` dict.
    """
    def __init__(self, addr: str = None, port: int = None,
                 reader: asyncio.StreamReader = None,
                 writer: asyncio.StreamWriter = None):
        
        self.addr = addr
        self.port = port
        self.reader = reader
        self.writer = writer
        self.command_queue: asyncio.Queue[bytes] = asyncio.Queue(10)

    async def start(self, HOST = "127.0.0.1", PORT = 8000):
        server = await asyncio.start_server(self._interface, HOST, PORT)
        logger.info(f"[LISTENING] Server is running on {HOST}:{PORT}")
        async with server:
            await server.serve_forever()

    async def _interface(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        global conn_count
        addr, port = writer.get_extra_info("peername")
        logger.info(f"[CONNECTION] Connection opened for {addr} #{conn_count}")
        conn_count += 1
        try:
            conn = self.__class__(addr, port, reader, writer)
            connections[port] = conn
            await conn._run()
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
            conn_count -= 1
            logger.info(f"[DISCONNECTED] Connection closed for {addr} #{conn_count-1}")
        finally:
            conn_count -= 1
            logger.info(f"[DISCONNECTED] Connection closed for {addr} #{conn_count-1}")

    async def _run(self):
        listen_task = asyncio.create_task(self._listen())

        await self._send()

        listen_task.cancel()
        self.writer.close()
        await self.writer.wait_closed()

    async def _listen(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                await self.on_message(data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[ERROR] Reading error: {e}")

    async def on_message(self):
        """Override this function."""
        pass

    async def send_message(self, message: bytes):
        if isinstance(message, str):
            message = message.encode()
        await self.command_queue.put(message)

    async def _send(self):
        try:
            while True:
                user_input: bytes = await self.command_queue.get()
                message = user_input.strip()
                
                if not message:
                    continue
                    
                if message.lower() == b"exit":
                    logger.info("Closing connection...")
                    break
                    
                self.writer.write(message)
                await self.writer.drain()
                
        except Exception as e:
            logger.error(f"Writing error: {e}")

asyncio.run(Server().start())