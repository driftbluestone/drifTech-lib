import asyncio
from .. import logging
logger = logging.Logger("ipc/server")
__all__ = ["Server", "ConcurrentServer", "logger"]

class Server:
    """
    Inherit this class to overwrite the on_message() function.

    This class opens a new port whenever a new connection is
    created, use `ConcurrentServer` for same-port communication.

    Run `await Server().start()` to initialize a server.
    Do not apply class attributes when initializing,
    they are only used for the connections,
    which can be accessed within the `Server.connections` dict.
    """
    def __init__(self, addr: str = None, port: int = None,
                 reader: asyncio.StreamReader = None,
                 writer: asyncio.StreamWriter = None):
        
        self.addr = addr
        self.port = port
        self.reader = reader
        self.writer = writer
        self.command_queue: asyncio.Queue[bytes] = asyncio.Queue(10)
        self.conn_count = 1
        self.connections: dict[int, Server] = {}

    async def start(self, HOST = "127.0.0.1", PORT = 8000):
        server = await asyncio.start_server(self._interface, HOST, PORT)
        logger.info(f"[LISTENING] Server is running on {HOST}:{PORT}")
        async with server:
            await server.serve_forever()

    async def _interface(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr, port = writer.get_extra_info("peername")
        logger.info(f"[CONNECTION] Connection opened for {addr}:{port} #{self.conn_count}")
        self.conn_count += 1
        try:
            conn = self.__class__(addr, port, reader, writer)
            self.connections[port] = conn
            await conn._run()
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
            pass
        self.conn_count -= 1
        logger.info(f"[DISCONNECTED] Connection closed for {addr}:{port} #{self.conn_count-1}")

    async def _run(self):
        listen_task = asyncio.create_task(self._listen())

        self.write_task = asyncio.create_task(self._send())
        await self.write_task

        listen_task.cancel()
        self.writer.close()
        await self.writer.wait_closed()

    async def _listen(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    self.write_task.cancel()
                    break
                await self.on_message(data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[ERROR] Reading error: {e}")

    async def on_message(self, message: bytes):
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

class ConcurrentServer():
    """Implementation of `Server` that supports multiple connections on the same port."""
    def __init__(self, HOST: str = "127.0.0.1", PORT: int = 8000):
        self.HOST = HOST
        self.PORT = PORT
        self.connections: dict[str, asyncio.Task] = {}
        self.command_queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(10)

    async def start(self):
        asyncio.create_task(self._send())
        await Server.start(self, self.HOST, self.PORT)

    async def _interface(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr, _ = writer.get_extra_info("peername")
        message = await reader.readline()
        message = message.split(b" ", 1)
        
        port = int(message[0])
        conn = f"{addr}:{port}"
        if self.connections.get(f"{addr}:{port}") is None:
            logger.info(f"[CONNECTION] Connection opened for {conn}")
            heartbeat = asyncio.create_task(self._heartbeat(conn))
            self.connections[conn] = heartbeat
        if len(message) != 1:
            message = message[1].strip()
            if (message == b"exit"):
                await self._close(conn)
            else:
                asyncio.create_task(self.on_message(conn, message))
        writer.close()
        await writer.wait_closed()

    async def send_message(self, target: str, message: bytes):
        if isinstance(message, str):
            message = message.encode()
        if not message.endswith(b"\n"):
            message += b"\n"
        await self.command_queue.put((target, message))

    async def _close(self, conn: str):
        self.connections[conn].cancel()
        self.connections.pop(conn, None)
        logger.info(f"[DISCONNECTED] Connection closed for {conn}")
    
    async def _heartbeat(self, conn: str):
        connection = (conn.split(":")[0], int(conn.split(":")[1]))
        try:
            while True:
                await asyncio.sleep(30)
                async with _AsyncConnection(*connection) as (reader, writer):
                    writer.write(b"heartbeat\n")
                    await writer.drain()
                    try:
                        hb = await asyncio.wait_for(reader.readline(), 30)
                        if hb != b"heartbeat\n":
                            break
                    except asyncio.TimeoutError:
                        break
            await self._close(conn)
        except:
            await self._close(conn)

    async def on_message(self, conn: str, message: bytes):
        """Override this function."""
        pass

    async def _send(self):
        while True:
            user_input: tuple[str, bytes] = await self.command_queue.get()
            message = user_input[1]
            conn = user_input[0]
            connection = (conn.split(":")[0], int(conn.split(":")[1]))

            if not message:
                continue
            
            if message.lower().strip() == b"exit":
                logger.info(f"Closing connection for {conn}")
                await self._close(conn)
                break

            async with _AsyncConnection(*connection) as (reader, writer):
                writer.write(message)
                await writer.drain()

class _AsyncConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.writer = None

    async def __aenter__(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        return self.reader, self.writer

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
