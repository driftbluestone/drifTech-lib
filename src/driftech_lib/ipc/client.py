import asyncio
from .. import logging
from . import server
logger = logging.Logger("ipc/client")
__all__ = ["Client", "logger"]

class Client:
    """
    Inherit this class to overwrite the on_message() function.

    Run `await Client().start()` to initialize a connection.
    """
    def __init__(self, HOST, PORT):
        self.HOST = HOST
        self.PORT = PORT
        self.command_queue: asyncio.Queue[bytes] = asyncio.Queue(10)

    async def start(self):
        logger.info(f"Connecting to server at {self.HOST}:{self.PORT}...")
        
        # Establish the asynchronous socket connection
        reader, writer = await asyncio.open_connection(self.HOST, self.PORT)
        logger.info("Connected successfully!")
        
        # Start the background listening loop as a concurrent task
        listen_task = asyncio.create_task(self._listen(reader))
        
        # Start the foreground writing loop
        self.write_task = asyncio.create_task(self._send(writer))
        await self.write_task
        
        # Clean up tasks and close connection when exiting
        listen_task.cancel()
        writer.close()
        await writer.wait_closed()

    async def _listen(self, reader: asyncio.StreamReader):
        """Background task to continuously read any data pushed by the server."""
        try:
            while True:
                data = await reader.readline()
                if not data:
                    logger.error("[DISCONNECTED] Server closed the connection.")
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

    async def send_message(self, message: str | bytes):
        if isinstance(message, str):
            message = message.encode()
        await self.command_queue.put(message)

    async def _send(self, writer: asyncio.StreamWriter):
        try:
            while True:
                user_input: bytes = await self.command_queue.get()
                message = user_input.strip()
                
                if not message:
                    continue
                    
                if message.lower() == b"exit":
                    logger.info("Closing connection...")
                    break
                    
                writer.write(message)
                await writer.drain()
                
        except Exception as e:
            logger.error(f"Writing error: {e}")

class ConcurrentClient(server.ConcurrentServer):
    """
    Implementation of `Client` that connects to a ConcurrentServer.
    
    `CLIENT_PORT` must be different from `PORT` if the two are on the same computer.
    """
    def __init__(self, HOST = "127.0.0.1", PORT = 8000, CLIENT_PORT = 8001):
        super().__init__(HOST, CLIENT_PORT)
        self.CLIENT_PORT = PORT
        self.command_queue: asyncio.Queue[bytes]
    
    async def start(self):
        async with server._AsyncConnection(self.HOST, self.CLIENT_PORT) as (reader, writer):
            writer.write(f"connreq:{self.PORT}".encode())
            await writer.drain()
            logger.info(f"[CONNECTION] Connection to {self.HOST}:{self.CLIENT_PORT} opened")
        self.heartbeat = asyncio.create_task(self._heartbeat())
        await super().start()

    async def _interface(self, reader, writer):
        message = await reader.readline()
        if message == b"heartbeat\n":
            self.heartbeat.cancel()
            self.heartbeat = asyncio.create_task(self._heartbeat())
            writer.write(b"heartbeat\n")
            await writer.drain()
            return
        
        await self.on_message(message)

    async def _heartbeat(self):
        failed_attempts = 0
        while True:
            await asyncio.sleep(30 * (2 << failed_attempts))
            failed_attempts += 1
            logger.warn("Client did not recieve heartbeat. Retrying connection...")
            try:
                async with server._AsyncConnection(self.HOST, self.CLIENT_PORT) as (reader, writer):
                    writer.write(f"connreq:{self.PORT}".encode())
                    await writer.drain()
                    logger.info(f"[CONNECTION] Connection to {self.HOST}:{self.CLIENT_PORT} reopened")
            except ConnectionRefusedError:
                continue

    async def on_message(self, message):
        """Override this function."""
        pass

    async def send_message(self, message):
        if isinstance(message, str):
            message = message.encode()
        await self.command_queue.put(message)

    async def _send(self):
        while True:
            user_input = await self.command_queue.get()
            message = user_input.strip()
            
            if not message:
                continue
            
            async with server._AsyncConnection(self.HOST, self.CLIENT_PORT) as (reader, writer):
                writer.write(message)
                await writer.drain()

            if message.lower() == b"exit":
                logger.info(f"Closing connection for {self.HOST}:{self.CLIENT_PORT}")
                break
