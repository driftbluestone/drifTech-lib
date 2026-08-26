import asyncio, logging
__all__ = ["Connector"]

class Connector:
    """
    Inherit this class to overwrite the on_message() function

    run await Connector().start() to connect to DCM-core
    """
    def __init__(self, HOST, PORT):
        self.HOST = HOST
        self.PORT = PORT
        self.command_queue: asyncio.Queue[bytes] = asyncio.Queue(10)

    async def start(self):
        logging.info(f"Connecting to server at {self.HOST}:{self.PORT}...")
        
        # Establish the asynchronous socket connection
        reader, writer = await asyncio.open_connection(self.HOST, self.PORT)
        logging.info("Connected successfully!")
        
        # Start the background listening loop as a concurrent task
        listen_task = asyncio.create_task(self._listen(reader))
        
        # Start the foreground writing loop
        await self._send(writer)
        
        # Clean up tasks and close connection when exiting
        listen_task.cancel()
        writer.close()
        await writer.wait_closed()

    async def _listen(self, reader: asyncio.StreamReader):
        """Background task to continuously read any data pushed by the server."""
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    logging.error("[DISCONNECTED] Server closed the connection.")
                    break
                await self.on_message(data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"[ERROR] Reading error: {e}")

    async def on_message(self, message: bytes):
        """
        Override this function
        """
        pass

    async def send_message(self, message: str | bytes):
        if isinstance(message, str):
            message = message.encode()
        await self.command_queue.put(message)

    async def _send(self, writer: asyncio.StreamWriter):
        loop = asyncio.get_running_loop()
        try:
            while True:
                user_input: bytes = await self.command_queue.get()
                message = user_input.strip()
                
                if not message:
                    continue
                    
                if message.lower() == b"exit":
                    logging.info("Closing connection...")
                    break
                    
                writer.write(message)
                await writer.drain()
                
        except Exception as e:
            logging.error(f"Writing error: {e}")
    