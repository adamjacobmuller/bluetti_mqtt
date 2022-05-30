import asyncio
import logging
from typing import Callable, List, Union
from bluetti_mqtt.commands import DeviceCommand
from bluetti_mqtt.device import BluettiDevice
from bluetti_mqtt.parser import DataParser

class ParserMessage:
    def __init__(self, device: BluettiDevice, parser: DataParser):
        self.device = device
        self.parser = parser

class CommandMessage:
    def __init__(self, device: BluettiDevice, command: DeviceCommand):
        self.device = device
        self.command = command

class EventBus:
    parser_listeners: List[Callable[[ParserMessage], None]]
    command_listeners: List[Callable[[CommandMessage], None]]
    queue: asyncio.Queue

    def __init__(self):
        self.parser_listeners = []
        self.command_listeners = []
        self.queue = None

    def add_parser_listener(self, cb: Callable[[ParserMessage], None]):
        self.parser_listeners.append(cb)

    def add_command_listener(self, cb: Callable[[CommandMessage], None]):
        self.command_listeners.append(cb)

    async def put(self, msg: Union[ParserMessage, CommandMessage]):
        if not self.queue:
            self.queue = asyncio.Queue()

        await self.queue.put(msg)

    """Reads messages and notifies listeners"""
    async def run(self):
        if not self.queue:
            self.queue = asyncio.Queue()

        while True:
            msg = await self.queue.get()
            logging.debug(f'queue size: {self.queue.qsize()}')
            if isinstance(msg, ParserMessage):
                await asyncio.gather(*[l(msg) for l in self.parser_listeners])
            elif isinstance(msg, CommandMessage):
                await asyncio.gather(*[l(msg) for l in self.command_listeners])
            self.queue.task_done()
