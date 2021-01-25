import io
import asyncio
import scipy.io as scio
import numpy as np
from queue import Queue, Empty
from collections import defaultdict
from utils import *
import base64
import os

"""

"""
# 优化：建立多个连接传输

topic_dict = defaultdict(dict)

async def handle_topic(reader, writer):
    addr = writer.get_extra_info('peername')

    print(f"Received header from {addr!r}")
    msg = await reader.readexactly(8)
    msg = await reader.readexactly(int.from_bytes(msg, 'big'))
    header = loads(msg)
    ttype, topic_name, queue_size = header["type"][0], header["topic_name"][0], header["queue_size"][0]

    print(f"Received: message")
    if ttype == "sender":
        
        while 1:
            msg = await reader.readexactly(8)
            msg = await reader.readexactly(int.from_bytes(msg, 'big'))
            for q in topic_dict[topic_name].values():
                await q_put(q, msg)
            data = loads(msg)
    elif ttype == "reciver":
        key = base64.b64encode(os.urandom(16))
        topic_dict[topic_name][key] = asyncio.Queue(queue_size)
        while 1:
            msg = await topic_dict[topic_name][key].get()
            writer.write(len(msg).to_bytes(8, 'big'))
            await writer.drain()
            writer.write(msg)
            await writer.drain()
        del topic_dict[topic_name][key]
    else:
        print("NotExpect {}!".format(ttype))
        # print(len(msg), topic_dict[topic_name].qsize())

    print("Close the connection")
    writer.close()


async def main():
    server = await asyncio.start_server(handle_topic, '127.0.0.1', 8765)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
