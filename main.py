import io
import asyncio
import scipy.io as scio
import numpy as np
import pygame
from utils import *
import argparse


async def sender_client(topic, q_size=2):
    tick = pygame.time.Clock()
    reader, writer = await asyncio.open_connection('127.0.0.1', 8765)

    # =============== header ===============
    print(f'Send: header')
    header = {"type": ["sender"], "topic_name": [topic], "queue_size": [q_size]}
    msg = dumps(header)
    writer.write(len(msg).to_bytes(8, 'big'))
    await writer.drain()
    writer.write(msg)
    await writer.drain()

    # =============== body ===============
    print(f'Send: data')
    for i in range(30000):
        data = {"16": np.eye(1024)}
        msg = dumps(data)

        writer.write(len(msg).to_bytes(8, 'big'))
        await writer.drain()
        writer.write(msg)
        await writer.drain()

        print(i, tick.get_fps())
        tick.tick(30)

    # =============== close ===============
    print('Close the connection')
    writer.close()


async def recver_client(topic, q_size=2):
    tick = pygame.time.Clock()
    reader, writer = await asyncio.open_connection('127.0.0.1', 8765)

    # =============== header ===============
    print(f'Send: header')
    header = {"type": ["reciver"], "topic_name": [topic], "queue_size": [q_size]}
    msg = dumps(header)
    writer.write(len(msg).to_bytes(8, 'big'))
    await writer.drain()
    writer.write(msg)
    await writer.drain()

    # =============== body ===============
    print(f'Recv: data')
    while 1:
        msg = await reader.readexactly(8)
        msg = await reader.readexactly(int.from_bytes(msg, 'big'))
        data = loads(msg)
        print(data["16"].shape, tick.get_fps())
        tick.tick()

    # =============== close ===============
    print('Close the connection')
    writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str, default="sender")
    args = parser.parse_args()

    if args.type == "sender":
        asyncio.run(sender_client('Hello_World'))
    elif args.type == "reciver":
        asyncio.run(recver_client('Hello_World'))

