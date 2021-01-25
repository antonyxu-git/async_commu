from utils import loads, dumps, send, recv, q_put_force
import queue
import threading
import asyncio
import warnings

import copy


class Sender(threading.Thread):
    def __init__(self, name, qsize=2, callback_fn=None, ip='127.0.0.1', port=8765):
        f = lambda : asyncio.run(self.loop())
        super().__init__(target=f, daemon=True)
        # self.q = queue.Queue(qsize)
        self.cond = threading.Condition()
        self.msg = b""
        
        self.name = name
        self.callback_fn = callback_fn
        self.ip = ip
        self.port = port
        self.daemon = True
        self.start()

    def add_data(self, data):
        msg = dumps(data)
        with self.cond:
            self.msg = msg
            self.cond.notify_all()
        # q_put_force(self.q, msg)

    async def loop(self):
        # =============== header ===============
        _, writer = await asyncio.open_connection(self.ip, self.port)
        header = {"ntype": ["sender"], "name": [self.name]}
        await send(writer, dumps(header))
        # =============== body ===============
        try:
            while 1:
                # msg = self.q.get()
                # print(1)
                with self.cond:
                    self.cond.wait()
                    msg = self.msg
                await send(writer, msg)
                    # msg = copy.copy(self.msg)
                
                if self.callback_fn is not None:
                    self.callback_fn(msg)
        finally:
            writer.close()


class Reciver(threading.Thread):
    def __init__(self, name, qsize=2, callback_fn=None, ip='127.0.0.1', port=8765):
        f = lambda : asyncio.run(self.loop())
        super().__init__(target=f, daemon=True)
        self.name = name
        self.qsize = qsize
        self.callback_fn = callback_fn
        if callback_fn is None:
            warnings.warn("callback_fn is None.")
        self.ip = ip
        self.port = port
        self.start()

    async def loop(self):
        # =============== header ===============
        reader, writer = await asyncio.open_connection(self.ip, self.port)
        header = {"ntype": ["reciver"], "name": [self.name], "qsize": [self.qsize]}
        await send(writer, dumps(header))
        # =============== body ===============
        try:
            while 1:
                msg = await recv(reader)
                data = loads(msg)
                if self.callback_fn is not None:
                    self.callback_fn(data)
        finally:
            writer.close()
        exit()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str, default="sender")
    args = parser.parse_args()

    import numpy as np
    import pygame
    import time
    if args.type == "sender":
        s = Sender("test")
        tick = pygame.time.Clock()
        for i in range(2000):
            data = {"16": np.eye(1024), "time": time.time()}
            s.add_data(data)
            print(i, tick.get_fps())
            tick.tick(30)
    elif args.type == "reciver":
        tick = pygame.time.Clock()
        def f(data):
            print(time.time() - data["time"], data["16"].shape, tick.get_fps())
            tick.tick()
        s = Reciver("test", callback_fn=f)
        # import time
        # for i in range(2000):
        #     time.sleep(1.)
        s.join()
            # print(i, tick.get_fps())
            
