from collections import defaultdict
from utils import loads, dumps, send, recv, q_put_force
import asyncio
import argparse
import logging


# 可能的优化方向：
#     建立多个连接传输


class Master():
    def __init__(self, args):
        self.args = args
        self.topic = defaultdict(dict)
        self.log = logging.Logger("Master")
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(args.log_level)

    async def sender(self, reader, name):
        """ 从节点接收数据，然后立刻分发给topic中的各个接收队列。 """
        while 1:
            msg = await recv(reader)
            for q in self.topic[name].values():
                q_put_force(q, msg)  # 分发数据

    async def reciver(self, writer, name, qsize):
        """ 对应name的topic中加入一个接收队列，然后不断阻塞地从队列里得到数据发给节点，结束后需要删掉这个队列 """
        import secrets
        key = secrets.token_bytes(8)  # 创建一个独特的key
        self.topic[name][key] = asyncio.Queue(qsize)
        # print(self.topic[name])
        try:
            while 1:
                msg = await self.topic[name][key].get()
                await send(writer, msg)
        finally:
            del self.topic[name][key]

    async def on_connection(self, reader, writer):
        """
        header:
          ntype: [server/client, sender/reciver, action_server/action_client]
          name: [server_name, topic_name, action_name]
          qsize: queue's maxsize
        """
        # ======== Header ========
        addr = writer.get_extra_info('peername')
        # msg = await reader.readexactly(8)
        # msg = await reader.readexactly(int.from_bytes(msg, 'big'))
        msg = await recv(reader)
        header = loads(msg)
        ntype, name = header["ntype"][0], header["name"][0]
        self.log.info(f"[Info] ========  {addr!r} ========")
        self.log.info(f"[Info] ntype: {ntype!r}")
        self.log.info(f"[Info] name : {name!r}")
        self.log.info(f"[Info]")
        assert ntype in ["server", "client", "sender", "reciver", "action_server", "action_client"]

        # ======== Handle ========
        try:
            if ntype == "sender":
                await self.sender(reader, name)
            elif ntype == "reciver":
                qsize = header["qsize"][0][0]
                await self.reciver(writer, name, qsize)
        finally:
            self.log.info(f"[Info] Close the connection {addr!r}")
            writer.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--log_level", type=int, default=20)  # https://docs.python.org/3.8/library/logging.html#levels
    args = parser.parse_args()

    master = Master(args)
    server = await asyncio.start_server(master.on_connection, args.ip, args.port)
    addr = server.sockets[0].getsockname()
    master.log.info(f'[Info]')
    master.log.info(f'[Info] Serving on {addr}')
    master.log.info(f'[Info]')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
