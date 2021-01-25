import io
import scipy.io as scio
# import zlib as zip
import gzip as zip
import lz4.frame as zip


def dumps(obj):
    with io.BytesIO() as f:
        scio.savemat(f, obj)
        msg = f.getvalue()
    msg = zip.compress(msg, 1)
    return msg


def loads(msg):
    msg = zip.decompress(msg)
    with io.BytesIO(msg) as f:
        obj = scio.loadmat(f)
    return obj


async def send(writer, msg):
    writer.write(len(msg).to_bytes(8, 'big'))
    await writer.drain()
    writer.write(msg)
    await writer.drain()


async def recv(reader):
    msg = await reader.readexactly(8)
    msg = await reader.readexactly(int.from_bytes(msg, 'big'))
    return msg


def q_put_force(q, msg):
    try:
        if q.full():
            q.get_nowait()
        q.put_nowait(msg)
    finally:
        pass
