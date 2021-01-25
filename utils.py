import io
import scipy.io as scio


def dumps(obj):
    with io.BytesIO() as f:
        scio.savemat(f, obj)
        msg = f.getvalue()
    return msg

def loads(msg):
    with io.BytesIO(msg) as f:
        obj = scio.loadmat(f)
    return obj

async def q_put(q, data):
    try:
        if q.full():
            q.get_nowait()
        q.put_nowait(data)
    finally:
        pass
