import os
import json
import aiofiles


async def read_file(path, mode='rb'):
    """Read a file either as bytes or string.

    """
    buffer = []
    if not os.path.isfile(path):
        return b''

    async with aiofiles.open(path, mode) as f:
        while True:
            buff = await f.read()
            if not buff:
                break
            buffer.append(buff)
        if 'b' in mode:
            return b''.join(buffer)
        else:
            return ''.join(buffer)


def read_json(path):
    """Read a json file as string

    """
    if not os.path.isfile(path):
        return ''

    with open(path, 'r') as f:
        data = json.load(f)
    return json.dumps(data)