from time import time
import argparse

import websocket
websocket.enableTrace(False)
from websocket import _logging
_logging._logger.setLevel('ERROR')

def pub(ws, tube, payload):
    ws.send('7:publish,%s:%s,%s:%s,' % (
        len(tube), tube, len(payload), payload))
    return ws.recv()

def sub(ws, tube, offset=None):
    msg = '9:subscribe,%s:%s,' % (len(tube), tube)
    if offset:
        offset = str(offset)
        msg += '%s:%s,' % (len(offset), offset)
    ws.send(msg)
    data = ws.recv()
    yield data
    while data:
        data = ws.recv()
        yield data

def bench(ws):
    payload = 'bench' * 1000
    start = time()
    for i in range(100):
        resp = pub(ws, 'bench', payload)
        assert resp == b'OK'
    print('PUB', time() - start)

    start = time()
    cnt = 0
    for msg in sub(ws, 'bench'):
        assert payload == msg.decode()
        cnt += 1
        if cnt == 100:
            break

    print('SUB', time() - start)

def main(cli):
    ws = websocket.create_connection("ws://localhost:9090/ws")
    action, *args = cli['action']
    if action == 'pub':
        pub(ws, *args)
    elif action == 'sub':
        print(sub(ws, *args))
    elif action == 'bench':
        bench(ws)
    else:
        print('Action "%s" not supported' % action)

    ws.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', nargs='*', help='pub|sub')
    cli = parser.parse_args()

    main(vars(cli))
